package network

import (
	"log"
	"net"
	"time"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/metric"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/raw"
)

type Downstream[T tuple.Tuple] struct {

	// Store the global config
	Config *configuration.Configuration

	// Copy of operator name this Downstream belongs to
	OperatorName string

	// Downstream address
	DownstreamAddr string

	// Output buffer
	OutputBuffer *buffer.RingBuffer[T]

	// Pointer to the MetricCollector. This is used to record the network
	// related metrics during runtime
	MetricCollector *metric.MetricCollector
}

// Initialize a downstream struct - including allocate output buffer
func NewDownStream[T tuple.Tuple](
	downstreamAddr string,
	config *configuration.Configuration,
	metricCollector *metric.MetricCollector,
	operatorName string,
) *Downstream[T] {

	if metricCollector == nil {
		log.Println("[info] No metric collector is configured")
	}

	return &Downstream[T]{
		Config:          config,
		DownstreamAddr:  downstreamAddr,
		OutputBuffer:    buffer.NewOutputRingBuffer[T](config),
		MetricCollector: metricCollector,
		OperatorName:    operatorName,
	}
}

// Network routine that consumes the output buffer
func (downstream *Downstream[T]) ConsumeOutputBuffer(
	encoder TupleEncoder,
	channelType uint8,
	workerId uint16,
) {

	// Establish the connection to downstream
	conn, err := net.Dial("tcp", downstream.DownstreamAddr)
	if err != nil {
		log.Fatalln(
			"Fail to establish tcp connection to " + downstream.DownstreamAddr,
		)
	}

	// Construct the header metadata for this connection:
	// 1. Length of the sender operator name (8 bytes uint64)
	// 2. Sender operator name (variable length string)
	// 3. Connection type (1 byte uint8):
	// 		0 - regular upstream channel;
	//    	1 - single peer channel (single-upstream op)
	//      2 - multi-peer channel (multi-upstream op) - primary
	// 	    3 - multi-peer channel (multi-upstream op) - secondary
	// 4. Sender Worker ID (2 bytes uint16)
	operatorIDLength := ord.SizeString(downstream.OperatorName, nil)
	buf := make([]byte, 8)
	raw.MarshalUint64(uint64(operatorIDLength), buf)
	WriteAll(conn, buf)

	buf = make([]byte, operatorIDLength)
	ord.MarshalString(downstream.OperatorName, nil, buf)
	WriteAll(conn, buf)

	buf = make([]byte, 1)
	raw.MarshalUint8(channelType, buf)
	WriteAll(conn, buf)

	buf = make([]byte, 2)
	raw.MarshalUint16(workerId, buf)
	WriteAll(conn, buf)

	// TODO: formally define the tcp protocol and specify each feild. Now the
	// protocol bytes format is hard-coded as described below.
	// Pre-allocate network buffer byte[] limited by the configured batch size
	// The first 2 bytes are used to identify the type of the WorkUnit
	// If the WorkUnit is a *Batch[T]:
	//   The next 1-4 bytes are used to store size of the batch (# bytes)
	//   The next 5-8 bytes are used to store the number of records in the batch
	// 	 The next 9-17 bytes are used to store the time when the batch was
	// pushed
	//   to the network
	// So we need at most extra 18 bytes for the batch byte buffer
	buf = make([]byte, downstream.Config.BatchSize+18)

	var workUnit buffer.WorkUnit
	var ok bool
	var offset int
	sleepInterval := downstream.Config.BufferSleepInterval

	for {

		// Read a batch from buffer until success
		for {
			workUnit, ok = downstream.OutputBuffer.Dequeue()
			if ok {
				break
			}
			time.Sleep(sleepInterval)
		}

		// Based on the type of the WorkUnit, use different encoding schema
		switch value := workUnit.(type) {

		// This is a regular Batch element
		case *buffer.Batch[T]:

			// Record the time when the batch is consumed from the output buffer
			// This is used to calculate:
			// 1. OutputBufferQueueTime: time spent in the output buffer
			// 2. CommunicationTime: time spent in the network
			consumeTime := time.Now().UnixNano()

			// Update the OutputBufferQueueTime metric
			downstream.MetricCollector.UpdateOutputBufferQueueTime(time.Since(value.PushToOutputbufferTime).Nanoseconds())

			// Encode the type to 1 of uint16 (2 bytes)
			raw.MarshalUint16(uint16(buffer.BatchWorkUnit), buf)
			// Encode the batch size and record num into the next 8 bytes
			raw.MarshalUint32(uint32(value.TotalNumBytes), buf[2:])
			raw.MarshalUint32(uint32(value.TotalNumRecords), buf[6:])

			// Record the time the batch was pushed to the network in the next 8
			// bytes This is used to calculate the communication latency to
			// trasfer the batch to the downstream
			raw.MarshalInt64(consumeTime, buf[10:])

			// Encode the batch records - get the actual number of bytes encoded
			offset = 18
			for _, record := range value.Records {
				offset += encoder(record, buf[offset:])
			}

		// This is a DrainBarrier element
		case *buffer.DrainBarrier:
			// Encode the type to 2 of uint16 (2 bytes)
			raw.MarshalUint16(uint16(buffer.DrainBarrierWorkUnit), buf)
			offset = 2

		// This is a Watermark element
		case *buffer.Watermark:
			// Encode the type to 3 of uint16 (2 bytes)
			raw.MarshalUint16(uint16(buffer.WatermarkWorkUnit), buf)
			// Encode the watermark timestamp into the next 8 bytes
			raw.MarshalInt64(value.Timestamp, buf[2:])
			offset = 10

		// This is a in-flight barrier for lazy protocol
		case *buffer.InflightBarrier:
			// Encode the type to 4 of uint16 (2 bytes)
			raw.MarshalUint16(uint16(buffer.InflightBarrierWorkUnit), buf)
			offset = 2

		// This is a termination element - terminate the routine
		case *buffer.TerminationSignal:
			// Explicitly free the buffer
			downstream.OutputBuffer = nil
			// Close the tcp connection and exit the routine
			conn.Close()
			return

		// This is a fast-forward metadata for lazy protocol
		case *buffer.FastForwardMetadata:
			// Data is already serialized by operator. We manually push the
			// serialized metadata to the network without interfering with
			// pre-allocated buffer buf
			err = WriteAll(conn, value.SerializedMetadata)
			if err != nil {
				log.Fatalf("Error tcp sending: %v", err)
			}
			continue

		// This is a migrating key map for lazy-by-key protocol
		case *buffer.MigratingKeyMap:
			// Data is already serialized, push it to the network
			err = WriteAll(conn, value.SerializedKeyMap)
			if err != nil {
				log.Fatalf("Error tcp sending: %v", err)
			}
			continue

		default:
			log.Fatalf("Unknown WorkUnit type: %v", value)
		}

		// Push the encoded byte[] to the network
		err = WriteAll(conn, buf[:offset])
		if err != nil {
			log.Fatalf("Error tcp sending: %v", err)
		}
	}
}

// Push a batch into the buffer
func (downstream *Downstream[T]) PushToBuffer(workUnit buffer.WorkUnit) {

	sleepInterval := downstream.Config.BufferSleepInterval

	// Collect metrics for batch workUnit
	isBatch := workUnit.GetType() == buffer.BatchWorkUnit
	if isBatch {
		// Update the output rate
		downstream.MetricCollector.UpdateOutputRate(workUnit)
	}

	// Keep trying enqueue until success
	var ok bool
	for {

		// Update PushToOutputbufferTime for batch workUnit - used to calculate
		// output queue time. We need to update this at each iteration of the
		// loop to exclude blocking time if output buffer is full
		if isBatch {
			workUnit.(*buffer.Batch[T]).PushToOutputbufferTime = time.Now()
		}

		ok = downstream.OutputBuffer.Enqueue(workUnit)
		if ok {
			// Report 0 to ensure there is data point reported
			downstream.MetricCollector.UpdateBackpressureTime(0)
			break
		}
		time.Sleep(sleepInterval)

		// Update the BackpressureTime metric to capture this sleep time
		downstream.MetricCollector.UpdateBackpressureTime(sleepInterval)
	}
}
