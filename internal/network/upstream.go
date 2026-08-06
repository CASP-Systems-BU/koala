package network

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
	"github.com/mus-format/mus-go/raw"
)

type Upstream[T tuple.Tuple] struct {

	// Upstream worker id
	UpstreamWorkerId uint16

	// Store the global config
	Config *configuration.Configuration

	// Established connection from the upstream
	Connection net.Conn

	// Input buffer
	InputBuffer *buffer.RingBuffer[T]

	// Current watermark of this upstream
	WM *buffer.Watermark

	// Pointer to the MetricCollector. This is used to record the network
	// related metrics during runtime
	MetricCollector *metric.MetricCollector
}

// Initialize an upstream struct - including allocate input buffer
func NewUpstream[T tuple.Tuple](
	conn net.Conn,
	config *configuration.Configuration,
	metricCollector *metric.MetricCollector,
	upstreamWorkerId uint16,
) *Upstream[T] {

	if metricCollector == nil {
		log.Fatalln("[info] No metric collector is configured")
	}

	return &Upstream[T]{
		Config:           config,
		Connection:       conn,
		InputBuffer:      buffer.NewInputRingBuffer[T](config),
		WM:               buffer.NewWatermark(-1),
		MetricCollector:  metricCollector,
		UpstreamWorkerId: upstreamWorkerId,
	}
}

// Network routine that reads from connection and supplies input buffer
func (upstream *Upstream[T]) SupplyInputBuffer(
	decoder TupleDecoder,
	isPeer bool,
) {

	// Pre-allocate network buffer byte[] limited by the configured batch size
	// The first 2 bytes are used to identify the type of the WorkUnit
	// If the WorkUnit is a *Batch[T]:
	//   The next 1-4 bytes are used to store size of the batch (# bytes)
	//   The next 5-8 bytes are used to store the number of records in the batch
	// The next 9-16 bytes are used to store the time when the batch was pushed
	//   to the network
	// So we need at most extra 18 bytes for the batch byte buffer
	buf := make([]byte, upstream.Config.BatchSize+18)

	var ok bool
	var offset int
	var err error
	sleepInterval := upstream.Config.BufferSleepInterval
	for {

		// Read a WorkUnit raw data from the network
		var workUnit buffer.WorkUnit

		// Read WorkUnit type
		err = ReadAll(upstream.Connection, buf, 2)
		if err != nil {

			// Handle connection close during runtime. Distinguish between:
			// 1. PeerCollector is deactivated after reconfiguration
			// 2. An upstream is removed during scale-down
			if err == io.EOF {

				// Push the termination signal into the input buffer
				workUnit := buffer.NewTerminationSignal()
				if isPeer {
					workUnit.SetPeer()
				}
				for {
					ok = upstream.InputBuffer.Enqueue(workUnit)
					if ok {
						break
					}
					time.Sleep(sleepInterval)
				}
				// Gracefully exit the network routine
				return

			} else {
				log.Fatalf("Error tcp reading: %v", err)
			}
		}
		workUnitType, _, err := raw.UnmarshalUint16(buf)
		if err != nil {
			log.Fatalf("Error decoding WorkUnit type: %v", err)
		}

		// Do further decoding based on the WorkUnit type
		var networkPushTime int64
		switch buffer.WorkUnitType(workUnitType) {

		// This is a regular Batch WorkUnit
		case buffer.BatchWorkUnit:
			// Read batch metadata - batch size
			err = ReadAll(upstream.Connection, buf[2:], 4)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}
			batchSize, _, err := raw.UnmarshalUint32(buf[2:])
			if err != nil {
				log.Fatalf("Error decoding batch metadata: %v", err)
			}

			// Read batch metadata - num records
			err = ReadAll(upstream.Connection, buf[6:], 4)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}
			numRecords, _, err := raw.UnmarshalUint32(buf[6:])
			if err != nil {
				log.Fatalf("Error decoding batch metadata: %v", err)
			}

			// Read batch metadata - time when the batch enters the network
			// stack in the upstream operator
			err = ReadAll(upstream.Connection, buf[10:], 8)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}
			networkPushTime, _, err = raw.UnmarshalInt64(buf[10:])
			if err != nil {
				log.Fatalf("Error decoding batch metadata: %v", err)
			}

			// Read actual batch data
			err = ReadAll(upstream.Connection, buf[18:], uint64(batchSize))
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}

			// Decode a batch
			// Request a pre-allocated batch to receive the decoded records
			receiverBatch := upstream.InputBuffer.GetFromBatchPool()
			if uint64(numRecords) > receiverBatch.Capacity {
				// The pre-allocated batch doesn't have enough record slots ->
				// expand it
				receiverBatch.ExpandBatchCapacity(
					uint64(numRecords) - receiverBatch.Capacity,
				)
			}

			// Decode data from buffer to pre-allocated batch
			offset = 18
			var n int
			for i := range numRecords {
				n, err = decoder(receiverBatch.Records[i], buf[offset:])
				if err != nil {
					log.Fatalf("Error decoding batch data: %v", err)
				}
				offset += n
			}
			// Reset batch metadata
			receiverBatch.TotalNumBytes = uint64(batchSize)
			receiverBatch.TotalNumRecords = uint64(numRecords)

			if upstream.Config.DebugMode {
				log.Printf(
					"Received a batch [size: %d, num records: %d]\n",
					batchSize,
					numRecords,
				)
			}

			// Cast the batch to an interface
			workUnit = receiverBatch

			// Time the batch is deserialized and ready for the input buffer
			networkConsumeTime := time.Now()
			// Update communication latency
			upstream.MetricCollector.UpdateCommunicationLatency(
				networkConsumeTime.UnixNano() - networkPushTime,
			)

			// Update the time when the batch was pushed to the input buffer
			// This includes blocking time if input buffer is full
			receiverBatch.InputBufferInsertTime = networkConsumeTime

		// This is a DrainBarrier WorkUnit
		case buffer.DrainBarrierWorkUnit:
			workUnit = buffer.NewDrainBarrier()

		// This is a Watermark WorkUnit
		case buffer.WatermarkWorkUnit:
			// Read watermark timestamp
			err = ReadAll(upstream.Connection, buf[2:], 8)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}
			timestamp, _, err := raw.UnmarshalInt64(buf[2:])
			if err != nil {
				log.Fatalf("Error decoding batch metadata: %v", err)
			}
			workUnit = buffer.NewWatermark(timestamp)

		// This is an InflightBarrier WorkUnit
		case buffer.InflightBarrierWorkUnit:
			workUnit = buffer.NewInflightBarrier()

		// This is a FastForwardMetadata WorkUnit
		case buffer.FastForwardMetadataWorkUnit:
			// Read the next 4 bytes (uint32) to get the size of the serialized
			// metadata
			err = ReadAll(upstream.Connection, buf[2:], 4)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}
			metadataSize, _, err := raw.UnmarshalUint32(buf[2:])
			if err != nil {
				log.Fatalf("Error decoding FastForwardMetadata size: %v", err)
			}

			// Allocate a buffer to hold the serialized metadata
			metadataBuf := make([]byte, metadataSize)
			// Read the serialized metadata
			err = ReadAll(
				upstream.Connection,
				metadataBuf,
				uint64(metadataSize),
			)
			if err != nil {
				log.Fatalf("Error tcp reading: %v", err)
			}

			// Create a new FastForwardMetadata WorkUnit with the serialized
			// metadata
			workUnit = buffer.NewFastForwardMetadata(metadataBuf)

		default:
			log.Fatalf("Unsupported WorkUnit type: %d", workUnitType)
		}

		// Push the batch to the input buffer
		for {
			ok = upstream.InputBuffer.Enqueue(workUnit)
			if ok {
				break
			}
			time.Sleep(sleepInterval)
		}
	}
}

// Try to read a batch from the buffer - this is unblocking operation
func (upstream *Upstream[T]) ReadFromBufferNonblock() (buffer.WorkUnit, bool) {

	// Try to read a WorkUnit from the buffer. If fail (buffer empty), return
	// false
	workUnit, ok := upstream.InputBuffer.Dequeue()
	if ok {
		return workUnit, true
	}
	return nil, false
}
