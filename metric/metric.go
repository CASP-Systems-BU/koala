package metric

import (
	"context"
	"log"
	"time"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MetricCollector collects metrics for each deployed task
type MetricCollector struct {

	// Coordinator address
	coordinatorAddr string

	// Metrics reporting interval
	metricsInterval time.Duration

	// Metric collector service connection for this worker
	stream pb.MetricCollectorService_CollectMetricsClient

	// Operator id:worker id
	opWorkerID string

	// If to report source-only metrics
	isSource bool

	// If to report sink-only metrics
	isSink bool

	// Termination signal channel
	termination chan struct{}

	/********* Metrics *********/

	// Input rate (records/sec)
	inputRate *Rate

	// Output rate (records/sec)
	outputRate *Rate

	// Operator latency
	latency *Latency

	// Idle percentage (%): percentage of time spent on waiting for the input.
	// For non-source operators, this is the time spent on waiting for the
	// input buffer to have data. For source operator, this is never updated
	// except KafkaSource, which updates it when waiting for messages from Kafka
	// topic.
	idleRate *Percentage

	// Backpressure percentage (%): percentage of time spent on waiting for the
	// output buffer to have available space
	backpressureRate *Percentage

	// Avg GetMany time per batch per interval
	getManyTime *Average

	// Avg SetMany time per batch per interval
	setManyTime *Average

	// Avg readLocalState time per batch per interval
	readLocalStateTime *Average

	// Avg overwriteLocalState time per batch per interval
	overwriteLocalStateTime *Average

	// Avg keylookup time per batch per interval
	keyLookupTime *Average

	// Avg remote read time per batch per interval (for remote pebble state backend)
	remoteReadTime *Average

	// Avg remote write time per batch per interval (for remote pebble state backend)
	remoteWriteTime *Average

	// Avg remote ReadRequestNumber per batch per interval(for remote pebble state backend)
	remoteReadRequestNumber *Average

	// Avg remote WriteRequestNumber per batch per interval(for remote pebble state backend)
	remoteWriteRequestNumber *Average

	// Avg remote read key number per batch per interval(for remote pebble state backend)
	remoteReadKeyNumber *Average

	// Avg remote write key number per batch per interval(for remote pebble state backend)
	remoteWriteKeyNumber *Average

	// Avg remote read time per request per batch per interval (for remote pebble state backend)
	remoteReadTimePerRequest *Average

	// Avg remote write time per request per batch per interval (for remote pebble state backend)
	remoteWriteTimePerRequest *Average

	// Avg generating dummy field time per batch per interval
	generateDummyFieldTime *Average

	// Avg record number per batch
	recordNumberPerBatch *Average

	// Avg key number per batch
	keyNumberPerBatch *Average

	// Avg record size
	recordSizePerBatch *Average

	// Sum of bytes transferred by lazy keyby protocol (request + response) per period
	numBytesTransferred *Sum
}

func NewMetricCollector(
	metricsInterval time.Duration,
	coordinatorAddr string,
	isSource bool,
	isSink bool,
	opWorkerID string,
) *MetricCollector {

	return &MetricCollector{
		opWorkerID:                opWorkerID,
		metricsInterval:           metricsInterval,
		coordinatorAddr:           coordinatorAddr,
		isSource:                  isSource,
		isSink:                    isSink,
		termination:               make(chan struct{}, 2), // non-blocking channel
		inputRate:                 NewRate(metricsInterval),
		outputRate:                NewRate(metricsInterval),
		latency:                   NewLatency(),
		idleRate:                  NewPercentage(metricsInterval),
		backpressureRate:          NewPercentage(metricsInterval),
		getManyTime:               NewAverage(),
		setManyTime:               NewAverage(),
		readLocalStateTime:        NewAverage(),
		overwriteLocalStateTime:   NewAverage(),
		keyLookupTime:             NewAverage(),
		remoteReadTime:            NewAverage(),
		remoteWriteTime:           NewAverage(),
		remoteReadRequestNumber:   NewAverage(),
		remoteWriteRequestNumber:  NewAverage(),
		remoteReadKeyNumber:       NewAverage(),
		remoteWriteKeyNumber:      NewAverage(),
		remoteReadTimePerRequest:  NewAverage(),
		remoteWriteTimePerRequest: NewAverage(),
		generateDummyFieldTime:    NewAverage(),
		recordNumberPerBatch:      NewAverage(),
		keyNumberPerBatch:         NewAverage(),
		recordSizePerBatch:        NewAverage(),
		numBytesTransferred:       NewSum(),
	}
}

// Metric collector routine that periodically reports metrics to coordinator
func (mc *MetricCollector) Run() {
	mc.connectToMetricCollectorService()

	for {
		time.Sleep(mc.metricsInterval)
		select {
		// First check termination signal for graceful shutdown
		case <-mc.termination:
			log.Printf(
				"Metric collector on task %s (op:workerId) is terminating\n",
				mc.opWorkerID,
			)
			terminationMsg, err := mc.stream.CloseAndRecv()
			if terminationMsg.Message != "Metric collector shutdown received" ||
				err != nil {
				log.Fatalln(
					"Failed to terminate metric collector service:",
					err,
				)
			}
			return
		default:
			mc.reportMetrics()
		}
	}
}

func (mc *MetricCollector) Terminate() {
	mc.termination <- struct{}{}
}

func (mc *MetricCollector) connectToMetricCollectorService() {
	conn, err := grpc.NewClient(
		mc.coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the coordinator: %v", err)
	}

	client := pb.NewMetricCollectorServiceClient(conn)
	stream, err := client.CollectMetrics(context.Background())
	if err != nil {
		log.Fatalf("Failed to create stream to the coordinator: %v", err)
	}

	mc.stream = stream
}

func (mc *MetricCollector) reportMetrics() {
	metrics := mc.collectMetrics()
	msg := &pb.MetricBatch{
		OperatorId: mc.opWorkerID,
		Metrics:    metrics,
	}

	if err := mc.stream.Send(msg); err != nil {
		log.Printf("Failed to send metric message: %v\n", err)
	}
}

func (mc *MetricCollector) collectMetrics() []*pb.MetricData {

	metrics := []*pb.MetricData{
		{
			MetricValue: mc.outputRate.Get(), MetricType: "OutputRate",
		},
		{
			MetricValue: mc.inputRate.Get(), MetricType: "InputRate",
		},
		{
			MetricValue: mc.latency.OutputBufferQueueTime.Get(), MetricType: "Latency.OutputBufferQueueTime",
		},
		{
			MetricValue: mc.latency.ProcessingTime.Get(), MetricType: "Latency.ProcessingTime",
		},
		{
			MetricValue: mc.latency.CommunicationTime.Get(), MetricType: "Latency.CommunicationTime",
		},
		{
			MetricValue: mc.idleRate.Get(), MetricType: "IdleRate",
		},
		{
			MetricValue: mc.backpressureRate.Get(), MetricType: "BackpressureRate",
		},
		{
			MetricValue: mc.getManyTime.Get(), MetricType: "GetManyTime",
		},
		{
			MetricValue: mc.setManyTime.Get(), MetricType: "SetManyTime",
		},
		{
			MetricValue: mc.readLocalStateTime.Get(), MetricType: "ReadLocalStateTime",
		},
		{
			MetricValue: mc.overwriteLocalStateTime.Get(), MetricType: "OverWriteLocalStateTime",
		},
		{
			MetricValue: mc.keyLookupTime.Get(), MetricType: "KeyLookUpTime",
		},
		{
			MetricValue: mc.remoteReadTime.Get(), MetricType: "RemoteReadTime",
		},
		{
			MetricValue: mc.remoteWriteTime.Get(), MetricType: "RemoteWriteTime",
		},
		{
			MetricValue: mc.remoteReadRequestNumber.Get(), MetricType: "RemoteReadRequestNumber",
		},
		{
			MetricValue: mc.remoteWriteRequestNumber.Get(), MetricType: "RemoteWriteRequestNumber",
		},
		{
			MetricValue: mc.remoteReadKeyNumber.Get(), MetricType: "RemoteReadKeyNumber",
		},
		{
			MetricValue: mc.remoteWriteKeyNumber.Get(), MetricType: "RemoteWriteKeyNumber",
		},
		{
			MetricValue: mc.remoteReadTimePerRequest.Get(), MetricType: "RemoteReadTimePerRequest",
		},
		{
			MetricValue: mc.remoteWriteTimePerRequest.Get(), MetricType: "RemoteWriteTimePerRequest",
		},
		{
			MetricValue: mc.generateDummyFieldTime.Get(), MetricType: "GenerateDummyFieldTime",
		},
		{
			MetricValue: mc.recordNumberPerBatch.Get(), MetricType: "RecordNumberPerBatch",
		},
		{
			MetricValue: mc.keyNumberPerBatch.Get(), MetricType: "KeyNumberPerBatch",
		},
		{
			MetricValue: mc.recordSizePerBatch.Get(), MetricType: "RecordSizePerBatch",
		},
		{
			MetricValue: mc.numBytesTransferred.Get(), MetricType: "NumBytesTransferred",
		},
	}

	// Check for source only metrics
	if mc.isSource {
		metrics = append(metrics, &pb.MetricData{
			MetricValue: mc.latency.KafkaQueueTime.Get(),
			MetricType:  "Latency.KafkaQueueTime",
		})
	} else {
		metrics = append(metrics, &pb.MetricData{
			MetricValue: mc.latency.InputBufferQueueTime.Get(),
			MetricType:  "Latency.InputBufferQueueTime",
		})
	}

	// Print out sink input rate for runtime monitoring purposes
	if mc.isSink {
		log.Printf("Sink input rate: %f\n", metrics[1].MetricValue)
	}

	return metrics
}

/******************************************************************************
			   Exposed APIs by MetricCollector to update metrics
******************************************************************************/

// Update the InputRate during runtime. This is called before ProcessBatch()
// in main routine
func (mc *MetricCollector) UpdateInputRate(workUnit buffer.WorkUnit) {

	// Convert buffer.WorkUnit to BatchWorkUnit
	batchWorkUnit := getBatchWorkUnit(workUnit)
	mc.inputRate.Inc(batchWorkUnit.GetNumRecords())
}

// Update the OutputRate during runtime. This is called every time a batch is
// pushed into the output buffer
func (mc *MetricCollector) UpdateOutputRate(workUnit buffer.WorkUnit) {

	// Convert buffer.WorkUnit to BatchWorkUnit
	batchWorkUnit := getBatchWorkUnit(workUnit)
	mc.outputRate.Inc(batchWorkUnit.GetNumRecords())
}

// Update the OutputBufferQueueTime during runtime. This is called every time
// a batch is consumed from the output buffer in Downstream
func (mc *MetricCollector) UpdateOutputBufferQueueTime(queueTime int64) {
	mc.latency.UpdateOutputBufferQueueTime(queueTime)
}

// Record the timestamp when a batch is pulled from the input buffer.
// This method is called right before starting to process a batch,
// for example, before invoking ProcessBatch().
func (mc *MetricCollector) UpdateBatchPullTime(pullTime time.Time) {
	mc.latency.InputBufferPullTime = pullTime
}

// Calculate and record the processing latency for a batch.
// This method is called once the batch has been processed and pushed
// to the output buffer. The latency is computed as the time difference between
// two consecutive batch pulls (e.g., the time between ProcessBatch() and next
// GetWorkUnit()).
func (mc *MetricCollector) UpdateProcessingLatency() {
	if mc.latency.InputBufferPullTime.IsZero() {
		// Skip latency calculation if the batch pull time was not recorded.
		return
	}
	mc.latency.UpdateProcessingTime(
		time.Since(mc.latency.InputBufferPullTime).Nanoseconds(),
	)
	mc.latency.InputBufferPullTime = time.Time{}
}

// Update the communication latency for a batch. This method is called
// when the batch is deserialized from network and ready for input buffer.
func (mc *MetricCollector) UpdateCommunicationLatency(duration int64) {
	mc.latency.UpdateCommunicationTime(duration)
}

// Update the input buffer queue time. This method is called when a batch
// is pulled from the input buffer for processing. The queue time is computed
// as the time difference between the current time and the time when the
// batch was inserted into the input buffer.
func (mc *MetricCollector) UpdateInputBufferQueueTime(
	workunit buffer.WorkUnit,
) {

	// Convert buffer.WorkUnit to BatchWorkUnit
	batchWorkUnit := getBatchWorkUnit(workunit)
	insertTime := batchWorkUnit.GetInputBufferInsertTime()
	mc.latency.UpdateInputBufferQueueTime(time.Since(insertTime).Nanoseconds())
}

// Update the Kafka source input buffer queue time. This method is called
// when a record is pulled from the Kafka topic for processing. The queue time
// is computed as the time difference between the current time and the time
// when the record was inserted into the Kafka topic.
func (mc *MetricCollector) UpdateKafkaQueueTime(
	duration int64,
) {
	mc.latency.UpdateKafkaQueueTime(duration)
}

// Increment the idle time - waiting for the input buffer to have data.
func (mc *MetricCollector) UpdateIdleTime(dur time.Duration) {
	mc.idleRate.Inc(dur)
}

// Increment the backpressure time - waiting for the output buffer to have
// available space. This is called upon pushing work unit into the Collector
func (mc *MetricCollector) UpdateBackpressureTime(dur time.Duration) {
	mc.backpressureRate.Inc(dur)
}

// Update the getMany time per batch
func (mc *MetricCollector) UpdateGetManyTime(dur time.Duration) {
	mc.getManyTime.Inc(dur.Nanoseconds())
}

// Update the setMany time per batch
func (mc *MetricCollector) UpdateSetManyTime(dur time.Duration) {
	mc.setManyTime.Inc(dur.Nanoseconds())
}

// Update the readLocalState time per batch
func (mc *MetricCollector) UpdateReadLocalStateTime(dur time.Duration) {
	mc.readLocalStateTime.Inc(dur.Nanoseconds())
}

// Update the overWriteLocalState time per batch
func (mc *MetricCollector) UpdateOverWriteLocalStateTime(dur time.Duration) {
	mc.overwriteLocalStateTime.Inc(dur.Nanoseconds())
}

// Update the keyLookUp time per batch
func (mc *MetricCollector) UpdateKeyLookUpTime(dur time.Duration) {
	mc.keyLookupTime.Inc(dur.Nanoseconds())
}

// Update the remote read time per batch
func (mc *MetricCollector) UpdateRemoteReadTime(dur time.Duration) {
	mc.remoteReadTime.Inc(dur.Nanoseconds())
}

// Update the remote write time per batch
func (mc *MetricCollector) UpdateRemoteWriteTime(dur time.Duration) {
	mc.remoteWriteTime.Inc(dur.Nanoseconds())
}

// Update the remote read request number per batch
func (mc *MetricCollector) UpdateRemoteReadRequestNumber(num int) {
	mc.remoteReadRequestNumber.Inc(int64(num))
}

// Update the remote write request number per batch
func (mc *MetricCollector) UpdateRemoteWriteRequestNumber(num int) {
	mc.remoteWriteRequestNumber.Inc(int64(num))
}

// Update the remote read key number per batch
func (mc *MetricCollector) UpdateRemoteReadKeyNumber(num int) {
	mc.remoteReadKeyNumber.Inc(int64(num))
}

// Update the remote write key number per batch
func (mc *MetricCollector) UpdateRemoteWriteKeyNumber(num int) {
	mc.remoteWriteKeyNumber.Inc(int64(num))
}

// Update the remote read time per request per batch
func (mc *MetricCollector) UpdateRemoteReadTimePerRequest(dur time.Duration) {
	mc.remoteReadTimePerRequest.Inc(dur.Nanoseconds())
}

// Update the remote write time per request per batch
func (mc *MetricCollector) UpdateRemoteWriteTimePerRequest(dur time.Duration) {
	mc.remoteWriteTimePerRequest.Inc(dur.Nanoseconds())
}

// Update the generate dummy field time per batch
func (mc *MetricCollector) UpdateGenerateDummyFieldTime(dur time.Duration) {
	mc.generateDummyFieldTime.Inc(dur.Nanoseconds())
}

// Update record number per batch
func (mc *MetricCollector) UpdateRecordNumberPerBatch(recordNumber int64) {
	mc.recordNumberPerBatch.Inc(recordNumber)
}

// Update key number per batch
func (mc *MetricCollector) UpdateKeyNumberPerBatch(keyNumber int64) {
	mc.keyNumberPerBatch.Inc(keyNumber)
}

// Update record size per batch
func (mc *MetricCollector) UpdateRecordSizePerBatch(size int64) {
	mc.recordSizePerBatch.Inc(size)
}

// Update num bytes transferred by lazy keyby protocol (request + response bytes)
func (mc *MetricCollector) UpdateNumBytesTransferred(bytes uint64) {
	mc.numBytesTransferred.Inc(bytes)
}
