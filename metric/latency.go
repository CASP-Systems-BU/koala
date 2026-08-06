package metric

import "time"

// Latency metrics for an operator, measured per batch in ns:
// 1. InputBufferQueuingTime: time spent in the input buffer before processing
// 2. ProcessingTime: time taken to process the batch and push to output buffer
// 3. OutputBufferQueueTime: time spent in the output buffer before pushing to
// the network
// 4. CommunicationTime: time taken to send the batch over the network to the
// downstream operator.

type Latency struct {

	// Time spent in the output buffer: from the time the batch is pushed to the
	// output buffer to the time the batch is consumed from the output buffer
	// e.g. time between PushToBuffer() to ConsumeFromBuffer()
	// Note that time spent waiting for PushToBuffer() is not included
	OutputBufferQueueTime *Average

	// Time taken by the operator to process a batch:
	// Measured from the moment a batch is pulled from the input buffer until
	// the operator attempts to pull the next batch. If the next batch is not
	// yet available, the time is measured between two consecutive pull
	// attempts. This duration includes both the batch processing time and any
	// waiting time for the output buffer to become available
	// (i.e., before the batch can be pushed to the output buffer).
	ProcessingTime *Average

	// Time taken to send the batch over the network to the downstream
	// operator. Measured from the moment the batch is pushed to the network
	// until the moment the batch is inserted into the input buffer of the
	// downstream operator e.g. time between ConsumeOutputBuffer() to
	// SupplyInputBuffer(). This includes the time spent in the network and the
	// time taken to serialize and deserialize the batch.
	CommunicationTime *Average

	// Time spent in the input buffer before processing: from the time the batch
	// is inserted into the input buffer to the time the batch is pulled from
	// the input buffer for processing. This includes any waiting time if the
	// input buffer is full or unavailable.
	// E.g. time between SupplyInputBuffer() to ProcessBatch()
	InputBufferQueueTime *Average

	// Time spent by a record in kafka before being pulled by the source
	// operator. This includes the total time a record remains in the Kafka
	// topic queue and network transfer time to Source operator.
	// It differs from InputBufferQueueTime in two ways:
	// - InputBufferQueueTime is measured per batch, whereas this is per record.
	// - InputBufferQueueTime is not tracked for source operators.
	// Note: This metric is only relevant for Kafka source operators.
	KafkaQueueTime *Average

	////////// helper variables //////////

	// Time when the last batch is pulled from the input buffer to be processed
	// This is used to calculate the ProcessingTime
	InputBufferPullTime time.Time
}

func NewLatency() *Latency {
	return &Latency{
		OutputBufferQueueTime: NewAverage(),
		ProcessingTime:        NewAverage(),
		CommunicationTime:     NewAverage(),
		InputBufferQueueTime:  NewAverage(),
		KafkaQueueTime:        NewAverage(),
	}
}

func (l *Latency) UpdateOutputBufferQueueTime(queueTime int64) {
	l.OutputBufferQueueTime.Inc(queueTime)
}

func (l *Latency) UpdateProcessingTime(processingTime int64) {
	l.ProcessingTime.Inc(processingTime)
}

func (l *Latency) UpdateCommunicationTime(communicationTime int64) {
	l.CommunicationTime.Inc(communicationTime)
}

func (l *Latency) UpdateInputBufferQueueTime(queueTime int64) {
	l.InputBufferQueueTime.Inc(queueTime)
}

func (l *Latency) UpdateKafkaQueueTime(queueTime int64) {
	l.KafkaQueueTime.Inc(queueTime)
}
