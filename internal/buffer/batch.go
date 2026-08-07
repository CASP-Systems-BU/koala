package buffer

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/tuple"
)

type Batch[T tuple.Tuple] struct {

	// # of bytes after serialization
	TotalNumBytes uint64

	// # of records in this batch
	TotalNumRecords uint64

	// Number of pre-allocated record slots. This is only used by batchPool for
	// input buffer where we pre-allocate all batches. For output batch, records
	// are constructed by users during runtime
	Capacity uint64

	// Slice of data records
	Records []T

	// Time when the batch is pushed to output queue
	// This is used to calculate the output queue time of the batch
	PushToOutputbufferTime time.Time

	// Time when the batch is inserted into the input buffer of the operator -
	// used to calculate the input queueing time i.e. time spent in the input
	// buffer of an operator before it is pulled for processing
	InputBufferInsertTime time.Time
}

var _ WorkUnit = (*Batch[tuple.Tuple])(nil)

// Implement WorkUnit interface
func (b *Batch[T]) GetType() WorkUnitType {
	return BatchWorkUnit
}

/******************************************************************************
							Batch specific methods
******************************************************************************/

// Pre-allocate memory space for a batch -> batch pool. This is used to avoid
// frequent memory allocation for deserialization (input buffer)
func PreAllocateBatch[T tuple.Tuple](initialCapacity int) *Batch[T] {

	// Pre-allocate memory space for the batch
	records := make([]T, initialCapacity)
	for i := range initialCapacity {
		records[i] = records[i].New().(T)
	}

	return &Batch[T]{
		TotalNumBytes:   0,
		TotalNumRecords: 0,
		Capacity:        uint64(initialCapacity),
		Records:         records,
	}
}

// Pre-allocate more space for the batch. num: extra num of records to allocate
// If a batch in batch pool doesn't have sufficient space, allocate more space
// TODO: capacity only increases now, add logic to free the space if not needed
func (b *Batch[T]) ExpandBatchCapacity(num uint64) {
	for i := uint64(0); i < num; i++ {
		var newRecordSlot T
		newRecordSlot = newRecordSlot.New().(T)
		b.Records = append(b.Records, newRecordSlot)
	}
	b.Capacity += num
}

// Append a record into the current batch - this is for output Emit()
// Check configured batch size before calling this function
func (b *Batch[T]) AddRecord(record T, recordSize int) {

	b.Records = append(b.Records, record)
	b.TotalNumBytes += uint64(recordSize)
	b.TotalNumRecords += 1
}

// Allocate an empty batch for output collector. Since output records are
// created by users, we don't need to pre-allocate records
func AllocateOutputBatch[T tuple.Tuple]() *Batch[T] {
	return &Batch[T]{}
}

// Implement BatchWorkUnit interface to return the number of records without
// type conversion to Batch[T]. This is used to collect InputRate metric
func (b *Batch[T]) GetNumRecords() uint64 {
	return b.TotalNumRecords
}

// Implement BatchWorkUnit interface to return the time when a batch was
// inserted in the input buffer without type conversion to Batch[T]. This is
// used to collect InputBufferQueueTime metric
func (b *Batch[T]) GetInputBufferInsertTime() time.Time {
	return b.InputBufferInsertTime
}
