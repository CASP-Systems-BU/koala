package buffer

import "github.com/CASP-Systems-BU/koala/api/tuple"

// Pre-allocate a pool of batches for input buffer (deserialization)
// The size of batch bool is configured as (buffer size n + 2). At the max
// usage:
//  1. n batches are waiting in the buffer
//  2. 1 batch is being processed
//  3. 1 batch is blocked to be pushed into the buffer
//
// This is not thread-safe - we won't have concurrent access to the batch pool
type BatchPool[T tuple.Tuple] struct {
	nextBatch int64
	capacity  int64
	pool      []*Batch[T]
}

func NewBatchPool[T tuple.Tuple](
	batchPoolSize int,
	batchInitCapacity int,
) *BatchPool[T] {

	// Allocate memory for the batch pool
	pool := make([]*Batch[T], 0, batchPoolSize)
	for i := 0; i < batchPoolSize; i++ {
		pool = append(pool, PreAllocateBatch[T](batchInitCapacity))
	}

	return &BatchPool[T]{
		nextBatch: 0,
		capacity:  int64(batchPoolSize),
		pool:      pool,
	}
}

// Get the next available batch
func (bp *BatchPool[T]) Get() *Batch[T] {
	batch := bp.pool[bp.nextBatch]

	// Move the next batch pointer
	bp.nextBatch += 1
	if bp.nextBatch == bp.capacity {
		bp.nextBatch = 0
	}

	return batch
}
