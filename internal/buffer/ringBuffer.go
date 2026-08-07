package buffer

import (
	"sync/atomic"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// Lock-free ring buffer implementation
type RingBuffer[T tuple.Tuple] struct {

	// Capacity of the buffer
	capacity int64

	// Current number of batches in the buffer
	count int64

	// Read and write pointer
	read_idx  int64
	write_idx int64

	// Pre-allocated Batch pool - this is only used for input buffer
	// Records/Batch for output buffer will be created by user
	batchPool *BatchPool[T]

	// Buffer
	buffer []WorkUnit
}

func NewInputRingBuffer[T tuple.Tuple](
	config *configuration.Configuration,
) *RingBuffer[T] {

	return &RingBuffer[T]{
		capacity:  config.BufferSize,
		count:     0,
		read_idx:  0,
		write_idx: 0,
		batchPool: NewBatchPool[T](
			int(config.BufferSize)+2,
			int(config.PreAllocatedBatchInitCapacity),
		),
		buffer: make([]WorkUnit, config.BufferSize),
	}
}

func NewOutputRingBuffer[T tuple.Tuple](
	config *configuration.Configuration,
) *RingBuffer[T] {

	return &RingBuffer[T]{
		capacity:  config.BufferSize,
		count:     0,
		read_idx:  0,
		write_idx: 0,
		buffer:    make([]WorkUnit, config.BufferSize),
	}
}

func (rb *RingBuffer[T]) Enqueue(workUnit WorkUnit) bool {

	// Check if there are available slots
	count := atomic.LoadInt64(&rb.count)
	if count == rb.capacity {
		return false
	}

	// Write the element into the empty slot
	rb.buffer[rb.write_idx] = workUnit

	// Increment the write idx - synchronized and no protection needed
	rb.write_idx += 1
	if rb.write_idx == rb.capacity {
		rb.write_idx = 0
	}

	// Atomic increment operaiton (use CAS under the hood)
	atomic.AddInt64(&rb.count, 1)
	return true
}

func (rb *RingBuffer[T]) Dequeue() (WorkUnit, bool) {

	// Check if the buffer is empty
	count := atomic.LoadInt64(&rb.count)
	if count == 0 {
		return nil, false
	}

	// Pull the next available element
	next := rb.buffer[rb.read_idx]

	// Increment the read idx - synchronized and no protection needed
	rb.read_idx += 1
	if rb.read_idx == rb.capacity {
		rb.read_idx = 0
	}

	// Atomic decrease operaiton (use CAS under the hood)
	atomic.AddInt64(&rb.count, -1)
	return next, true
}

func (rb *RingBuffer[T]) CurCount() int64 {
	return atomic.LoadInt64(&rb.count)
}

// Get a pre-allocated batch from batch pool
func (rb *RingBuffer[T]) GetFromBatchPool() *Batch[T] {
	return rb.batchPool.Get()
}
