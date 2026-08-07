package dataflow

import (
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

// Aggregator holds the aggregation methods for window operator
type Aggregator[IN, OUT tuple.Tuple, ACC stateType.StateType] struct {

	// Allocate and initialize a new accumulator
	NewFunc func() ACC

	// Given existing accumulator and a new input record, return the updated
	// accumulator - it defines how to update exisitng accumulator with new
	// input record.
	// [Best Practice] Modify and return the passed-in accumulator to avoid
	// unnecessary memory allocation. However, it's also safe to return a new
	// accumulator.
	AddFunc func(ACC, IN) ACC

	// Output the final result when the accumulator is ready to be emitted.
	// [Best Practice] If OUT and ACC.InternalTuple are of the same type, just
	// return the InternalTuple field directly. However, it's also ok to
	// allocate a new OUT tuple. It's necessary to allocate a new OUT tuple if
	// OUT and ACC.InternalTuple are of different types.
	OutFunc func(ACC) OUT

	// Merge two accumulators and return the merged accumulator.
	// [Best Practice] Modify and return the first accumulator to avoid
	// unnecessary memory allocation. However, it's also safe to return a new
	// accumulator.
	MergeFunc func(ACC, ACC) ACC
}

// NewAggregator creates a new Aggregator struct
func NewAggregator[IN, OUT tuple.Tuple, ACC stateType.StateType](
	newFunc func() ACC,
	addFunc func(ACC, IN) ACC,
	outFunc func(ACC) OUT,
	mergeFunc func(ACC, ACC) ACC,
) *Aggregator[IN, OUT, ACC] {
	return &Aggregator[IN, OUT, ACC]{
		NewFunc:   newFunc,
		AddFunc:   addFunc,
		OutFunc:   outFunc,
		MergeFunc: mergeFunc,
	}
}
