package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/collector"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
)

type StatefulFlatmap[IN, OUT tuple.Tuple, K comparable, V stateType.StateType] struct {
	*StatefulOperatorBase1Upstream[IN, K]

	// StatefulFlatmap only has 1 state
	StateID uint16

	// UDF that takes 1 input record and allows user to
	// output 0 or more output records through the collector
	F func(IN, V, collector.Collector)
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*StatefulFlatmap[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*StatefulFlatmap[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

// API exposed to users
func NewStatefulFlatmap[IN, OUT tuple.Tuple, K comparable, V stateType.StateType](
	name string,
	keyAssigner *ka.KeyAssigner[IN, K],
	f func(IN, V, collector.Collector),
) *StatefulFlatmap[IN, OUT, K, V] {

	statefulFlatmap := &StatefulFlatmap[IN, OUT, K, V]{
		StatefulOperatorBase1Upstream: NewStatefulOperatorBase1Upstream(
			name,
			keyAssigner,
			stateClient.SimpleStateClient,
		),
		F: f,
	}

	// Register state type to StateClient.
	statefulFlatmap.StateID = stateClient.RegisterState[V](
		statefulFlatmap.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	statefulFlatmap.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	statefulFlatmap.Collector = collector.NewRoundRobinCollector[OUT](name)

	return statefulFlatmap
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (sfm *StatefulFlatmap[IN, OUT, K, V]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	// Extract all key fields from the batch
	keys := make([]K, batch.TotalNumRecords)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {
		keys[i] = sfm.KeyAssigner.GetKey(record)
	}

	// Prefetch the batch state to memory before processing
	sfm.StateClient.FetchSimpleState(keys, []uint16{sfm.StateID})

	var curState V
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get state for current record
		curState, ok = sfm.StateClient.GetSimpleState(sfm.StateID, keys[i]).(V)
		if !ok {
			log.Fatalln("Fetched state failed for type assertion")
		}

		// Store the timestamp of the input record - this is used to assign
		// timestamp for the output record in Emit()
		sfm.Collector.SetCurrentTimestamp(record.GetTimestamp())

		// Execute UDF to process the input record
		sfm.F(record, curState, sfm.Collector)
	}

	// Flush local cache to state store
	sfm.StateClient.FlushSimpleState()
}

// Validate input type at compile time
func (sfm *StatefulFlatmap[IN, OUT, K, V]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (sfm *StatefulFlatmap[IN, OUT, K, V]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
