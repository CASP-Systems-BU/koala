package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
)

type StatefulMapper[IN, OUT tuple.Tuple, K comparable, V stateType.StateType] struct {
	*StatefulOperatorBase1Upstream[IN, K]

	// StatefulMapper only has 1 state
	StateID uint16

	// UDF that takes 1 input record and output 1 output record with state api
	F func(IN, V) OUT
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*StatefulMapper[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*StatefulMapper[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

// API exposed to users
func NewStatefulMapper[IN, OUT tuple.Tuple, K comparable, V stateType.StateType](
	name string,
	keyAssigner *ka.KeyAssigner[IN, K],
	f func(IN, V) OUT,
) *StatefulMapper[IN, OUT, K, V] {

	statefulMapper := &StatefulMapper[IN, OUT, K, V]{
		StatefulOperatorBase1Upstream: NewStatefulOperatorBase1Upstream(
			name,
			keyAssigner,
			stateClient.SimpleStateClient,
		),
		F: f,
	}

	// Register state type to StateClient.
	statefulMapper.StateID = stateClient.RegisterState[V](
		statefulMapper.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	statefulMapper.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	statefulMapper.Collector = collector.NewRoundRobinCollector[OUT](name)

	return statefulMapper
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (sm *StatefulMapper[IN, OUT, K, V]) ProcessBatch(
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
		keys[i] = sm.KeyAssigner.GetKey(record)
	}

	// Prefetch the batch state to memory before processing
	sm.StateClient.FetchSimpleState(keys, []uint16{sm.StateID})

	var curState V
	var output OUT
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get state for current record
		curState, ok = sm.StateClient.GetSimpleState(sm.StateID, keys[i]).(V)
		if !ok {
			log.Fatalln("Fetched state failed for type assertion")
		}

		// Execute UDF to process the input record
		output = sm.F(record, curState)

		// Store the timestamp of the input record - this is used to assign
		// timestamp for the output record in Emit()
		sm.Collector.SetCurrentTimestamp(record.GetTimestamp())

		// Call collector to push the output record to the downstream
		sm.Collector.Emit(output)
	}

	// Flush local cache to StateService
	sm.StateClient.FlushSimpleState()
}

// Validate input type at compile time
func (sm *StatefulMapper[IN, OUT, K, V]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (sm *StatefulMapper[IN, OUT, K, V]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
