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

// StatefulMapper2 supports 2 states as compared to StatefulMapper
// This is an example to demonstrate how to implement new operators with
// multiple states.

type StatefulMapper2[IN, OUT tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType] struct {
	*StatefulOperatorBase1Upstream[IN, K]

	// StatefulMapper2 has 2 states
	StateID1 uint16
	StateID2 uint16

	// UDF that takes 1 input record and output 1 output record with state api
	F func(IN, V1, V2) OUT
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*StatefulMapper2[tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*StatefulMapper2[tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

// API exposed to users to define the operator
func NewStatefulMapper2[IN, OUT tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType](
	name string,
	keyAssigner *ka.KeyAssigner[IN, K],
	f func(IN, V1, V2) OUT,
) *StatefulMapper2[IN, OUT, K, V1, V2] {

	statefulMapper := &StatefulMapper2[IN, OUT, K, V1, V2]{
		StatefulOperatorBase1Upstream: NewStatefulOperatorBase1Upstream(
			name,
			keyAssigner,
			stateClient.SimpleStateClient,
		),
		F: f,
	}

	// Register state types to StateClient.
	statefulMapper.StateID1 = stateClient.RegisterState[V1](
		statefulMapper.StateClient,
	)
	statefulMapper.StateID2 = stateClient.RegisterState[V2](
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
func (sm *StatefulMapper2[IN, OUT, K, V1, V2]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {
	//t1 := time.Now()
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
	sm.StateClient.FetchSimpleState(keys, []uint16{sm.StateID1, sm.StateID2})
	//t2 := time.Now()

	var state1 V1
	var state2 V2
	var output OUT
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get state for current record
		state1, ok = sm.StateClient.GetSimpleState(sm.StateID1, keys[i]).(V1)
		if !ok {
			log.Fatalln("Fetched state1 failed for type assertion")
		}
		state2, ok = sm.StateClient.GetSimpleState(sm.StateID2, keys[i]).(V2)
		if !ok {
			log.Fatalln("Fetched state2 failed for type assertion")
		}

		// Execute UDF to process the input record
		output = sm.F(record, state1, state2)

		// Store the timestamp of the input record - this is used to assign
		// timestamp for the output record in Emit()
		sm.Collector.SetCurrentTimestamp(record.GetTimestamp())

		// Call collector to push the output record to the downstream
		sm.Collector.Emit(output)
	}
	//t3 := time.Now()

	// Flush local cache to StateService
	sm.StateClient.FlushSimpleState()
	//t4 := time.Now()
	//log.Println("Batch total time:", t4.Sub(t1), "Fetch state time:", t2.Sub(t1), "Process batch time:", t3.Sub(t2), "Flush state time:", t4.Sub(t3))
}

// Validate input type at compile time
func (sm *StatefulMapper2[IN, OUT, K, V1, V2]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (sm *StatefulMapper2[IN, OUT, K, V1, V2]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
