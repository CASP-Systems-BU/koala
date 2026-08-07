package dataflow

import (
	"log"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
	"github.com/CASP-Systems-BU/koala/metric"
)

// Join operator accepts 2 input streams and produces 1 output stream. It
// maintains 2 states internally - generally corresponds to 2 input streams but
// users can define their own logic on state usage. This is an incremental join
// implmementation that considers all historical data without deletion.

// Required generic types:
// - OUT: output tuple type
// - IN1: input tuple type from upstream operator 1
// - IN2: input tuple type from upstream operator 2
// - K: key type
// - V1: state type for 1st state
// - V2: state type for 2nd state
// Why define OUT as the first generic type: For NewJoin() API, all other types
// can be inferred except OUT. Putting OUT as the first generic type to let
// users explicitly specify it when calling NewJoin() API.

type Join[OUT, IN1, IN2 tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType] struct {
	*StatefulOperatorBase2Upstream[IN1, IN2, K]

	// Join has 2 states
	StateID1 uint16
	StateID2 uint16

	// UDF that takes 1 input record from the 1st upstream and output 0 or more
	// output records
	F1 func(IN1, V1, V2, collector.Collector) time.Duration

	// UDF that takes 1 input record from the 2nd upstream and output 0 or more
	// output records
	F2 func(IN2, V1, V2, collector.Collector) time.Duration
}

// Type validation at compile time
var _ OperatorWith2InputStream[tuple.Tuple, tuple.Tuple] = (*Join[tuple.Tuple, tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*Join[tuple.Tuple, tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

// API exposed to users to define the operator
func NewJoin[OUT, IN1, IN2 tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType](
	name string,
	/*************************** 1st input stream ****************************/
	upstream1 OperatorWith1OutputStream[IN1],
	keyAssigner1 *ka.KeyAssigner[IN1, K],
	f1 func(IN1, V1, V2, collector.Collector) time.Duration,
	/*************************** 2nd input stream ****************************/
	upstream2 OperatorWith1OutputStream[IN2],
	keyAssigner2 *ka.KeyAssigner[IN2, K],
	f2 func(IN2, V1, V2, collector.Collector) time.Duration,
) *Join[OUT, IN1, IN2, K, V1, V2] {

	join := &Join[OUT, IN1, IN2, K, V1, V2]{
		StatefulOperatorBase2Upstream: NewStatefulOperatorBase2Upstream(
			name,
			upstream1,
			keyAssigner1,
			upstream2,
			keyAssigner2,
			stateClient.SimpleStateClient,
		),
		F1: f1,
		F2: f2,
	}

	// Register state types to StateClient
	join.StateID1 = stateClient.RegisterState[V1](
		join.StateClient,
	)
	join.StateID2 = stateClient.RegisterState[V2](
		join.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 2 upstream operators expected
	join.Supplier = supplier.NewRoundRobinSupplier(name, 2)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	join.Collector = collector.NewRoundRobinCollector[OUT](name)

	return join
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (j *Join[OUT, IN1, IN2, K, V1, V2]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	switch subSupplierName {
	case j.UpstreamName1:
		processBatchJoin(
			workUnit,
			j.KeyAssigner1,
			j.StateClient,
			j.StateID1,
			j.StateID2,
			j.F1,
			j.Collector,
			j.MetricCollector,
		)
	case j.UpstreamName2:
		processBatchJoin(
			workUnit,
			j.KeyAssigner2,
			j.StateClient,
			j.StateID1,
			j.StateID2,
			j.F2,
			j.Collector,
			j.MetricCollector,
		)
	default:
		log.Fatalf(
			"Join: workUnit comes from unexpected SubSupplier %s",
			subSupplierName,
		)
	}
}

// Validate input type at compile time
func (j *Join[OUT, IN1, IN2, K, V1, V2]) InputTupleType2() (IN1, IN2) {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (j *Join[OUT, IN1, IN2, K, V1, V2]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}

/******************************************************************************
							Utils for Join operator
******************************************************************************/

// Given input batch from a determined upstream with generic types determined,
// process the batch
func processBatchJoin[IN tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType](
	workUnit buffer.WorkUnit,
	keyAssigner *ka.KeyAssigner[IN, K],
	stateClient *stateClient.StateClient[K],
	stateID1 uint16,
	stateID2 uint16,
	f func(IN, V1, V2, collector.Collector) time.Duration,
	collector collector.Collector,
	metricCollector *metric.MetricCollector,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Failed to convert workUnit to Batch[IN]")
	}

	// Extract all key fields from the batch
	keys := make([]K, batch.TotalNumRecords)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {
		keys[i] = keyAssigner.GetKey(record)
	}

	// Prefetch the batch state to memory before processing

	stateClient.FetchSimpleState(keys, []uint16{stateID1, stateID2})

	var state1 V1
	var state2 V2
	generateDummyFieldTime := time.Duration(0)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get state for current record
		state1, ok = stateClient.GetSimpleState(stateID1, keys[i]).(V1)
		if !ok {
			log.Fatalln("Fetched state1 failed for type assertion")
		}
		state2, ok = stateClient.GetSimpleState(stateID2, keys[i]).(V2)
		if !ok {
			log.Fatalln("Fetched state2 failed for type assertion")
		}

		// Store the timestamp of the input record - this is used to assign
		// timestamp for the output record in Emit()
		collector.SetCurrentTimestamp(record.GetTimestamp())

		// Execute UDF to process the input record
		generateDummyFieldTime += f(record, state1, state2, collector)

	}
	metricCollector.UpdateGenerateDummyFieldTime(generateDummyFieldTime)
	// Flush local cache to StateService
	stateClient.FlushSimpleState()
}
