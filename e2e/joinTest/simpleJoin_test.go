package joinTest

import (
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
	"github.com/mus-format/mus-go/varint"
)

// Basic test for Join operator with only ValueState.

// Test config parameters. Note that be careful about the hardcoded sleep time
// for experiment to finish, large parameters and slow hardware can cause
// runtime to exceed the sleep time, leading to test failure. Try larger sleep
// time by WAIT_TIME if that happens.
const NUM_KEYS = 1000
const REPEAT_SOURCE1 = 8000
const REPEAT_SOURCE2 = 7000
const SOURCE1_PARALLELISM = 2
const SOURCE2_PARALLELISM = 2
const JOIN_PARALLELISM = 2
const WAIT_TIME = 30 // in seconds

// Pointer to the last received record by Sink for testing purposes
var lastReceived *tuple.Tuple3[int, int64, int64]

func TestSimpleJoin(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := SOURCE1_PARALLELISM + SOURCE2_PARALLELISM + JOIN_PARALLELISM + 1
	_, workers, _ := testutils.DeployJob(numWorkers, query, config)

	// Wait for the test to be completed
	time.Sleep(WAIT_TIME * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	checkCorrectness(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkCorrectness(t *testing.T, workers []*worker.Worker) {

	// Print out the last received record by Sink for reference. 2 states should
	// be opposite numbers
	log.Println(
		"Last received record by Sink - state1:",
		lastReceived.V2,
		" state2:",
		lastReceived.V3,
	)

	// Get all workers for join operator
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get the state
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}
	if len(iters) != JOIN_PARALLELISM {
		t.Errorf("Expect %d join workers, got %d", JOIN_PARALLELISM, len(iters))
	}

	/*
		The processing logic is as follows:
		For each record from source1, it increases state1 by in.V2, and
		decreases state2 by in.V2
		For each record from source2, it increases state2 by in.V2, and
		decreases state1 by in.V2
		Each key is repeated REPEAT_SOURCE1 times in source1 and REPEAT_SOURCE2
		times in source2. Each time the value is the sequence number of the
		repetition. Also consider that parallelism for source1 and source2 are
		SOURCE1_PARALLELISM and SOURCE2_PARALLELISM respectively, exactly same
		logic is applied to each source instance.

		Therefore, for each key, the final value of state1 and state2 should be:
		state1 = (0 + 1 + ... + REPEAT_SOURCE1-1) * SOURCE1_PARALLELISM
				- (0 + 1 + ... + REPEAT_SOURCE2-1) * SOURCE2_PARALLELISM
		state2 = (0 + 1 + ... + REPEAT_SOURCE2-1) * SOURCE2_PARALLELISM
				- (0 + 1 + ... + REPEAT_SOURCE1-1) * SOURCE1_PARALLELISM
	*/
	expectedState1 := (REPEAT_SOURCE1*(REPEAT_SOURCE1-1)/2)*SOURCE1_PARALLELISM -
		(REPEAT_SOURCE2*(REPEAT_SOURCE2-1)/2)*SOURCE2_PARALLELISM
	expectedState2 := (REPEAT_SOURCE2*(REPEAT_SOURCE2-1)/2)*SOURCE2_PARALLELISM -
		(REPEAT_SOURCE1*(REPEAT_SOURCE1-1)/2)*SOURCE1_PARALLELISM

	// All keys for each state have the same final value, so we only have 2
	// expected values: state_id -> expected_value. Note that state_id is
	// generated at state registration time, starting from 0.
	expectedVal := make(map[uint16]int64)
	expectedVal[0] = int64(expectedState1)
	expectedVal[1] = int64(expectedState2)

	// Iterate the state
	numKeys := 0
	expectedKeys := make(map[uint16]map[int]struct{})
	expectedKeys[0] = make(map[int]struct{})
	expectedKeys[1] = make(map[int]struct{})
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			serializedKey := iter.Key()
			serializedValue := iter.Value()

			// Extract the state id, key, and value
			stateIDOffset := constant.OperatorIDSize + constant.BucketIdxSize
			stateID := binary.BigEndian.Uint16(
				serializedKey[stateIDOffset : stateIDOffset+constant.StateIDSize],
			)
			key, _, _ := varint.UnmarshalInt(
				serializedKey[constant.KeyPrefixSize:],
			)
			value, _, _ := varint.UnmarshalInt64(serializedValue)

			numKeys++
			// Add the key to the map
			expectedKeys[stateID][key] = struct{}{}

			if value != expectedVal[stateID] {
				t.Error(
					"Expect ",
					expectedVal[stateID],
					" for key ",
					key,
					", but got ",
					value,
				)
			}
		}
	}

	// Check the number of keys
	if numKeys != 2*NUM_KEYS {
		t.Error("Expect ", 2*NUM_KEYS, " serialized keys, but got ", numKeys)
	}

	// All expected keys are int from 0 to NUM_KEYS-1
	for i := range NUM_KEYS {
		if _, ok := expectedKeys[0][i]; !ok {
			t.Error("Expected key ", i, " not found in the state 1")
		}
		if _, ok := expectedKeys[1][i]; !ok {
			t.Error("Expected key ", i, " not found in the state 2")
		}
	}
}

// Simple query to join 2 streams
func query() *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	// Define Source 1
	source1 := dataflow.NewSource[*tuple.Tuple2[int, int64]](
		"source1",
		func(co collector.Collector) {
			for r := range REPEAT_SOURCE1 {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int, int64]{
						V1: i,
						V2: int64(r),
					})
				}
			}
			log.Println("Source1 completed")
		},
	)
	source1.SetParallelism(SOURCE1_PARALLELISM)
	dataflow.AddOperator(query, source1)

	// Define Source 2
	source2 := dataflow.NewSource[*tuple.Tuple2[int, int64]](
		"source2",
		func(co collector.Collector) {
			for r := range REPEAT_SOURCE2 {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int, int64]{
						V1: i,
						V2: int64(r),
					})
				}
			}
			log.Println("Source2 completed")
		},
	)
	source2.SetParallelism(SOURCE2_PARALLELISM)
	dataflow.AddOperator(query, source2)

	// KeyAssigner for source1 output
	keyAssigner1 := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, int64]) int {
			return t.V1
		},
	)

	// KeyAssigner for source2 output
	keyAssigner2 := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, int64]) int {
			return t.V1
		},
	)

	// Define Join (generic type in [] is the output tuple type)
	join := dataflow.NewJoin[*tuple.Tuple3[int, int64, int64]](
		"join",
		/************************* 1st input stream **************************/
		source1,
		keyAssigner1,
		func(
			in *tuple.Tuple2[int, int64],
			state1 *stateType.ValueState[*tuple.Tuple1[int64]],
			state2 *stateType.ValueState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) {

			// When Join receives record from source1, it increases state1 by
			// in.V2, and decreases state2 by in.V2

			// Increase state1 by in.V2
			state1Val, exist := state1.Get()
			if !exist {
				state1Val = tuple.NewTuple1(int64(0))
			}
			state1Val.V1 += in.V2
			state1.Set(state1Val)

			// Decrease state2 by in.V2
			state2Val, exist := state2.Get()
			if !exist {
				state2Val = tuple.NewTuple1(int64(0))
			}
			state2Val.V1 -= in.V2
			state2.Set(state2Val)

			co.Emit(&tuple.Tuple3[int, int64, int64]{
				V1: in.V1,
				V2: state1Val.V1,
				V3: state2Val.V1,
			})
		},
		/************************* 2nd input stream **************************/
		source2,
		keyAssigner2,
		func(
			in *tuple.Tuple2[int, int64],
			state1 *stateType.ValueState[*tuple.Tuple1[int64]],
			state2 *stateType.ValueState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) {

			// When Join receives record from source2, it increases state2 by
			// in.V2, and decreases state1 by in.V2

			// Increase state2 by in.V2
			state2Val, exist := state2.Get()
			if !exist {
				state2Val = tuple.NewTuple1(int64(0))
			}
			state2Val.V1 += in.V2
			state2.Set(state2Val)

			// Decrease state1 by in.V2
			state1Val, exist := state1.Get()
			if !exist {
				state1Val = tuple.NewTuple1(int64(0))
			}
			state1Val.V1 -= in.V2
			state1.Set(state1Val)

			co.Emit(&tuple.Tuple3[int, int64, int64]{
				V1: in.V1,
				V2: state1Val.V1,
				V3: state2Val.V1,
			})
		},
	)
	join.SetParallelism(JOIN_PARALLELISM)
	dataflow.AddOperator(query, join)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int, int64, int64]) {
			lastReceived = t
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect the operators
	dataflow.Add2To1Stream(query, source1, source2, join)
	dataflow.Add1To1Stream(query, join, sink)

	return query
}
