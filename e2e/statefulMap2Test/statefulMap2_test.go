package statefulMap2Test

import (
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

// We have 100 target keys: [0, 99]
// Each key is repeated 100 times
const NUM_KEYS = 100
const REPEAT = 100

// Test StatefulMapper2 operator with 2 embeded states.

func TestStatefulMap2(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, query, config)

	// Wait for the test to be compeleted
	time.Sleep(5 * time.Second)

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

func query() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple3[int64, int, int]](
		"source",
		func(co collector.Collector) {
			// First send the target keys, each key is repeated REPEAT times
			for r := range REPEAT {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple3[int64, int, int]{
						V1: int64(i),
						V2: r,
						V3: 2 * r,
					})
				}
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[int64, int, int]) int64 {
			return t.V1
		},
	)

	// Define Counter
	counter := dataflow.NewStatefulMapper2(
		"statefulMapper",
		keyAssigner,
		func(in *tuple.Tuple3[int64, int, int], state1 *stateType.ValueState[*tuple.Tuple1[int]], state2 *stateType.ValueState[*tuple.Tuple1[int]]) *tuple.Tuple3[int64, int, int] {
			// Read the state
			state1Val, exist := state1.Get()
			if !exist {
				state1Val = tuple.NewTuple1(0)
			}
			state2Val, exist := state2.Get()
			if !exist {
				state2Val = tuple.NewTuple1(0)
			}

			// Add the new value to the states
			state1Val.V1 += in.V2
			state2Val.V1 += in.V3

			// Write the state
			state1.Set(state1Val)
			state2.Set(state2Val)

			return &tuple.Tuple3[int64, int, int]{
				V1: in.V1,
				V2: state1Val.V1,
				V3: state2Val.V1,
			}
		},
	)
	counter.SetParallelism(2)
	dataflow.AddOperator(query, counter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple3[int64, int, int]) {})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, counter)
	dataflow.Add1To1Stream(query, counter, sink)

	return query
}

func checkCorrectness(t *testing.T, workers []*worker.Worker) {

	// Get the two stateful mappers
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get the state
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}

	// Check the state
	if len(iters) != 2 {
		t.Error("Expect 2 stateful mappers, but got ", len(iters))
	}

	// Expected value for state 1: 0 + 1 + ... + 99
	expectedValState1 := 0
	for i := 0; i < 100; i++ {
		expectedValState1 += i
	}
	// Expected value for state 2: 0 + 2 + ... + 198
	expectedValState2 := 0
	for i := 0; i < 100; i++ {
		expectedValState2 += 2 * i
	}
	expectedVal := make(map[uint16]int)
	expectedVal[0] = expectedValState1
	expectedVal[1] = expectedValState2

	// iterate the state
	numKeys := 0
	expectedKeys := make(map[uint16]map[int64]struct{})
	expectedKeys[0] = make(map[int64]struct{})
	expectedKeys[1] = make(map[int64]struct{})
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			serializedKey := iter.Key()
			serializedValue := iter.Value()

			// Extract the state id, key, and value
			stateIDOffset := constant.OperatorIDSize + constant.BucketIdxSize
			stateID := binary.BigEndian.Uint16(
				serializedKey[stateIDOffset : stateIDOffset+constant.StateIDSize],
			)
			key, _, _ := varint.UnmarshalInt64(
				serializedKey[constant.KeyPrefixSize:],
			)
			value, _, _ := varint.UnmarshalInt(serializedValue)

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

	// All expected keys are int64 from 0 to NUM_KEYS-1
	for i := range NUM_KEYS {
		if _, ok := expectedKeys[0][int64(i)]; !ok {
			t.Error("Expected key ", i, " not found in the state 1")
		}
		if _, ok := expectedKeys[1][int64(i)]; !ok {
			t.Error("Expected key ", i, " not found in the state 2")
		}
	}
}
