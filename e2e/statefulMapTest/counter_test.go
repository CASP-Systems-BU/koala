package statefulMapTest

import (
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

// TestCounter is an end-to-end test that validates the behavior of a simple
// streaming pipeline. The pipeline includes a source, a stateful mapper, and a
// sink. The test ensures:
// 1. Each target key (0 to 99) is correctly counted 100 times.
// 2. The stateful mappers correctly maintain and update state.
// 3. The keys are processed in order, and the total number of keys matches the
// expectation.

func TestCounter(t *testing.T) {

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
	source := dataflow.NewSource[*tuple.Tuple2[int64, int]](
		"source",
		func(co collector.Collector) {
			// First send the target keys, each key is repeated REPEAT times
			for r := range REPEAT {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int64, int]{
						V1: int64(i),
						V2: r,
					})
				}
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int]) int64 {
		return t.V1
	})

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(in *tuple.Tuple2[int64, int], state *stateType.ValueState[*tuple.Tuple1[int]]) *tuple.Tuple2[int64, int] {
			// Read the state
			curCount, exist := state.Get()
			if !exist {
				curCount = tuple.NewTuple1(0)
			}

			// Increment the counter
			curCount.V1++

			// Write the state
			state.Set(curCount)

			return &tuple.Tuple2[int64, int]{
				V1: in.V1,
				V2: curCount.V1,
			}
		},
	)
	counter.SetParallelism(2)
	dataflow.AddOperator(query, counter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int64, int]) {})
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

	// iterate the state
	numKeys := 0
	expectedKeys := make(map[int64]struct{})
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			keyI, _, _ := varint.UnmarshalInt64(key[constant.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

			numKeys++
			// Add the key to the map
			expectedKeys[keyI] = struct{}{}

			if valueI != REPEAT {
				t.Error(
					"Expect ",
					REPEAT,
					" for key ",
					keyI,
					", but got ",
					valueI,
				)
			}
			log.Printf("Key: %d, Value: %d\n", keyI, valueI)
		}
	}

	// Check the number of keys
	if numKeys != NUM_KEYS {
		t.Error("Expect ", NUM_KEYS, " keys, but got ", numKeys)
	}

	// All expected keys are int64 from 0 to NUM_KEYS-1
	for i := range NUM_KEYS {
		if _, ok := expectedKeys[int64(i)]; !ok {
			t.Error("Expected key ", i, " not found in the state")
		}
	}
}
