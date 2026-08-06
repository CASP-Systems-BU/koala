package statefulMapTest

import (
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

const DEDUP_NUM_KEYS = 10

// DEDUP_MAX_LEN is the maximum sequence length across all keys
const DEDUP_MAX_LEN = 10

// TestDedup is an end-to-end test for a dedup/last-seen use case of a stateful
// mapper in a basic streaming pipeline. The pipeline includes a source,
// stateful mapper and a
// sink. The test ensures:
// 1. Source emits a non-contiguous fixed sequence of values per each target key
// (0 to 9) (with repeats).
// 2. The stateful mapper correctly stores last seen value
// per key and flags when a value changes.
// 3. The new counts per key match the expected number of value changes. And the
// 	  final state per key equals the last value in that key's sequence.

// Each slice is the value stream for one key (key index == slice index).
// Repeats are intentional to test the dedup logic.
var dedupSequences = [][]int{
	{1, 1, 2, 2, 2, 3, 1},          // len 7
	{5, 5, 5, 4, 4},                // len 5
	{9, 8, 8, 9},                   // len 4
	{2, 2, 2},                      // len 3
	{7, 6, 6, 6, 7},                // len 5
	{3, 3, 4, 4, 5, 5, 5, 2, 2, 1}, // len 10
	{8, 8, 8, 8, 8, 7},             // len 6
	{0, 1, 1, 2, 3, 3, 4},          // len 7
	{6, 6, 5, 5, 6, 6, 7, 7, 7},    // len 9
	{4, 4, 4, 4, 4, 4, 4, 3, 2, 2}, // len 10
}

// Tracks how many new outputs the sink observed per key.
var dedupNewCounts map[int64]int

func TestDedup(t *testing.T) {
	dedupNewCounts = make(map[int64]int)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, dedupQuery, config)

	// Wait for the test to be completed
	time.Sleep(5 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkDedupCorrectness(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func dedupQuery() *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	// Define source interleaving keys
	source := dataflow.NewSource[*tuple.Tuple2[int64, int]](
		"source",
		func(co collector.Collector) {
			// Emit values in round-robin order across keys
			for i := 0; i < DEDUP_MAX_LEN; i++ {
				for k := 0; k < DEDUP_NUM_KEYS; k++ {
					seq := dedupSequences[k]
					if i < len(seq) {
						co.Emit(&tuple.Tuple2[int64, int]{
							V1: int64(k),
							V2: seq[i],
						})
					}
				}
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int]) int64 {
		return t.V1
	})

	// Define stateful map dedup which tracks last-seen value per key and mark
	// changes
	dedup := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int64, int],
			state *stateType.ValueState[*tuple.Tuple1[int]],
		) *tuple.Tuple3[int64, int, int] {
			lastVal, exist := state.Get()
			isNew := 0

			// If this is the first value for the key, mark as new and store it
			if !exist {
				lastVal = tuple.NewTuple1(in.V2)
				state.Set(lastVal)
				isNew = 1
			} else if lastVal.V1 != in.V2 {
				// If the value changed, update state and mark as new
				lastVal.V1 = in.V2
				state.Set(lastVal)
				isNew = 1
			}

			return &tuple.Tuple3[int64, int, int]{
				V1: in.V1,
				V2: in.V2,
				V3: isNew,
			}
		},
	)
	dedup.SetParallelism(2)
	dataflow.AddOperator(query, dedup)

	// Sink which will count how many new values were observed per key
	sink := dataflow.NewSink(
		"sink",
		func(in *tuple.Tuple3[int64, int, int]) {
			if in.V3 == 1 {
				dedupNewCounts[in.V1]++
			}
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	dataflow.Add1To1Stream(query, source, dedup)
	dataflow.Add1To1Stream(query, dedup, sink)

	return query
}

func checkDedupCorrectness(t *testing.T, workers []*worker.Worker) {

	// Compute expected counts and final values directly from the input
	// sequences
	expectedNewCounts, expectedLastValues := expectedDedupResults()

	// Validate new output counts
	for key, expected := range expectedNewCounts {
		if got := dedupNewCounts[key]; got != expected {
			t.Errorf(
				"Expected %d new values for key %d, got %d",
				expected,
				key,
				got,
			)
		}
	}

	// Read final state from each stateful mapper instance
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}

	if len(iters) != 2 {
		t.Error("Expect 2 stateful mappers, but got ", len(iters))
	}

	// Load the final state per key and ensure no duplicates
	observed := make(map[int64]int)
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			keyI, _, _ := varint.UnmarshalInt64(key[constant.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

			if _, exist := observed[keyI]; exist {
				t.Errorf("Duplicate state for key %d", keyI)
				continue
			}

			observed[keyI] = valueI
		}
	}

	if len(observed) != DEDUP_NUM_KEYS {
		t.Error("Expect ", DEDUP_NUM_KEYS, " keys, but got ", len(observed))
	}

	// Validate last-seen values match expected final state per key
	for key, expected := range expectedLastValues {
		if got, exist := observed[key]; !exist {
			t.Errorf("Expected key %d not found in the state", key)
		} else if got != expected {
			t.Errorf("Expected last value %d for key %d, got %d", expected, key, got)
		}
	}
}

func expectedDedupResults() (map[int64]int, map[int64]int) {
	newCounts := make(map[int64]int)
	lastValues := make(map[int64]int)

	// For each key, count the number of value changes and record the last value
	for k := 0; k < DEDUP_NUM_KEYS; k++ {
		seq := dedupSequences[k]
		var last int
		for i, v := range seq {
			if i == 0 || v != last {
				newCounts[int64(k)]++
			}
			last = v
		}
		lastValues[int64(k)] = last
	}

	return newCounts, lastValues
}
