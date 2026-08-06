package keyAssignerTest

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
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constants"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
	"github.com/mus-format/mus-go/varint"
)

// In this test, we define KeyAssigner with multiple data fields e.g. a
// combination of multiple fields as the key for keyby operation. We perform all
// tests used in keyby_collector_test.go. We also test if all composite keys are
// present in state store.

const NUM_KEYS = 100
const REPEAT = 100

func TestCompositeKey(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, compositeKeyQuery, config)

	// wait for test to complete
	time.Sleep(5 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	verifyCompositeKeyPresence(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func compositeKeyQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {
			// First send the target keys, each key is repeated REPEAT times
			for range REPEAT {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int64, int64]{
						V1: int64(i),
						V2: int64(i),
					})
				}
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	// We use a composite key here: V1 + V2
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int64]) int64 {
		return t.V1 + t.V2
	})

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int64, int64],
			state *stateType.ValueState[*tuple.Tuple1[int]],
		) *tuple.Tuple2[int64, int] {

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

// Verify if all the combined keys are present in the state store
func verifyCompositeKeyPresence(
	t *testing.T,
	workers []*worker.Worker,
) {

	// Construct set of expected keys
	expectedKeys := make(map[int]struct{})
	for i := range NUM_KEYS {
		expectedKeys[i+i] = struct{}{}
	}

	// Traverse all stateful tasks and access their state
	for _, w := range workers {

		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get state for current task
		mapper := w.StateService.StateBackendImpl.GetIterator()
		for mapper.First(); mapper.Valid(); mapper.Next() {
			key := mapper.Key()
			keyI, _, _ := varint.UnmarshalInt(key[constants.KeyPrefixSize:])

			// Check if keyI is present in expected key set
			if _, ok := expectedKeys[keyI]; !ok {
				t.Errorf("Key %d not found in expected keys", keyI)
			} else {
				// Remove key from expected keys so that we don't allow
				// duplicate keys present in different takes
				delete(expectedKeys, keyI)
			}
		}
	}
}
