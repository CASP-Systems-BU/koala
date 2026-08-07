package keybyCollectorTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/coordinator"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constants"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

const NUM_KEYS = 100
const REPEAT = 100

// TestKeyByCollector validates the KeyBy operation in the streaming pipeline.
// It ensures correct key partitioning, expected key counts (REPEAT), no
// duplicate processing across workers, and proper task-to-worker assignments.
func TestKeyByCollector(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 5
	_, workers, coordinator := testutils.DeployJob(numWorkers, query, config)

	// wait for test to complete
	time.Sleep(5 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	VerifyKeyByResults(t, workers, coordinator)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func VerifyKeyByResults(
	t *testing.T,
	workers []*worker.Worker,
	coordinator *coordinator.Coordinator,
) {

	mappers := make([]stateBackend.StateIterator, 0)

	// Track keys in all mappers
	seenKeys := make(map[int]struct{})

	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		// Get the state
		mapper := w.StateService.StateBackendImpl.GetIterator()
		mappers = append(mappers, mapper)

		for mapper.First(); mapper.Valid(); mapper.Next() {
			key := mapper.Key()
			value := mapper.Value()

			keyI, _, _ := varint.UnmarshalInt(key[constants.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

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

			// Find expected worker id this key should be routed to
			workerId := coordinator.KeyPartitions["statefulMapper"].KeyToWorkerID(
				key[constants.KeyPrefixSize:],
			)

			// Check if the expected worker id matches the actual worker that
			// contains the key
			if workerId != w.WorkerId {
				t.Error(
					"Expected key ",
					keyI,
					" to be processed by worker ",
					workerId,
					" but got worker ",
					w.WorkerId,
				)
			}

			// Track the key appearance
			if _, exist := seenKeys[keyI]; exist {
				t.Errorf("Key %d appears in multiple workers", keyI)
			} else {
				seenKeys[keyI] = struct{}{}
			}
		}
	}

	// Check the number of keys
	if len(seenKeys) != NUM_KEYS {
		t.Error("Expect ", NUM_KEYS, " keys, but got ", len(seenKeys))
	}
	if len(mappers) != 3 {
		t.Error("expected 3 mappers got ", len(mappers))
	}
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
	keyAssigner := keyAssigner.NewKeyAssigner(
		func(t *tuple.Tuple2[int64, int]) int64 {
			return t.V1
		},
	)

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int64, int],
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
	counter.SetParallelism(3)
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
