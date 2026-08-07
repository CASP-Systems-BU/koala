package statefulMapTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/coordinator"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constants"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

// [Testing Note] this test requires longer time to finish. Default testing
// timeout in vscode is 30s. You should change the setting to a larger duration
// Change the VS Code setting file settings.json: e.g. "go.testTimeout": "2m"

// In this test, we apply scale-up operation under stop-and-restart protocol.
// We verify that the count of all keys is still correct with reconfiguration.
// Specifically, we check the following:
// 1. The count of each key == REPEAT
// 2. The number of appeared keys == NUM_KEYS
// 3. The keys affected by repartition are processed by the new worker
// Note that new worker fetches state from old workers and old workers do not
// delete the migrated state after state migration.

// We have target keys in range [0, NUM_KEYS)
// Each key is repeated REPEAT times
const NUM_KEYS = 6000
const REPEAT = 6000

// Sink counter to count the number of events
var sinkCounter int64 = 0

func TestStateMigrationWordCount(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()

	numWorkers := 5
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Find which worker is the empty worker for scale up
	var workerIdForScaleup uint16
	for _, w := range workers {
		if w.AssignedTask == nil {
			// This is the empty worker
			workerIdForScaleup = w.WorkerId
			break
		}
	}

	// Wait for 10s before rescaling
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(20 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/
	checkCorrectness(
		t,
		workerIdForScaleup,
		workers,
		coordinator,
		"consistent-hashing",
		true,
	)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}

func query(counterParallelism int) *dataflow.Dataflow {

	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple1[int64]](
		"source",
		func(co collector.Collector) {
			// First send the target keys, each key is repeated REPEAT times
			for r := 0; r < REPEAT; r++ {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple1[int64]{
						V1: int64(i),
					})
				}
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Define counter
	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple1[int64]) int64 {
		return t.V1
	})

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple1[int64],
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
	counter.SetParallelism(counterParallelism)
	dataflow.AddOperator(query, counter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int64, int]) {

		sinkCounter += 1

		// Send signal to indicate the end of the stream
		// e.g. through channel
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, counter)
	dataflow.Add1To1Stream(query, counter, sink)

	return query
}

func checkCorrectness(
	t *testing.T,
	newWorkerId uint16,
	workers []*worker.Worker,
	coordinator *coordinator.Coordinator,
	partitionPolicy string,
	isScaleUp bool,
) {

	// Get the stateful mappers
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get the state iterator
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}

	// We track all existing keys in the map. If a duplicate key appears, it
	// indicates that this is a migrated key and the larger value should be
	// kept for evaluation - since we do not delete key after migration.
	results := make(map[int64]int)

	// Iterate the state
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			keyI, _, _ := varint.UnmarshalInt64(key[constants.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

			val, exist := results[keyI]
			if !exist {
				results[keyI] = valueI
			} else {
				if valueI > val {
					results[keyI] = valueI
				}

				// This is a key that is affected by the repartition. For
				// consistent hashing policy, the key should belong to new
				// worker after rescaling
				if partitionPolicy == "consistent-hashing" && isScaleUp {
					workerId := coordinator.KeyPartitions["statefulMapper"].KeyToWorkerID(
						key[constants.KeyPrefixSize:],
					)

					// Check if the repartitioned key is processed by the new
					// worker
					if workerId != newWorkerId {
						t.Error(
							"Expected key ",
							keyI,
							" to be processed by worker ",
							workerId,
							" but got worker ",
							newWorkerId,
						)
					}
				}
			}
		}
	}

	// Check the number of appeared keys
	if len(results) != NUM_KEYS {
		t.Error("Expect ", NUM_KEYS, " keys, but got ", len(results))
	}

	// Check if the count for each key is correct
	for k, v := range results {
		if v != REPEAT {
			t.Error(
				"Expect ",
				REPEAT,
				" for key ",
				k,
				", but got ",
				v,
			)
		}
	}
}
