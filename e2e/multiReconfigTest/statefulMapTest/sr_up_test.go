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
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constant"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

// Test multiple reconfigurations (2 scale-up) with stop-and-restart protocol.

// We have target keys in range [0, NUM_KEYS)
// Each key is repeated REPEAT times
const NUM_KEYS = 10000
const REPEAT = 8000

// Track received counts at the sink
var sinkCounter map[int64]int = make(map[int64]int)

func Test2ScaleUpSR(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()

	numWorkers := 6
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Wait for 8s before 1st scale-up
	time.Sleep(8 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("1st job rescale response: %v\n", resp.Info)

	// Wait for 8s before 2nd scale-up
	time.Sleep(8 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 4,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("2nd job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(30 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/

	checkCorrectness(t, workers)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}

func checkCorrectness(t *testing.T, workers []*worker.Worker) {

	// First evaluate the sinkCounter: each key should have count of REPEAT
	for i := range NUM_KEYS {
		count, exist := sinkCounter[int64(i)]
		if !exist {
			t.Fatalf("Key %d does not exist in the sink counter\n", i)
		}
		if count != REPEAT {
			t.Fatalf("Key %d has count %d, expected %d\n", i, count, REPEAT)
		}
	}

	// Evaluate negative keys - only appear twice
	for i := 1; i <= 200; i++ {
		count, exist := sinkCounter[int64(-i)]
		if !exist {
			t.Fatalf("Key %d does not exist in the sink counter\n", -i)
		}
		if count != 2 {
			t.Fatalf("Key %d has count %d, expected %d\n", -i, count, 2)
		}
	}

	// Iterate state backends of all workers to check if there is any unexpected
	// state at the end of the test
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
	// indicates it has been migrated during reconfig (we do not remove keys
	// after reconfig) - we always store the max count as the final count
	results := make(map[int64]int)

	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			keyI, _, _ := varint.UnmarshalInt64(key[constant.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

			val, exist := results[keyI]
			if !exist {
				results[keyI] = valueI
			} else {
				if valueI > val {
					results[keyI] = valueI
				}
			}
		}
	}

	// Finally check the results
	if len(results) != NUM_KEYS+200 {
		t.Fatalf(
			"Unexpected number of keys in the state backends, expected %d, got %d\n",
			NUM_KEYS+200,
			len(results),
		)
	}
	for k, v := range results {

		if k >= 0 {
			if v != REPEAT {
				t.Fatalf(
					"Key %d has count %d in state backend, expected %d\n",
					k,
					v,
					REPEAT,
				)
			}
		} else {
			if v != 2 {
				t.Fatalf(
					"Key %d has count %d in state backend, expected %d\n",
					k,
					v,
					2,
				)
			}
		}
	}
	log.Println("[E2E] Sink counter is correct")
}

func query(counterParallelism int) *dataflow.Dataflow {

	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple1[int64]](
		"source",
		func(co collector.Collector) {
			// First generate 200 negative keys -1 to -200; they will be
			// re-accessed at the end. This is guaratee these keys are fetched
			// from the original workers after multiple reconfigs
			for i := 1; i <= 200; i++ {
				co.Emit(&tuple.Tuple1[int64]{
					V1: int64(-i),
				})
			}

			for range REPEAT {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple1[int64]{
						V1: int64(i),
					})
				}
			}

			for i := 1; i <= 200; i++ {
				co.Emit(&tuple.Tuple1[int64]{
					V1: int64(-i),
				})
			}

			log.Println("[Test] Source has emitted all the data!")
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
		sinkCounter[in.V1] = in.V2
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, counter)
	dataflow.Add1To1Stream(query, counter, sink)

	return query
}
