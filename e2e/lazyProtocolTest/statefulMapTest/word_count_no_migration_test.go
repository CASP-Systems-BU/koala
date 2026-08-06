package statefulMapTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constants"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
	"github.com/mus-format/mus-go/varint"
)

func TestLazyNoMigrationWordCount(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "no-migration"

	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Find new worker for scale up and old workers
	var newWorker *worker.Worker
	var oldWorkers []*worker.Worker
	for _, w := range workers {

		if w.AssignedTask == nil {
			newWorker = w
			continue
		}

		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		oldWorkers = append(oldWorkers, w)
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

	checkCorrectnessNoMigration(t, newWorker, oldWorkers)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}

func checkCorrectnessNoMigration(
	t *testing.T,
	newWorker *worker.Worker,
	oldWorkers []*worker.Worker,
) {

	// Check the new worker: there should be zero state written into the local
	// state backend
	newWorkerIter := newWorker.StateService.StateBackendImpl.GetIterator()
	numKeys := 0
	for newWorkerIter.First(); newWorkerIter.Valid(); newWorkerIter.Next() {
		numKeys++
	}
	if numKeys != 0 {
		t.Fatalf(
			"New worker %d has non-zero number of keys: %d\n",
			newWorker.WorkerId,
			numKeys,
		)
	}

	// Check the old workers: all flushed state should have the expected counts
	results := make(map[int64]int)

	for _, w := range oldWorkers {
		stateIter := w.StateService.StateBackendImpl.GetIterator()
		for stateIter.First(); stateIter.Valid(); stateIter.Next() {
			key := stateIter.Key()
			value := stateIter.Value()
			keyI, _, _ := varint.UnmarshalInt64(key[constants.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)
			results[keyI] = valueI
		}
	}

	// Check the number of appeared keys
	if len(results) != NUM_KEYS {
		t.Fatalf(
			"Number of appeared keys %d != expected %d\n",
			len(results),
			NUM_KEYS,
		)
	}

	// Check the count of each key
	for k, v := range results {
		if v != REPEAT {
			t.Fatalf(
				"Key %d has count %d != expected %d\n",
				k,
				v,
				REPEAT,
			)
		}
	}
}
