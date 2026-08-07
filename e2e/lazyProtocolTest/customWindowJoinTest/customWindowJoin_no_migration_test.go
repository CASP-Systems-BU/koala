package customWindowJoinTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constant"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

func TestCustomWindowJoinNoMigration(t *testing.T) {

	if Num_Buckets_Per_Period < 2 {
		t.Error("Num_Buckets_Per_Period must be >= 2")
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "no-migration"
	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(1) },
		config,
	)

	// Find which worker is used to deploy join at the beginning and which
	// worker is the empty worker for scale up
	var oldWorker *worker.Worker
	var newWorker *worker.Worker
	for _, w := range workers {

		if w.AssignedTask == nil {
			// This is the empty worker
			newWorker = w
			continue
		}

		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// This is the worker that has join deployed at the beginning
		oldWorker = w
	}

	// Wait for 8s before rescaling
	time.Sleep(8 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "customWindowJoin",
		TargetParallelism: 2,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(Period_Span * Num_Period)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait to reach to the ending watermark
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessNoMigration(t, newWorker, oldWorker)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkCorrectnessNoMigration(
	t *testing.T,
	newWorker *worker.Worker,
	oldWorker *worker.Worker,
) {

	// Expected results:
	// For each key in each period, the expected sum1 and sum2 are:
	// sum1 = period
	// sum2 = period * (Num_Buckets_Per_Period - 1)
	for period_id, keys_map := range results {

		// Each period should have all keys
		if len(keys_map) != Numn_Keys {
			t.Errorf(
				"Period %d: expect %d keys, got %d keys",
				period_id,
				Numn_Keys,
				len(keys_map),
			)
		}

		// Check sum1 and sum2 for each key
		for _, sums := range keys_map {
			if sums[0] != period_id {
				t.Errorf(
					"Period %d: expect sum1 %d, got %d",
					period_id,
					period_id,
					sums[0],
				)
			}
			if sums[1] != period_id*(Num_Buckets_Per_Period-1) {
				t.Errorf(
					"Period %d: expect sum2 %d, got %d",
					period_id,
					period_id*(Num_Buckets_Per_Period-1),
					sums[1],
				)
			}
		}
	}

	// The new worker should have no state
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

	// In the new worker, all state should have been removed except the state
	// for ending key -1
	oldWorkerIter := oldWorker.StateService.StateBackendImpl.GetIterator()
	numKeys = 0
	for oldWorkerIter.First(); oldWorkerIter.Valid(); oldWorkerIter.Next() {

		serializedKey := oldWorkerIter.Key()
		key, _, _ := varint.UnmarshalInt(
			serializedKey[constant.KeyPrefixSize:],
		)

		if key != -1 {
			t.Fatalf(
				"Old worker %d has key %d remaining in the state backend\n",
				oldWorker.WorkerId,
				key,
			)
		}
		numKeys++
	}

	// Same has 2 keys
	if numKeys != 2 {
		t.Fatalf(
			"Old worker %d should have only 1 keys remaining, but got %d keys\n",
			oldWorker.WorkerId,
			numKeys,
		)
	}
}
