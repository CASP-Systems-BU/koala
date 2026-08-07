package tumblingWindowTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/worker"
)

func TestTumblingWindowLazyNoMigration(t *testing.T) {

	// Check input
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%WINDOWSPAN != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by WINDOWSPAN",
		)
		return
	}
	if (WINDOWSPAN % TIMEBUCKETSPAN) != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by TIMEBUCKETSPAN",
		)
		t.Errorf(
			"WINDOWSPAN: %v, TIMEBUCKETSPAN: %v, result %d",
			WINDOWSPAN,
			TIMEBUCKETSPAN,
			WINDOWSPAN%TIMEBUCKETSPAN,
		)
		return
	}
	if WINDOWSPAN <= TIMEBUCKETSPAN {
		t.Error(
			"WINDOWSPAN must be larger than TIMEBUCKETSPAN",
		)
		return
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
		tumblingWindowQuery,
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
		TargetRescaleOp:   "window",
		TargetParallelism: 3,
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
	expectedWM := int64(TIMEBUCKETSPAN * NUMTIMEBUCKETS)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessTumblingWindowNoMigration(t, newWorker, oldWorkers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkCorrectnessTumblingWindowNoMigration(
	t *testing.T,
	newWorker *worker.Worker,
	oldWorkers []*worker.Worker,
) {
	log.Println("[E2E] Checking correctness of the results")

	if len(tumblingResults) != NUMWINDOWS {
		t.Errorf(
			"Expect %v tumbling windows, but got %v\n",
			NUMWINDOWS,
			len(tumblingResults),
		)
	}
	sum := 0
	for _, result := range tumblingResults {
		if result.V1 != EXPECTEDCOUNT {
			t.Errorf(
				"Counter in each window should be %v, but got %v\n",
				EXPECTEDCOUNT,
				result.V1,
			)
		}
		sum += int(result.V1)
	}

	if sum != (NUMKEYS * NUMTIMEBUCKETS) {
		t.Errorf(
			"Expect total count of all windows to be %v, but got %v\n",
			NUMKEYS*NUMTIMEBUCKETS,
			sum,
		)
	}

	if numDuplicatedWindows > 0 {
		t.Errorf(
			"Found %v duplicated windows in the results.\n",
			numDuplicatedWindows,
		)
	}

	// Check the new worker: there should be zero state
	newWorkerIter := newWorker.StateService.StateBackendImpl.GetIterator()
	stateCnt := 0
	for newWorkerIter.First(); newWorkerIter.Valid(); newWorkerIter.Next() {
		stateCnt++
	}
	if stateCnt != 0 {
		t.Errorf(
			"Expect new worker to have 0 state, but got %v\n",
			stateCnt,
		)
	}

	// The old workers should only have 1 state for the last ending key
	stateCnt = 0
	for _, w := range oldWorkers {
		iter := w.StateService.StateBackendImpl.GetIterator()
		for iter.First(); iter.Valid(); iter.Next() {
			stateCnt++
		}
	}
	if stateCnt != 1 {
		t.Errorf(
			"Expect old workers to have 1 state, but got %v\n",
			stateCnt,
		)
	}
}
