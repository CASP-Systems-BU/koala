package slidingWindowTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
)

func TestSlidingWindowLazyNoMigration(t *testing.T) {

	// Check input
	// WINDOWSPAN must be divisible by SLIDE
	if WINDOWSPAN%SLIDE != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by SLIDE",
		)
		return
	}
	// TIMEBUCKETSPAN*NUMTIMEBUCKETS must be divisible by SLIDE
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%SLIDE != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by SLIDE",
		)
		return
	}
	// SLIDE must be divisible by TIMEBUCKETSPAN
	if SLIDE%TIMEBUCKETSPAN != 0 {
		t.Error(
			"SLIDE must be divisible by TIMEBUCKETSPAN",
		)
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
		slidingWindowQuery,
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

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

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

	log.Println("[E2E] Checking correctness of the results")

	if len(slidingWindowResults) != NUMWINDOWS {
		t.Errorf(
			"Expect %v results at sink, but got %v\n",
			NUMWINDOWS,
			len(slidingWindowResults),
		)
	}

	for _, result := range slidingWindowResults {
		if result.V1 != EXPECTEDCOUNT {
			t.Errorf(
				"Counter in each window should be %v, but got %v\n",
				EXPECTEDCOUNT,
				result.V1,
			)
		}
	}

	if numDuplicatedWindows > 0 {
		t.Errorf(
			"Found %v duplicated windows in the results.\n",
			numDuplicatedWindows,
		)
	}

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

	// At the end, most panes should be cleared except the ones from the last
	// sliding window duration. Only the old workers should have state.
	numActivePanes := 0
	for _, w := range oldWorkers {
		stateIter := w.StateService.StateBackendImpl.GetIterator()
		for stateIter.First(); stateIter.Valid(); stateIter.Next() {
			numActivePanes++
		}
	}

	// Remaining panes should be (num_keys * (num_slide - 1) + 1) for the last
	// triggerring watermark
	expectedActivePanes := (NUMKEYS * (WINDOWSPAN/SLIDE - 1)) + 1
	if numActivePanes != expectedActivePanes {
		t.Errorf(
			"Expect only %v active panes, but got %v\n",
			expectedActivePanes,
			numActivePanes,
		)
	}
}
