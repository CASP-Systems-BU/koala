package slidingWindowTest

import (
	"log"
	"testing"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// TestSlidingWindow tests the sliding window operator under lazy protocol
// In this e2e test we create a sliding window of 500ns duration and 100ns slide
// We emit 500 events for each key in the range [0, 10) with a timestamp
// interval of 1ns

func TestSlidingWindowCounterLazy(t *testing.T) {

	slidingResults = make(map[int64][]IntTuple2)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	numWorkers := 4
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"

	_, workers, _ := testutils.DeployJob(
		numWorkers,
		slidingWindowCounterQuery,
		config,
	)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	go testutils.MonitorEndOfTest(sink, done, END_WATERMARK)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
