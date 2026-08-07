package tumblingWindowTest

import (
	"log"
	"testing"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// TestTumblingWindowLazy is an end-to-end test that validates the behavior of a
// window under lazy protocol. The test generates 10000 records and grouped into
// 2000 windows, with each window containing 5 records.

func TestTumblingWindowLazy(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	tumblingResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		tumblingWindowQuery,
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
	expectedWM := int64(100000)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessTumblingWindow(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
