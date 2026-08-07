package tumblingWindowTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// Test out-of-order records in tumbling window with lazy protocol

func TestOutOfOrderTumblingWindowLazy(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	tumblingResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"

	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, outOfOrderQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(
		1000*200*time.Millisecond+1*time.Second,
	) - int64(
		200*time.Millisecond,
	)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessOutOfOrderTumblingWindow(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
