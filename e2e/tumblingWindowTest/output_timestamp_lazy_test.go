package tumblingWindowTest

import (
	"log"
	"testing"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

func TestTumblingWindowOutputTimestampLazy(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})
	tumblingResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	numWorkers := 3
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		simpleTumblingWindowQuery,
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

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	/**************************************************************************
								CHECK CORRECTNESS
	**************************************************************************/

	checkCorrectnessOutputTimestamp(t)

	/**************************************************************************
										CLEAN UP
	**************************************************************************/
	testutils.CleanUpDataFolder()
}
