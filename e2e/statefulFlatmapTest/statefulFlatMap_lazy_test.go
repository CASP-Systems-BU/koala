package statefulFlatmapTest

import (
	"log"
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// In this test, we define a dataflow to count the number of occurrences of
// each word in a stream of sentences under lazy protocol

func TestWordCountKeyLazy(t *testing.T) {
	result = make(map[string]int)
	resultCount = make(map[string]int64)
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	numWorkers := 3
	_, workers, _ := testutils.DeployJob(numWorkers, wordCountQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	// This flatmap query doesn't need watermark and timestamp assigner
	// We add them here only to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(10_000_000)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t, result, resultCount)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
