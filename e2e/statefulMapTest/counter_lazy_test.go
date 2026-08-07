package statefulMapTest

import (
	"log"
	"testing"
	"time"

	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// TestCounterLazy tests the StatefulMap operator uner lazy protocol
// 1. Each target key (0 to 99) is correctly counted 100 times.
// 2. The stateful mappers correctly maintain and update state.
// 3. The keys are processed in order, and the total number of keys matches the
// expectation.

func TestCounterLazy(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, query, config)

	// Wait for the test to be completed
	time.Sleep(5 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
