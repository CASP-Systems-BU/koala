package keybyCollectorTest

import (
	"log"
	"testing"
	"time"

	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

func TestKeyByCollectorEvenPartition(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.PartitionPolicy = "consistent-even"
	numWorkers := 5
	_, workers, coordinator := testutils.DeployJob(numWorkers, query, config)

	// wait for test to complete
	time.Sleep(5 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	VerifyKeyByResults(t, workers, coordinator)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
