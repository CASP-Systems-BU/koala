package keybyCollectorTest

import (
	"log"
	"testing"
	"time"

	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

func TestKeyByCollectorUniformPartition(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.PartitionPolicy = "uniform"
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
