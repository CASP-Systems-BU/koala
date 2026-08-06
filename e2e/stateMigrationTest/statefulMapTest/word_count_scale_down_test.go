package statefulMapTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
)

// Test stop-and-restart scale-down with consistent hashing partitioning

func TestStateMigrationWordCountScaleDown(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()

	numWorkers := 4
	// client, workers, coordinator := testutils.DeployJob(
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Wait for 10s before scale down
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 1,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(40 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/
	checkCorrectness(t, 0, workers, coordinator, "consistent-hashing", false)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}
