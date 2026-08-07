package statefulMapTest

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
)

// Test stop-and-restart scale-down with uniform hashing partitioning

func TestStateMigrationWordCountScaleDownUniform(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.PartitionPolicy = "uniform"

	numWorkers := 5
	// client, workers, coordinator := testutils.DeployJob(
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(3) },
		config,
	)

	// Wait for 10s before scale down
	time.Sleep(6 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 2,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be completed
	time.Sleep(10 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/
	checkCorrectness(t, 0, workers, coordinator, "uniform", false)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}
