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

func TestScaleUpAndDownLazy(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "by-key"

	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Wait for 8s before 1st scale-up
	time.Sleep(8 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("1st job rescale response: %v\n", resp.Info)

	// Wait for 18s before scale-down
	time.Sleep(18 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 2,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("2nd job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(30 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/

	checkCorrectness(t, workers)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}
