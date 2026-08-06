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

func TestMultiUpAndDownLazyEventual(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "by-key"
	config.LazyByKeyStateCommAPIType = "tcp"
	config.LazyByKeyCancellingTaskMigrationMode = "eventual"
	config.LazyByKeyGradualMigrationBatchSize = -1

	numWorkers := 7
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Scale up
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

	// Scale down
	time.Sleep(6 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 1,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("2nd job rescale response: %v\n", resp.Info)

	// Scale up again
	time.Sleep(6 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 2,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("3rd job rescale response: %v\n", resp.Info)

	// Scale up again
	time.Sleep(6 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 3,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("4th job rescale response: %v\n", resp.Info)

	// Scale down again
	time.Sleep(6 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 2,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("5th job rescale response: %v\n", resp.Info)

	// Scale down again
	time.Sleep(6 * time.Second)
	rescaleConfig = &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 1,
	}
	resp, err = client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("6th job rescale response: %v\n", resp.Info)

	// Wait for the test to be completed
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
