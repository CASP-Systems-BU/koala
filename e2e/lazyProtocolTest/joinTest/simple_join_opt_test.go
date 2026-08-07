package joinTest

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

func TestSimpleJoinRescaleLazyOpt(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "optimized"

	// We need extra empty workers for scale up
	numWorkers := SOURCE1_PARALLELISM + SOURCE2_PARALLELISM + 5
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Find which worker is the empty worker for scale up
	workerIdsForScaleup := make(map[uint16]struct{})
	for _, w := range workers {
		if w.AssignedTask == nil {
			// This is the empty worker
			workerIdsForScaleup[w.WorkerId] = struct{}{}
		}
	}

	// Wait for some time before rescaling
	time.Sleep(WAIT_TIME_BEFORE_RESCALE * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "join",
		TargetParallelism: 4,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be completed
	time.Sleep(WAIT_TIME_AFTER_RESCALE * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	checkCorrectness(t, workerIdsForScaleup, workers, coordinator)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
