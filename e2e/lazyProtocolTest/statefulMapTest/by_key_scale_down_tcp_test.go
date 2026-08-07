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

func TestLazyByKeyWordCountScaleDownTcp(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "by-key"
	config.LazyByKeyStateCommAPIType = "tcp"

	numWorkers := 4
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Wait for 10s before rescaling
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
	time.Sleep(25 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/
	checkCorrectness(t, 0, workers, coordinator, false)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}
