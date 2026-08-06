package tumblingWindowTest

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

func TestTumblingWindowDRRSScaleUpUniform(t *testing.T) {

	// Check input
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%WINDOWSPAN != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by WINDOWSPAN",
		)
		return
	}
	if (WINDOWSPAN % TIMEBUCKETSPAN) != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by TIMEBUCKETSPAN",
		)
		t.Errorf(
			"WINDOWSPAN: %v, TIMEBUCKETSPAN: %v, result %d",
			WINDOWSPAN,
			TIMEBUCKETSPAN,
			WINDOWSPAN%TIMEBUCKETSPAN,
		)
		return
	}
	if WINDOWSPAN <= TIMEBUCKETSPAN {
		t.Error(
			"WINDOWSPAN must be larger than TIMEBUCKETSPAN",
		)
		return
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "drrs"
	config.PartitionPolicy = "uniform"
	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return tumblingWindowQuery(2) },
		config,
	)

	time.Sleep(8 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "window",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(TIMEBUCKETSPAN * NUMTIMEBUCKETS)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessTumblingWindow(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
