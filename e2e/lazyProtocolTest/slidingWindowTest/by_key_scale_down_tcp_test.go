package slidingWindowTest

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

func TestSlidingWindowLazyByKeyScaleDownTcp(t *testing.T) {

	// Check input
	// WINDOWSPAN must be divisible by SLIDE
	if WINDOWSPAN%SLIDE != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by SLIDE",
		)
		return
	}
	// TIMEBUCKETSPAN*NUMTIMEBUCKETS must be divisible by SLIDE
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%SLIDE != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by SLIDE",
		)
		return
	}
	// SLIDE must be divisible by TIMEBUCKETSPAN
	if SLIDE%TIMEBUCKETSPAN != 0 {
		t.Error(
			"SLIDE must be divisible by TIMEBUCKETSPAN",
		)
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "by-key"
	config.LazyByKeyStateCommAPIType = "tcp"

	numWorkers := 4
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		slidingWindowQuery,
		config,
	)

	// Wait for 10s before rescaling
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "window",
		TargetParallelism: 1,
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

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessSlidingWindow(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
