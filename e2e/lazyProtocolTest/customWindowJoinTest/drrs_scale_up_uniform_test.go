package customWindowJoinTest

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

func TestCustomWindowJoinDRRSScaleUpUniform(t *testing.T) {

	if Num_Buckets_Per_Period < 2 {
		t.Error("Num_Buckets_Per_Period must be >= 2")
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "drrs"
	config.PartitionPolicy = "uniform"
	numWorkers := 6
	client, workers, coordinator := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	time.Sleep(4 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "customWindowJoin",
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
	expectedWM := int64(Period_Span * Num_Period)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait to reach to the ending watermark
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t, coordinator, nil)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}
