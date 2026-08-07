package testutils

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/coordinator"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/********************************************
 * Helper functions for e2e testing
 ********************************************/

// Starts a new worker
func StartWorker(
	dataPort string,
	statePort string,
	query *dataflow.Dataflow,
	config *configuration.Configuration,
) *worker.Worker {
	cfg := configuration.DeepCopyConfig(config)
	cfg.DataPlanePort = dataPort
	cfg.StateCommPort = statePort
	w := worker.NewWorker(cfg, query)
	go w.Run()
	return w
}

// Starts a new coordinator
func StartCoordinator(
	query *dataflow.Dataflow,
	config *configuration.Configuration,
) *coordinator.Coordinator {
	cfg := configuration.DeepCopyConfig(config)
	c := coordinator.NewCoordinator(cfg, query)
	go c.Run()
	return c
}

// Clean up data folder
func CleanUpDataFolder() {
	os.RemoveAll("./data")
}

func DeployJob(
	numWorkers int,
	targetQuery func() *dataflow.Dataflow,
	config *configuration.Configuration,
) (pb.APIServiceClient, []*worker.Worker, *coordinator.Coordinator) {
	CleanUpDataFolder()

	// Starts the coordinator
	coordinator := StartCoordinator(targetQuery(), config)

	// Starts the workers
	// NOTE: we are creating new query for each worker
	//       because the stateful mappers are not supposed to be shared
	port := 9001
	workers := make([]*worker.Worker, numWorkers)
	for i := range numWorkers {
		worker := StartWorker(
			strconv.Itoa(port),
			strconv.Itoa(port+1),
			targetQuery(),
			config,
		)
		workers[i] = worker
		port = port + 2
	}
	// Wait for the workers to be ready
	time.Sleep(1 * time.Second)
	log.Println("[E2E] Workers started")

	// Connect to the coordinator API service
	conn, err := grpc.NewClient(
		configuration.Default().CoordinatorAPIAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the Coordinator API service: %v", err)
	}
	client := pb.NewAPIServiceClient(conn)

	// Deploy the job
	jobConfig := &pb.JobConfig{}
	log.Println("[E2E] Deploying job")
	resp, err := client.RunJob(context.Background(), jobConfig)
	if err != nil {
		log.Fatalf("Failed to deploy the job: %v", err)
	}

	log.Println("[E2E] Job deployed" + resp.Info)
	return client, workers, coordinator
}

// Periodically monitor the watermark progress at the sink
func MonitorEndOfTest(
	sink dataflow.Operator,
	done chan struct{},
	targetTimestamp int64,
) {

	for {
		wm := sink.GetTaskWatermarkForTesting()
		log.Println(
			"[E2E] Termination checker - Sink current watermark: ",
			wm.Timestamp,
			" termination watermark: ",
			targetTimestamp,
		)
		// Test ends when sink watermark progresses to target timestamp
		if wm.Timestamp == targetTimestamp {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Signal the end of the test
	done <- struct{}{}
}
