package testutils

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	remotepebbleserver "github.com/CASP-Systems-BU/disaggregated-streaming/cmd/remotePebble/remotePebbleServer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/coordinator"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
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
	cfg.BufferSize = 1
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

// StartRemotePebbleTestServers spins up pebble-backed gRPC servers for tests.
func StartRemotePebbleTestServers(
	numServers int,
) ([]string, []*stateBackend.PebbleStateBackend, func()) {

	addrs := make([]string, numServers)
	servers := make([]*grpc.Server, numServers)
	listeners := make([]net.Listener, numServers)
	backends := make([]*stateBackend.PebbleStateBackend, numServers)

	stateCommPortBase := 9101
	for i := range numServers {

		cfg := configuration.Default()
		cfg.StateCommPort = strconv.Itoa(stateCommPortBase + i)
		backends[i] = stateBackend.NewPebbleStateBackend(cfg)

		lis, err := net.Listen("tcp", "127.0.0.1:"+cfg.StateCommPort)
		if err != nil {
			log.Fatalf(
				"Failed to listen for remote pebble test server: %v",
				err,
			)
		}
		listeners[i] = lis

		server := grpc.NewServer(
			grpc.MaxRecvMsgSize(100*1024*1024),
			grpc.MaxSendMsgSize(100*1024*1024),
		)
		pb.RegisterRemotePebbleServiceServer(
			server,
			remotepebbleserver.NewRemotePebbleServer(backends[i]),
		)

		addrs[i] = lis.Addr().String()
		servers[i] = server
		go server.Serve(lis)
	}

	cleanupRemotePebbleServers := func() {
		CleanupRemotePebbleTestServers(servers, listeners, backends)
	}

	return addrs, backends, cleanupRemotePebbleServers
}

// CleanupRemotePebbleTestServers stops servers and closes backends.
func CleanupRemotePebbleTestServers(
	servers []*grpc.Server,
	listeners []net.Listener,
	backends []*stateBackend.PebbleStateBackend,
) {
	for i := range servers {
		if servers[i] != nil {
			servers[i].Stop()
		}
		if listeners[i] != nil {
			listeners[i].Close()
		}
		if backends[i] != nil {
			backends[i].Close()
		}
	}
	CleanUpDataFolder()
}

func DeployJob(
	numWorkers int,
	targetQuery func() *dataflow.Dataflow,
	config *configuration.Configuration,
) (pb.APIServiceClient, []*worker.Worker, *coordinator.Coordinator) {
	client, workers, coordinator := deployJob(
		numWorkers,
		targetQuery,
		config,
	)
	return client, workers, coordinator
}
func DeployJobWithoutCleanUp(
	numWorkers int,
	targetQuery func() *dataflow.Dataflow,
	config *configuration.Configuration,
) (pb.APIServiceClient, []*worker.Worker, *coordinator.Coordinator) {
	client, workers, coordinator := deployJob(
		numWorkers,
		targetQuery,
		config,
	)
	return client, workers, coordinator
}

func deployJob(
	numWorkers int,
	targetQuery func() *dataflow.Dataflow,
	config *configuration.Configuration,
) (pb.APIServiceClient, []*worker.Worker, *coordinator.Coordinator) {

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
