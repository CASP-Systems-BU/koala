package coordinator

import (
	"context"
	"log"
	"net"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// APIServer is for communication between the Coordinator and Clients
// All APIs are unary RPCs

type APIServer struct {
	pb.UnimplementedAPIServiceServer

	Coordinator *Coordinator
}

func (c *Coordinator) StartAPIService() {

	// Start listening on the API port
	lisAddr := c.Config.CoordinatorAPIAddr
	apiServer := &APIServer{Coordinator: c}

	// Setup the gRPC server
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		log.Fatalf("Error start API server: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterAPIServiceServer(grpcServer, apiServer)
	log.Printf(
		"API service starts listening on %s ...\n",
		lisAddr,
	)
	grpcServer.Serve(lis)
}

/******************************************************************************
				   			gRPC API Implementations
******************************************************************************/

// Deploy and start the job. Returns only after the job is started.
func (s *APIServer) RunJob(
	ctx context.Context,
	jobConfig *pb.JobConfig,
) (*pb.Response, error) {

	// Get total number of tasks to be deployed
	numTasks := s.Coordinator.Dataflow.GetTotalNumTasks()
	numWorkers := s.Coordinator.WorkerManager.GetNumWorkers()

	// Check if there are enough workers to deploy the job
	if numTasks > numWorkers {
		return nil, status.Errorf(
			codes.Internal,
			"Number of tasks exceeds the number of workers, numTasks=%d, numWorkers=%d",
			numTasks,
			numWorkers,
		)
	}

	// Generate a task placement plan
	s.Coordinator.GetTaskPlacementPlan()

	// Generate key partitions for stateful operators
	s.Coordinator.InitKeyPartitions()

	// Deploy the tasks to workers
	s.Coordinator.StartJob()

	return &pb.Response{
		Info: "Job started",
	}, nil
}

// Re-scale the job. Returns only after the job is re-scaled.
func (s *APIServer) Rescale(
	ctx context.Context,
	rescaleConfig *pb.RescaleConfig,
) (*pb.Response, error) {

	// Reject if there is an ongoing reconfiguration
	if !s.Coordinator.ReconfigInProgress.CompareAndSwap(false, true) {
		return nil, status.Errorf(
			codes.Internal,
			"Another reconfiguration is in progress, please retry later",
		)
	}
	defer s.Coordinator.ReconfigInProgress.Store(false)

	// Check if there is deployed job by checking the task placement plan
	if s.Coordinator.TaskPlacementPlan == nil {
		return nil, status.Errorf(
			codes.Internal,
			"No deployed job to rescale",
		)
	}

	switch s.Coordinator.Config.ReconfigProtocol {
	case "stop-and-restart":
		s.rescaleStopAndRestart(rescaleConfig)
	case "lazy":
		s.rescaleLazy(rescaleConfig)
	default:
		return nil, status.Errorf(
			codes.Internal,
			"Unsupported reconfiguration protocol: %s",
			s.Coordinator.Config.ReconfigProtocol,
		)
	}

	return &pb.Response{
		Info: "Job re-scaled",
	}, nil
}

/******************************************************************************
				   		  Protocol specific utils
******************************************************************************/

func (s *APIServer) rescaleStopAndRestart(rescaleConfig *pb.RescaleConfig) {

	// Get updated worker list and prepare the reconfiguration
	updatedWorkers, newWorkers, removedWorkers, targetOperator := s.prepareReconfiguration(
		rescaleConfig,
	)

	// Drain in-flight data for stop and restart protocol
	upstreamTasks := s.drainInflightData(targetOperator)

	// Apply key space re-partition and state migration for stateful operators
	if targetOperator.IsStatefulOperator() {
		s.reconfigureStatefulOp(targetOperator, updatedWorkers)
	}

	// Deploy new tasks for scale-up and remove old tasks for scale-down
	s.rescaleTasksStopAndRestart(
		targetOperator,
		updatedWorkers,
		newWorkers,
		removedWorkers,
		int32(len(upstreamTasks)),
	)

	// Notify all upstream tasks to update downstream routing
	s.notifyUpstreamTasks(
		targetOperator,
		newWorkers,
		removedWorkers,
		upstreamTasks,
	)

	// Resume the upstreams
	s.resumeUpstreams(upstreamTasks)
}

func (s *APIServer) rescaleLazy(rescaleConfig *pb.RescaleConfig) {

	// Step 0: Get updated worker list and prepare the reconfiguration
	updatedWorkers, newWorkers, removedWorkers, targetOperator := s.prepareReconfiguration(
		rescaleConfig,
	)

	// Now we only allow rescaling stateful operators under lazy protocol
	// TODO: support it for stateless operator is easy, add it later
	if !targetOperator.IsStatefulOperator() {
		log.Fatalln(
			"Lazy reconfiguration protocol is only supported for stateful operators currently",
		)
	}

	// Step 1: Start new tasks if needed and notify all target tasks required
	// metadata for reconfiguration
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	upstreamTasks, bucketOwnerChanges, receiverWorkers := s.initializeTargetTasksLazy(
		targetOperator,
		updatedWorkers,
		newWorkers,
		removedWorkers,
	)

	// Step 2: Notify all sender tasks to start fast forwarding
	s.fastForward(targetOperator, bucketOwnerChanges)

	// Step 3: Wait all receiver tasks to successfully receive all expected
	// inbound peer connections - for watermark progress safety
	s.waitAllInboundPeersToConnect(receiverWorkers)

	// Step 4: Update upstream routing and broadcast inflight barriers
	s.notifyUpstreamTasks(
		targetOperator,
		newWorkers,
		removedWorkers,
		upstreamTasks,
	)

	// Step 5: Wait for termination signal from all target tasks
	s.waitAllTasksReconfigDone(updatedWorkers, removedWorkers)

	// Step 6: Stop removed tasks and update task placement plan in coordinator
	s.terminateTasks(targetOperator, updatedWorkers, removedWorkers)
}
