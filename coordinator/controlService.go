package coordinator

import (
	"log"
	"net"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// ControlPlaneServer is for communication between the Coordinator and Workers
// Only 1 bi-directional RPC channel is used for all messages

type ControlPlaneServer struct {
	pb.UnimplementedCooridnatorServiceServer

	coordinator *Coordinator
}

func (c *Coordinator) StartControlPlaneService() {

	// Start listening on the control plane port

	lisAddr := c.Config.CoordinatorAddr
	controlPlaneServer := &ControlPlaneServer{coordinator: c}

	// Setup the gRPC server
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		log.Fatalf("Errow start Control plane server: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterCooridnatorServiceServer(grpcServer, controlPlaneServer)
	log.Printf(
		"Control plane service starts listening on %s ...\n",
		lisAddr,
	)
	grpcServer.Serve(lis)
}

/******************************************************************************
				   			gRPC API Implementations
******************************************************************************/

// Implement the API RegisterToCoordinator
func (s *ControlPlaneServer) RegisterToCoordinator(
	stream pb.CooridnatorService_RegisterToCoordinatorServer,
) error {

	// Receive the registration message from the worker
	in, err := stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive registration message: %v\n", err)
	}

	// The 1st message has to be the Registration type
	registrationMsg, ok := in.Message.(*pb.WorkerToCoordinator_RegistrationMsg)
	if !ok {
		log.Fatalf("1st message has to be the Registration type\n")
	}

	// Get the IP address of the Worker
	workerIP := GetWorkerIP(stream)

	// Get the data plane address of the Worker
	workerDataPlaneAddr := workerIP + ":" + registrationMsg.RegistrationMsg.DataPlanePort

	// Get the state comm address of the Worker
	workerStateCommAddr := workerIP + ":" + registrationMsg.RegistrationMsg.StateCommPort

	// Init a new worker and add it to the worker list
	newWorker := &ManagedWorker{
		Stream:        stream,
		DataPlaneAddr: workerDataPlaneAddr,
		StateCommAddr: workerStateCommAddr,
		IsAvailable:   true,
	}
	s.coordinator.WorkerManager.AddWorker(newWorker)

	// ACK the Worker registration
	newWorker.AckRegistration(newWorker.WorkerId, "Registration success")

	log.Printf(
		"New Worker registered with worker id %d\n",
		newWorker.WorkerId,
	)

	// Keep the connection alive
	// TODO: terminate the connection when the worker is removed
	select {}
}

/******************************************************************************
				   				Helper functions
******************************************************************************/

// Get the IP address from the Worker-to-Coordinator connection
func GetWorkerIP(
	stream grpc.BidiStreamingServer[pb.WorkerToCoordinator, pb.CoordinatorToWorker],
) string {

	var workerIP string
	var err error
	p, ok := peer.FromContext(stream.Context())
	if ok {
		workerIP, _, err = net.SplitHostPort(p.Addr.String())
		if err != nil {
			log.Fatalf("Failed to parse the Worker IP: %v\n", err)
		}
	} else {
		log.Fatalf("Failed to get the Worker IP\n")
	}

	return workerIP
}
