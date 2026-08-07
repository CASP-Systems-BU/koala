package worker

import (
	"log"
	"net"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/state"
	"google.golang.org/grpc"
)

// gRPC server for remote state access. This server accesses local state service

type StateServiceCommServer struct {
	pb.UnimplementedStateCommServiceServer

	// Pointer to the local state service
	stateService *state.StateService

	// Pointer to the outer worker
	worker *Worker
}

// State Comm Server routine
func (w *Worker) startStateCommService(stateService *state.StateService) {

	stateServer := &StateServiceCommServer{
		stateService: stateService,
		worker:       w,
	}

	// Get listening address
	lisAddr := "0.0.0.0:" + stateService.Config.StateCommPort

	// Setup the gRPC server
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		log.Fatalf("Error start State Comm server: %v", err)
	}

	// The default message size limit is 4 MB. Increase it to 100 MB for large
	// message size: lazy no-migration connections are based on single gRPC
	// message instead of StateChunk streaming
	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.MaxRecvMsgSize(500*1024*1024), // 100 MB
		grpc.MaxSendMsgSize(500*1024*1024), // 100 MB
	)

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterStateCommServiceServer(grpcServer, stateServer)
	log.Printf(
		"State comm service starts listening on %s ...\n",
		lisAddr,
	)
	grpcServer.Serve(lis)
}
