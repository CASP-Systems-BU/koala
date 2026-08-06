package worker

import (
	"log"
	"net"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state"
	"google.golang.org/grpc"
)

// Worker exposes state comm service for remote state access. We support 2 types
// of state comm APIs: (i) gRPC, and (ii) TCP. Note that TCP API only supports
// per-key state fetch for lazy-by-key protocol now

func (w *Worker) startStateCommService() {

	if w.Config.ReconfigProtocol == "lazy" &&
		w.Config.LazyProtocolVersion == "by-key" &&
		w.Config.LazyByKeyStateCommAPIType == "tcp" {
		w.startStateCommTcpServer()
	} else {
		w.startStateCommRpcServer()
	}
}

/******************************************************************************
								   gRPC Server
******************************************************************************/

type StateCommRpcServer struct {
	pb.UnimplementedStateCommServiceServer

	// Pointer to the local state service
	stateService *state.StateService

	// Pointer to the outer worker
	worker *Worker
}

// State Comm Server routine
func (w *Worker) startStateCommRpcServer() {

	stateServer := &StateCommRpcServer{
		stateService: w.StateService,
		worker:       w,
	}

	// Get listening address
	lisAddr := "0.0.0.0:" + w.StateService.Config.StateCommPort

	// Setup the gRPC server
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		log.Fatalf("Error start State Comm server: %v", err)
	}

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.MaxRecvMsgSize(constant.RpcMaxMessageSize),
		grpc.MaxSendMsgSize(constant.RpcMaxMessageSize),
	)

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterStateCommServiceServer(grpcServer, stateServer)
	log.Printf(
		"State comm gRPC service starts listening on %s ...\n",
		lisAddr,
	)
	grpcServer.Serve(lis)
}

/******************************************************************************
								    TCP Server
******************************************************************************/

func (w *Worker) startStateCommTcpServer() {

	// Start listening on the state comm port
	listener, err := net.Listen("tcp", ":"+w.StateService.Config.StateCommPort)
	if err != nil {
		log.Fatalf("Error starting state comm TCP server: %v", err)
	}
	defer listener.Close()
	log.Printf(
		"State comm TCP service starts listening on port %s ...\n",
		w.StateService.Config.StateCommPort,
	)

	for {
		// Accept a connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error accepting state comm TCP connection: %v", err)
		}
		go w.handleStateCommTcpConnection(conn)
	}
}
