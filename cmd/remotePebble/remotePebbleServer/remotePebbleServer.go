package remotepebbleserver

import (
	"context"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"google.golang.org/grpc"
)

// RemotePebbleServer exposes a Pebble-backed state store over gRPC.
type RemotePebbleServer struct {
	pb.UnimplementedRemotePebbleServiceServer
	pebbleBackend *stateBackend.PebbleStateBackend

	// Read request key count, output every 5 seconds
	readReqKeyCount int64

	// Write request key count, output every 5 seconds
	writeReqKeyCount int64

	// Begin time of the current 5-second interval for logging request counts
	nextPrintTime int64
}

func NewRemotePebbleServer(
	backend *stateBackend.PebbleStateBackend,
) *RemotePebbleServer {
	nextPrintTime := time.Now().Add(5 * time.Second).UnixNano()
	return &RemotePebbleServer{pebbleBackend: backend, nextPrintTime: nextPrintTime}
}

func (s *RemotePebbleServer) Read(
	ctx context.Context,
	req *pb.PebbleReadRequest,
) (*pb.PebbleReadResponse, error) {
	_ = ctx
	start := time.Now()
	values := s.pebbleBackend.GetMany(req.Keys)
	readDuration := time.Since(start)
	atomic.AddInt64(&s.readReqKeyCount, int64(len(req.Keys)))
	s.checkPrintReqCounts()
	return &pb.PebbleReadResponse{Values: values, ReadTime: readDuration.Nanoseconds()}, nil
}

func (s *RemotePebbleServer) Write(
	ctx context.Context,
	req *pb.PebbleWriteRequest,
) (*pb.PebbleWriteResponse, error) {
	_ = ctx
	start := time.Now()
	if req.Merge {
		s.pebbleBackend.MergeMany(req.Keys, req.Values)
	} else {
		s.pebbleBackend.SetMany(req.Keys, req.Values)
	}
	writeDuration := time.Since(start)
	atomic.AddInt64(&s.writeReqKeyCount, int64(len(req.Keys)))
	s.checkPrintReqCounts()
	return &pb.PebbleWriteResponse{Info: "Success", WriteTime: writeDuration.Nanoseconds()}, nil
}

func (s *RemotePebbleServer) Delete(
	ctx context.Context,
	req *pb.PebbleDeleteRequest,
) (*pb.Response, error) {
	_ = ctx
	s.pebbleBackend.DeleteMany(req.Keys)
	return &pb.Response{Info: "Success"}, nil
}

func (s *RemotePebbleServer) RangeQuery(
	ctx context.Context,
	req *pb.PebbleRangeQueryRequest,
) (*pb.PebbleRangeQueryResponse, error) {
	_ = ctx
	keys, values := s.pebbleBackend.RangeQuery(req.Lower, req.Upper)
	return &pb.PebbleRangeQueryResponse{
		Keys:   keys,
		Values: values,
	}, nil
}

// ServeRemotePebbleServer starts a gRPC server for the provided backend.
func ServeRemotePebbleServer(
	listenAddr string,
	backend *stateBackend.PebbleStateBackend,
) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}

	var opts []grpc.ServerOption
	opts = append(opts,
		// gRPC send/recv buffer sizes
		grpc.WriteBufferSize(1<<18), // 1 MB
		grpc.ReadBufferSize(1<<18),  // 1 MB
		grpc.MaxRecvMsgSize(constant.RpcMaxMessageSize),
		grpc.MaxSendMsgSize(constant.RpcMaxMessageSize),
		// 🔹 HTTP/2 flow control (THIS reduces Send() blocking)
		grpc.InitialWindowSize(1<<18), // 2MB per stream
		grpc.InitialConnWindowSize(1<<18),
	)
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterRemotePebbleServiceServer(
		grpcServer,
		NewRemotePebbleServer(backend),
	)
	log.Printf("Remote pebble server listening on %s", listenAddr)
	grpcServer.Serve(lis)
}

func (s *RemotePebbleServer) checkPrintReqCounts() {
	now := time.Now().UnixNano()
	targetTime := atomic.LoadInt64(&s.nextPrintTime)

	if now >= targetTime {
		if atomic.CompareAndSwapInt64(&s.nextPrintTime, targetTime, now+int64(5*time.Second)) {
			reads := atomic.SwapInt64(&s.readReqKeyCount, 0)
			writes := atomic.SwapInt64(&s.writeReqKeyCount, 0)
			log.Printf("Metrics [inline 5s] - Read keys: %d, Write keys: %d", reads, writes)
		}
	}
}
