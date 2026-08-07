package utils

import (
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/metric"
	"github.com/CASP-Systems-BU/koala/state"
)

// Parameters passed in for operator runtime setup based on Coordinator task
// assignment request. A subset of parameters are required for different
// operators
type OperatorSetupParas struct {

	// Global configuration
	Config *configuration.Configuration

	// Name of current operator
	OperatorName string

	// Worker ID assigned by Coordinator
	WorkerID uint16

	// Worker state service impl for stateful operators
	StateService *state.StateService

	// Followings are runtime parameters from Coordinator

	// List of downstream task data plane addresses (IP:port)
	DownstreamInfoList []*pb.DownstreamInfo

	// Number of expected upstreams at initial task deployment phase
	// This is used to block the progress of TaskWatermark.
	// This considers upstream connections from all SubSuppliers
	ExpectNumUpstream int

	// KeybyCollector Routing table (only apply for operators with keyed output)
	KeybyCollectorRoutingTable *keyby.KeyLookupTable

	// If the operator is a source operator, the coordinator will supply
	// the replica id for the source operator. Each source operator's id is
	// unique and select from [0, #source).
	SourceReplicaID int64

	// Pointer to the metric collector. This is used to setup Collector where
	// it needs to update output related metrics to the MetricCollector
	MetricCollector *metric.MetricCollector

	// [lazy protocol] Expected number of in-flight barriers to reach alignment
	ExpectedDrainBarriers *int32

	// [lazy protocol] Expected number inbound peer connections
	ExpectedInboundPeers *int32
}
