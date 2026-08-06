package state

import (
	"sync"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/grpc"
)

// State Service: Disaggregated State Management

type StateService struct {
	Config *configuration.Configuration

	// Local state backend implementation (e.g. memory, pebble, TiKV)
	StateBackendImpl stateBackend.StateBackend

	// OperatorID that this StateService instance is in charge of. Now a
	// StateService instance only serves one operator.
	OperatorID uint16

	MetricCollector *metric.MetricCollector

	// [lazy protocol] Local worker ID
	WorkerID uint16

	// [lazy protocol] State Lookup Table: lookup physical address of the state
	StateLookupTable *keyby.KeyLookupTable

	// [lazy-by-key] Per-key based state lookup table
	StateLookupTableV2 *keyby.KeyLookupTableV2

	// [lazy-opt] Wait group for background async bucket migration. When there
	// is a ongoing async bucket migration, we block any new state access
	// requests until the active migration is done
	AsyncMigrationWaitGroup sync.WaitGroup

	// [lazy-by-key] Wait group for background key flush and key lookup table
	// update. When there is a ongoing background key flush and lookup table
	// update, we block any new state access requests
	ByKeyMigrationWaitGroup sync.WaitGroup

	// [lazy-by-key] Wait group for background key lookup table update
	ByKeyLookupTableUpdateWaitGroup sync.WaitGroup

	/**************************************************************************
			   		 Peer State Connections for lazy protocol
	**************************************************************************/

	// We have multiple types of the long-lived remote state access APIs. They
	// are maintained separately in the mapping: worker id -> gRPC connection

	// [lazy-basic] Connections for remote bucket migration
	BucketMigrationConn     map[uint16]grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk]
	BucketMigrationConnLock sync.Mutex

	// [lazy-no-migration] Connections for remote read-only by keys
	ReadConn     map[uint16]grpc.BidiStreamingClient[pb.ReadRequest, pb.ReadResponse]
	ReadConnLock sync.Mutex

	// [lazy-no-migration] Connections for remote write (overwrite)
	OverwriteConn     map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response]
	OverwriteConnLock sync.Mutex

	// [lazy-no-migration] Connections for remote write (merge)
	MergeConn     map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response]
	MergeConnLock sync.Mutex

	// [lazy-no-migration] Connections for remote delete
	DeleteConn     map[uint16]grpc.BidiStreamingClient[pb.DeleteRequest, pb.Response]
	DeleteConnLock sync.Mutex

	// [lazy-optimized] Connections for remote async bucket migration
	AsyncBucketMigrationConn     map[uint16]grpc.BidiStreamingClient[pb.LazyOptStateRequest, pb.LazyOptStateResponse]
	AsyncBucketMigrationConnLock sync.Mutex

	// [lazy-optimized] Routine pool for async state flush
	LazyOptStateFlushPool *ants.Pool

	// [lazy-by-key] Connections for remote per-key based state migration (RPC)
	ByKeyMigrationConn     map[uint16]grpc.BidiStreamingClient[pb.LazyByKeyStateRequest, pb.KeyResponse]
	ByKeyMigrationConnLock sync.Mutex

	// [lazy-by-key] Connections for remote per-key based state migration (TCP)
	ByKeyMigrationTcpConn     map[uint16]*LazyByKeyTcpConn
	ByKeyMigrationTcpConnLock sync.Mutex
}

// StateService is initiated at NewWorker(): StateBackend and StateCommService
// are started immediately; WorkerID is set after registration with the
// coordinator; StateLookupTable and PeerStateServiceMap are set after task
// assignment under lazy protocol
func NewStateService(
	config *configuration.Configuration,
) *StateService {

	return &StateService{
		Config:           config,
		StateBackendImpl: stateBackend.NewStateBackend(config),
	}
}
