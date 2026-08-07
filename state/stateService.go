package state

import (
	"sync"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/metric"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
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

	// [lazy protocol] Peer state service addresses: map from worker ID to
	// state service address
	PeerStateServiceMap map[uint16]string

	// PeerStateServiceMap can be concurrently accessed: (i) access it to query
	// remote state service address to build connection, (ii) to deal with
	// InitLazyReconfig step, we could insert more addresses for new tasks
	PeerStateServiceMapLock sync.Mutex

	// [Lazy-optimized] Wait group for background async bucket migration. When
	// there is a ongoing async bucket migration, we block any new state access
	// requests until the active migration is done
	AsyncMigrationWaitGroup sync.WaitGroup

	/**************************************************************************
			   		 Peer State Connections for lazy protocol
	**************************************************************************/

	// We have multiple types of the long-lived remote state access APIs. They
	// are maintained separately in the mapping: worker id -> gRPC connection

	// [lazy-basic] Connections for remote bucket migration
	BucketMigrationConn map[uint16]grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk]

	// [lazy-no-migration] Connections for remote read-only by keys
	ReadConn map[uint16]grpc.BidiStreamingClient[pb.ReadRequest, pb.ReadResponse]

	// [lazy-no-migration] Connections for remote write (overwrite)
	OverwriteConn map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response]

	// [lazy-no-migration] Connections for remote write (merge)
	MergeConn map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response]

	// [lazy-no-migration] Connections for remote delete
	DeleteConn map[uint16]grpc.BidiStreamingClient[pb.DeleteRequest, pb.Response]

	// [lazy-optimized] Connections for remote async bucket migration
	AsyncBucketMigrationConn map[uint16]grpc.BidiStreamingClient[pb.AsyncBucketMigrationRequest, pb.AsyncBucketMigrationResponse]

	/**************************************************************************
			   		 					DRRS
	**************************************************************************/

	// [DRRS] Bucket ownership transfer map: track the buckets that are being
	// migrated during a reconfiguration. If the bucket is not involved in the
	// migration, it will not appear in this map.
	// Map structure: map[bucketId] -> status of the bucket:
	// 0: pending and not yet migrated
	// 1: migrated
	BucketMigrationMap      map[uint64]int8
	MutexBucketMigrationMap sync.Mutex

	// Atomically updated by migration routines after a batch of buckets are
	// migrated. 0: no progress; 1: progressed. This is used by main routine
	// to decide if to scan the WaitBuffer for processable records
	MigrationProgressed int32

	// Remaining number of migration routines to complete. Used to determine
	// when the migration is fully done
	RemainingMigrationRoutines int32
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
		// Only used for DRRS
		BucketMigrationMap:      make(map[uint64]int8),
		MutexBucketMigrationMap: sync.Mutex{},
	}
}
