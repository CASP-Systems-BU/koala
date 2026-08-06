package state

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/syncflag"
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

	/**************************************************************************
		   [lazy-by-key] Eventual migration for cancelling task
	**************************************************************************/

	// Affected buckets for eventual migration grouped by cancelling worker
	// map[cancelling worker ID] -> list of affected bucket IDs
	// There can be overlap in affected buckets across cancelling workers
	eventualMigrationAffectedBuckets map[uint16][]int64

	// Per-bucket RWLocks for concurrent key lookup table access during
	// eventual migration. Multiple goroutines can read-lock the same bucket
	// (scan for additional keys), while writes (lookup table updates) take
	// exclusive locks. map[bucket ID] -> lock
	eventualMigrationBucketLocks map[int64]*sync.RWMutex

	// Tracks which buckets have been migrated, so we can skip them in future
	// scans - only applied in gradual migration mode
	// map[cancelling worker ID] -> map[bucket ID]
	eventualMigrationFinishedBuckets map[uint16]map[int64]bool

	// Tracks which cancelling workers have already decremented
	// eventualMigrationRemainingWorkers. Prevents double decrement when
	// fetchAdditionalKeys is re-invoked for a completed worker in a later
	// batch (the worker remains in eventualMigrationAffectedBuckets).
	eventualMigrationDoneWorkers sync.Map // map[uint16]bool

	// Number of cancelling workers remaining. Decremented by each goroutine
	// when a cancelling worker is fully migrated. When reaching 0, signals
	// EventualMigrationDone.
	eventualMigrationRemainingWorkers atomic.Int32

	// Signaled when all affected are migrated from cancelling workers.
	// The control plane handler blocks on this before ACKing.
	EventualMigrationDone *syncflag.SyncFlag

	// Atomic flag: flipped to 1 to start eventual migration process
	EventualMigrationEnabled atomic.Int32
}

// StateService is initiated at NewWorker(): StateBackend and StateCommService
// are started immediately; WorkerID is set after registration with the
// coordinator; StateLookupTable and PeerStateServiceMap are set after task
// assignment under lazy protocol
func NewStateService(
	config *configuration.Configuration,
) *StateService {

	return &StateService{
		Config:                config,
		StateBackendImpl:      stateBackend.NewStateBackend(config),
		EventualMigrationDone: syncflag.NewSyncFlag(),
	}
}

// [eventual migration for cancelling task] Initialize eventual migration
// process on this worker:
// 1. Setup all affected buckets to be migrated
// 2. Initialize synchronization primitives for each bucket
// 3. Initialize tracking structures for migration progress
// 4. Flip the flag to enable eventual migration logic in the data path
func (s *StateService) SetEventualMigrationMeta(
	affectedBuckets []*pb.AffectedBucketInfo,
) {

	if len(affectedBuckets) == 0 {
		log.Fatalln("No affected buckets to be migrated")
	}

	// 1. Group affected buckets by cancelling worker ID
	// affected: map[cancelling worker ID] -> list of affected bucket IDs
	affected := make(map[uint16][]int64)
	for _, affectedBucket := range affectedBuckets {
		for _, wid := range affectedBucket.CancellingWorkerIds {
			affected[uint16(wid)] = append(
				affected[uint16(wid)],
				affectedBucket.BucketId,
			)
		}
	}
	s.eventualMigrationAffectedBuckets = affected

	// 2. Init per-bucket RWLocks for all affected buckets
	bucketLocks := make(map[int64]*sync.RWMutex, len(affectedBuckets))
	for _, affectedBucket := range affectedBuckets {
		bucketLocks[affectedBucket.BucketId] = &sync.RWMutex{}
	}
	s.eventualMigrationBucketLocks = bucketLocks

	// 3. Init tracking structures for migration progress
	finishedBuckets := make(map[uint16]map[int64]bool, len(affected))
	for wid := range affected {
		finishedBuckets[wid] = make(map[int64]bool)
	}
	s.eventualMigrationFinishedBuckets = finishedBuckets
	if s.eventualMigrationRemainingWorkers.Load() != 0 {
		log.Fatalf(
			"eventualMigrationRemainingWorkers is not 0, got %d",
			s.eventualMigrationRemainingWorkers.Load(),
		)
	}
	s.eventualMigrationDoneWorkers = sync.Map{}
	s.eventualMigrationRemainingWorkers.Store(int32(len(affected)))
	s.EventualMigrationDone.Reset()

	// Log eventual migration details
	log.Printf(
		"[Eventual Migration][Worker %d] Fetching buckets from cancelling workers:\n",
		s.WorkerID,
	)
	for cancellingWid, buckets := range affected {
		log.Printf(
			"  - [Worker %d] From cancelling worker %d, fetching %d affected buckets: %s\n",
			s.WorkerID,
			cancellingWid,
			len(buckets),
			formatBucketRanges(buckets),
		)
	}

	// 4. Enable eventual migration by flipping the flag
	s.EventualMigrationEnabled.Store(1)
}

// [eventual migration for cancelling task] Block until all affected buckets
// are migrated from all cancelling workers
func (s *StateService) WaitEventualMigrationDone() {
	s.EventualMigrationDone.Wait()
}

/******************************************************************************
				                      Utils
******************************************************************************/

// formatBucketRanges sorts bucket IDs and compacts consecutive runs into
// ranges, e.g. [1-4, 6, 9-11].
func formatBucketRanges(buckets []int64) string {
	sorted := make([]int64, len(buckets))
	copy(sorted, buckets)
	slices.Sort(sorted)

	var parts []string
	i := 0
	for i < len(sorted) {
		start := sorted[i]
		end := start
		for i+1 < len(sorted) && sorted[i+1] == end+1 {
			i++
			end = sorted[i]
		}
		if start == end {
			parts = append(parts, fmt.Sprintf("%d", start))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, end))
		}
		i++
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
