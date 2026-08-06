package worker

import (
	"context"
	"log"
	"sync"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker/stateCommUtil"
	"github.com/panjf2000/ants/v2"
)

// [Stop-and-restart] API for state migration. Multiple workers can call this
// api concurrently to pull state. This is a directional connection which is
// only maintained during state migration
func (s *StateCommRpcServer) PullStatePartition(
	targetBuckets *pb.BucketsToBePulled,
	stream pb.StateCommService_PullStatePartitionServer,
) error {

	// Only allow state migration if local state backend is memory or pebble
	switch s.stateService.StateBackendImpl.(type) {
	case *stateBackend.MemoryStateBackend, *stateBackend.PebbleStateBackend:
	default:
		log.Fatalln("Unsupported state service type for PullStatePartition()")
	}

	// List of target buckets to be pulled: [[lowBucketIdx, upBucketIdx], ...]
	bucketRanges := targetBuckets.BucketRanges

	// State migration chunk size
	maxChunkSize := s.stateService.Config.StateMigrationChunkSize
	stateChunkSender := stateCommUtil.NewStateChunkSender(
		stream,
		maxChunkSize,
		stateCommUtil.NewStateChunkMsg,
	)

	// Migrate state for all target buckets
	for _, bucketIdx := range bucketRanges {

		// Get all states for the requested bucket range
		resKeys, resValues := s.stateService.ReadLocalBucketRange(
			bucketIdx.LowerBucketIdx,
			bucketIdx.UpperBucketIdx,
		)

		// Send states in chunks
		stateChunkSender.Send(resKeys, resValues)
	}

	// Flush the remaining states in the buffer and notify end of stream
	stateChunkSender.Flush()

	// Close the stream
	return nil
}

// [Stop-and-restart] Migrate task in-memory metadata e.g. timer, active window
func (s *StateCommRpcServer) MigrateTaskMetadata(
	ctx context.Context,
	targetBuckets *pb.BucketsToBePulled,
) (*pb.TaskMetadata, error) {

	// Extract all affected buckets from the request
	affectedBuckets := make(map[uint64]struct{})
	for _, bucketRange := range targetBuckets.BucketRanges {
		for bucketIdx := bucketRange.LowerBucketIdx; bucketIdx <= bucketRange.UpperBucketIdx; bucketIdx++ {
			affectedBuckets[uint64(bucketIdx)] = struct{}{}
		}
	}

	return &pb.TaskMetadata{
		Metadata: s.worker.AssignedTask.FetchMetadataForMigration(
			affectedBuckets,
		),
	}, nil
}

// [Lazy-basic] Migrate requested state buckets
func (s *StateCommRpcServer) RemoteBucketMigration(
	stream pb.StateCommService_RemoteBucketMigrationServer,
) error {

	maxChunkSize := s.stateService.Config.StateMigrationChunkSize
	stateChunkSender := stateCommUtil.NewStateChunkSender(
		stream,
		maxChunkSize,
		stateCommUtil.NewStateChunkMsg,
	)

	for {

		// Receive a remote state fetch request
		stateFetchRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote state fetch request: %v", err)
		}

		// [Validation] Requested operator ID must match the local operator ID
		if stateFetchRequest.OperatorId != uint32(s.stateService.OperatorID) {
			log.Fatalf(
				"Invalid operator ID: %d, expected: %d",
				stateFetchRequest.OperatorId,
				s.stateService.OperatorID,
			)
		}

		// Migrate state for all requested buckets
		for _, bucketIdx := range stateFetchRequest.BucketIds {

			// [Validation] Requested bucket must belong to the local worker
			if s.stateService.StateLookupTable.BucketIdxToWorkerID(
				bucketIdx,
			) != s.stateService.WorkerID {
				log.Fatalf(
					"Requested bucket %d does not belong to the local worker %d",
					bucketIdx,
					s.stateService.WorkerID,
				)
			}

			// Get all states for a requested bucket
			resKeys, resValues := s.stateService.ReadLocalBucketRange(
				bucketIdx,
				bucketIdx,
			)

			// Send states in chunks
			stateChunkSender.Send(resKeys, resValues)

			// Current bucket is successfully migrated, update the ownership
			s.stateService.StateLookupTable.ChangeBucketOwner(
				bucketIdx,
				uint16(stateFetchRequest.SourceWorkerId),
			)
		}

		// Flush the remaining states in the buffer and notify end of stream
		stateChunkSender.Flush()
	}
}

// [Lazy-no-migration] Remote state read without migration.
func (s *StateCommRpcServer) RemoteRead(
	stream pb.StateCommService_RemoteReadServer,
) error {

	for {
		// Receive a remote state read request
		readRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote read request: %v", err)
		}

		// Read values from local state backend
		values := s.stateService.StateBackendImpl.GetMany(readRequest.Keys)

		// Send the response
		err = stream.Send(&pb.ReadResponse{
			Values: values,
		})
		if err != nil {
			log.Fatalf("Failed to send remote read response: %v", err)
		}
	}
}

// [Lazy-no-migration] Remote state write (overwrite)
func (s *StateCommRpcServer) RemoteOverwrite(
	stream pb.StateCommService_RemoteOverwriteServer,
) error {

	for {
		// Receive a remote state overwrite request
		overwriteRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote overwrite request: %v", err)
		}

		// Overwrite values to local state backend
		s.stateService.StateBackendImpl.SetMany(
			overwriteRequest.Keys,
			overwriteRequest.Values,
		)

		// Send the response
		err = stream.Send(&pb.Response{
			Info: "Success",
		})
		if err != nil {
			log.Fatalf("Failed to send remote overwrite response: %v", err)
		}
	}
}

// [Lazy-no-migration] Remote state write (merge)
func (s *StateCommRpcServer) RemoteMerge(
	stream pb.StateCommService_RemoteMergeServer,
) error {

	for {
		// Receive a remote state merge request
		mergeRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote merge request: %v", err)
		}

		// Merge values to local state backend
		s.stateService.StateBackendImpl.MergeMany(
			mergeRequest.Keys,
			mergeRequest.Values,
		)

		// Send the response
		err = stream.Send(&pb.Response{
			Info: "Success",
		})
		if err != nil {
			log.Fatalf("Failed to send remote merge response: %v", err)
		}
	}
}

// [Lazy-no-migration] Remote state delete
func (s *StateCommRpcServer) RemoteDelete(
	stream pb.StateCommService_RemoteDeleteServer,
) error {

	for {
		// Receive a remote state delete request
		deleteRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote delete request: %v", err)
		}

		// Delete keys from local state backend
		s.stateService.StateBackendImpl.DeleteMany(deleteRequest.Keys)

		// Send the response
		err = stream.Send(&pb.Response{
			Info: "Success",
		})
		if err != nil {
			log.Fatalf("Failed to send remote delete response: %v", err)
		}
	}
}

// [Lazy-optimized] Remote state read with async bucket migration
func (s *StateCommRpcServer) RemoteAsyncBucketMigration(
	stream pb.StateCommService_RemoteAsyncBucketMigrationServer,
) error {

	// Init utils for async bucket migration
	maxChunkSize := s.stateService.Config.LazyOptBucketMigrationChunkSize
	stateChunkSender := stateCommUtil.NewStateChunkSender(
		stream,
		maxChunkSize,
		stateCommUtil.NewAsyncBucketMigrationResMsg,
	)

	// Init utils for deduplication of migrated keys. After we transfer the
	// required keys, we should exclude these keys in the following bucket
	// migration process to avoid redundant data transfer. This map should be
	// reset after each flush of the async migration process.
	transferredKeys := make(map[string]struct{})

	// Init routine pool for executing state read
	stateReadRoutinePool, err := ants.NewPool(
		s.stateService.Config.StateReadRoutinePoolSize,
	)
	if err != nil {
		log.Fatalf("Failed to create state read routine pool: %v", err)
	}

	// Lock for concurrent bucket send - stateChunkSender is not thread-safe
	var bucketSendLock sync.Mutex

	for {
		req, err := stream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed to receive remote async bucket migration request: %v",
				err,
			)
		}

		// [Validation] Requested operator ID must match the local operator
		if req.OperatorId != uint32(s.stateService.OperatorID) {
			log.Fatalf(
				"Invalid operator ID: %d, expected: %d",
				req.OperatorId,
				s.stateService.OperatorID,
			)
		}

		/**********************************************************************
							  Stage 1: respond needed keys
		**********************************************************************/

		// Construct the return value lists
		var stateValueLists []*pb.StateValueList
		for _, stateKeyList := range req.StateKeyLists {
			stateValueLists = append(stateValueLists, &pb.StateValueList{
				StateId: stateKeyList.StateId,
			})
		}

		var wg sync.WaitGroup
		for i, stateKeyList := range req.StateKeyLists {
			wg.Add(1)
			stateReadRoutinePool.Submit(func() {
				defer wg.Done()
				stateValueLists[i].Values = s.stateService.StateBackendImpl.GetMany(
					stateKeyList.Keys,
				)
			})
		}
		wg.Wait()

		// Return the fetched key values
		err = stream.Send(&pb.LazyOptStateResponse{
			Message: &pb.LazyOptStateResponse_KeyResponse{
				KeyResponse: &pb.KeyResponse{
					StateValueLists: stateValueLists,
				},
			},
		})
		if err != nil {
			log.Fatalf(
				"Failed to send async key fetch response: %v",
				err,
			)
		}

		// Record the returned keys for deduplication so that we don't send
		// them again in the following bucket migration
		for _, stateKeyList := range req.StateKeyLists {
			for _, key := range stateKeyList.Keys {
				transferredKeys[string(key)] = struct{}{}
			}
		}

		/**********************************************************************
							Stage 2: async bucket migration
		**********************************************************************/

		var wgMigration sync.WaitGroup
		for _, bucketId := range req.BucketIds {

			// [Validation] Requested bucket must belong to the local worker
			if s.stateService.StateLookupTable.BucketIdxToWorkerID(
				bucketId,
			) != s.stateService.WorkerID {
				log.Fatalf(
					"Requested bucket %d does not belong to the local worker %d",
					bucketId,
					s.stateService.WorkerID,
				)
			}

			// Get all states for this bucket
			wgMigration.Add(1)
			stateReadRoutinePool.Submit(func() {
				defer wgMigration.Done()

				resKeys, resValues := s.stateService.ReadLocalBucketRange(
					bucketId,
					bucketId,
				)

				// Deduplicate the fetched states
				resKeys, resValues = stateCommUtil.DedupStates(
					resKeys,
					resValues,
					transferredKeys,
				)

				// Send state chunk in critical section
				bucketSendLock.Lock()
				stateChunkSender.Send(resKeys, resValues)
				bucketSendLock.Unlock()

				// Current bucket is successfully migrated, update the ownership
				// no lock needed since the bucket are not looked up or updated
				// by other routines
				s.stateService.StateLookupTable.ChangeBucketOwner(
					bucketId,
					uint16(req.SourceWorkerId),
				)
			})

		}
		wgMigration.Wait()

		// Flush the remaining states in the buffer and notify end of stream
		stateChunkSender.Flush()

		// Reset the dedup key map for next async migration batch
		transferredKeys = make(map[string]struct{})
	}
}

// [Lazy-by-key] Remote state fetch. This request will not access the key
// lookup table
func (s *StateCommRpcServer) RemoteByKeyMigration(
	stream pb.StateCommService_RemoteByKeyMigrationServer,
) error {

	for {
		// Receive a remote state fetch request
		fetchRequest, err := stream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed to receive lazy-by-key state fetch request: %v",
				err,
			)
		}

		// Construct the return value lists
		var stateValueLists []*pb.StateValueList
		for _, stateKeyList := range fetchRequest.StateKeyLists {
			stateValueLists = append(stateValueLists, &pb.StateValueList{
				StateId: stateKeyList.StateId,
				Values: s.stateService.StateBackendImpl.GetMany(
					stateKeyList.Keys,
				),
			})
		}

		// Send the response
		err = stream.Send(&pb.KeyResponse{
			StateValueLists: stateValueLists,
		})
		if err != nil {
			log.Fatalf("Failed to send lazy-by-key key fetch response: %v", err)
		}
	}
}
