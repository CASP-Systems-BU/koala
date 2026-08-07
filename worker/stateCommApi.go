package worker

import (
	"context"
	"io"
	"log"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
)

/******************************************************************************
		   Exposed State Service APIs for remote state access
******************************************************************************/

// [Stop-and-restart] API for state migration. Multiple workers can call this
// api concurrently to pull state. This is a directional connection which is
// only maintained during state migration
func (s *StateServiceCommServer) PullStatePartition(
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
	stateChunkSender := NewStateChunkSender(
		stream,
		maxChunkSize,
		newStateChunkMsg,
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
func (s *StateServiceCommServer) MigrateTaskMetadata(
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
func (s *StateServiceCommServer) RemoteBucketMigration(
	stream pb.StateCommService_RemoteBucketMigrationServer,
) error {

	maxChunkSize := s.stateService.Config.StateMigrationChunkSize
	stateChunkSender := NewStateChunkSender(
		stream,
		maxChunkSize,
		newStateChunkMsg,
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
func (s *StateServiceCommServer) RemoteRead(
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
func (s *StateServiceCommServer) RemoteOverwrite(
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
func (s *StateServiceCommServer) RemoteMerge(
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
func (s *StateServiceCommServer) RemoteDelete(
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
func (s *StateServiceCommServer) RemoteAsyncBucketMigration(
	stream pb.StateCommService_RemoteAsyncBucketMigrationServer,
) error {

	// Init utils for async bucket migration
	maxChunkSize := s.stateService.Config.StateMigrationChunkSize
	stateChunkSender := NewStateChunkSender(
		stream,
		maxChunkSize,
		newAsyncBucketMigrationResMsg,
	)

	// Init utils for deduplication of migrated keys. After we transfer the
	// required keys, we should exclude these keys in the following bucket
	// migration process to avoid redundant data transfer. This map should be
	// reset after each flush of the async migration process.
	transferredKeys := make(map[string]struct{})

	for {
		req, err := stream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed to receive remote async bucket migration request: %v",
				err,
			)
		}

		switch msg := req.Message.(type) {
		// 1. This request is for keys (each batch can send multiple
		//    ReadRequests)
		case *pb.AsyncBucketMigrationRequest_ReadRequest:

			// Read values from local state backend
			values := s.stateService.StateBackendImpl.GetMany(
				msg.ReadRequest.Keys,
			)

			// Send values as the response
			err = stream.Send(&pb.AsyncBucketMigrationResponse{
				Message: &pb.AsyncBucketMigrationResponse_ReadResponse{
					ReadResponse: &pb.ReadResponse{
						Values: values,
					},
				},
			})
			if err != nil {
				log.Fatalf("Failed to send async bucket migration response: %v", err)
			}

			// Record the transferred keys for deduplication
			for _, key := range msg.ReadRequest.Keys {
				transferredKeys[string(key)] = struct{}{}
			}

		// 2. This batch is for aysnc bucket migration (each batch only has one
		//    BucketMigrationRequest)
		case *pb.AsyncBucketMigrationRequest_BucketMigrationRequest:

			bucketMigrationReq := msg.BucketMigrationRequest

			// [Validation] Requested operator ID must match the local operator
			if bucketMigrationReq.OperatorId != uint32(s.stateService.OperatorID) {
				log.Fatalf(
					"Invalid operator ID: %d, expected: %d",
					bucketMigrationReq.OperatorId,
					s.stateService.OperatorID,
				)
			}

			for _, bucketIdx := range bucketMigrationReq.BucketIds {

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

				// Dedup keys and values for the ones that are already sent
				resKeys, resValues = dedupStates(
					resKeys,
					resValues,
					transferredKeys,
				)

				// Send states in chunks
				stateChunkSender.Send(resKeys, resValues)

				// Current bucket is successfully migrated, update the ownership
				s.stateService.StateLookupTable.ChangeBucketOwner(
					bucketIdx,
					uint16(bucketMigrationReq.SourceWorkerId),
				)
			}

			// Reset the dedup key map for next async migration batch
			transferredKeys = make(map[string]struct{})

			// Flush the remaining states in the buffer and notify end of stream
			stateChunkSender.Flush()

		default:
			log.Fatalf(
				"Invalid remote async bucket migration request message type: %v\n",
				req,
			)
		}
	}
}

// [DRRS] Push based state migration API
func (s *StateServiceCommServer) PushStateMigrationDRRS(
	stream pb.StateCommService_PushStateMigrationDRRSServer,
) error {

	// Number of buckets received before reporting migration progress to state
	// service - main routings uses this to aggresively trigger scanning of wait
	// buffer for processable records. -1 indicates no progress report until all
	// buckets are received in this routine
	migrationProgressGranularity := s.stateService.Config.MigrationProgressGranularity

	receivedBucketCnt := int64(0)
	receivedBucketIds := make([]uint64, 0)
	for {

		// Receive a pushed state bucket
		bucketState, err := stream.Recv()
		if err == io.EOF {
			err = stream.SendAndClose(&pb.Response{
				Info: "All state buckets received",
			})
			if err != nil {
				log.Fatalf(
					"Failed to send close response for pushed state migration: %v",
					err,
				)
			}
			break
		}
		if err != nil {
			log.Fatalf("Failed to receive pushed state bucket: %v", err)
		}

		// Write the received states to local state backend
		if len(bucketState.Keys) > 0 {
			s.stateService.StateBackendImpl.SetMany(
				bucketState.Keys,
				bucketState.Values,
			)
		}
		receivedBucketCnt++
		receivedBucketIds = append(
			receivedBucketIds,
			uint64(bucketState.BucketId),
		)

		// Update the bucket ownership map and report progress if needed
		if migrationProgressGranularity != -1 &&
			receivedBucketCnt >= migrationProgressGranularity {

			// Mark the received buckets as migrated and report progress
			s.stateService.MarkBucketsAsMigrated(receivedBucketIds)

			// Reset the counter and list
			receivedBucketCnt = 0
			receivedBucketIds = make([]uint64, 0)
		}
	}

	// Mark the remaining received buckets as migrated. -1 will be covered here
	// as we didn't report progress until all buckets are received
	if receivedBucketCnt > 0 {
		s.stateService.MarkBucketsAsMigrated(receivedBucketIds)
	}

	// Mark the migration routine as done
	s.stateService.MigrationRoutineFinish()
	return nil
}
