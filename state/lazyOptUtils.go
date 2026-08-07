package state

import (
	"log"
	"sync"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
)

// [lazy opt] Read remote state with specified keys and trigger async
// bucket migration in background. This function returns immediately if all
// requested keys are fetched while keeping the background migration ongoing
func (s *StateService) remoteReadWithAsyncBucketMigration(
	operatorID uint16,
	keys map[uint16][][]byte,
	bucketIDs []int64,
) map[uint16][][]byte {

	// Group requested keys and their belonging buckets by owner workers
	keysByWorker, indexTracker, bucketsByWorker := s.groupKeysAndBucketsByWorker(
		keys,
		bucketIDs,
	)

	// Allocate space to store/merge results for each worker's request
	// Map: worker ID -> state ID -> list of values. This map structure aligns
	// with keysByWorker
	resByWorker := make(map[uint16]map[uint16][][]byte)
	for workerID := range keysByWorker {
		resByWorker[workerID] = make(map[uint16][][]byte)
	}

	// Use separate goroutines to handle state requests for each worker. Use 2
	// wait groups to separately track key fetch and background bucket migration
	// (1) keyWaitGroup: tracks key fetch completion
	// (2) asyncMigrationWaitGroup: tracks background bucket migration
	//     completion. This wait group is stored in StateService to allow
	//     tracking in future flush operations
	var keyWaitGroup sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {

		keyWaitGroup.Add(1)
		if workerID == s.WorkerID {

			// This batch of keys is local - get from local state backend. For
			// local worker, no background bucket migration is needed
			go s.readLocalStateRoutine(
				workerKeys,
				resByWorker[workerID],
				&keyWaitGroup,
			)
			continue
		}

		// This batch of keys is remote - get from remote state service. This
		// routine also triggers background bucket migration, so we increment
		// the async bucket migration wait group counter
		s.AsyncMigrationWaitGroup.Add(1)
		go s.asyncStateFetchExecutor(
			workerID,
			operatorID,
			workerKeys,
			bucketsByWorker[workerID],
			resByWorker[workerID],
			&keyWaitGroup,
		)
	}

	// Only block on key fetch completion. Will block on bucket migration
	// completion in future flush operations
	keyWaitGroup.Wait()

	// Merge state read results from all workers
	return s.mergeResultsFromAllWorkers(
		keys,
		resByWorker,
		indexTracker,
	)
}

// [Lazy opt] Routine executor to async fetch state from a remote worker. Use
// 2 wait groups to separately track key fetch and background bucket migration:
// (1) keyWaitGroup: tracks needed key fetch completion
// (2) asyncMigrationWaitGroup: tracks background bucket migration completion
func (s *StateService) asyncStateFetchExecutor(
	remoteWorkerId uint16,
	targetOperatorId uint16,
	keys map[uint16][][]byte,
	bucketIds map[int64]struct{},
	res map[uint16][][]byte,
	keyWaitGroup *sync.WaitGroup,
) {
	defer s.AsyncMigrationWaitGroup.Done()

	// Get connection to remote state comm service
	remoteReadStream := s.getAsyncBucketMigrationConn(remoteWorkerId)

	// Construct a single request for needed keys and their associated buckets
	stateKeyLists := make([]*pb.StateKeyList, 0, len(keys))
	for stateId, keyList := range keys {
		stateKeyLists = append(stateKeyLists, &pb.StateKeyList{
			StateId: uint32(stateId),
			Keys:    keyList,
		})
	}
	bucketIdList := make([]int64, 0, len(bucketIds))
	for bucketId := range bucketIds {
		bucketIdList = append(bucketIdList, bucketId)
	}
	request := &pb.LazyOptStateRequest{
		OperatorId:     uint32(targetOperatorId),
		SourceWorkerId: uint32(s.WorkerID),
		StateKeyLists:  stateKeyLists,
		BucketIds:      bucketIdList,
	}

	// Send the request
	if err := remoteReadStream.Send(request); err != nil {
		log.Fatalf(
			"Failed to send async state fetch request to worker %d: %v",
			remoteWorkerId,
			err,
		)
	}

	/**************************************************************************
					   Stage 1: receive values for needed keys
	**************************************************************************/

	// Receive the KeyResponse message
	response, err := remoteReadStream.Recv()
	if err != nil {
		log.Fatalf(
			"Failed to receive async key fetch response from worker %d: %v",
			remoteWorkerId,
			err,
		)
	}
	msg, ok := response.Message.(*pb.LazyOptStateResponse_KeyResponse)
	if !ok {
		log.Fatalf(
			"Unexpected async state fetch response type from worker %d",
			remoteWorkerId,
		)
	}

	// Populate the result map with received values
	for _, stateValueList := range msg.KeyResponse.StateValueLists {
		stateId := uint16(stateValueList.StateId)
		res[stateId] = stateValueList.Values
	}

	// Notify the key fetch completion such that we can start processing the
	// fetched keys in the main routine without waiting for the background
	// bucket migration to finish
	keyWaitGroup.Done()

	// Flush the received keys to local state backend
	var flushWg sync.WaitGroup
	for stateId, keyList := range keys {
		flushWg.Add(1)
		s.LazyOptStateFlushPool.Submit(func() {
			defer flushWg.Done()
			s.StateBackendImpl.SetMany(keyList, res[stateId])
		})
	}

	/**************************************************************************
						  Stage 2: receive bucket migration
	**************************************************************************/

	for {

		bucketRes, err := remoteReadStream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed to receive async bucket fetch from worker %d: %v",
				remoteWorkerId,
				err,
			)
		}
		msg, ok := bucketRes.Message.(*pb.LazyOptStateResponse_StateChunk)
		if !ok {
			log.Fatalf(
				"Unexpected async bucket fetch response type from worker %d",
				remoteWorkerId,
			)
		}

		// All migrating buckets have been received
		if msg.StateChunk.EndOfStream {
			break
		}

		// Flush the received state chunk to local state backend
		localStateChunk := msg.StateChunk
		flushWg.Add(1)
		s.LazyOptStateFlushPool.Submit(func() {
			defer flushWg.Done()
			s.StateBackendImpl.SetMany(
				localStateChunk.Keys,
				localStateChunk.Values,
			)
		})
	}

	// Update the bucket ownership in state lookup table
	for _, bucketId := range bucketIdList {
		s.StateLookupTable.ChangeBucketOwner(
			bucketId,
			s.WorkerID,
		)
	}

	// Wait until all received state is successfully flushed
	flushWg.Wait()
}
