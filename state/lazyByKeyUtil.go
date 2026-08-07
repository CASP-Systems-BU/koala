package state

import (
	"log"
	"sync"
	"time"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
)

// [lazy-by-key] Fetch and migrate requested keys from remote state services.
// This function returns immediately after all requested keys are fetched and
// leaves fetched key flush to state backend in background
func (s *StateService) remoteReadWithByKeyMigration(
	keys map[uint16][][]byte,
	bucketIDs []int64,
) map[uint16][][]byte {

	// Group requested keys by owner workers
	// keysByWorker: map[worker ID]map[state ID][][]byte
	// indexTrackerByWorker: map[worker ID]map[state ID][]int for the indices of
	// requested keys in the input
	start := time.Now()
	keysByWorker, indexTrackerByWorker := s.groupKeysByWorker(
		keys,
		bucketIDs,
	)
	duration := time.Since(start)
	s.MetricCollector.UpdateKeyLookUpTime(duration)

	// Allocate space to store/merge results for each worker's request
	// resByWorker: map[worker ID]map[state ID][][]byte (list if values)
	// This map structure aligns with keysByWorker
	resByWorker := make(map[uint16]map[uint16][][]byte)
	for workerID := range keysByWorker {
		resByWorker[workerID] = make(map[uint16][][]byte)
	}

	// Use separate goroutines to handle state requests for each worker. Use 2
	// wait groups to separately track key fetch and background key flush
	// (1) keyWaitGroup: tracks key fetch completion
	// (2) byKeyMigrationWaitGroup: tracks background key flush completion.
	//     This wait group is stored in StateService to allow tracking in future
	//     state access
	var keyWaitGroup sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {

		keyWaitGroup.Add(1)
		if workerID == s.WorkerID {

			// This batch of keys is local - get from local state backend. For
			// local worker, no background key flush is needed
			go s.readLocalStateRoutine(
				workerKeys,
				resByWorker[workerID],
				&keyWaitGroup,
			)
			continue
		}

		// This batch of keys is remote - get from remote state service. This
		// routine also triggers background key flush, so we increment the
		// by-key migration wait group counter
		s.ByKeyMigrationWaitGroup.Add(1)
		go s.byKeyStateFetchExecutor(
			workerID,
			workerKeys,
			resByWorker[workerID],
			&keyWaitGroup,
		)
	}

	// Update the key lookup table for all keys that are being fetched from
	// remote workers - now they belong to the local worker. Block new state
	// access until all key lookup table updates are done
	s.ByKeyMigrationWaitGroup.Add(1)
	go s.updateAddrForFetchedRemoteKeys(
		keysByWorker,
		indexTrackerByWorker,
		bucketIDs,
	)

	// Wait all needed keys are fetched or read from local
	keyWaitGroup.Wait()

	// Merge the fetched results from all workers
	return s.mergeResultsFromAllWorkersLazyByKey(
		keys,
		resByWorker,
		indexTrackerByWorker,
	)
}

// [lazy-by-key] Routine executor to async fetch/migrate keys from a remote
// worker. Use 2 wait groups to track key fetch and background key flush:
// (1) keyWaitGroup: tracks needed key fetch completion
// (2) byKeyMigrationWaitGroup: tracks background key flush completion
func (s *StateService) byKeyStateFetchExecutor(
	remoteWorkerId uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
	keyWaitGroup *sync.WaitGroup,
) {
	defer s.ByKeyMigrationWaitGroup.Done()

	/**************************************************************************
				   Stage 1: fetch keys from remote state service
	**************************************************************************/

	switch s.Config.LazyByKeyStateCommAPIType {
	case "grpc":
		s.fetchStateByRpc(remoteWorkerId, keys, res)
	case "tcp":
		s.fetchStateByTcp(remoteWorkerId, keys, res)
	default:
		log.Fatalf(
			"Unsupported state comm API type for lazy-by-key protocol: %s\n",
			s.Config.LazyByKeyStateCommAPIType,
		)
	}

	// Notify the key fetch completion such that we can start processing the
	// fetched keys without waiting for the background key flush to finish
	keyWaitGroup.Done()

	/**************************************************************************
			  				Stage 2: flush fetched keys
	**************************************************************************/

	// res contains all fetched keys' values - flush them to local state backend
	// in the background. Note: 1. safe to access res after return since it will
	// not be modified after this point. 2. key lookup table update is handled
	// in a separate goroutine
	for stateId, keyList := range keys {
		s.StateBackendImpl.SetMany(keyList, res[stateId])
	}
}

// [gRPC] Fetch remote state over gRPC API
func (s *StateService) fetchStateByRpc(
	remoteWorkerId uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
) {

	// Get gRPC connection to the remote state service
	remoteFetchStream := s.getByKeyMigrationConn(remoteWorkerId)

	// Construct the state fetch request
	stateKeyLists := make([]*pb.StateKeyList, 0, len(keys))
	for stateID, keyList := range keys {
		stateKeyLists = append(stateKeyLists, &pb.StateKeyList{
			StateId: uint32(stateID),
			Keys:    keyList,
		})
	}
	req := &pb.LazyByKeyStateRequest{
		StateKeyLists: stateKeyLists,
	}

	// Send the request
	if err := remoteFetchStream.Send(req); err != nil {
		log.Fatalf(
			"Failed to send lazy-by-key gRPC state fetch request to worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Receive the KeyResponse message
	response, err := remoteFetchStream.Recv()
	if err != nil {
		log.Fatalf(
			"Failed to receive lazy-by-key gRPC key fetch response from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Populate the result map with received values
	for _, stateValueList := range response.StateValueLists {
		stateId := uint16(stateValueList.StateId)
		res[stateId] = stateValueList.Values
	}
}

// Helper function to update the addresses for all fetched remote keys in the
// key lookup table - now they belong to the local worker. Hold the migration
// wait group counter such that we block new state access until current key
// lookup table updates are done
func (s *StateService) updateAddrForFetchedRemoteKeys(
	keysByWorker map[uint16]map[uint16][][]byte,
	indexTrackerByWorker map[uint16]map[uint16][]int,
	bucketIDs []int64,
) {
	defer s.ByKeyMigrationWaitGroup.Done()

	for workerID, workerKeys := range keysByWorker {

		// Only update keys that are fetched from remote workers
		if workerID == s.WorkerID {
			continue
		}

		// Update the key lookup table for all fetched keys from this worker
		for stateID, keyList := range workerKeys {

			// Get the indices of these keys in the original request
			indices := indexTrackerByWorker[workerID][stateID]

			for i, key := range keyList {

				// Get the bucket ID for this key from the original request
				bucketID := bucketIDs[indices[i]]

				// Update the key lookup table: set the owner worker ID to
				// local worker ID
				s.StateLookupTableV2.UpdateKey(
					key,
					bucketID,
					s.WorkerID,
				)
			}
		}
	}
}

// Insert or update the key addresses in the lookup table
func (s *StateService) insertOrUpdateAddrForLocalKeys(
	keys map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	for stateID, keyList := range keys {
		for i, key := range keyList {

			// Insert or update the key address to local worker ID
			s.StateLookupTableV2.InsertOrUpdateKey(
				key,
				bucketIDs[stateID][i],
				s.WorkerID,
			)
		}
	}
}

// Delete the keys from the lookup table
func (s *StateService) deleteKeysFromLookupTable(
	keys map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	for stateID, keyList := range keys {
		for i, key := range keyList {

			// Delete the key from the lookup table
			s.StateLookupTableV2.RemoveKey(
				key,
				bucketIDs[stateID][i],
			)
		}
	}
}
