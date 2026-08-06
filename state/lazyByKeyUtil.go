package state

import (
	"log"
	"sync"
	"time"
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

	// Check if eventual migration from cancelling workers is in progress.
	eventualMigrationTriggered := s.EventualMigrationEnabled.Load() == 1

	// Snapshot the bucket locks to ensure the locks for cancelling workers
	// are safe to access - other routine might reset the lock map while the
	// updateAddrForFetchedNeededRemoteKeys routine is still accessing the locks
	// [note] this is not ideal implementation but it's safe
	var bucketLocks map[int64]*sync.RWMutex
	if eventualMigrationTriggered {
		bucketLocks = s.eventualMigrationBucketLocks
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
			eventualMigrationTriggered,
		)
	}

	// [Eventual migration for cancelling task] For cancelling workers that
	// have no needed keys in this batch, we explicitly fetch additional keys
	// for eventual migration
	if eventualMigrationTriggered {
		for cancellingWorkerId := range s.eventualMigrationAffectedBuckets {
			if _, alreadyFetched := keysByWorker[cancellingWorkerId]; alreadyFetched {
				continue
			}
			s.ByKeyMigrationWaitGroup.Add(1)
			go func(wid uint16) {
				defer s.ByKeyMigrationWaitGroup.Done()
				s.fetchAdditionalKeys(wid, nil)
			}(cancellingWorkerId)
		}
	}

	// Update the key lookup table for all keys that are being fetched from
	// remote workers - now they belong to the local worker. Block new state
	// access until all key lookup table updates are done
	s.ByKeyMigrationWaitGroup.Add(1)
	go s.updateAddrForFetchedNeededRemoteKeys(
		keysByWorker,
		indexTrackerByWorker,
		bucketIDs,
		bucketLocks,
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
//
// eventualMigrationTriggered: if true, check whether this remote worker is a
// cancelling worker and fetch additional keys for eventual migration.
func (s *StateService) byKeyStateFetchExecutor(
	remoteWorkerId uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
	keyWaitGroup *sync.WaitGroup,
	eventualMigrationTriggered bool,
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

	/**************************************************************************
	       Stage 3: fetch additional keys for eventual state migration
	**************************************************************************/

	if eventualMigrationTriggered {
		_, isCancellingWorker := s.eventualMigrationAffectedBuckets[remoteWorkerId]
		if isCancellingWorker {
			// Build set of needed keys to avoid duplicate fetch
			neededKeySet := make(map[string]bool)
			for _, keyList := range keys {
				for _, key := range keyList {
					neededKeySet[string(key)] = true
				}
			}
			s.fetchAdditionalKeys(remoteWorkerId, neededKeySet)
		}
	}
}

// Helper function to update the addresses for all fetched remote keys in the
// key lookup table - now they belong to the local worker. Hold the migration
// wait group counter such that we block new state access until current key
// lookup table updates are done
func (s *StateService) updateAddrForFetchedNeededRemoteKeys(
	keysByWorker map[uint16]map[uint16][][]byte,
	indexTrackerByWorker map[uint16]map[uint16][]int,
	bucketIDs []int64,
	bucketLocks map[int64]*sync.RWMutex,
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

				// If eventual migration is active and this bucket is affected,
				// acquire write lock to avoid racing with cancelling worker
				// routines
				if lock := bucketLocks[bucketID]; lock != nil {
					lock.Lock()
					s.StateLookupTableV2.UpdateKey(key, bucketID, s.WorkerID)
					lock.Unlock()
				} else {
					s.StateLookupTableV2.UpdateKey(key, bucketID, s.WorkerID)
				}
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

/******************************************************************************
		            Eventual migration for cancelling task
******************************************************************************/

// [Eventual migration for cancelling task] Fetch keys from a cancelling worker
// for eventual migration:
//  1. Get all keys to fetch - limited by LazyByKeyGradualMigrationBatchSize
//     (-1 for unlimited)
//  2. Fetch and flush the keys
//  3. Notify the end of eventual migration if all cancelling workers are done
func (s *StateService) fetchAdditionalKeys(
	cancellingWorkerId uint16,
	keysAlreadyFetched map[string]bool,
) {

	// Use configured batch size: -1 fetches all keys at once
	maxKeys := s.Config.LazyByKeyGradualMigrationBatchSize

	// 1. Get keys for eventual migration for this cancelling worker
	additionalKeys, bucketIDs, allDone := s.getKeysForEventualMigration(
		cancellingWorkerId, keysAlreadyFetched, maxKeys,
	)

	// 2. Fetch and flush the keys (local IO runs in background goroutines)
	var ioWg sync.WaitGroup
	if len(additionalKeys) > 0 {
		switch s.Config.LazyByKeyStateCommAPIType {
		case "tcp":
			s.fetchAdditionalKeysByTcp(
				cancellingWorkerId,
				additionalKeys,
				bucketIDs,
				&ioWg,
			)
		case "grpc":
			log.Fatalf("gRPC additional key fetch not implemented yet\n")
		default:
			log.Fatalf(
				"Unsupported state comm API type for eventual migration: %s\n",
				s.Config.LazyByKeyStateCommAPIType,
			)
		}
	}

	// Wait for all background IO routines to finish before checking allDone
	ioWg.Wait()

	// 3. If this cancelling worker is fully migrated, notify termination.
	// Guard with LoadOrStore to prevent double decrement: a completed worker
	// can be re-invoked in a later batch because it remains in
	// eventualMigrationAffectedBuckets.
	if allDone {
		if _, alreadyDone := s.eventualMigrationDoneWorkers.LoadOrStore(cancellingWorkerId, true); !alreadyDone {
			if s.eventualMigrationRemainingWorkers.Add(-1) == 0 {
				s.EventualMigrationEnabled.Store(0)
				s.eventualMigrationBucketLocks = nil
				s.eventualMigrationFinishedBuckets = nil
				s.EventualMigrationDone.Signal()
			}
			log.Printf(
				"[Eventual Migration][Worker %d] Finished eventual migration\n",
				s.WorkerID,
			)
		}
	}
}

// [Eventual migration for cancelling task] Get keys to fetch for a cancelling
// worker limited by maxKeys (-1 for unlimited). Scan key lookup table for all
// affected buckets, and collect keys whose
// state location is on the cancelling worker. Skip finished buckets and
// duplicated keys.
// Returns the collected keys, their corresponding bucket IDs, and whether all
// affected buckets for this worker are fully migrated.
func (s *StateService) getKeysForEventualMigration(
	cancellingWorkerId uint16,
	duplicatedKeys map[string]bool,
	maxKeys int,
) (additionalKeys [][]byte, bucketIDs []int64, allDone bool) {

	affectedBucketIds, ok := s.eventualMigrationAffectedBuckets[cancellingWorkerId]
	if !ok {
		log.Fatalf(
			"No affected buckets found for cancelling worker %d\n",
			cancellingWorkerId,
		)
	}

	// Count of keys collected
	count := 0

	for _, bucketId := range affectedBucketIds {

		// Skip buckets already fully migrated from previous batches
		if s.eventualMigrationFinishedBuckets[cancellingWorkerId][bucketId] {
			continue
		}

		bucket := s.StateLookupTableV2.Buckets[bucketId]
		if bucket.Map == nil {
			log.Fatalf(
				"Affected bucket %d has nil key map on current worker\n",
				bucketId,
			)
		}

		// Read-lock: concurrent scans from other cancelling workers are safe,
		// but writes from updateAddrForFetchedNeededRemoteKeys and
		// fetchAdditionalKeysByTcp need exclusive access
		lock := s.eventualMigrationBucketLocks[bucketId]
		lock.RLock()
		for keyStr, stateLocWorkerId := range bucket.Map {
			if stateLocWorkerId != cancellingWorkerId {
				continue
			}

			// Dedup: key is already being fetched in the normal needed flow,
			// it will be migrated and updated in the lookup table — skip it
			if duplicatedKeys[keyStr] {
				continue
			}

			additionalKeys = append(additionalKeys, []byte(keyStr))
			bucketIDs = append(bucketIDs, bucketId)
			count++
			if maxKeys > 0 && count >= maxKeys {
				lock.RUnlock()
				return additionalKeys, bucketIDs, false
			}
		}
		lock.RUnlock()

		// All keys in this bucket for this cancelling worker are covered
		// (either collected above or being fetched as needed keys)
		s.eventualMigrationFinishedBuckets[cancellingWorkerId][bucketId] = true
	}
	return additionalKeys, bucketIDs, true
}
