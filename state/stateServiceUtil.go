package state

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/******************************************************************************
						Helpers for local state access
******************************************************************************/

// Read keys from local state backend
func (s *StateService) readLocalState(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
) {
	start := time.Now()
	for stateID, keys := range keys {
		values[stateID] = s.StateBackendImpl.GetMany(keys)
	}
	duration := time.Since(start)
	s.MetricCollector.UpdateReadLocalStateTime(duration)
}

// Routine executor to read local state
func (s *StateService) readLocalStateRoutine(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	s.readLocalState(keys, values)
}

// Write keys and values to local state backend - overwrite
func (s *StateService) overwriteLocalState(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
) {
	start := time.Now()
	for stateID, keys := range keys {
		if vals, ok := values[stateID]; ok {
			s.StateBackendImpl.SetMany(keys, vals)
		} else {
			log.Fatalf("Values for state ID %d not found\n", stateID)
		}
	}
	duration := time.Since(start)
	s.MetricCollector.UpdateOverWriteLocalStateTime(duration)
}

// Routine executor to overwrite local state
func (s *StateService) overwriteLocalStateRoutine(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	s.overwriteLocalState(keys, values)
}

// Write keys to local state backend - merge
func (s *StateService) mergeLocalState(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
) {

	for stateID, keys := range keys {
		if vals, ok := values[stateID]; ok {
			s.StateBackendImpl.MergeMany(keys, vals)
		} else {
			log.Fatalf("Values for state ID %d not found\n", stateID)
		}
	}
}

// Routine executor to merge local state
func (s *StateService) mergeLocalStateRoutine(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	s.mergeLocalState(keys, values)
}

// Delete keys from local state backend
func (s *StateService) deleteLocalState(
	keys map[uint16][][]byte,
) {
	for _, keys := range keys {
		s.StateBackendImpl.DeleteMany(keys)
	}
}

// Routine executor to delete local state
func (s *StateService) deleteLocalStateRoutine(
	keys map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	s.deleteLocalState(keys)
}

/******************************************************************************
						Helpers for remote state access
******************************************************************************/

// [lazy-basic] Fetch remote state to local state backend at bucket granularity.
// Ttransfer the bucket ownership in StateLookupTable
func (s *StateService) migrateRemoteBuckets(
	operatorID uint16,
	bucketIDs []int64,
) {

	// Iterate all bucketIDs and group them by destination workers
	var workerID uint16
	bucketMigrationRequests := make(map[uint16]*pb.BucketMigrationRequest)
	requestedBucketIDs := make(map[int64]struct{})
	for _, bucketID := range bucketIDs {

		workerID = s.StateLookupTable.BucketIdxToWorkerID(bucketID)

		// Skip local state request
		if workerID == s.WorkerID {
			continue
		}

		// Check if this bucketID is already requested
		if _, ok := requestedBucketIDs[bucketID]; ok {
			continue
		}
		requestedBucketIDs[bucketID] = struct{}{}

		// Check if the request to the worker is already created. If not,
		// create a new request
		if _, ok := bucketMigrationRequests[workerID]; !ok {
			bucketMigrationRequests[workerID] = &pb.BucketMigrationRequest{
				OperatorId:     uint32(operatorID),
				SourceWorkerId: uint32(s.WorkerID),
			}
		}
		// Append the bucketID to the request
		bucketMigrationRequests[workerID].BucketIds = append(
			bucketMigrationRequests[workerID].BucketIds,
			bucketID,
		)
	}

	// Early exit if no remote state is requested
	if len(bucketMigrationRequests) == 0 {
		return
	}

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent migration routines to avoid race
	// condition on connection map
	for workerID := range bucketMigrationRequests {
		s.getBucketMigrationConn(workerID)
	}

	// Concurrently fetch remote state for each group
	var wg sync.WaitGroup
	for workerID, request := range bucketMigrationRequests {
		wg.Add(1)
		go s.migrateRemoteBucketsFromWorker(workerID, request, &wg)
	}
	wg.Wait()

	// Update the StateLookupTable after bucket migration
	for migratedBucketID := range requestedBucketIDs {
		s.StateLookupTable.ChangeBucketOwner(
			migratedBucketID,
			s.WorkerID,
		)
	}
}

// [lazy-basic] Routine executor to fetch remote state from a single remote
// worker - executed concurrently by migrateRemoteBuckets()
func (s *StateService) migrateRemoteBucketsFromWorker(
	workerID uint16,
	remoteStateRequest *pb.BucketMigrationRequest,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Get the remote state service connection for bucket migration
	remoteStateAccessStream := s.getBucketMigrationConn(workerID)

	// Send the request to the remote state service
	err := remoteStateAccessStream.Send(remoteStateRequest)
	if err != nil {
		log.Fatalf("Failed to send remote bucket migration request: %v\n",
			err)
	}

	// Receive the state from the remote state service
	for {
		chunk, err := remoteStateAccessStream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed receiving remote bucket migration response: %v\n",
				err,
			)
		}
		if chunk.EndOfStream {
			return
		}

		// Write the state chunk to the local state store
		keys := chunk.Keys
		values := chunk.Values
		if len(keys) != len(values) || len(keys) == 0 {
			log.Fatalf("Invalid bucket chunk: %v\n", chunk)
		}

		s.StateBackendImpl.SetMany(keys, values)
	}
}

// [lazy-no-migration] Read remote state by specified keys. Note that the
// fetched values should respect the order as in the input keys - should be
// careful about ordering when values are fetched from multiple remote workers
func (s *StateService) remoteRead(
	keys map[uint16][][]byte,
	bucketIDs []int64,
) map[uint16][][]byte {

	// Group keys by worker ID to prepare for remote read requests
	// Map: worker ID -> state ID -> keys
	keysByWorker := make(map[uint16]map[uint16][][]byte)

	// Track original index of each key in the input keys. We need this to
	// restore the order of the fetched values to match the input keys
	// Map: worker ID -> list of indices in the input keys.
	// Note: state ID does not matter here since multiple states of the same
	// key have the same index position in the input keys
	indexTracker := make(map[uint16][]int)

	var workerID uint16
	for i, bucketID := range bucketIDs {

		workerID = s.StateLookupTable.BucketIdxToWorkerID(bucketID)
		workerKeys, ok := keysByWorker[workerID]
		if !ok {
			workerKeys = make(map[uint16][][]byte)
			keysByWorker[workerID] = workerKeys
			indexTracker[workerID] = []int{}
		}

		// Append the keys to the per worker group
		for stateID, keyList := range keys {
			workerKeys[stateID] = append(workerKeys[stateID], keyList[i])
		}

		// Append the original index to the per worker index tracker
		indexTracker[workerID] = append(indexTracker[workerID], i)
	}

	// Now all requested keys are grouped by their owner workers. We request
	// their owner workers separately in parallel to get the values

	// First initialize the result map by workers: Map: worker ID -> state ID
	// -> list of values
	resByWorker := make(map[uint16]map[uint16][][]byte)
	for workerID := range keysByWorker {
		resByWorker[workerID] = make(map[uint16][][]byte)
	}

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent read routines to avoid race
	// condition on connection map
	for workerID := range keysByWorker {
		if workerID != s.WorkerID {
			s.getReadConn(workerID)
		}
	}

	// Concurrently fetch remote state for each worker
	var wg sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {

		wg.Add(1)
		if workerID == s.WorkerID {

			// This batch of keys is local - get from local state backend
			go s.readLocalStateRoutine(workerKeys, resByWorker[workerID], &wg)
		} else {

			// This batch of keys is remote - get from remote state service
			go s.remoteReadFromWorker(workerID, workerKeys, resByWorker[workerID], &wg)
		}
	}
	wg.Wait()

	// Construct the final result map by merging results from all workers
	return s.mergeResultsFromAllWorkers(
		keys,
		resByWorker,
		indexTracker,
	)
}

// [Lazy-no-migration] Routine executor to read remote state from a single
// remote worker - executed concurrently by remoteRead()
func (s *StateService) remoteReadFromWorker(
	workerID uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Get the remote state service connection for read
	remoteReadStream := s.getReadConn(workerID)

	// Send the request to the remote state service. Each iteration handles
	// a single state ID
	for stateID, keys := range keys {
		remoteReadRequest := &pb.ReadRequest{
			OperatorId: uint32(s.OperatorID),
			Keys:       keys,
		}
		err := remoteReadStream.Send(remoteReadRequest)
		if err != nil {
			log.Fatalf("Failed to send remote read request: %v\n", err)
		}

		// Receive the response from the remote state service
		remoteReadResponse, err := remoteReadStream.Recv()
		if err != nil {
			log.Fatalf("Failed receiving remote read response: %v\n", err)
		}

		// Write the received values to the result map
		if len(remoteReadResponse.Values) != len(keys) {
			log.Fatalf(
				"Invalid remote read response: number of values %d does not match number of keys %d\n",
				len(remoteReadResponse.Values),
				len(keys),
			)
		}
		res[stateID] = remoteReadResponse.Values
	}
}

// [Lazy-no-migration] Overwrite state to remote state service
func (s *StateService) remoteOverwrite(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	// Group keys and values by worker ID to prepare for remote requests
	// Map: worker ID -> state ID -> keys/values
	keysByWorker, valuesByWorker := s.groupKeysAndValuesByWorker(
		keys,
		values,
		bucketIDs,
	)

	// Now all keys/values are grouped by their owner workers. We request their
	// owner workers separately in parallel to set the values

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent overwrite routines to avoid race
	// condition on connection map
	for workerID := range keysByWorker {
		if workerID != s.WorkerID {
			s.getOverwriteConn(workerID)
		}
	}

	// Concurrently overwrite remote state for each worker
	var wg sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {
		workerValues := valuesByWorker[workerID]
		wg.Add(1)

		if workerID == s.WorkerID {

			// This batch of keys is local - overwrite to local state backend
			go s.overwriteLocalStateRoutine(workerKeys, workerValues, &wg)
		} else {

			// This batch of keys is remote - overwrite to remote state service
			go s.remoteOverwriteToWorker(workerID, workerKeys, workerValues, &wg)
		}
	}
	wg.Wait()
}

// [Lazy-no-migration] Routine executor to overwrite remote state
func (s *StateService) remoteOverwriteToWorker(
	workerID uint16,
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Get the remote state service connection for overwrite
	remoteOverwriteStream := s.getOverwriteConn(workerID)

	// Send the request to the remote state service. Each iteration handles
	// a single state ID
	for stateID, keys := range keys {

		vals, ok := values[stateID]
		if !ok {
			log.Fatalf("Missing values for state ID %d\n", stateID)
		}

		remoteOverwriteRequest := &pb.WriteRequest{
			OperatorId: uint32(s.OperatorID),
			Keys:       keys,
			Values:     vals,
		}
		err := remoteOverwriteStream.Send(remoteOverwriteRequest)
		if err != nil {
			log.Fatalf("Failed to send remote overwrite request: %v\n", err)
		}

		// Receive the ack from a single remote write request
		ack, err := remoteOverwriteStream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote overwrite response: %v\n", err)
		}
		if ack.Info != "Success" {
			log.Fatalf(
				"Remote overwrite failed with err message: %s\n",
				ack.Info,
			)
		}
	}
}

// [lazy-no-migration] Merge state to remote state service
func (s *StateService) remoteMerge(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	// Group keys and values by worker ID to prepare for remote requests
	// Map: worker ID -> state ID -> keys/values
	keysByWorker, valuesByWorker := s.groupKeysAndValuesByWorker(
		keys,
		values,
		bucketIDs,
	)

	// Now all keys/values are grouped by their owner workers. We request their
	// owner workers separately in parallel to merge the values

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent merge routines to avoid
	// race condition on connection map
	for workerID := range keysByWorker {
		if workerID != s.WorkerID {
			s.getMergeConn(workerID)
		}
	}

	// Concurrently merge remote state for each worker
	var wg sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {
		workerValues := valuesByWorker[workerID]
		wg.Add(1)

		if workerID == s.WorkerID {

			// This batch of keys is local - merge to local state backend
			go s.mergeLocalStateRoutine(workerKeys, workerValues, &wg)
		} else {

			// This batch of keys is remote - merge to remote state service
			go s.remoteMergeToWorker(workerID, workerKeys, workerValues, &wg)
		}
	}
	wg.Wait()
}

// [Lazy-no-migration] Routine executor to merge remote state
func (s *StateService) remoteMergeToWorker(
	workerID uint16,
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Get the remote state service connection for merge
	remoteMergeStream := s.getMergeConn(workerID)

	// Send the request to the remote state service. Each iteration handles
	// a single state ID
	for stateID, keys := range keys {

		vals, ok := values[stateID]
		if !ok {
			log.Fatalf("Missing values for state ID %d\n", stateID)
		}

		remoteMergeRequest := &pb.WriteRequest{
			OperatorId: uint32(s.OperatorID),
			Keys:       keys,
			Values:     vals,
		}
		err := remoteMergeStream.Send(remoteMergeRequest)
		if err != nil {
			log.Fatalf("Failed to send remote merge request: %v\n", err)
		}

		// Receive the ack from a single remote write request
		ack, err := remoteMergeStream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote merge response: %v\n", err)
		}
		if ack.Info != "Success" {
			log.Fatalf(
				"Remote merge failed with err message: %s\n",
				ack.Info,
			)
		}
	}
}

// [Lazy-no-migration] Delete state from remote state service
func (s *StateService) remoteDelete(
	keys map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	// Group keys by worker ID to prepare for remote delete requests
	// Map: worker ID -> state ID -> keys
	keysByWorker := make(map[uint16]map[uint16][][]byte)

	var workerID uint16
	for stateID, keyList := range keys {

		for i, key := range keyList {

			workerID = s.StateLookupTable.BucketIdxToWorkerID(
				bucketIDs[stateID][i],
			)
			workerKeys, ok := keysByWorker[workerID]
			if !ok {
				workerKeys = make(map[uint16][][]byte)
				keysByWorker[workerID] = workerKeys
			}

			// Append the keys to the per worker group
			workerKeys[stateID] = append(workerKeys[stateID], key)
		}
	}

	// Now all requested keys are grouped by their owner workers. We request
	// their owner workers separately in parallel to delete the keys

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent delete routines to avoid race
	// condition on connection map
	for workerID := range keysByWorker {
		if workerID != s.WorkerID {
			s.getDeleteConn(workerID)
		}
	}

	// Concurrently delete remote state for each worker
	var wg sync.WaitGroup
	for workerID, workerKeys := range keysByWorker {
		wg.Add(1)

		if workerID == s.WorkerID {

			// This batch of keys is local - delete from local state backend
			go s.deleteLocalStateRoutine(workerKeys, &wg)
		} else {

			// This batch of keys is remote - delete from remote state service
			go s.remoteDeleteFromWorker(workerID, workerKeys, &wg)
		}
	}
	wg.Wait()
}

// [Lazy-no-migration] Routine executor to delete remote state
func (s *StateService) remoteDeleteFromWorker(
	workerID uint16,
	keys map[uint16][][]byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Get the remote state service connection for delete
	remoteDeleteStream := s.getDeleteConn(workerID)

	// Send the request to the remote state service. Each iteration handles
	// a single state ID
	for _, keys := range keys {
		remoteDeleteRequest := &pb.DeleteRequest{
			OperatorId: uint32(s.OperatorID),
			Keys:       keys,
		}
		err := remoteDeleteStream.Send(remoteDeleteRequest)
		if err != nil {
			log.Fatalf("Failed to send remote delete request: %v\n", err)
		}

		// Receive the ack from a single remote delete request
		ack, err := remoteDeleteStream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive remote delete response: %v\n", err)
		}
		if ack.Info != "Success" {
			log.Fatalf(
				"Remote delete failed with err message: %s\n",
				ack.Info,
			)
		}
	}
}

// [lazy-optimized] Read remote state with specified keys and trigger async
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

	// Allocate space to store results for each worker's request
	// Map: worker ID -> state ID -> list of values. This map structure aligns
	// with keysByWorker
	resByWorker := make(map[uint16]map[uint16][][]byte)
	for workerID := range keysByWorker {
		resByWorker[workerID] = make(map[uint16][][]byte)
	}

	// [hot fix - refactor later] Ensure all remote state connections are
	// initialized before starting concurrent async bucket migration routines to
	// avoid race condition
	for workerID := range keysByWorker {
		if workerID != s.WorkerID {
			s.getAsyncBucketMigrationConn(workerID)
		}
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
		go s.remoteReadWithAsyncBucketMigrationFromWorker(
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

// [Lazy-optimized] Routine executor to read remote state with async bucket
// migration from a single remote worker
func (s *StateService) remoteReadWithAsyncBucketMigrationFromWorker(
	targetWorkerID uint16,
	targetOperatorID uint16,
	keys map[uint16][][]byte,
	bucketIDs map[int64]struct{},
	res map[uint16][][]byte,
	keyWaitGroup *sync.WaitGroup,
) {
	defer s.AsyncMigrationWaitGroup.Done()

	// Get the remote state service connection for read with async bkt migration
	remoteReadStream := s.getAsyncBucketMigrationConn(targetWorkerID)

	/**************************************************************************
						 Stage 1: fetch the required keys
	**************************************************************************/

	// Each iteration handles a single state
	for stateID, keys := range keys {

		// Send the request to the remote state service
		remoteReadReq := &pb.AsyncBucketMigrationRequest{
			Message: &pb.AsyncBucketMigrationRequest_ReadRequest{
				ReadRequest: &pb.ReadRequest{
					OperatorId: uint32(targetOperatorID),
					Keys:       keys,
				},
			},
		}
		err := remoteReadStream.Send(remoteReadReq)
		if err != nil {
			log.Fatalf(
				"Failed to send remote read request in lazy-optimized mode: %v\n",
				err,
			)
		}

		// Receive the response from the remote state service
		remoteReadResponse, err := remoteReadStream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed receiving remote read response in lazy-optimized mode: %v\n",
				err,
			)
		}
		msg, ok := remoteReadResponse.Message.(*pb.AsyncBucketMigrationResponse_ReadResponse)
		if !ok {
			log.Fatalf(
				"Invalid remote read response message type in lazy-optimized mode: %v\n",
				remoteReadResponse,
			)
		}
		if len(msg.ReadResponse.Values) != len(keys) {
			log.Fatalf(
				"Invalid remote read response: number of values %d does not match number of keys %d\n",
				len(msg.ReadResponse.Values),
				len(keys),
			)
		}

		// Write the received values to the result map
		res[stateID] = msg.ReadResponse.Values
	}

	// Notify the key fetch completion such that we can start processing the
	// fetched keys without waiting for the background bucket migration to
	// finish in the main routine
	keyWaitGroup.Done()

	// Flush fetched keys to StateBackend after notifying key fetch completion
	for stateID, keys := range keys {
		s.StateBackendImpl.SetMany(
			keys,
			res[stateID],
		)
	}

	/**************************************************************************
						  Stage 2: async bucket migration
	**************************************************************************/

	// Send the bucket migration request
	bucketIDsList := make([]int64, 0, len(bucketIDs))
	for bucketID := range bucketIDs {
		bucketIDsList = append(bucketIDsList, bucketID)
	}
	bucketMigrationReq := &pb.AsyncBucketMigrationRequest{
		Message: &pb.AsyncBucketMigrationRequest_BucketMigrationRequest{
			BucketMigrationRequest: &pb.BucketMigrationRequest{
				OperatorId:     uint32(targetOperatorID),
				SourceWorkerId: uint32(s.WorkerID),
				BucketIds:      bucketIDsList,
			},
		},
	}
	err := remoteReadStream.Send(bucketMigrationReq)
	if err != nil {
		log.Fatalf(
			"Failed to send remote bucket migration request in lazy-optimized mode: %v\n",
			err,
		)
	}

	// Receive the migrated buckets from the remote state service
	for {

		bucketMigrationRes, err := remoteReadStream.Recv()
		if err != nil {
			log.Fatalf(
				"Failed receiving remote bucket migration response in lazy-optimized mode: %v\n",
				err,
			)
		}
		msg, ok := bucketMigrationRes.Message.(*pb.AsyncBucketMigrationResponse_StateChunk)
		if !ok {
			log.Fatalf(
				"Invalid remote bucket migration response message type in lazy-optimized mode: %v\n",
				bucketMigrationRes,
			)
		}

		// All migrated buckets are received
		if msg.StateChunk.EndOfStream {
			break
		}

		// Write the state chunk to the local state store
		keys := msg.StateChunk.Keys
		values := msg.StateChunk.Values
		if len(keys) != len(values) || len(keys) == 0 {
			log.Fatalf("Invalid bucket chunk: %v\n", msg.StateChunk)
		}
		s.StateBackendImpl.SetMany(keys, values)
	}

	// Update the StateLookupTable bucket ownership after migration
	for _, bucketID := range bucketIDsList {
		s.StateLookupTable.ChangeBucketOwner(
			bucketID,
			s.WorkerID,
		)
	}
}

/******************************************************************************
				Helpers to get remote state access connections
******************************************************************************/

// [lazy-basic] Get connection for remote bucket migration
func (s *StateService) getBucketMigrationConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk] {

	remoteBucketMigrationStream, ok := s.BucketMigrationConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteBucketMigrationStream, err = client.RemoteBucketMigration(
			context.Background(),
		)
		if err != nil {
			log.Fatalf(
				"Failed to establish remote bucket migration stream: %v\n",
				err,
			)
		}

		// Store the connection in the connection map
		s.BucketMigrationConn[workerID] = remoteBucketMigrationStream
	}
	return remoteBucketMigrationStream
}

// [lazy-no-migration] Get connection for remote read-only by keys
func (s *StateService) getReadConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.ReadRequest, pb.ReadResponse] {

	remoteReadStream, ok := s.ReadConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteReadStream, err = client.RemoteRead(
			context.Background(),
		)
		if err != nil {
			log.Fatalf("Failed to establish remote read stream: %v\n",
				err)
		}

		// Store the connection in the connection map
		s.ReadConn[workerID] = remoteReadStream
	}
	return remoteReadStream
}

// [lazy-no-migration] Get connection for remote write (overwrite)
func (s *StateService) getOverwriteConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.WriteRequest, pb.Response] {

	remoteOverwriteStream, ok := s.OverwriteConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteOverwriteStream, err = client.RemoteOverwrite(
			context.Background(),
		)
		if err != nil {
			log.Fatalf("Failed to establish remote overwrite stream: %v\n",
				err)
		}

		// Store the connection in the connection map
		s.OverwriteConn[workerID] = remoteOverwriteStream
	}
	return remoteOverwriteStream
}

// [lazy-no-migration] Get connection for remote write (merge)
func (s *StateService) getMergeConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.WriteRequest, pb.Response] {

	remoteMergeStream, ok := s.MergeConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteMergeStream, err = client.RemoteMerge(
			context.Background(),
		)
		if err != nil {
			log.Fatalf("Failed to establish remote merge stream: %v\n",
				err)
		}

		// Store the connection in the connection map
		s.MergeConn[workerID] = remoteMergeStream
	}
	return remoteMergeStream
}

// [lazy-no-migration] Get connection for remote delete
func (s *StateService) getDeleteConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.DeleteRequest, pb.Response] {

	remoteDeleteStream, ok := s.DeleteConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteDeleteStream, err = client.RemoteDelete(
			context.Background(),
		)
		if err != nil {
			log.Fatalf("Failed to establish remote delete stream: %v\n",
				err)
		}

		// Store the connection in the connection map
		s.DeleteConn[workerID] = remoteDeleteStream
	}
	return remoteDeleteStream
}

// [lazy-optimized] Get connection for remote async bucket migration
func (s *StateService) getAsyncBucketMigrationConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.AsyncBucketMigrationRequest, pb.AsyncBucketMigrationResponse] {

	remoteAsyncBucketMigrationStream, ok := s.AsyncBucketMigrationConn[workerID]

	// Establish the connection if it's not present
	var err error
	if !ok {

		client := s.getStateCommServiceClient(workerID)
		remoteAsyncBucketMigrationStream, err = client.RemoteAsyncBucketMigration(
			context.Background(),
		)
		if err != nil {
			log.Fatalf(
				"Failed to establish remote bucket migration stream: %v\n",
				err,
			)
		}

		// Store the connection in the connection map
		s.AsyncBucketMigrationConn[workerID] = remoteAsyncBucketMigrationStream
	}
	return remoteAsyncBucketMigrationStream
}

// Helper function to get gRPC client for state comm service
func (s *StateService) getStateCommServiceClient(
	workerID uint16,
) pb.StateCommServiceClient {

	s.PeerStateServiceMapLock.Lock()
	remoteStateServiceAddr, ok := s.PeerStateServiceMap[workerID]
	s.PeerStateServiceMapLock.Unlock()
	if !ok {
		log.Fatalf("Peer state service address not found for worker %d\n",
			workerID)
	}

	conn, err := grpc.NewClient(
		remoteStateServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// The default message size limit is 4 MB. Increase it to 100 MB for
		// large message size: lazy no-migration connections are based on
		// single gRPC message instead of StateChunk streaming
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(500*1024*1024), // 100 MB
			grpc.MaxCallSendMsgSize(500*1024*1024), // 100 MB
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect to remote state service: %v\n", err)
	}

	return pb.NewStateCommServiceClient(conn)
}

/******************************************************************************
								 Other Helpers
******************************************************************************/

// Group keys and values by their owner workers
// Return: map[worker ID]map[state ID][][]byte for keys and values
func (s *StateService) groupKeysAndValuesByWorker(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) (
	map[uint16]map[uint16][][]byte,
	map[uint16]map[uint16][][]byte,
) {

	keysByWorker := make(map[uint16]map[uint16][][]byte)
	valuesByWorker := make(map[uint16]map[uint16][][]byte)

	var workerID uint16
	for stateID, keyList := range keys {

		for i, key := range keyList {

			workerID = s.StateLookupTable.BucketIdxToWorkerID(
				bucketIDs[stateID][i],
			)
			workerKeys, ok := keysByWorker[workerID]
			workerValues := valuesByWorker[workerID]
			if !ok {
				workerKeys = make(map[uint16][][]byte)
				keysByWorker[workerID] = workerKeys
				workerValues = make(map[uint16][][]byte)
				valuesByWorker[workerID] = workerValues
			}

			// Append the keys/values to the per worker group
			workerKeys[stateID] = append(workerKeys[stateID], key)
			if vals, ok := values[stateID]; ok {
				workerValues[stateID] = append(workerValues[stateID], vals[i])
			} else {
				log.Fatalf("Values for state ID %d not found\n", stateID)
			}
		}
	}
	return keysByWorker, valuesByWorker
}

// Group keys and bucket IDs by their owner workers. Return:
// - map[worker ID]map[state ID][][]byte for keys
// - map[worker ID][]int for original indices of the keys in the input
// - map[worker ID]map[bucket ID]struct{} for unique bucket IDs
func (s *StateService) groupKeysAndBucketsByWorker(
	keys map[uint16][][]byte,
	bucketIDs []int64,
) (
	map[uint16]map[uint16][][]byte,
	map[uint16][]int,
	map[uint16]map[int64]struct{},
) {

	keysByWorker := make(map[uint16]map[uint16][][]byte)
	indexTracker := make(map[uint16][]int)
	bucketsByWorker := make(map[uint16]map[int64]struct{})

	for i, bucketID := range bucketIDs {

		workerID := s.StateLookupTable.BucketIdxToWorkerID(bucketID)
		wk, ok := keysByWorker[workerID]
		if !ok {
			wk = make(map[uint16][][]byte)
			keysByWorker[workerID] = wk
			indexTracker[workerID] = []int{}
		}

		// Append the keys to the identified worker group and record its
		// original index in the input keys
		for stateID, keyList := range keys {
			wk[stateID] = append(wk[stateID], keyList[i])
		}
		indexTracker[workerID] = append(indexTracker[workerID], i)

		// Group the remote bucket IDs by worker
		if workerID != s.WorkerID {
			bucketSet, ok := bucketsByWorker[workerID]
			if !ok {
				bucketSet = make(map[int64]struct{})
				bucketsByWorker[workerID] = bucketSet
			}
			bucketSet[bucketID] = struct{}{}
		}
	}
	return keysByWorker, indexTracker, bucketsByWorker
}

// Merge remote read results from all workers. Return:
// map[stateID][][]byte of merged values
func (s *StateService) mergeResultsFromAllWorkers(
	keys map[uint16][][]byte,
	resByWorker map[uint16]map[uint16][][]byte,
	indexTracker map[uint16][]int,
) map[uint16][][]byte {

	// Construct the final result map by merging results from all workers
	res := make(map[uint16][][]byte)
	for stateID, keyList := range keys {
		res[stateID] = make([][]byte, len(keyList))
	}
	for workerID, workerRes := range resByWorker {
		indices := indexTracker[workerID]
		for stateID, values := range workerRes {
			for i, v := range values {
				res[stateID][indices[i]] = v
			}
		}
	}
	return res
}
