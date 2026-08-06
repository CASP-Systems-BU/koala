package state

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
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

	// First initialize the result map by workers: Map: worker ID -> state ID
	// -> list of values
	resByWorker := make(map[uint16]map[uint16][][]byte)
	for workerID := range keysByWorker {
		resByWorker[workerID] = make(map[uint16][][]byte)
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

/******************************************************************************
				Helpers to get remote state access connections
******************************************************************************/

// [lazy-basic] Control plane pre-establish bucket migration connection
func (s *StateService) EstablishBucketMigrationConn(
	workerID uint16,
	stateServiceAddr string,
) {

	client := getStateCommRpcClient(stateServiceAddr)
	remoteBucketMigrationStream, err := client.RemoteBucketMigration(
		context.Background(),
	)
	if err != nil {
		log.Fatalf(
			"Failed to establish remote bucket migration stream: %v\n",
			err,
		)
	}
	s.BucketMigrationConnLock.Lock()
	s.BucketMigrationConn[workerID] = remoteBucketMigrationStream
	s.BucketMigrationConnLock.Unlock()
}

// [lazy-basic] Get connection for remote bucket migration
func (s *StateService) getBucketMigrationConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk] {

	s.BucketMigrationConnLock.Lock()
	remoteBucketMigrationStream, ok := s.BucketMigrationConn[workerID]
	s.BucketMigrationConnLock.Unlock()

	if !ok {
		log.Fatalf(
			"Bucket migration connection not found for worker %d\n",
			workerID,
		)
	}
	return remoteBucketMigrationStream
}

// [lazy-no-migration] Control plane pre-establish Read/Overwrite/Merge/Delete
// connections for no-migration protocols
func (s *StateService) EstablishNoMigrationConns(
	workerID uint16,
	stateServiceAddr string,
) {

	client := getStateCommRpcClient(stateServiceAddr)

	// Establish Read connection
	remoteReadStream, err := client.RemoteRead(
		context.Background(),
	)
	if err != nil {
		log.Fatalf("Failed to establish remote read stream: %v\n", err)
	}
	s.ReadConnLock.Lock()
	s.ReadConn[workerID] = remoteReadStream
	s.ReadConnLock.Unlock()

	// Establish Overwrite connection
	remoteOverwriteStream, err := client.RemoteOverwrite(
		context.Background(),
	)
	if err != nil {
		log.Fatalf("Failed to establish remote overwrite stream: %v\n", err)
	}
	s.OverwriteConnLock.Lock()
	s.OverwriteConn[workerID] = remoteOverwriteStream
	s.OverwriteConnLock.Unlock()

	// Establish Merge connection
	remoteMergeStream, err := client.RemoteMerge(
		context.Background(),
	)
	if err != nil {
		log.Fatalf("Failed to establish remote merge stream: %v\n", err)
	}
	s.MergeConnLock.Lock()
	s.MergeConn[workerID] = remoteMergeStream
	s.MergeConnLock.Unlock()

	// Establish Delete connection
	remoteDeleteStream, err := client.RemoteDelete(
		context.Background(),
	)
	if err != nil {
		log.Fatalf("Failed to establish remote delete stream: %v\n", err)
	}
	s.DeleteConnLock.Lock()
	s.DeleteConn[workerID] = remoteDeleteStream
	s.DeleteConnLock.Unlock()
}

// [lazy-no-migration] Get connection for remote read-only by keys
func (s *StateService) getReadConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.ReadRequest, pb.ReadResponse] {

	s.ReadConnLock.Lock()
	remoteReadStream, ok := s.ReadConn[workerID]
	s.ReadConnLock.Unlock()

	if !ok {
		log.Fatalf("Read connection not found for worker %d\n", workerID)
	}
	return remoteReadStream
}

// [lazy-no-migration] Get connection for remote write (overwrite)
func (s *StateService) getOverwriteConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.WriteRequest, pb.Response] {

	s.OverwriteConnLock.Lock()
	remoteOverwriteStream, ok := s.OverwriteConn[workerID]
	s.OverwriteConnLock.Unlock()

	if !ok {
		log.Fatalf("Overwrite connection not found for worker %d\n", workerID)
	}
	return remoteOverwriteStream
}

// [lazy-no-migration] Get connection for remote write (merge)
func (s *StateService) getMergeConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.WriteRequest, pb.Response] {

	s.MergeConnLock.Lock()
	remoteMergeStream, ok := s.MergeConn[workerID]
	s.MergeConnLock.Unlock()

	if !ok {
		log.Fatalf("Merge connection not found for worker %d\n", workerID)
	}
	return remoteMergeStream
}

// [lazy-no-migration] Get connection for remote delete
func (s *StateService) getDeleteConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.DeleteRequest, pb.Response] {

	s.DeleteConnLock.Lock()
	remoteDeleteStream, ok := s.DeleteConn[workerID]
	s.DeleteConnLock.Unlock()

	if !ok {
		log.Fatalf("Delete connection not found for worker %d\n", workerID)
	}
	return remoteDeleteStream
}

// [lazy-optimized] Control plane pre-establish async bucket migration conn
func (s *StateService) EstablishAsyncBucketMigrationConn(
	workerID uint16,
	stateServiceAddr string,
) {

	client := getStateCommRpcClient(stateServiceAddr)

	remoteAsyncBucketMigrationStream, err := client.RemoteAsyncBucketMigration(
		context.Background(),
	)
	if err != nil {
		log.Fatalf(
			"Failed to establish remote async bucket migration stream: %v\n",
			err,
		)
	}
	s.AsyncBucketMigrationConnLock.Lock()
	s.AsyncBucketMigrationConn[workerID] = remoteAsyncBucketMigrationStream
	s.AsyncBucketMigrationConnLock.Unlock()
}

// [lazy-optimized] Get connection for remote async bucket migration
func (s *StateService) getAsyncBucketMigrationConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.LazyOptStateRequest, pb.LazyOptStateResponse] {

	s.AsyncBucketMigrationConnLock.Lock()
	remoteAsyncBucketMigrationStream, ok := s.AsyncBucketMigrationConn[workerID]
	s.AsyncBucketMigrationConnLock.Unlock()

	if !ok {
		log.Fatalf(
			"Async bucket migration connection not found for worker %d\n",
			workerID,
		)
	}
	return remoteAsyncBucketMigrationStream
}

// [lazy-by-key] Control plane pre-establish by-key migration connection
func (s *StateService) EstablishByKeyMigrationConn(
	workerID uint16,
	stateServiceAddr string,
) {

	client := getStateCommRpcClient(stateServiceAddr)

	remoteByKeyMigrationStream, err := client.RemoteByKeyMigration(
		context.Background(),
	)
	if err != nil {
		log.Fatalf(
			"Failed to establish remote by-key migration stream: %v\n",
			err,
		)
	}
	s.ByKeyMigrationConnLock.Lock()
	s.ByKeyMigrationConn[workerID] = remoteByKeyMigrationStream
	s.ByKeyMigrationConnLock.Unlock()
}

// [lazy-by-key] Get connection for remote by-key migration
func (s *StateService) getByKeyMigrationConn(
	workerID uint16,
) grpc.BidiStreamingClient[pb.LazyByKeyStateRequest, pb.KeyResponse] {

	s.ByKeyMigrationConnLock.Lock()
	remoteByKeyMigrationStream, ok := s.ByKeyMigrationConn[workerID]
	s.ByKeyMigrationConnLock.Unlock()

	if !ok {
		log.Fatalf(
			"By-key migration connection not found for worker %d\n",
			workerID,
		)
	}
	return remoteByKeyMigrationStream
}

// [lazy-by-key][TCP] Control plane pre-establish by-key migration connection
func (s *StateService) EstablishByKeyMigrationTcpConn(
	workerID uint16,
	stateServiceAddr string,
) {

	conn, err := net.Dial("tcp", stateServiceAddr)
	if err != nil {
		log.Fatalf(
			"Failed to establish TCP connection for by-key migration: %v\n",
			err,
		)
	}
	buf := make([]byte, constant.TcpMaxMessageSize)
	var additionalBuf []byte
	if s.Config.LazyByKeyCancellingTaskMigrationMode == "eventual" {
		additionalBuf = make([]byte, constant.TcpMaxMessageSize)
	}

	tcpConn := &LazyByKeyTcpConn{
		Conn:          conn,
		Buf:           buf,
		AdditionalBuf: additionalBuf,
	}

	s.ByKeyMigrationTcpConnLock.Lock()
	s.ByKeyMigrationTcpConn[workerID] = tcpConn
	s.ByKeyMigrationTcpConnLock.Unlock()
}

// [lazy-by-key][TCP] Get connection for remote by-key migration
func (s *StateService) getByKeyMigrationTcpConn(
	workerID uint16,
) *LazyByKeyTcpConn {

	s.ByKeyMigrationTcpConnLock.Lock()
	conn, ok := s.ByKeyMigrationTcpConn[workerID]
	s.ByKeyMigrationTcpConnLock.Unlock()

	if !ok {
		log.Fatalf(
			"By-key migration TCP connection not found for worker %d\n",
			workerID,
		)
	}
	return conn
}

// Helper function to get gRPC client for remote state comm service
func getStateCommRpcClient(
	remoteStateServiceAddr string,
) pb.StateCommServiceClient {

	conn, err := grpc.NewClient(
		remoteStateServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(constant.RpcMaxMessageSize),
			grpc.MaxCallSendMsgSize(constant.RpcMaxMessageSize),
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

// [lazy-no-migration] Group keys and values by their owner workers
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

// [lazy-opt] Group keys and bucket IDs by their owner workers. Return:
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

// [lazy-by-key] Group keys by their owner workers. Return:
// - map[worker ID]map[state ID][][]byte for keys
// - map[worker ID]map[state ID][]int for original input indices of the keys
// The difference from lazy-opt is that now multi-states of the same key may map
// to different workers, so we need to determine the worker ID for each key and
// maintain the index tracker separately
func (s *StateService) groupKeysByWorker(
	keys map[uint16][][]byte,
	bucketIDs []int64,
) (
	map[uint16]map[uint16][][]byte,
	map[uint16]map[uint16][]int,
) {

	keysByWorker := make(map[uint16]map[uint16][][]byte)
	indexTracker := make(map[uint16]map[uint16][]int)

	var workerID uint16
	var ok bool
	var workerKeys map[uint16][][]byte
	var workerTracker map[uint16][]int
	for stateID, keyList := range keys {
		for i, key := range keyList {

			// Determine the owner worker for this key
			workerID = s.StateLookupTableV2.GetWorkerID(key, bucketIDs[i])

			workerKeys = keysByWorker[workerID]
			workerTracker, ok = indexTracker[workerID]
			if !ok {
				workerKeys = make(map[uint16][][]byte)
				keysByWorker[workerID] = workerKeys
				workerTracker = make(map[uint16][]int)
				indexTracker[workerID] = workerTracker
			}

			// Append the key to the identified worker group and record its
			// original index in the input keys
			workerKeys[stateID] = append(workerKeys[stateID], key)
			workerTracker[stateID] = append(workerTracker[stateID], i)
		}
	}
	return keysByWorker, indexTracker
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

// [lazy-by-key] Merge remote fetch results from all workers. Return:
// map[stateID][][]byte of merged values
func (s *StateService) mergeResultsFromAllWorkersLazyByKey(
	keys map[uint16][][]byte,
	resByWorker map[uint16]map[uint16][][]byte,
	indexTrackers map[uint16]map[uint16][]int,
) map[uint16][][]byte {

	// Construct the final result map by merging results from all workers
	res := make(map[uint16][][]byte)
	for stateID, keyList := range keys {
		res[stateID] = make([][]byte, len(keyList))
	}
	for workerID, workerRes := range resByWorker {
		indexTracker := indexTrackers[workerID]
		for stateID, values := range workerRes {
			indices := indexTracker[stateID]
			for i, v := range values {
				res[stateID][indices[i]] = v
			}
		}
	}
	return res
}
