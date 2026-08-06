package state

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
)

/******************************************************************************
			Exposed State Service APIs for local task state access
******************************************************************************/

// Now the task employs per-batch caching, so state is fetched and flushed in
// batch. Current APIs are oriented towards batch operations.
// WARNING: All APIs are NOT thread-safe. Local caller should ensure no
// concurrent access to the same StateService instance API (this is not related
// to StateCommAPIs - which allows concurrency).

/*
Request state from StateService.
Input parameters:
  - operatorID: which operator state this request is for
  - keys: each operator can hold multiple states, keys is a map of state IDs
    to the serialized keys to fetch. Note that the serialized keys for multiple
    states have the same fields except the state id:
    | op_id | bucket_id | state_id (different) | key |
  - bucketIDs: the bucket IDs corresponding to the keys - this is to avoid
    recomputation of bucket IDs for serialized keys (they are already computed
    in StateClient)

Return values:
  - a map of state IDs to the fetched serialized values
*/
func (s *StateService) GetManyMultiState(
	operatorID uint16,
	keys map[uint16][][]byte,
	bucketIDs []int64,
) map[uint16][][]byte {

	// TODO: operatorID is not used for now since we assume local StateService
	// only holds state for the local deployed operator. Future PR will make
	// this generalized - StateLookupTable is organized per operator.

	start := time.Now()

	// If lazy protocol is used, first fetch the remote state if applied
	if s.Config.ReconfigProtocol == "lazy" {

		switch s.Config.LazyProtocolVersion {
		case "basic":

			// Fetch remote state at bucket granularity (synchronously) and
			// update the ownership in StateLookupTable
			s.migrateRemoteBuckets(operatorID, bucketIDs)

		case "by-key":

			// Block if there are ongoing background key flush and key lookup
			// table update
			s.ByKeyMigrationWaitGroup.Wait()

			// Fetch and migrate requested keys from remote state services. It
			// immediately returns after all requested keys are fetched into
			// memory and async flush to state backend
			a := s.remoteReadWithByKeyMigration(
				keys,
				bucketIDs,
			)

			duration := time.Since(start)
			s.MetricCollector.UpdateGetManyTime(duration)

			return a

		case "optimized":

			// For lazy-optimized, if there exists background bucket migration
			// in progress, we block any new state access until it is finished
			s.AsyncMigrationWaitGroup.Wait()

			// Read state for requested keys with async bucket migration. It
			// immediately returns when all requested keys are fetched and
			// leaves the bucket migration in background
			return s.remoteReadWithAsyncBucketMigration(
				operatorID,
				keys,
				bucketIDs,
			)
		case "no-migration":

			// Read state for requested keys without bucket migration. Do not
			// update ownership in StateLookupTable
			return s.remoteRead(keys, bucketIDs)
		default:
			log.Fatalf(
				"Unsupported lazy protocol version: %s\n",
				s.Config.LazyProtocolVersion,
			)
		}
	}

	// Now all requested keys are migrated to local - get values from local
	// state backend
	res := make(map[uint16][][]byte)
	s.readLocalState(keys, res)

	duration := time.Since(start)

	s.MetricCollector.UpdateGetManyTime(duration)

	return res
}

// Write to local state backend - overwrite existing values
func (s *StateService) SetManyMultiState(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	start := time.Now()

	if len(keys) == 0 {
		log.Fatalln("No keys to set")
	}

	if s.Config.ReconfigProtocol == "lazy" {

		switch s.Config.LazyProtocolVersion {
		case "no-migration":
			// For lazy-no-migration, we need to flush the state back to the
			// remote state service if the key not belong to the local worker
			s.remoteOverwrite(keys, values, bucketIDs)
			return
		case "optimized":
			// For lazy-optimized, if there exists background bucket migration
			// in progress, we block any new state access until it is finished
			s.AsyncMigrationWaitGroup.Wait()
		case "by-key":
			// For lazy-by-key, if there exists ongoing background key flush and
			// key lookup table update, we block any new state access until it
			// is finished
			s.ByKeyMigrationWaitGroup.Wait()

			// Update or insert the keys in the per-key based key lookup table
			s.ByKeyLookupTableUpdateWaitGroup.Go(func() {
				s.insertOrUpdateAddrForLocalKeys(keys, bucketIDs)
			})
			s.overwriteLocalState(keys, values)
			s.ByKeyLookupTableUpdateWaitGroup.Wait()
			duration := time.Since(start)
			s.MetricCollector.UpdateSetManyTime(duration)
			return
		}
	}

	// Write to local state backend for each state
	s.overwriteLocalState(keys, values)

	duration := time.Since(start)
	s.MetricCollector.UpdateSetManyTime(duration)
}

// Merge values to local state backend - for ListState with append-only
func (s *StateService) MergeManyMultiState(
	keys map[uint16][][]byte,
	values map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	if len(keys) == 0 {
		log.Fatalln("No keys to merge")
	}
	start := time.Now()

	if s.Config.ReconfigProtocol == "lazy" {

		switch s.Config.LazyProtocolVersion {
		case "no-migration":
			// For lazy-no-migration, we need to merge to the remote state
			// service if the key does not belong to the local worker
			s.remoteMerge(keys, values, bucketIDs)
			return
		case "optimized":
			// For lazy-optimized, if there exists background bucket migration
			// in progress, we block any new state access until it is finished
			s.AsyncMigrationWaitGroup.Wait()
		case "by-key":
			// For lazy-by-key, if there exists ongoing background key flush and
			// key lookup table update, we block any new state access until it
			// is finished
			s.ByKeyMigrationWaitGroup.Wait()

			// Update or insert the keys in the per-key based key lookup table
			s.ByKeyLookupTableUpdateWaitGroup.Go(func() {
				s.insertOrUpdateAddrForLocalKeys(keys, bucketIDs)
			})
			s.mergeLocalState(keys, values)
			s.ByKeyLookupTableUpdateWaitGroup.Wait()
			duration := time.Since(start)
			s.MetricCollector.UpdateSetManyTime(duration)
			return
		}
	}

	// Merge to local state backend for each state
	s.mergeLocalState(keys, values)
}

// Delete keys from the local state backend
func (s *StateService) DeleteMany(
	keys map[uint16][][]byte,
	bucketIDs map[uint16][]int64,
) {

	if len(keys) == 0 {
		log.Fatalln("No keys to delete")
	}

	if s.Config.ReconfigProtocol == "lazy" {

		switch s.Config.LazyProtocolVersion {
		case "no-migration":
			// For lazy-no-migration, we need to delete from the remote state
			// service if the key does not belong to the local worker
			s.remoteDelete(keys, bucketIDs)
			return
		case "optimized":
			// For lazy-optimized, if there exists background bucket migration
			// in progress, we block any new state access until it is finished
			s.AsyncMigrationWaitGroup.Wait()
		case "by-key":
			// For lazy-by-key, if there exists ongoing background key flush and
			// key lookup table update, we block any new state access until it
			// is finished

			s.ByKeyMigrationWaitGroup.Wait()

			// Update the per-key based key lookup table to remove deleted keys
			s.ByKeyLookupTableUpdateWaitGroup.Go(func() {
				s.deleteKeysFromLookupTable(keys, bucketIDs)
			})
			s.deleteLocalState(keys)
			s.ByKeyLookupTableUpdateWaitGroup.Wait()
			return
		}
	}

	// Delete from local state backend for each state
	s.deleteLocalState(keys)
}

// [stop-and-restart] Set values for multiple keys to local state backend.
// SetMany() always write to local state backend. This API is currently only
// used for state migration under stop-and-restart protocol
func (s *StateService) SetMany(keys [][]byte, values [][]byte) {

	if len(keys) == 0 {
		return
	}

	s.StateBackendImpl.SetMany(keys, values)
}

/******************************************************************************
			    State Service APIs used by State Comm Server
******************************************************************************/

// Read all keys and values for a range of buckets using RangeQuery
func (s *StateService) ReadLocalBucketRange(
	lowerBucketIdx int64,
	upperBucketIdx int64,
) ([][]byte, [][]byte) {

	// The boundaries of the bucket should include operator ID and bucket Idx
	bufSize := constant.OperatorIDSize + constant.BucketIdxSize

	lower := make([]byte, bufSize)
	binary.BigEndian.PutUint16(lower, s.OperatorID)
	binary.BigEndian.PutUint32(
		lower[constant.OperatorIDSize:],
		uint32(lowerBucketIdx),
	)

	higher := make([]byte, bufSize)
	binary.BigEndian.PutUint16(higher, s.OperatorID)
	binary.BigEndian.PutUint32(
		higher[constant.OperatorIDSize:],
		uint32(upperBucketIdx+1),
	)

	keys, values := s.StateBackendImpl.RangeQuery(lower, higher)
	return keys, values
}
