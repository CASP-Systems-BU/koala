package stateCache

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
)

type SimpleStateCache[K comparable, V stateType.StateType] struct {
	*StateCacheBase[V]

	cache map[K]V
}

var _ StateCache[any] = (*SimpleStateCache[any, stateType.StateType])(
	nil,
)

// Init at job graph construction time
func NewSimpleStateCache[K comparable, V stateType.StateType]() *SimpleStateCache[K, V] {
	return &SimpleStateCache[K, V]{
		StateCacheBase: newStateCacheBase[V](),
	}
}

/******************************************************************************
						  Implement StateCache interface
******************************************************************************/

// Setup at runtime (task placement time) - init codec
func (s *SimpleStateCache[K, V]) Setup(allowIncSerialization bool) {
	s.StateCacheBase.setup(allowIncSerialization)
}

// Clear the cache
func (s *SimpleStateCache[K, V]) CleanUp() {
	s.cache = nil
}

// Check if the cache is unset
func (s *SimpleStateCache[K, V]) IsUnset() bool {
	return s.cache == nil
}

// Reset the cache with fetched values.
// 1. Hard reset the cache - cache is used within the scope of a batch
// 2. Deserialize the fetched values and populate the cache
func (s *SimpleStateCache[K, V]) SetSimpleStateCache(
	keys []K,
	serializedValues [][]byte,
) {

	// Hard reset the cache
	s.cache = make(map[K]V)

	for i, key := range keys {
		stateVal := s.deserializeValue(serializedValues[i])
		s.cache[key] = stateVal
	}
}

// Identify state in the cache that needs to be flushed to StateService
// (updated since last fetch). This includes both updated states and states
// marked for deletion. Return:
// 1. Serialized keys to write to StateService
// 2. Bucket IDs of the keys to write to StateService (match to the keys above)
// 3. Serialized values to write to StateService (match to the keys above)
// 4. Serialized keys to merge to StateService
// 5. Bucket IDs of the keys to merge to StateService (match to the keys above)
// 6. Serialized values to merge to StateService (match to the keys above)
// 7. Serialized keys to be deleted from StateService
// 8. Bucket IDs of the keys to be deleted from StateService (match to the keys
// above)
func (s *SimpleStateCache[K, V]) GetSimpleStateFlushData(
	keys []K,
	serializedKeys [][]byte,
	bucketIDs []int64,
) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64) {
	if len(keys) != len(serializedKeys) {
		log.Fatalln("keys and serializedKeys must have the same length")
	}

	// Keys and values to write
	serializedKeysToWrite := make([][]byte, 0, len(keys))
	bucketIDsToWrite := make([]int64, 0, len(keys))
	serializedValuesToWrite := make([][]byte, 0, len(keys))

	// Keys and values to merge
	serializedKeysToMerge := make([][]byte, 0, len(keys))
	bucketIDsToMerge := make([]int64, 0, len(keys))
	serializedValuesToMerge := make([][]byte, 0, len(keys))

	// Keys to delete
	serializedKeysToDelete := make([][]byte, 0, len(keys))
	bucketIDsToDelete := make([]int64, 0, len(keys))

	var val V
	var serializedValue []byte
	for i, key := range keys {
		val = s.cache[key]

		if !val.HasValue() {

			// This is an empty state and hasn't been updated since fetch
			continue
		} else if val.IsDeleted() {

			// Mark this key for deletion no matter if it has been updated
			serializedKeysToDelete = append(
				serializedKeysToDelete,
				serializedKeys[i],
			)
			bucketIDsToDelete = append(bucketIDsToDelete, bucketIDs[i])
		} else if val.IsUpdated() || (!s.allowIncrementalSerialization && val.IsAppended()) {

			// Serialize the full state and overwrite in 2 cases:
			// 1. State is explicitly updated
			// 2. State is appended but incremental serialization is disabled
			serializedKeysToWrite = append(
				serializedKeysToWrite,
				serializedKeys[i],
			)
			bucketIDsToWrite = append(bucketIDsToWrite, bucketIDs[i])
			serializedValue = val.Serialize(
				s.valueSizer,
				s.valueEncoder,
			)
			serializedValuesToWrite = append(
				serializedValuesToWrite,
				serializedValue,
			)

		} else if val.IsAppended() {

			// Only serialize the incremental update and merge in state service
			serializedKeysToMerge = append(
				serializedKeysToMerge,
				serializedKeys[i],
			)
			bucketIDsToMerge = append(bucketIDsToMerge, bucketIDs[i])
			serializedValue = val.SerializeInc(
				s.valueSizer,
				s.valueEncoder,
			)
			serializedValuesToMerge = append(
				serializedValuesToMerge,
				serializedValue,
			)
		}
	}
	return serializedKeysToWrite,
		bucketIDsToWrite,
		serializedValuesToWrite,
		serializedKeysToMerge,
		bucketIDsToMerge,
		serializedValuesToMerge,
		serializedKeysToDelete,
		bucketIDsToDelete
}

// Get the state by key. The key must be present in the cache even though it
// could be an empty state - SetSimpleStateCache init an empty state for
// non-existing keys
func (s *SimpleStateCache[K, V]) GetSimpleState(key K) any {

	val, ok := s.cache[key]
	if !ok {
		log.Fatalln(
			"[Simple state cache] All requested key must exist in cache - they could be empty state",
		)
	}
	return val
}

/******************************************************************************
				  Dummy API implementation for WindowStateCache
******************************************************************************/

func (s *SimpleStateCache[K, V]) SetWindowStateCache(
	keys []K,
	timestamps []int64,
	serializedValues [][]byte,
) {
	log.Fatalln("SetWindowStateCache not implemented")
}

func (s *SimpleStateCache[K, V]) GetWindowStateFlushData(
	keys []K,
	timestamps []int64,
	serializedValues [][]byte,
	bucketIDs []int64,
) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64) {
	log.Fatalln("GetWindowStateFlushData not implemented")
	return nil, nil, nil, nil, nil, nil, nil, nil
}

func (s *SimpleStateCache[K, V]) GetWindowState(
	key K,
	timestamp int64,
) any {
	log.Fatalln("GetWindowState not implemented")
	return nil
}
