package stateCache

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
)

type WindowStateCache[K comparable, V stateType.StateType] struct {
	*StateCacheBase[V]

	cache map[K]map[int64]V
}

var _ StateCache[any] = (*WindowStateCache[any, stateType.StateType])(
	nil,
)

// Init at job graph construction time
func NewWindowStateCache[K comparable, V stateType.StateType]() *WindowStateCache[K, V] {
	return &WindowStateCache[K, V]{
		StateCacheBase: newStateCacheBase[V](),
	}
}

/******************************************************************************
						  Implement StateCache interface
******************************************************************************/

// Setup at runtime (task placement time) - init codec
func (s *WindowStateCache[K, V]) Setup(allowIncSerialization bool) {
	s.StateCacheBase.setup(allowIncSerialization)
}

// Clear the cache
func (s *WindowStateCache[K, V]) CleanUp() {
	s.cache = nil
}

// Check if the cache is unset
func (s *WindowStateCache[K, V]) IsUnset() bool {
	return s.cache == nil
}

// Reset the cache with fetched values.
// 1. Hard reset the cache - cache is used within the scope of a batch
// 2. Deserialize the fetched values and populate the cache
func (s *WindowStateCache[K, V]) SetWindowStateCache(
	keys []K,
	timestamps []int64,
	serializedValues [][]byte,
) {

	// Hard reset the cache
	s.cache = make(map[K]map[int64]V)

	for i, key := range keys {
		stateVal := s.deserializeValue(serializedValues[i])

		if windows, ok := s.cache[key]; ok {
			windows[timestamps[i]] = stateVal
		} else {
			s.cache[key] = make(map[int64]V)
			s.cache[key][timestamps[i]] = stateVal
		}
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
func (s *WindowStateCache[K, V]) GetWindowStateFlushData(
	keys []K,
	timestamps []int64,
	serializedKeys [][]byte,
	bucketIDs []int64,
) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64) {

	if len(keys) != len(timestamps) || len(timestamps) != len(serializedKeys) {
		log.Fatalln(
			"keys, timestamps, and serializedKeys must have the same length",
		)
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
		val = s.cache[key][timestamps[i]]

		if !val.HasValue() {

			// This is an empty state and hasn't been updated since fetch
			continue
		} else if val.IsDeleted() {

			// Mark this key for deletion no matter if it has been updated
			serializedKeysToDelete = append(
				serializedKeysToDelete,
				serializedKeys[i],
			)
			bucketIDsToDelete = append(
				bucketIDsToDelete,
				bucketIDs[i],
			)
		} else if val.IsUpdated() || (!s.allowIncrementalSerialization && val.IsAppended()) {

			// Serialize the full state and overwrite in 2 cases:
			// 1. State is explicitly updated
			// 2. State is appended but incremental serialization is disabled
			serializedKeysToWrite = append(
				serializedKeysToWrite,
				serializedKeys[i],
			)
			bucketIDsToWrite = append(
				bucketIDsToWrite,
				bucketIDs[i],
			)
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
			bucketIDsToMerge = append(
				bucketIDsToMerge,
				bucketIDs[i],
			)
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

// Get the state by key and window start time. The key must be present in the
// cache even though it could be an empty state - SetWindowStateCache init an
// empty state for non-existing keys
func (s *WindowStateCache[K, V]) GetWindowState(
	key K,
	timestamp int64,
) any {

	if windows, ok := s.cache[key]; ok {
		if val, ok := windows[timestamp]; ok {
			return val
		}
	}
	log.Fatalln(
		"[Window state cache] All requested key must exist in cache - they could be empty state",
	)
	return nil
}

/******************************************************************************
				  Dummy API implementation for SimpleStateCache
******************************************************************************/

func (s *WindowStateCache[K, V]) SetSimpleStateCache(
	keys []K,
	serializedValues [][]byte,
) {
	log.Fatalln("SetSimpleStateCache not implemented")
}

func (s *WindowStateCache[K, V]) GetSimpleStateFlushData(
	keys []K,
	serializedKeys [][]byte,
	bucketIDs []int64,
) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64) {
	log.Fatalln("GetSimpleStateFlushData not implemented")
	return nil, nil, nil, nil, nil, nil, nil, nil
}

func (s *WindowStateCache[K, V]) GetSimpleState(key K) any {
	log.Fatalln("GetSimpleState not implemented")
	return nil
}
