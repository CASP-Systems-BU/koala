package stateCache

import (
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/internal/network"
)

/*
Interface for state cache. A StateCache corresponds to a state defined in the
stateful operator (multi-state operator will have multiple StateCache).
StateCache is used to cache state in the operator space (memory) to avoid
frequent StateService access.

We have 2 state cache implementations:
- SimpleStateCache: regular key to value mapping
- WindowStateCache: nested mapping from key to (timestamp to value)
*/

type StateCache[K comparable] interface {

	// Setup at task placement time. Input: if allow incremental serialization
	// at state flush (this is only needed for ListState with append-only)
	Setup(bool)

	// Clear the cache: reset the cache map to nil.
	// We adopt per-batch cache. CleanUp() is used to clear the cache before
	// prefetch for the new batch - this is to avoid mistakly access outdated
	// cache.
	CleanUp()

	// Check if the cache is unset. Safety check: raise error if accessing
	// unset cache.
	IsUnset() bool

	/**************************************************************************
							 APIs for SimpleStateCache
	**************************************************************************/

	// Set cache with pre-fetched serialized values.
	SetSimpleStateCache([]K, [][]byte)

	// Identify state in the cache that needs to be flushed to StateService
	// (updated since last fetch). This includes both updated states and states
	// marked for deletion. Return:
	// 1. Serialized keys to write to StateService
	// 2. Bucket IDs of the keys to write to StateService
	// 3. Serialized values to write to StateService
	// 4. Serialized keys to merge to StateService
	// 5. Bucket IDs of the keys to merge to StateService
	// 6. Serialized values to merge to StateService
	// 7. Serialized keys to be deleted from StateService
	// 8. Bucket IDs of the keys to be deleted from StateService
	GetSimpleStateFlushData(
		[]K,
		[][]byte,
		[]int64,
	) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64)

	// Point query: get the state value for a key from the cache. There must
	// exist the requested key though it could be an empty state.
	GetSimpleState(K) any

	/**************************************************************************
							 APIs for WindowStateCache
	**************************************************************************/

	// Set cache with pre-fetched serialized values.
	SetWindowStateCache([]K, []int64, [][]byte)

	// Identify state in the cache that needs to be flushed to StateService
	// (updated since last fetch). This includes both updated states and states
	// marked for deletion. Return:
	// 1. Serialized keys to write to StateService
	// 2. Bucket IDs of the keys to write to StateService
	// 3. Serialized values to write to StateService
	// 4. Serialized keys to merge to StateService
	// 5. Bucket IDs of the keys to merge to StateService
	// 6. Serialized values to merge to StateService
	// 7. Serialized keys to be deleted from StateService
	// 8. Bucket IDs of the keys to be deleted from StateService
	GetWindowStateFlushData(
		[]K,
		[]int64,
		[][]byte,
		[]int64,
	) ([][]byte, []int64, [][]byte, [][]byte, []int64, [][]byte, [][]byte, []int64)

	// Point query: get the state value for a window (key/windowStartTime pair)
	// from the cache. There must exist the requested key though it could be an
	// empty state.
	GetWindowState(K, int64) any
}

// Common struct for both SimpleStateCache and WindowStateCache.
type StateCacheBase[V stateType.StateType] struct {

	// Allow incremental serialization for state flush. This is only used for
	// ListState with append operation. If enabled and ListState update is
	// append-only, we only serialize the newly appended values for flush.
	allowIncrementalSerialization bool

	// Every StateCache can have different state types. We maintain Tuple
	// codec functions for each StateCache.
	valueEncoder network.TupleEncoder
	valueDecoder network.TupleDecoder
	valueSizer   network.TupleSizer
}

func newStateCacheBase[V stateType.StateType]() *StateCacheBase[V] {
	return &StateCacheBase[V]{}
}

/******************************************************************************
						      Common helper utils
******************************************************************************/

// Setup at runtime (task placement time) - init codec
func (s *StateCacheBase[V]) setup(allowIncSerialization bool) {

	var v V
	v = v.New().(V)
	s.valueEncoder = v.GetTupleEncoder()
	s.valueDecoder = v.GetTupleDecoder()
	s.valueSizer = v.GetTupleSizer()
	s.allowIncrementalSerialization = allowIncSerialization
}

// Allocate a new state value and deserialize the serializedValue into it.
func (s *StateCacheBase[V]) deserializeValue(serializedValue []byte) V {
	var value V
	value = value.New().(V)

	// Note that the fetched value bytes can be nil or empty:
	// nil: Local read returns nil for non-existing key
	// empty []byte: Remote read returns empty []byte for non-existing key
	if len(serializedValue) > 0 {
		value.Deserialize(s.valueDecoder, serializedValue)
	}
	return value
}
