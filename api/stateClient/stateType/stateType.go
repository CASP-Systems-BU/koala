package stateType

import (
	"reflect"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/network"
)

// StateType interface is exposed to users when defining the dataflow:
// 1. ValueState[V tuple.Tuple]: a single value of type V
// 2. ListState[V tuple.Tuple]: a list of values of type V

type StateType interface {

	// Allocate an empty state instance
	New() StateType

	// Initialize the state by deserializing raw bytes
	Deserialize(network.TupleDecoder, []byte)

	// Serialize the full state into bytes
	Serialize(network.TupleSizer, network.TupleEncoder) []byte

	// [ListState] Serialize the incremental state into bytes
	SerializeInc(network.TupleSizer, network.TupleEncoder) []byte

	// Deep copy from another state instance
	CopyFrom(StateType)

	// Check if the state has been updated for flush
	IsUpdated() bool

	// Check if the state has value (not empty state)
	HasValue() bool

	// User API: Clear the state
	Clear()

	// Check if the state is marked for deletion
	IsDeleted() bool

	// [ListState] Check if the state update contains append
	IsAppended() bool

	// Codec utils
	GetTupleEncoder() network.TupleEncoder
	GetTupleDecoder() network.TupleDecoder
	GetTupleSizer() network.TupleSizer
}

// Common struct for all state types
type StateTypeBase[V tuple.Tuple] struct {

	// Flag to indicate if this is an empty state
	// This is exposed to users when Get() is called to indicate if the state
	// originally has value in state service
	hasValue bool

	// If the state has been updated locally. This is referred by system to
	// identify if to flush it to state service upon Flush()
	updated bool

	// If the state should be deleted from the state service
	// This is set upon Clear(). At Flush(), if this is true, the state will be
	// deleted from state service
	deleted bool
}

/******************************************************************************
							StateType API for users
******************************************************************************/

// Label the state as to be deleted - deletion will be performed at Flush()
func (s *StateTypeBase[V]) Clear() {
	s.deleted = true
}

/******************************************************************************
					    StateType API for internal use
******************************************************************************/

// Check if the state has been updated since initialization
func (s *StateTypeBase[V]) IsUpdated() bool {
	return s.updated
}

// Check if the state update has append. This can only be true for ListState
func (s *StateTypeBase[V]) IsAppended() bool {
	return false
}

// Check if the state has value (not empty state)
func (s *StateTypeBase[V]) HasValue() bool {
	return s.hasValue
}

// Check if the state is marked for deletion
func (s *StateTypeBase[V]) IsDeleted() bool {
	return s.deleted
}

// Create the tuple encoder
func (s *StateTypeBase[V]) GetTupleEncoder() network.TupleEncoder {
	var tuple V
	return network.CreateEncoder(reflect.TypeOf(tuple).Elem())
}

// Create the tuple decoder
func (s *StateTypeBase[V]) GetTupleDecoder() network.TupleDecoder {
	var tuple V
	return network.CreateDecoder(reflect.TypeOf(tuple).Elem())
}

// Create the tuple sizer
func (s *StateTypeBase[V]) GetTupleSizer() network.TupleSizer {
	var tuple V
	return network.CreateSizer(reflect.TypeOf(tuple).Elem())
}
