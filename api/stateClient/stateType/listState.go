package stateType

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

type ListState[V tuple.Tuple] struct {
	*StateTypeBase[V]

	state []V

	// Flag to indicate if the state list is appended only.
	// The flags (i) updated, and (ii) appended collectively determine how to
	// flush the state to state service. Flag updated only indicate non-append
	// operations and the full state needs to be overwritten in state service.
	// If updated is false but appended is true, we only need to append the new
	// values to state service - where Pebble Merge operation can be used.
	appended bool

	// Track the incremental appended values since last fetch to determine what
	// to append to state service at Flush(). This is only used when appended is
	// true and updated is false. It stores the index from which the new
	// values are appended since last fetch: state[appendStartIdx:]
	// For example, if the state list is [1,2,3] at last fetch, and user appends
	// [4,5], the state list becomes [1,2,3,4,5], and appendStartIdx is 3.
	appendStartIdx int
}

var _ StateType = (*ListState[tuple.Tuple])(nil)

// Allocate a new ListState with initial list value
func NewListState[V tuple.Tuple](vals []V) *ListState[V] {

	if vals == nil {
		log.Fatalln("ListState must be initialized with non-nil list")
	}

	return &ListState[V]{
		StateTypeBase: &StateTypeBase[V]{
			hasValue: true,
			// Flag updated to true because this is a newly created state and
			// needs to be flushed to state service
			updated: true,
		},
		state:          vals,
		appendStartIdx: len(vals),
	}
}

/******************************************************************************
				ListState API for users (not interface method)
******************************************************************************/

// Get the full list. If the ListState hasValue is false, it contains an empty
// list. Directly return the list in any case
func (v *ListState[V]) Get() []V {
	return v.state
}

// Update the state by overwriting the full list
func (v *ListState[V]) Update(vals []V) {
	v.state = vals
	v.hasValue = true
	v.updated = true
}

// Notes for append APIs below: If user directly append elements to the list
// returned by Get(), Update() must be called to flush the changes with full
// list overwrite. To enable incremental append updates without overwrite,
// users must use the Add() or AddAll() APIs below

// Append single to the state list
func (v *ListState[V]) Add(val V) {
	v.state = append(v.state, val)
	v.hasValue = true
	v.appended = true
}

// Append multiple values to the state list
func (v *ListState[V]) AddAll(vals []V) {
	v.state = append(v.state, vals...)
	v.hasValue = true
	v.appended = true
}

/******************************************************************************
			   			    Implement StateType interface
******************************************************************************/

// Allocate an empty ListState. The default value for empty ListState is an
// empty list instead of nil
func (v *ListState[V]) New() StateType {

	return &ListState[V]{
		StateTypeBase:  &StateTypeBase[V]{},
		state:          []V{},
		appendStartIdx: 0,
	}
}

// Check if the state update contains append
func (v *ListState[V]) IsAppended() bool {
	return v.appended
}

// Set the ListState by deserializing the given bytes. The decoder function is
// also passed in as a parameter since it is maintained in StateCache
func (v *ListState[V]) Deserialize(
	tupleDecoder network.TupleDecoder,
	stateInBytes []byte,
) {

	// Keep decoding tuples and append to state list
	currPos := 0
	for currPos < len(stateInBytes) {
		var tuple V
		tuple = tuple.New().(V)
		usedBytes, err := tupleDecoder(tuple, stateInBytes[currPos:])
		if err != nil {
			log.Fatalln(
				"Failed to decode ListState:",
				err,
				", at position",
				currPos,
				", total bytes",
				len(stateInBytes),
			)
		}
		v.state = append(v.state, tuple)
		currPos += usedBytes
	}

	v.hasValue = true
	v.appendStartIdx = len(v.state)
}

// Serialize the full ListState into bytes. The sizer and encoder functions are
// passed in as parameters since they are maintained in StateCache
func (v *ListState[V]) Serialize(
	tupleSizer network.TupleSizer,
	tupleEncoder network.TupleEncoder,
) []byte {
	return v.serializeFromIndex(0, tupleSizer, tupleEncoder)
}

// Serialize the incremental ListState into bytes. This is only used when the
// state update is append-only (i.e., appended is true and updated is false).
func (v *ListState[V]) SerializeInc(
	tupleSizer network.TupleSizer,
	tupleEncoder network.TupleEncoder,
) []byte {
	return v.serializeFromIndex(v.appendStartIdx, tupleSizer, tupleEncoder)
}

// Deep copy from another ListState instance
func (v *ListState[V]) CopyFrom(other StateType) {
	otherListState, ok := other.(*ListState[V])
	if !ok {
		log.Fatalln("CopyFrom: other is not ListState")
	}
	v.state = otherListState.state
	v.hasValue = otherListState.hasValue
	v.updated = otherListState.updated
	v.deleted = otherListState.deleted
	v.appended = otherListState.appended
	v.appendStartIdx = otherListState.appendStartIdx
}

/******************************************************************************
			   				   Utils for ListState
******************************************************************************/

// Serialize the state list from the given start index to the end of list
func (v *ListState[V]) serializeFromIndex(
	startIdx int,
	tupleSizer network.TupleSizer,
	tupleEncoder network.TupleEncoder,
) []byte {

	// Reject invalid startIdx -  we do not allow serialize empty list
	if startIdx >= len(v.state) {
		log.Fatalf("[ListState] startIdx %d >= len(state) %d\n",
			startIdx, len(v.state))
	}

	// Calculate the total buffer size and allocate buffer
	totalSize := 0
	for _, val := range v.state[startIdx:] {
		totalSize += tupleSizer(val)
	}
	buf := make([]byte, totalSize)

	// Serialize the values one by one
	currPos := 0
	for _, val := range v.state[startIdx:] {
		usedBytes := tupleEncoder(val, buf[currPos:])
		currPos += usedBytes
	}
	return buf
}
