package stateType

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

type ValueState[V tuple.Tuple] struct {
	*StateTypeBase[V]

	state V
}

var _ StateType = (*ValueState[tuple.Tuple])(nil)

// Allocate a new ValueState with initial value
func NewValueState[V tuple.Tuple](val V) *ValueState[V] {
	return &ValueState[V]{
		StateTypeBase: &StateTypeBase[V]{
			hasValue: true,
			// Flag updated to true because this is a newly created state and
			// needs to be flushed to state service
			updated: true,
		},
		state: val,
	}
}

/******************************************************************************
				ValueState API for users (not interface method)
******************************************************************************/

func (v *ValueState[V]) Get() (V, bool) {

	// This is an empty state
	if !v.hasValue {
		var zero V
		return zero, false
	}
	return v.state, true
}

func (v *ValueState[V]) Set(value V) {
	v.state = value
	v.hasValue = true
	v.updated = true
}

/******************************************************************************
			   			    Implement StateType interface
******************************************************************************/

// Allocate an empty ValueState
func (v *ValueState[V]) New() StateType {
	return &ValueState[V]{
		StateTypeBase: &StateTypeBase[V]{},
	}
}

// Set the ValueState by deserializing the given bytes. The decoder function is
// also passed in as a parameter since it is maintained in StateCache
func (v *ValueState[V]) Deserialize(
	tupleDecoder network.TupleDecoder,
	stateInBytes []byte,
) {
	var tuple V
	tuple = tuple.New().(V)
	_, err := tupleDecoder(tuple, stateInBytes)
	if err != nil {
		log.Fatalf("Failed to decode value: %v", err)
	}
	v.state = tuple
	v.hasValue = true
}

// Serialize the ValueState into bytes. The sizer and encoder functions are
// passed in as parameters since they are maintained in StateCache.
func (v *ValueState[V]) Serialize(
	tupleSizer network.TupleSizer,
	tupleEncoder network.TupleEncoder,
) []byte {
	buf := make([]byte, tupleSizer(v.state))
	tupleEncoder(v.state, buf)
	return buf
}

// Dummy implementation since ValueState does not support incremental
func (v *ValueState[V]) SerializeInc(
	tupleSizer network.TupleSizer,
	tupleEncoder network.TupleEncoder,
) []byte {

	log.Fatalln("ValueState does not support incremental serialization")
	return nil
}

// Deep copy from another ValueState instance
func (v *ValueState[V]) CopyFrom(other StateType) {
	otherValueState, ok := other.(*ValueState[V])
	if !ok {
		log.Fatalln("CopyFrom: other is not ValueState")
	}
	v.state = otherValueState.state
	v.hasValue = otherValueState.hasValue
	v.updated = otherValueState.updated
	v.deleted = otherValueState.deleted
}
