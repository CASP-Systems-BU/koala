package state

// Type of state:
// 1. SimpleState: state for regular stateful operators
// 2. WindowState: state for window operators - key is re-formatted as
// user-defined Key + windowStart to identify windows
type StateType int

const (
	SimpleState StateType = iota
	WindowState
	ListState
)

// Metadata for batch state request sent to the State Service
// This struct contains extra information about the state request in addition
// to the keys or values
type BatchStateReqMetadata struct {

	// State request type (i) SimpleState (ii) WindowState
	StateType StateType

	// [WindowState][lazy protocol] In State Service under lazy protocol, we
	// need to use the user-defined key to query the StateLookupTable for owner
	// worker. The passed-in key has the format of [bucketIdx--user-defined
	// key--window start time]. We need to extract the user-defined key from it
	// for StateLookupTable query. KeyEndingIndices stores the ending index
	// (exclusive) for key bytes in the serialized key. We need this metadata
	// because key serialization is handled in StateClient but StateService
	// also needs to extract the user-defined key from the serialized key
	KeyEndingIndices []int
}

func NewBatchStateReqMetadata(stateType StateType) *BatchStateReqMetadata {

	return &BatchStateReqMetadata{
		StateType: stateType,
	}
}
