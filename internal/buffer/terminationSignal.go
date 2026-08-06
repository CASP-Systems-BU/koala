package buffer

// Termination signal used in both input and output buffers to indicate the end
// of a data plane channel (TCP)

type TerminationSignal struct {
	isPeer bool
}

var _ WorkUnit = (*TerminationSignal)(nil)

func NewTerminationSignal() *TerminationSignal {
	return &TerminationSignal{}
}

// Implement WorkUnit interface
func (e *TerminationSignal) GetType() WorkUnitType {
	return TerminationSignalWorkUnit
}

// Set the termination signal for peer connection
func (e *TerminationSignal) SetPeer() {
	e.isPeer = true
}

// Check if the termination signal is from a peer connection
func (e *TerminationSignal) IsPeer() bool {
	return e.isPeer
}
