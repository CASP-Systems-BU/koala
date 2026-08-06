package stateBackend

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// Define interfaces all state backend implementation should implement
// We now support 3 types of state backends:
// 1. In-memory (local)
// 2. Pebble (local)
// 3. TiKV (remote)

type StateBackend interface {

	// - Return nil if the key doesn't exist
	// - Lookup key can be nil. It will be converted to []byte{} for lookup
	// - If the key exists, the return val won't be nil
	// - Returned []byte is safe to modify by the caller
	Get([]byte) []byte

	// If the input key is nil, set it to []byte{}
	// If the input value is nil, also set it to []byte{}
	Set([]byte, []byte)

	// Same rules as Get()
	GetMany([][]byte) [][]byte

	// Same rules as Set()
	SetMany([][]byte, [][]byte)

	// Merge the values into the existing keys - this is to avoid overwriting
	// the full value when the state update is append-only (e.g., ListState with
	// append-only)
	MergeMany([][]byte, [][]byte)

	// Delete the keys from the StateBackend
	DeleteMany([][]byte)

	Close()

	// GetIterator returns an iterator for the state store
	// TODO: check the concurrent access safety if multiple iterators are
	// accessing state at the same time - for both memory and pebble DB
	GetIterator() StateIterator

	// RangeQuery. Returned []byte is safe to modify by the caller
	RangeQuery([]byte, []byte) ([][]byte, [][]byte)
}

// Init state backend based on config
func NewStateBackend(config *configuration.Configuration) StateBackend {

	var stateBackend StateBackend

	switch config.StateBackendType {
	case "memory":
		stateBackend = NewMemoryStateBackend()
	case "pebble":
		stateBackend = NewPebbleStateBackend(config)
	case "tikv":
		stateBackend = NewTikvStateBackend(config)
	default:
		log.Fatalln("Invalid state backend type")
		stateBackend = NewMemoryStateBackend()
	}

	return stateBackend
}
