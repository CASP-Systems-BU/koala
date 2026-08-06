package stateBackend

import (
	"log"
)

// TODO: the memory state iterator is not thread safe. We cannot do concurrent
// operations on the memory map yet.

type MemoryStateIterator struct {

	// List of keys for the iterator: idx go from 0 to len(keys)-1
	keys []string

	// Pointer to the memory state backend
	memoryStateBackend *MemoryStateBackend

	// Current iterator idx
	idx int
}

var _ StateIterator = (*MemoryStateIterator)(nil)

// Init the iterator
func NewMemoryStateIterator(ss *MemoryStateBackend) *MemoryStateIterator {

	return &MemoryStateIterator{
		keys:               ss.GetKeys(),
		memoryStateBackend: ss,
	}
}

// Implement the interface Key()
func (m *MemoryStateIterator) Key() []byte {
	if m.idx < 0 || m.idx >= len(m.keys) {
		log.Fatalf("MemoryStateIterator: invalid idx")
	}

	return []byte(m.keys[m.idx])
}

// Implement the interface Value()
func (m *MemoryStateIterator) Value() []byte {
	if m.idx < 0 || m.idx >= len(m.keys) {
		log.Fatalf("MemoryStateIterator: invalid idx")
	}

	return m.memoryStateBackend.Get([]byte(m.keys[m.idx]))
}

// Implement the interface First()
func (m *MemoryStateIterator) First() bool {

	m.idx = 0
	if len(m.keys) == 0 {
		return false
	} else {
		return true
	}
}

// Implement the interface Valid()
func (m *MemoryStateIterator) Valid() bool {
	return m.idx >= 0 && m.idx < len(m.keys)
}

// Implement the interface Next()
func (m *MemoryStateIterator) Next() bool {
	m.idx++
	if m.idx >= len(m.keys) {
		return false
	} else {
		return true
	}
}

// Implement the interface Close()
func (m *MemoryStateIterator) Close() error {
	// Nothing to do
	return nil
}
