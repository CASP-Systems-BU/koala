package stateBackend

import (
	"bytes"
	"log"
	"sync"
)

type MemoryStateBackend struct {
	// Unused now
	sync.Mutex

	data map[string][]byte
}

var _ StateBackend = (*MemoryStateBackend)(nil)

func NewMemoryStateBackend() *MemoryStateBackend {
	return &MemoryStateBackend{
		data: make(map[string][]byte),
	}
}

/******************************************************************************
	 					Implement StateBackend interface
******************************************************************************/

func (m *MemoryStateBackend) Get(key []byte) []byte {

	// If key is nil, string(key) will be empty string

	val, ok := m.data[string(key)]
	if ok {
		return bytes.Clone(val)
	} else {
		return nil
	}
}

func (m *MemoryStateBackend) Set(key []byte, value []byte) {

	// If key is nil, string(key) will be empty string
	if value == nil {
		value = []byte{}
	}

	m.data[string(key)] = value
}

func (m *MemoryStateBackend) GetMany(keys [][]byte) [][]byte {
	if keys == nil {
		log.Fatalln("GetMany(): keys is nil")
	}

	results := make([][]byte, len(keys))

	for i := 0; i < len(keys); i++ {
		results[i] = m.Get(keys[i])
	}
	return results
}

func (m *MemoryStateBackend) SetMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil {
		log.Fatalln("SetMany(): keys or values is nil")
	}

	if len(keys) != len(values) {
		log.Fatalln("SetMany(): len(keys) != len(values)")
	}

	for i := 0; i < len(keys); i++ {
		if values[i] == nil {
			values[i] = []byte{}
		}
		m.data[string(keys[i])] = values[i]
	}
}

func (m *MemoryStateBackend) MergeMany(keys [][]byte, values [][]byte) {
	log.Fatalln("MergeMany() not supported for MemoryStateBackend")
}

func (m *MemoryStateBackend) DeleteMany(keys [][]byte) {
	if keys == nil {
		log.Fatalln("Delete(): keys is nil")
	}

	for _, key := range keys {
		delete(m.data, string(key))
	}
}

func (m *MemoryStateBackend) Close() {
}

func (m *MemoryStateBackend) GetIterator() StateIterator {
	return NewMemoryStateIterator(m)
}

func (m *MemoryStateBackend) RangeQuery(
	start []byte,
	end []byte,
) ([][]byte, [][]byte) {
	log.Fatalln("MemoryStateBackend doesn't support RangeQuery yet")
	return nil, nil
}

/******************************************************************************
						MemoryStateBackend specific utils
******************************************************************************/

// TODO: this is not thread safe - it's ok if we don't have concurrent access
func (m *MemoryStateBackend) GetKeys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}
