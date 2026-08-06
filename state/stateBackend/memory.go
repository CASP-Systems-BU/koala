package stateBackend

import (
	"bytes"
	"log"
	"sync"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/cockroachdb/pebble"
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

// Preload data into memory state backend from pebble when initializing new memory state backend
func NewPreLoadedMemoryStateBackend(
	config *configuration.Configuration,
) *MemoryStateBackend {
	path := "data/pebble/" + config.DataPlanePort + "localPebble.DB"

	db, err := pebble.Open(path, &pebble.Options{
		DisableWAL:   config.PebbleDisableWAL,
		MemTableSize: config.PebbleMemTableSize,
		Cache:        pebble.NewCache(512 << 20),
	})

	if err != nil {
		log.Fatalf("Failed to open pebble DB: %v", err)
	}

	defer db.Close()

	m := &MemoryStateBackend{
		data: make(map[string][]byte),
	}

	pebbleIterator, err := db.NewIter(nil)
	if err != nil {
		log.Fatalf("Failed to create pebble iterator: %v", err)
	}

	defer pebbleIterator.Close()

	keycount := 0
	totalSize := 0

	for pebbleIterator.First(); pebbleIterator.Valid(); pebbleIterator.Next() {
		m.data[string(pebbleIterator.Key())] = bytes.Clone(pebbleIterator.Value())
		keycount++
		totalSize += len(pebbleIterator.Value()) + len(string(pebbleIterator.Key()))
	}

	log.Printf("PreLoadedMemoryStateBackend: loaded %d keys from pebble DB", keycount)

	// Get the size of loaded data
	log.Printf("PreLoadedMemoryStateBackend: loaded data size %d bytes from pebble DB", totalSize)

	return m
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
	if keys == nil || values == nil {
		log.Fatalln("MergeMany(): keys or values is nil")
	}

	if len(keys) != len(values) {
		log.Fatalln("MergeMany(): len(keys) != len(values)")
	}

	for i := 0; i < len(keys); i++ {
		existingValue, ok := m.data[string(keys[i])]
		valClone := make([]byte, len(values[i]))
		copy(valClone, values[i])
		if ok {
			// Append the new value to the existing value
			mergedValue := append(existingValue, valClone...)
			m.data[string(keys[i])] = mergedValue
		} else {
			// Key does not exist, set the value directly
			m.data[string(keys[i])] = valClone
		}
	}
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

func (m *MemoryStateBackend) IsEmbeddedState() bool {
	return true
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
