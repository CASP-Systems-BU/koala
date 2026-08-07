package stateBackend_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
)

func encodeTestKey(key []byte, bucketIdx uint32) []byte {
	buf := make([]byte, constant.KeyPrefixSize+len(key))
	binary.BigEndian.PutUint16(buf[0:], 1) // operator ID
	binary.BigEndian.PutUint32(buf[constant.OperatorIDSize:], bucketIdx)
	stateIDOffset := constant.OperatorIDSize + constant.BucketIdxSize
	binary.BigEndian.PutUint16(buf[stateIDOffset:], 1) // state ID
	copy(buf[constant.KeyPrefixSize:], key)
	return buf
}

func TestRemotePebbleCreatesShards(t *testing.T) {
	addrs, _, cleanup := testutils.StartRemotePebbleTestServers(3)
	defer cleanup()

	config := configuration.Default()
	config.RemotePebbleAddrs = addrs
	backend := stateBackend.NewRemotePebbleStateBackend(config)
	defer backend.Close()
}

func TestRemotePebbleBasicOperations(t *testing.T) {
	addrs, _, cleanup := testutils.StartRemotePebbleTestServers(3)
	defer cleanup()

	config := configuration.Default()
	config.RemotePebbleAddrs = addrs
	backend := stateBackend.NewRemotePebbleStateBackend(config)
	defer backend.Close()

	bucketFor := func(idx int) uint32 {
		return uint32(idx % int(config.NumBuckets))
	}

	key1 := encodeTestKey([]byte("key1"), bucketFor(1))
	key2 := encodeTestKey([]byte("key2"), bucketFor(2))
	backend.Set(key1, []byte("value1"))
	backend.Set(key2, []byte("value2"))

	if !bytes.Equal(backend.Get(key1), []byte("value1")) {
		t.Fatalf("expected key1=value1")
	}
	if !bytes.Equal(backend.Get(key2), []byte("value2")) {
		t.Fatalf("expected key2=value2")
	}

	keys := [][]byte{
		encodeTestKey([]byte("key3"), bucketFor(3)),
		encodeTestKey([]byte("key4"), bucketFor(4)),
		encodeTestKey([]byte("key5"), bucketFor(5)),
	}
	values := [][]byte{[]byte("value3"), []byte("value4"), []byte("value5")}
	backend.SetMany(keys, values)

	got := backend.GetMany(keys)
	for i := range keys {
		if !bytes.Equal(got[i], values[i]) {
			t.Fatalf("expected %s=%s", keys[i], values[i])
		}
	}
}

func TestRemotePebbleDeleteMany(t *testing.T) {
	addrs, _, cleanup := testutils.StartRemotePebbleTestServers(3)
	defer cleanup()

	config := configuration.Default()
	config.RemotePebbleAddrs = addrs
	backend := stateBackend.NewRemotePebbleStateBackend(config)
	defer backend.Close()

	bucketFor := func(idx int) uint32 {
		return uint32(idx % int(config.NumBuckets))
	}

	// Create keys
	key1 := encodeTestKey([]byte("DeleteMe1"), bucketFor(1))
	key2 := encodeTestKey([]byte("KeepMe"), bucketFor(2))
	key3 := encodeTestKey([]byte("DeleteMe2"), bucketFor(3))

	// Set values
	backend.Set(key1, []byte("val1"))
	backend.Set(key2, []byte("val2"))
	backend.Set(key3, []byte("val3"))

	// Verify they exist
	if len(backend.Get(key1)) == 0 {
		t.Fatalf("key1 should exist")
	}

	// Delete key1 and key3
	keysToDelete := [][]byte{key1, key3}
	backend.DeleteMany(keysToDelete)

	// Verify key1 and key3 are gone - expecting []byte{} or nil
	if val := backend.Get(key1); len(val) != 0 {
		t.Errorf("key1 should be deleted, got %v", val)
	}
	if val := backend.Get(key3); len(val) != 0 {
		t.Errorf("key3 should be deleted, got %v", val)
	}

	// Verify key2 still exists
	if val := backend.Get(key2); !bytes.Equal(val, []byte("val2")) {
		t.Errorf("key2 should still exist with val2")
	}
}

func TestRemotePebbleMergeMany(t *testing.T) {
	addrs, _, cleanup := testutils.StartRemotePebbleTestServers(3)
	defer cleanup()

	config := configuration.Default()
	config.RemotePebbleAddrs = addrs
	backend := stateBackend.NewRemotePebbleStateBackend(config)
	defer backend.Close()

	bucketFor := func(idx int) uint32 {
		return uint32(idx % int(config.NumBuckets))
	}

	key1 := encodeTestKey([]byte("MergeKey1"), bucketFor(1))

	// Test merge to non-existing key
	// Expectation: it inserts the key
	backend.MergeMany([][]byte{key1}, [][]byte{[]byte("val1")})

	val1 := backend.Get(key1)
	if !bytes.Equal(val1, []byte("val1")) {
		t.Errorf(
			"Merge to non-existing key failed, expected 'val1', got %s",
			val1,
		)
	}

	// Test merge to existing key
	backend.MergeMany([][]byte{key1}, [][]byte{[]byte("val2")})
	val2 := backend.Get(key1)

	expected := []byte("val1val2")
	if !bytes.Equal(val2, expected) {
		t.Errorf(
			"Merge to existing key failed, expected '%s', got '%s'",
			expected,
			val2,
		)
	}
}

// TestRemotePebbleConcurrentAccess tests that GetMany/SetMany work
// correctly with concurrent shard access. Keys span multiple
// shards (round-robin: bucket i -> shard i % numShards), and multiple
// goroutines call GetMany/SetMany concurrently to fetch the values.
func TestRemotePebbleConcurrentAccess(t *testing.T) {
	addrs, _, cleanup := testutils.StartRemotePebbleTestServers(3)
	defer cleanup()

	config := configuration.Default()
	config.RemotePebbleAddrs = addrs
	backend := stateBackend.NewRemotePebbleStateBackend(config)
	defer backend.Close()

	numShards := len(addrs)
	bucketFor := func(idx int) uint32 {
		return uint32(idx % int(config.NumBuckets))
	}

	// Create keys that span all shards (buckets 0, 1, 2 -> shards 0, 1, 2)
	numKeys := numShards * 4 // 4 keys per shard
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := range numKeys {
		keys[i] = encodeTestKey(fmt.Appendf(nil, "key%d", i), bucketFor(i))
		values[i] = fmt.Appendf(nil, "value%d", i)
	}
	backend.SetMany(keys, values)

	// Verify GetMany returns correct values - keys span multiple shards,
	// triggers concurrent reads
	got := backend.GetMany(keys)
	for i := range keys {
		if !bytes.Equal(got[i], values[i]) {
			t.Errorf("key %d: expected %s, got %s", i, values[i], got[i])
		}
	}

	// Concurrent SetMany + GetMany: each goroutine writes a distinct key range
	// (no overlap) then reads back.
	// Each worker issues a GetMany/SetMany to the same set of shards,
	// triggering concurrent access to those shards.
	numWorkers := 4
	var wg sync.WaitGroup
	for g := range numWorkers {

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each worker has distinct keys (no overlap) but all span all
			// shards. Each worker issues SetMany then GetMany concurrently.
			keys := make([][]byte, numKeys)
			values := make([][]byte, numKeys)
			for i := range numKeys {
				keys[i] = encodeTestKey(
					fmt.Appendf(nil, "key%d_%d", id, i),
					bucketFor(i),
				)
				values[i] = fmt.Appendf(nil, "value%d_%d", id, i)
			}
			backend.SetMany(keys, values)

			got := backend.GetMany(keys)
			for i := range keys {
				if !bytes.Equal(got[i], values[i]) {
					t.Errorf(
						"worker %d key %d: expected %s, got %s",
						id,
						i,
						values[i],
						got[i],
					)
				}
			}
		}(g)
	}
	wg.Wait()
}
