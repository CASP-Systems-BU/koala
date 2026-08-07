package stateBackend_test

import (
	"bytes"
	"testing"

	"github.com/CASP-Systems-BU/koala/state/stateBackend"
)

func TestMemoryStateBackend(t *testing.T) {
	memory := stateBackend.NewMemoryStateBackend()

	// Test Set and Get
	memory.Set([]byte("key1"), []byte("value1"))
	value := memory.Get([]byte("key1"))
	if string(value) != "value1" {
		t.Fatalf("expected value 'value1', got %s", value)
	}

	// Test Get with non-existing key
	value = memory.Get([]byte("key2"))
	if value != nil {
		t.Fatalf("expected nil, got %s", value)
	}

	// Test SetMany and GetMany
	keys := [][]byte{[]byte("key3"), []byte("key4")}
	values := [][]byte{[]byte("value3"), []byte("value4")}
	memory.SetMany(keys, values)

	results := memory.GetMany(keys)
	for i, result := range results {
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("expected value '%s', got %s", values[i], result)
		}
	}
}

func TestMemoryStateBackend_Delete(t *testing.T) {
	memory := stateBackend.NewMemoryStateBackend()

	// Test Delete
	memory.Set([]byte("key1"), []byte("value1"))
	memory.DeleteMany([][]byte{[]byte("key1")})
	value := memory.Get([]byte("key1"))
	if value != nil {
		t.Fatalf("expected nil, got %s", value)
	}
}
