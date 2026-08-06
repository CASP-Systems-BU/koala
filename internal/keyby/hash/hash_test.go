package hash_test

import (
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby/hash"
)

func TestMurmurhash(t *testing.T) {

	var hashFunc hash.HashFunc

	hashFunc = hash.NewMurmurHash(uint64(1234))

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("apple"),
	}

	var hashValue uint64
	for _, key := range keys {

		hashValue = hashFunc.Hash(key)
		t.Logf("Hash value for key %s: %d", key, hashValue)
	}

	lower, upper := hashFunc.HashValueRange()
	t.Logf("Hash value range: [%d, %d]", lower, upper)
	// t.Fatalf("Just let it fail to see the log")
}
