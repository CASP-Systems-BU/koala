package keyby_test

import (
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby/partition"
)

func TestNewKeyLookupTable(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)
	table := keyby.NewKeyLookupTable(policy, []uint16{1, 2, 3}, config)

	key := []byte("test")
	workerID := table.KeyToWorkerID(key)
	if workerID != 3 {
		t.Fatalf("Invalid worker id: %d\n", workerID)
	}

	key = []byte("test2")
	workerID = table.KeyToWorkerID(key)
	if workerID != 2 {
		t.Fatalf("Invalid worker id: %d\n", workerID)
	}
}

func TestSerializationKeyLookupTable(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)
	table := keyby.NewKeyLookupTable(policy, []uint16{1, 2, 3}, config)

	// Serialize the key lookup table
	bucketRanges := table.Serialize()

	// The expected bucket list should be:
	// Owner: |-w3-|-w3-|-w2-|-w2-|-w3-|-w3-|-w3-|-w2-|-w1-|-w3-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |

	if len(bucketRanges) != 6 {
		t.Fatalf("Invalid number of bucket ranges: %d\n", len(bucketRanges))
	}

	// Check the serialized results
	expectedWorkerId := []uint16{3, 2, 3, 2, 1, 3}
	expectedLowerBucketIdx := []int64{0, 2, 4, 7, 8, 9}
	expectedUpperBucketIdx := []int64{1, 3, 6, 7, 8, 9}

	for i, br := range bucketRanges {
		if br.WorkerId != uint32(expectedWorkerId[i]) {
			t.Fatalf("Invalid worker id: %d\n", br.WorkerId)
		}
		if br.LowerBucketIdx != expectedLowerBucketIdx[i] {
			t.Fatalf("Invalid lower bucket index: %d\n", br.LowerBucketIdx)
		}
		if br.UpperBucketIdx != expectedUpperBucketIdx[i] {
			t.Fatalf("Invalid upper bucket index: %d\n", br.UpperBucketIdx)
		}
	}

	deserializedTable := keyby.DeserializeKeyLookupTable(bucketRanges, config)

	// Check the Bucket list in deserialized table
	expectedBucketOwners := []uint16{3, 3, 2, 2, 3, 3, 3, 2, 1, 3}
	for i, b := range deserializedTable.Buckets {
		if b.WorkerID != expectedBucketOwners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Use the deserialized table to check the worker id
	key := []byte("test")
	workerID := deserializedTable.KeyToWorkerID(key)
	if workerID != 3 {
		t.Fatalf("Invalid worker id: %d\n", workerID)
	}

	key = []byte("test2")
	workerID = deserializedTable.KeyToWorkerID(key)
	if workerID != 2 {
		t.Fatalf("Invalid worker id: %d\n", workerID)
	}
}

func TestReconfigure(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)
	table := keyby.NewKeyLookupTable(policy, []uint16{1, 2, 3}, config)

	key := []byte("test")
	workerId := table.KeyToWorkerID(key)
	if workerId != 3 {
		t.Fatalf("Invalid worker id: %d\n", workerId)
	}

	table.Reconfigure([]uint16{1, 2, 3, 4}, policy)

	workerId = table.KeyToWorkerID(key)
	if workerId != 4 {
		t.Fatalf("Invalid worker id: %d\n", workerId)
	}
}
