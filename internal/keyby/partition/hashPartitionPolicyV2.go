package partition

import (
	"encoding/binary"
	"log"
	"sort"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby/hash"
	"github.com/mus-format/mus-go/varint"
)

type HashPartitionPolicyV2 struct {
	*PartitionPolicyBase

	// Single hash function to generate all tokens for all workers
	HashFunc hash.HashFunc
	// Number of tokens per worker
	NumTokens int
}

var _ PartitionPolicy = (*HashPartitionPolicyV2)(nil)

func NewHashPartitionPolicyV2(
	config *configuration.Configuration,
) *HashPartitionPolicyV2 {

	// Validate config parameters for hash partition policy
	if config.HashPartitionNumTokens <= 0 {
		log.Fatalf(
			"Invalid number of tokens: %d\n",
			config.HashPartitionNumTokens,
		)
	}

	// Construct single hash function to calculate all tokens
	var hashFunc hash.HashFunc
	switch config.HashFuncType {
	case "murmurhash":
		if len(config.HashPartitionSeeds) == 0 {
			log.Fatalf("No hash seeds provided for murmurhash")
		}
		hashFunc = hash.NewMurmurHash(config.HashPartitionSeeds[0])
	default:
		log.Fatalf("Unsupported hash function type: %s\n", config.HashFuncType)
	}

	return &HashPartitionPolicyV2{
		PartitionPolicyBase: NewPartitionPolicyBase(config),
		HashFunc:            hashFunc,
		NumTokens:           config.HashPartitionNumTokens,
	}
}

/******************************************************************************
					 Implement the PartitionPolicy interface
******************************************************************************/

func (hp *HashPartitionPolicyV2) GenerateBuckets(workers []uint16) []*Bucket {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate tokens\n")
	}

	if hp.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Generate tokens for all workers
	type Token struct {
		HashValue uint64
		BucketIdx uint64
		WorkerID  uint16
	}

	tokens := make([]*Token, 0, hp.NumTokens*len(workers))

	// Buffer for token generation
	var buf [12]byte

	for _, workerID := range workers {
		// 1. Hash the workerID first
		workerIDBytes := make([]byte, varint.SizeUint16(workerID))
		varint.MarshalUint16(workerID, workerIDBytes)
		h1 := hp.HashFunc.Hash(workerIDBytes)

		// Set the hash of workerID once since it doesn't change in the inner
		// loop
		binary.LittleEndian.PutUint64(buf[0:8], h1)

		// 2. Generate tokens combining h1 and index i
		for i := 0; i < hp.NumTokens; i++ {
			binary.LittleEndian.PutUint32(buf[8:12], uint32(i))

			hashValue := hp.HashFunc.Hash(buf[:])
			bucketIdx := hashValue % uint64(hp.NumBuckets)
			tokens = append(tokens, &Token{hashValue, bucketIdx, workerID})
		}
	}

	// Sort tokens by hash value
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].BucketIdx == tokens[j].BucketIdx {
			return tokens[i].HashValue < tokens[j].HashValue
		}
		return tokens[i].BucketIdx < tokens[j].BucketIdx
	})

	// Generate bucket list based on tokens - O(n)
	buckets := make([]*Bucket, hp.NumBuckets)

	// Handle last token to last bucket
	owner := tokens[len(tokens)-1].WorkerID
	for i := tokens[len(tokens)-1].BucketIdx + 1; i < uint64(hp.NumBuckets); i++ {
		buckets[i] = &Bucket{WorkerID: owner}
	}

	// Handle first bucket to first token
	for i := 0; i <= int(tokens[0].BucketIdx); i++ {
		buckets[i] = &Bucket{WorkerID: owner}
	}

	// Handle the rest of the tokens
	for tokenIdx := 0; tokenIdx < len(tokens)-1; tokenIdx++ {
		owner = tokens[tokenIdx].WorkerID
		for i := tokens[tokenIdx].BucketIdx + 1; i <= tokens[tokenIdx+1].BucketIdx; i++ {
			buckets[i] = &Bucket{WorkerID: owner}
		}
	}

	return buckets
}

func (hp *HashPartitionPolicyV2) RePartition(
	updatedWorkers []uint16,
	oldBuckets []*Bucket,
) ([]*Bucket, map[uint16]map[uint16][]int) {

	if len(updatedWorkers) == 0 {
		log.Fatalf("New worker list invalid\n")
	}

	if oldBuckets == nil || len(oldBuckets) != int(hp.NumBuckets) {
		log.Fatalf("Existing bucket list invalid\n")
	}

	// Generate new bucket list
	newBuckets := hp.GenerateBuckets(updatedWorkers)

	// Compute bucket owner changes
	bucketOwnerChanges := hp.computeBucketOwnerChanges(oldBuckets, newBuckets)
	return newBuckets, bucketOwnerChanges
}

func (hp *HashPartitionPolicyV2) GenerateBucketsV2(
	workers []uint16,
) []BucketV2 {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate tokens\n")
	}

	if hp.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Generate tokens for all workers
	type Token struct {
		HashValue uint64
		BucketIdx uint64
		WorkerID  uint16
	}

	tokens := make([]*Token, 0, hp.NumTokens*len(workers))

	// Buffer for token generation
	var buf [12]byte

	for _, workerID := range workers {
		// 1. Hash the workerID first
		workerIDBytes := make([]byte, varint.SizeUint16(workerID))
		varint.MarshalUint16(workerID, workerIDBytes)
		h1 := hp.HashFunc.Hash(workerIDBytes)

		// Set the hash of workerID once since it doesn't change in the inner
		// loop
		binary.LittleEndian.PutUint64(buf[0:8], h1)

		// 2. Generate tokens combining h1 and index i
		for i := 0; i < hp.NumTokens; i++ {
			binary.LittleEndian.PutUint32(buf[8:12], uint32(i))

			hashValue := hp.HashFunc.Hash(buf[:])
			bucketIdx := hashValue % uint64(hp.NumBuckets)
			tokens = append(tokens, &Token{hashValue, bucketIdx, workerID})
		}
	}

	// Sort tokens by hash value
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].BucketIdx == tokens[j].BucketIdx {
			return tokens[i].HashValue < tokens[j].HashValue
		}
		return tokens[i].BucketIdx < tokens[j].BucketIdx
	})

	// Generate bucket list based on tokens - O(n)
	buckets := make([]BucketV2, hp.NumBuckets)

	// Handle last token to last bucket
	owner := tokens[len(tokens)-1].WorkerID
	for i := tokens[len(tokens)-1].BucketIdx + 1; i < uint64(hp.NumBuckets); i++ {
		buckets[i] = BucketV2{WorkerID: owner, Map: nil}
	}

	// Handle first bucket to first token
	for i := 0; i <= int(tokens[0].BucketIdx); i++ {
		buckets[i] = BucketV2{WorkerID: owner, Map: nil}
	}

	// Handle the rest of the tokens
	for tokenIdx := 0; tokenIdx < len(tokens)-1; tokenIdx++ {
		owner = tokens[tokenIdx].WorkerID
		for i := tokens[tokenIdx].BucketIdx + 1; i <= tokens[tokenIdx+1].BucketIdx; i++ {
			buckets[i] = BucketV2{WorkerID: owner, Map: nil}
		}
	}

	return buckets
}

func (hp *HashPartitionPolicyV2) RePartitionV2(
	updatedWorkers []uint16,
	oldBuckets []BucketV2,
) ([]BucketV2, map[uint16]map[uint16][]int) {

	if len(updatedWorkers) == 0 {
		log.Fatalf("New worker list invalid\n")
	}

	if oldBuckets == nil || len(oldBuckets) != int(hp.NumBuckets) {
		log.Fatalf("Existing bucket list invalid\n")
	}

	// Generate new bucket list
	newBuckets := hp.GenerateBucketsV2(updatedWorkers)

	// Compute bucket owner changes
	bucketOwnerChanges := make(map[uint16]map[uint16][]int)
	for i := range oldBuckets {
		if oldBuckets[i].WorkerID != newBuckets[i].WorkerID {
			if _, ok := bucketOwnerChanges[oldBuckets[i].WorkerID]; !ok {
				bucketOwnerChanges[oldBuckets[i].WorkerID] = make(
					map[uint16][]int,
				)
			}
			bucketOwnerChanges[oldBuckets[i].WorkerID][newBuckets[i].WorkerID] = append(
				bucketOwnerChanges[oldBuckets[i].WorkerID][newBuckets[i].WorkerID],
				i,
			)
		}
	}
	return newBuckets, bucketOwnerChanges
}
