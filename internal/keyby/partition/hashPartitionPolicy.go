package partition

import (
	"log"
	"sort"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby/hash"
	"github.com/mus-format/mus-go/varint"
)

type HashPartitionPolicy struct {
	*PartitionPolicyBase

	// List of hash functions to generate tokens for each worker
	HashFuncs []hash.HashFunc
}

var _ PartitionPolicy = (*HashPartitionPolicy)(nil)

func NewHashPartitionPolicy(
	config *configuration.Configuration,
) *HashPartitionPolicy {

	// Validate config parameters for hash partition policy
	if config.HashPartitionNumTokens <= 0 {
		log.Fatalf(
			"Invalid number of tokens: %d\n",
			config.HashPartitionNumTokens,
		)
	} else if config.HashPartitionNumTokens != len(config.HashPartitionSeeds) {
		log.Fatalf("Number of tokens does not match number of seeds: %d vs %d\n",
			config.HashPartitionNumTokens, len(config.HashPartitionSeeds))
	}

	// Construct list of hash functions to calculate tokens
	var hashFuncs []hash.HashFunc
	switch config.HashFuncType {
	case "murmurhash":
		for i := 0; i < config.HashPartitionNumTokens; i++ {
			hashFuncs = append(
				hashFuncs,
				hash.NewMurmurHash(config.HashPartitionSeeds[i]),
			)
		}
	default:
		log.Fatalf("Unsupported hash function type: %s\n", config.HashFuncType)
	}

	return &HashPartitionPolicy{
		PartitionPolicyBase: NewPartitionPolicyBase(config),
		HashFuncs:           hashFuncs,
	}
}

/******************************************************************************
					 Implement the PartitionPolicy interface
******************************************************************************/

func (hp *HashPartitionPolicy) GenerateBuckets(workers []uint16) []*Bucket {

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

	tokens := make([]*Token, 0, len(hp.HashFuncs)*len(workers))
	for _, workerID := range workers {

		// Serialize the workerID
		workerIDBytes := make([]byte, varint.SizeUint16(workerID))
		varint.MarshalUint16(workerID, workerIDBytes)

		// Generate tokens for the worker
		for _, hashFunc := range hp.HashFuncs {
			hashValue := hashFunc.Hash(workerIDBytes)
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

func (hp *HashPartitionPolicy) RePartition(
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

func (hp *HashPartitionPolicy) GenerateBucketsV2(workers []uint16) []BucketV2 {

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

	tokens := make([]*Token, 0, len(hp.HashFuncs)*len(workers))
	for _, workerID := range workers {

		// Serialize the workerID
		workerIDBytes := make([]byte, varint.SizeUint16(workerID))
		varint.MarshalUint16(workerID, workerIDBytes)

		// Generate tokens for the worker
		for _, hashFunc := range hp.HashFuncs {
			hashValue := hashFunc.Hash(workerIDBytes)
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

func (hp *HashPartitionPolicy) RePartitionV2(
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
