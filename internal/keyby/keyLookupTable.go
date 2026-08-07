package keyby

import (
	"log"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby/hash"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

// KeyLookupTable maps Key to Worker. This is the data structure for Task
// Routing Table and State Service State Lookup Table. It evenly partitions
// the key space into fixed number of buckets. The key partitioning policy
// (consistent hashing) decides which worker owns which bucket.
// Please see issue #116 for detailed documentation.

type KeyLookupTable struct {

	// Key hash function
	H hash.HashFunc

	// Buckets of fixed partitioning
	Buckets []*partition.Bucket
}

// Coordinator initializes KeyLookupTable - bucket list is constructed in
// the coordinator
func NewKeyLookupTable(
	policy partition.PartitionPolicy,
	workers []uint16,
	config *configuration.Configuration,
) *KeyLookupTable {

	return &KeyLookupTable{
		// Coordinator doesn't need the key hash function H for routing keys
		// We still generate it here for testing purposes. When the Coordiantor
		// sends the KeyLookupTable to workers, we only send the Buckets field
		// while the H field is consistently constructed in the workers
		H: hash.GetKeyHashFunc(config),

		// Generate bucket list based on the partitioning policy
		Buckets: policy.GenerateBuckets(workers),
	}
}

/******************************************************************************
      					KeyLookupTable API exposed to Worker
******************************************************************************/

// Deserialize KeyLookupTable from gRPC - this is how to create KeyLookupTable
// in workers
func DeserializeKeyLookupTable(
	bucketRanges []*grpc.BucketRange,
	config *configuration.Configuration,
) *KeyLookupTable {

	// Deserialize bucket ranges
	buckets := make([]*partition.Bucket, 0, config.NumBuckets)
	for _, bucketRange := range bucketRanges {
		for i := bucketRange.LowerBucketIdx; i <= bucketRange.UpperBucketIdx; i++ {
			buckets = append(
				buckets,
				&partition.Bucket{WorkerID: uint16(bucketRange.WokerId)},
			)
		}
	}

	// Validate the length of deserialized buckets
	if int64(len(buckets)) != config.NumBuckets {
		log.Fatalf("Deserialized bucket length is not equal to NumBuckets\n")
	}

	return &KeyLookupTable{
		H:       hash.GetKeyHashFunc(config),
		Buckets: buckets,
	}
}

// Key lookup: key to worker id
func (table *KeyLookupTable) KeyToWorkerID(key []byte) uint16 {

	hashValue := table.H.Hash(key)
	bucketIdx := hashValue % uint64(len(table.Buckets))
	return table.Buckets[bucketIdx].WorkerID
}

// Key lookup: key to bucket index and worker id
func (table *KeyLookupTable) KeyToBucketIdxAndWorkerID(
	key []byte,
) (int64, uint16) {

	hashValue := table.H.Hash(key)
	bucketIdx := hashValue % uint64(len(table.Buckets))
	return int64(bucketIdx), table.Buckets[bucketIdx].WorkerID
}

// Map bucketIdx to workerID
func (table *KeyLookupTable) BucketIdxToWorkerID(bucketIdx int64) uint16 {
	if bucketIdx < 0 || int(bucketIdx) >= len(table.Buckets) {
		log.Fatalf("Invalid bucket index %d\n", bucketIdx)
	}

	return table.Buckets[bucketIdx].WorkerID
}

// Change the owner of a bucket
func (table *KeyLookupTable) ChangeBucketOwner(
	bucketIdx int64,
	workerID uint16,
) {
	if bucketIdx < 0 || int(bucketIdx) >= len(table.Buckets) {
		log.Fatalf("Invalid bucket index %d\n", bucketIdx)
	}

	table.Buckets[bucketIdx].WorkerID = workerID
}

/******************************************************************************
						   Utils used by coordinator
******************************************************************************/

// Serialize KeyLookupTable to pass it over gRPC - called by coordinator
// For consecutive buckets owned by the same worker, merge them into a range
func (table *KeyLookupTable) Serialize() []*grpc.BucketRange {

	var bucketRanges []*grpc.BucketRange

	lastOwner := table.Buckets[0].WorkerID
	bucketRange := &grpc.BucketRange{
		WokerId:        uint32(lastOwner),
		LowerBucketIdx: 0,
		UpperBucketIdx: 0,
	}
	for i, bucket := range table.Buckets {

		if bucket.WorkerID == lastOwner {
			bucketRange.UpperBucketIdx = int64(i)
		} else {
			bucketRanges = append(bucketRanges, bucketRange)
			lastOwner = bucket.WorkerID
			bucketRange = &grpc.BucketRange{
				WokerId:        uint32(lastOwner),
				LowerBucketIdx: int64(i),
				UpperBucketIdx: int64(i),
			}
		}
	}

	bucketRanges = append(bucketRanges, bucketRange)
	return bucketRanges
}

// Reconfigure the key lookup table in coordinator based on new wokrer list
func (table *KeyLookupTable) Reconfigure(
	updatedWorkers []uint16,
	policy partition.PartitionPolicy,
	updateTable bool,
) map[uint16]map[uint16][]int {

	if len(updatedWorkers) == 0 {
		log.Fatalf("No worker to reconfigure\n")
	}

	// Reconfigure the bucket list
	newBuckets, bucketOwnerChanges := policy.RePartition(
		updatedWorkers,
		table.Buckets,
	)
	if updateTable {
		table.Buckets = newBuckets
	}

	// Print out the number of buckets assigned to each worker
	bucketCountPerWorker := make(map[uint16]int)
	for _, br := range newBuckets {
		bucketCountPerWorker[br.WorkerID] += 1
	}
	log.Printf(
		"[Reconfig KeyPartition INFO] bucket count per worker: %+v\n",
		bucketCountPerWorker,
	)

	return bucketOwnerChanges
}

// [DRRS] Partially update the key lookup table based on bucket owner changes
func (table *KeyLookupTable) UpdateBucketOwnersDRRS(
	bucketOwnerChanges map[uint16]map[uint16][]int,
) {

	for _, destMap := range bucketOwnerChanges {
		for destWorkerId, bucketIdxList := range destMap {
			for _, bucketIdx := range bucketIdxList {
				table.ChangeBucketOwner(int64(bucketIdx), destWorkerId)
			}
		}
	}
}
