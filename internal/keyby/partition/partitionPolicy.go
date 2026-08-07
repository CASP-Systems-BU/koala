package partition

import (
	"log"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// Key space partitioning policy interface.
// We adopt consistent hashing on top of fixed partitioning (buckets).
// Different partitioning policies specify where to locate tokens on the ring
// for each worker. E.g.:
// (1) HashPartition: For each worker, use multiple hash functions to hash the
// worker id and generate multiple tokens on the ring.
// (2) BalancedPartition: To address data skew (e.g. hot bucket), we can keep
// track of the number of the traffic falling into each bucket and generate
// tokens based on the traffic volume.

type PartitionPolicy interface {

	// Generate the bucket list based on list of workers
	GenerateBuckets([]uint16) []*Bucket

	// Change the list of workers
	// This api works for adding/removing/migrating workers
	// Input 1: List of updated workers
	// Input 2: Existing bucket list
	// Output 1: New bucket list
	// Output 2: Bucket owner changes
	//    - map[source worker]map[dest worker][]bucket indices
	RePartition(
		[]uint16,
		[]*Bucket,
	) ([]*Bucket, map[uint16]map[uint16][]int)

	// [KeyLookupTableV2]
	GenerateBucketsV2([]uint16) []BucketV2

	// [KeyLookupTableV2]
	RePartitionV2(
		[]uint16,
		[]BucketV2,
	) ([]BucketV2, map[uint16]map[uint16][]int)
}

// Shared fields for all partition policies
type PartitionPolicyBase struct {

	// Fixed number of buckets
	NumBuckets int64
}

func NewPartitionPolicyBase(
	config *configuration.Configuration,
) *PartitionPolicyBase {

	// Validate config parameters for partition policy base
	if config.NumBuckets <= 0 {
		log.Fatalf("Invalid number of buckets: %d\n", config.NumBuckets)
	}

	if config.NumBuckets > MaxSupportedBuckets() {
		log.Fatalf(
			"Configured num of buckets exceeds the maximum supported value: %d\n",
			MaxSupportedBuckets(),
		)
	}

	return &PartitionPolicyBase{
		NumBuckets: config.NumBuckets,
	}
}

// Compute bucket movements from old bucket assignment to the new one.
// It returns map[sourceWorker] -> map[destWorker][]bucketIndices
func (ppb *PartitionPolicyBase) computeBucketOwnerChanges(
	oldBuckets []*Bucket,
	newBuckets []*Bucket,
) map[uint16]map[uint16][]int {

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
	return bucketOwnerChanges
}
