package partition

import (
	"log"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// Uniform key partitioning policy assigns consecutive, evenly sized bucket
// ranges to workers
type UniformPartitionPolicy struct {
	*PartitionPolicyBase
}

var _ PartitionPolicy = (*UniformPartitionPolicy)(nil)

func NewUniformPartitionPolicy(
	config *configuration.Configuration,
) *UniformPartitionPolicy {
	return &UniformPartitionPolicy{
		PartitionPolicyBase: NewPartitionPolicyBase(config),
	}
}

/******************************************************************************
                     Implement the PartitionPolicy interface
******************************************************************************/

func (up *UniformPartitionPolicy) GenerateBuckets(workers []uint16) []*Bucket {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate buckets\n")
	}

	if up.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Prepare bucket slice
	buckets := make([]*Bucket, int(up.NumBuckets))

	total := int(up.NumBuckets)
	n := len(workers)
	base := total / n
	rem := total % n

	start := 0
	for i, workerID := range workers {
		size := base
		if i < rem {
			size += 1
		}
		end := start + size
		for j := start; j < end; j++ {
			buckets[j] = &Bucket{WorkerID: workerID}
		}
		start = end
	}

	return buckets
}

func (up *UniformPartitionPolicy) RePartition(
	updatedWorkers []uint16,
	oldBuckets []*Bucket,
) ([]*Bucket, map[uint16]map[uint16][]int) {

	if len(updatedWorkers) == 0 {
		log.Fatalf("New worker list invalid\n")
	}

	if oldBuckets == nil || len(oldBuckets) != int(up.NumBuckets) {
		log.Fatalf("Existing bucket list invalid\n")
	}

	// Generate new bucket list
	newBuckets := up.GenerateBuckets(updatedWorkers)

	// Compute bucket owner changes
	bucketOwnerChanges := up.computeBucketOwnerChanges(oldBuckets, newBuckets)
	return newBuckets, bucketOwnerChanges
}

func (up *UniformPartitionPolicy) GenerateBucketsV2(
	workers []uint16,
) []BucketV2 {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate buckets\n")
	}

	if up.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Prepare bucket slice
	buckets := make([]BucketV2, int(up.NumBuckets))

	total := int(up.NumBuckets)
	n := len(workers)
	base := total / n
	rem := total % n

	start := 0
	for i, workerID := range workers {
		size := base
		if i < rem {
			size += 1
		}
		end := start + size
		for j := start; j < end; j++ {
			buckets[j] = BucketV2{WorkerID: workerID, Map: nil}
		}
		start = end
	}

	return buckets
}

func (up *UniformPartitionPolicy) RePartitionV2(
	updatedWorkers []uint16,
	oldBuckets []BucketV2,
) ([]BucketV2, map[uint16]map[uint16][]int) {

	if len(updatedWorkers) == 0 {
		log.Fatalf("New worker list invalid\n")
	}

	if oldBuckets == nil || len(oldBuckets) != int(up.NumBuckets) {
		log.Fatalf("Existing bucket list invalid\n")
	}

	// Generate new bucket list
	newBuckets := up.GenerateBucketsV2(updatedWorkers)

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
