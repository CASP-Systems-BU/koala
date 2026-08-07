package keyby

import (
	"fmt"
	"log"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby/hash"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

// PartitionTable defines the key space partition: BucketID -> WorkerID
type PartitionTable struct {

	// Key hash function
	H hash.HashFunc

	// Global key space organized into buckets
	Buckets []uint16
}

func NewPartitionTable(
	policy partition.PartitionPolicy,
	workers []uint16,
	config *configuration.Configuration,
) *PartitionTable {

	// Generate BucketV2 list based on the partitioning policy
	bucketV2List := policy.GenerateBucketsV2(workers)

	// Convert BucketV2 list to simple worker ID list
	buckets := make([]uint16, len(bucketV2List))
	for i, bucket := range bucketV2List {
		buckets[i] = bucket.WorkerID
	}

	return &PartitionTable{
		H:       hash.GetKeyHashFunc(config),
		Buckets: buckets,
	}
}

/******************************************************************************
					   PartitionTable API exposed to Worker
******************************************************************************/

// Deserialize PartitionTable from gRPC
func DeserializePartitionTable(
	bucketRanges []*grpc.BucketRange,
	config *configuration.Configuration,
) *PartitionTable {

	// Deserialize bucket ranges
	buckets := make([]uint16, config.NumBuckets)
	for _, bucketRange := range bucketRanges {
		for i := bucketRange.LowerBucketIdx; i <= bucketRange.UpperBucketIdx; i++ {
			buckets[i] = uint16(bucketRange.WorkerId)
		}
	}

	// Validate the length of deserialized buckets
	if int64(len(buckets)) != config.NumBuckets {
		log.Fatalf("Deserialized bucket length is not equal to NumBuckets\n")
	}

	return &PartitionTable{
		H:       hash.GetKeyHashFunc(config),
		Buckets: buckets,
	}
}

// Key lookup: key to worker id
func (table *PartitionTable) KeyToWorkerID(key []byte) uint16 {
	hashValue := table.H.Hash(key)
	bucketIdx := hashValue % uint64(len(table.Buckets))
	return table.Buckets[bucketIdx]
}

/******************************************************************************
						   Utils used by coordinator
******************************************************************************/

// Serialize PartitionTable to pass it over gRPC - called by coordinator
// For consecutive buckets owned by the same worker, merge them into a range
func (table *PartitionTable) Serialize() []*grpc.BucketRange {

	var bucketRanges []*grpc.BucketRange

	lastOwner := table.Buckets[0]
	bucketRange := &grpc.BucketRange{
		WorkerId:       uint32(lastOwner),
		LowerBucketIdx: 0,
		UpperBucketIdx: 0,
	}
	for i, bucketId := range table.Buckets {

		if bucketId == lastOwner {
			bucketRange.UpperBucketIdx = int64(i)
		} else {
			bucketRanges = append(bucketRanges, bucketRange)
			lastOwner = bucketId
			bucketRange = &grpc.BucketRange{
				WorkerId:       uint32(lastOwner),
				LowerBucketIdx: int64(i),
				UpperBucketIdx: int64(i),
			}
		}
	}

	bucketRanges = append(bucketRanges, bucketRange)
	return bucketRanges
}

// Reconfigure the partition table in coordinator based on new worker list
func (table *PartitionTable) Reconfigure(
	updatedWorkers []uint16,
	policy partition.PartitionPolicy,
) map[uint16]map[uint16][]int {

	if len(updatedWorkers) == 0 {
		log.Fatalf("No worker to reconfigure\n")
	}

	// Convert the []BucketV2 to match the RePartitionV2 API
	buckets := make([]partition.BucketV2, len(table.Buckets))
	for i, workerID := range table.Buckets {
		buckets[i] = partition.BucketV2{WorkerID: workerID, Map: nil}
	}

	// Reconfigure the bucket list
	newBuckets, bucketOwnerChanges := policy.RePartitionV2(
		updatedWorkers,
		buckets,
	)

	fmt.Println("newBuckets:", newBuckets, " bucketOwnerChanges", bucketOwnerChanges)

	/*
		// Hard coded the reconfigured bucket list for skew mitigationexperiment
		newBuckets :=
		bucketOwnerChanges :=
	*/
	table.Buckets = make([]uint16, len(newBuckets))
	for i, bucket := range newBuckets {
		table.Buckets[i] = bucket.WorkerID
	}
	// Print out the number of buckets assigned to each worker
	bucketCountPerWorker := make(map[uint16]int)
	for _, br := range table.Buckets {
		bucketCountPerWorker[br] += 1
	}

	log.Printf(
		"[Bucket Partition INFO] bucket count per worker: %+v\n",
		bucketCountPerWorker,
	)

	return bucketOwnerChanges
}
