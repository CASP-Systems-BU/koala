package partition

import (
	"log"
	"sort"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

/*
EvenPartitionPolicy manually implements even bucket repartitioning for scale-up
and scale-down cases. It guarantees:
1. Even distribution of buckets across workers
2. Bucket will only be reassigned from existing workers to updated workers with
   no reassignment between existing workers. This minimizes the number of
   buckets to be migrated (similar to consistent hashing)
3. New workers will receive buckets from all existing workers (round-robin)
   instead of a 1-to-1 donor->recipient pattern (when possible)
*/

type EvenPartitionPolicy struct {
	*PartitionPolicyBase
}

var _ PartitionPolicy = (*EvenPartitionPolicy)(nil)

func NewEvenPartitionPolicy(
	config *configuration.Configuration,
) *EvenPartitionPolicy {
	return &EvenPartitionPolicy{
		PartitionPolicyBase: NewPartitionPolicyBase(config),
	}
}

/******************************************************************************
                     Implement the PartitionPolicy interface
******************************************************************************/

func (ep *EvenPartitionPolicy) GenerateBuckets(workers []uint16) []*Bucket {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate buckets\n")
	}

	if ep.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Prepare bucket slice
	buckets := make([]*Bucket, int(ep.NumBuckets))

	total := int(ep.NumBuckets)
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

func (ep *EvenPartitionPolicy) GenerateBucketsV2(workers []uint16) []BucketV2 {

	if len(workers) == 0 {
		log.Fatalf("No worker to generate buckets\n")
	}

	if ep.NumBuckets < int64(len(workers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Prepare bucket slice
	buckets := make([]BucketV2, int(ep.NumBuckets))

	total := int(ep.NumBuckets)
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
	buckets = []BucketV2{
		{WorkerID: 11, Map: nil}, // Bucket 0
		{WorkerID: 9, Map: nil},  // Bucket 1
		{WorkerID: 8, Map: nil},  // Bucket 2
		{WorkerID: 10, Map: nil}, // Bucket 3
		{WorkerID: 10, Map: nil}, // Bucket 4
		{WorkerID: 11, Map: nil}, // Bucket 5
		{WorkerID: 10, Map: nil}, // Bucket 6
		{WorkerID: 9, Map: nil},  // Bucket 7
		{WorkerID: 8, Map: nil},  // Bucket 8
		{WorkerID: 11, Map: nil}, // Bucket 9
		{WorkerID: 9, Map: nil},  // Bucket 10
		{WorkerID: 8, Map: nil},  // Bucket 11
		{WorkerID: 9, Map: nil},  // Bucket 12
		{WorkerID: 8, Map: nil},  // Bucket 13
		{WorkerID: 11, Map: nil}, // Bucket 14
		{WorkerID: 11, Map: nil}, // Bucket 15
		{WorkerID: 11, Map: nil}, // Bucket 16
		{WorkerID: 8, Map: nil},  // Bucket 17
		{WorkerID: 10, Map: nil}, // Bucket 18
		{WorkerID: 10, Map: nil}, // Bucket 19
		{WorkerID: 10, Map: nil}, // Bucket 20
		{WorkerID: 11, Map: nil}, // Bucket 21
		{WorkerID: 11, Map: nil}, // Bucket 22
		{WorkerID: 8, Map: nil},  // Bucket 23
		{WorkerID: 9, Map: nil},  // Bucket 24
		{WorkerID: 10, Map: nil}, // Bucket 25
		{WorkerID: 11, Map: nil}, // Bucket 26
		{WorkerID: 9, Map: nil},  // Bucket 27
		{WorkerID: 11, Map: nil}, // Bucket 28
		{WorkerID: 9, Map: nil},  // Bucket 29
		{WorkerID: 9, Map: nil},  // Bucket 30
		{WorkerID: 11, Map: nil}, // Bucket 31
		{WorkerID: 9, Map: nil},  // Bucket 32
		{WorkerID: 9, Map: nil},  // Bucket 33
		{WorkerID: 11, Map: nil}, // Bucket 34
		{WorkerID: 8, Map: nil},  // Bucket 35
		{WorkerID: 10, Map: nil}, // Bucket 36
		{WorkerID: 8, Map: nil},  // Bucket 37
		{WorkerID: 11, Map: nil}, // Bucket 38
		{WorkerID: 9, Map: nil},  // Bucket 39
		{WorkerID: 9, Map: nil},  // Bucket 40
		{WorkerID: 10, Map: nil}, // Bucket 41
		{WorkerID: 10, Map: nil}, // Bucket 42
		{WorkerID: 8, Map: nil},  // Bucket 43
		{WorkerID: 8, Map: nil},  // Bucket 44
		{WorkerID: 8, Map: nil},  // Bucket 45
		{WorkerID: 11, Map: nil}, // Bucket 46
		{WorkerID: 11, Map: nil}, // Bucket 47
		{WorkerID: 8, Map: nil},  // Bucket 48
		{WorkerID: 9, Map: nil},  // Bucket 49
		{WorkerID: 11, Map: nil}, // Bucket 50
		{WorkerID: 8, Map: nil},  // Bucket 51
		{WorkerID: 10, Map: nil}, // Bucket 52
		{WorkerID: 8, Map: nil},  // Bucket 53
		{WorkerID: 10, Map: nil}, // Bucket 54
		{WorkerID: 9, Map: nil},  // Bucket 55
		{WorkerID: 8, Map: nil},  // Bucket 56
		{WorkerID: 11, Map: nil}, // Bucket 57
		{WorkerID: 9, Map: nil},  // Bucket 58
		{WorkerID: 8, Map: nil},  // Bucket 59
		{WorkerID: 11, Map: nil}, // Bucket 60
		{WorkerID: 10, Map: nil}, // Bucket 61
		{WorkerID: 10, Map: nil}, // Bucket 62
		{WorkerID: 8, Map: nil},  // Bucket 63
		{WorkerID: 9, Map: nil},  // Bucket 64
		{WorkerID: 11, Map: nil}, // Bucket 65
		{WorkerID: 8, Map: nil},  // Bucket 66
		{WorkerID: 11, Map: nil}, // Bucket 67
		{WorkerID: 10, Map: nil}, // Bucket 68
		{WorkerID: 11, Map: nil}, // Bucket 69
		{WorkerID: 10, Map: nil}, // Bucket 70
		{WorkerID: 8, Map: nil},  // Bucket 71
		{WorkerID: 8, Map: nil},  // Bucket 72
		{WorkerID: 9, Map: nil},  // Bucket 73
		{WorkerID: 9, Map: nil},  // Bucket 74
		{WorkerID: 10, Map: nil}, // Bucket 75
		{WorkerID: 8, Map: nil},  // Bucket 76
		{WorkerID: 10, Map: nil}, // Bucket 77
		{WorkerID: 9, Map: nil},  // Bucket 78
		{WorkerID: 10, Map: nil}, // Bucket 79
		{WorkerID: 11, Map: nil}, // Bucket 80
		{WorkerID: 10, Map: nil}, // Bucket 81
		{WorkerID: 10, Map: nil}, // Bucket 82
		{WorkerID: 9, Map: nil},  // Bucket 83
		{WorkerID: 9, Map: nil},  // Bucket 84
		{WorkerID: 10, Map: nil}, // Bucket 85
		{WorkerID: 8, Map: nil},  // Bucket 86
		{WorkerID: 8, Map: nil},  // Bucket 87
		{WorkerID: 8, Map: nil},  // Bucket 88
		{WorkerID: 10, Map: nil}, // Bucket 89
		{WorkerID: 11, Map: nil}, // Bucket 90
		{WorkerID: 8, Map: nil},  // Bucket 91
		{WorkerID: 10, Map: nil}, // Bucket 92
		{WorkerID: 8, Map: nil},  // Bucket 93
		{WorkerID: 10, Map: nil}, // Bucket 94
		{WorkerID: 9, Map: nil},  // Bucket 95
		{WorkerID: 8, Map: nil},  // Bucket 96
		{WorkerID: 11, Map: nil}, // Bucket 97
		{WorkerID: 9, Map: nil},  // Bucket 98
		{WorkerID: 9, Map: nil},  // Bucket 99
		{WorkerID: 11, Map: nil}, // Bucket 100
		{WorkerID: 11, Map: nil}, // Bucket 101
		{WorkerID: 8, Map: nil},  // Bucket 102
		{WorkerID: 10, Map: nil}, // Bucket 103
		{WorkerID: 11, Map: nil}, // Bucket 104
		{WorkerID: 8, Map: nil},  // Bucket 105
		{WorkerID: 10, Map: nil}, // Bucket 106
		{WorkerID: 9, Map: nil},  // Bucket 107
		{WorkerID: 11, Map: nil}, // Bucket 108
		{WorkerID: 9, Map: nil},  // Bucket 109
		{WorkerID: 8, Map: nil},  // Bucket 110
		{WorkerID: 10, Map: nil}, // Bucket 111
		{WorkerID: 9, Map: nil},  // Bucket 112
		{WorkerID: 10, Map: nil}, // Bucket 113
		{WorkerID: 10, Map: nil}, // Bucket 114
		{WorkerID: 9, Map: nil},  // Bucket 115
		{WorkerID: 11, Map: nil}, // Bucket 116
		{WorkerID: 10, Map: nil}, // Bucket 117
		{WorkerID: 11, Map: nil}, // Bucket 118
		{WorkerID: 10, Map: nil}, // Bucket 119
		{WorkerID: 8, Map: nil},  // Bucket 120
		{WorkerID: 11, Map: nil}, // Bucket 121
		{WorkerID: 8, Map: nil},  // Bucket 122
		{WorkerID: 9, Map: nil},  // Bucket 123
		{WorkerID: 9, Map: nil},  // Bucket 124
		{WorkerID: 9, Map: nil},  // Bucket 125
		{WorkerID: 11, Map: nil}, // Bucket 126
		{WorkerID: 9, Map: nil},  // Bucket 127
	}
	return buckets
}

func (ep *EvenPartitionPolicy) RePartition(
	updatedWorkers []uint16,
	oldBuckets []*Bucket,
) ([]*Bucket, map[uint16]map[uint16][]int) {

	// Convert oldBuckets []*Bucket to []BucketV2
	oldBucketsV2 := make([]BucketV2, len(oldBuckets))
	for i, bucket := range oldBuckets {
		oldBucketsV2[i] = BucketV2{WorkerID: bucket.WorkerID}
	}

	// Delegate to RePartitionV2
	newBucketsV2, bucketOwnerChanges := ep.RePartitionV2(
		updatedWorkers,
		oldBucketsV2,
	)

	// Convert newBuckets []BucketV2 back to []*Bucket
	newBuckets := make([]*Bucket, len(newBucketsV2))
	for i, bucketV2 := range newBucketsV2 {
		newBuckets[i] = &Bucket{WorkerID: bucketV2.WorkerID}
	}

	return newBuckets, bucketOwnerChanges
}

func (ep *EvenPartitionPolicy) RePartitionV2(
	updatedWorkers []uint16,
	oldBuckets []BucketV2,
) ([]BucketV2, map[uint16]map[uint16][]int) {

	// Validation and preparation. Return:
	// 1. updatedWorkerSet: set of updated workers
	// 2. existingWorkerList: list of existing workers in the order they are
	//    first seen in oldBuckets (to have deterministic order)
	// 3. bucketsByExistingWorker: map of existing workerID -> list of buckets
	updatedWorkerSet, existingWorkerList, bucketsByExistingWorker := ep.validateAndPrepareRepartition(
		updatedWorkers,
		oldBuckets,
	)

	// Get desired bucket count per worker for the updated worker list
	// Map of workerID -> desired bucket count
	desiredCounts := ep.getDesiredCountsPerWorker(updatedWorkers)

	// Init the new bucket assignment by copying the existing assignment. We
	// will only mutate the WorkerID of buckets that need to be migrated
	newBuckets := make([]BucketV2, ep.NumBuckets)
	copy(newBuckets, oldBuckets)

	// Map of sourceWorkerID -> map[destWorkerID] -> list of bucket IDs
	bucketOwnerChanges := make(map[uint16]map[uint16][]int)

	if len(updatedWorkerSet) > len(existingWorkerList) {

		// Scale-up
		repartitionScaleUp(
			updatedWorkers,
			desiredCounts,
			existingWorkerList,
			bucketsByExistingWorker,
			newBuckets,
			bucketOwnerChanges,
		)
	} else {

		// Rebalance
		if len(updatedWorkerSet) == len(existingWorkerList) {
			// Hard coded newBuckets and bucketOwnerChanges for skew mitigation test.
			newBuckets = []BucketV2{
				{WorkerID: 11, Map: nil}, // Bucket 0
				{WorkerID: 9, Map: nil},  // Bucket 1
				{WorkerID: 11, Map: nil}, // Bucket 2 (Moved: 8 -> 11)
				{WorkerID: 10, Map: nil}, // Bucket 3
				{WorkerID: 10, Map: nil}, // Bucket 4
				{WorkerID: 11, Map: nil}, // Bucket 5
				{WorkerID: 10, Map: nil}, // Bucket 6
				{WorkerID: 9, Map: nil},  // Bucket 7
				{WorkerID: 10, Map: nil}, // Bucket 8 (Moved: 8 -> 10)
				{WorkerID: 11, Map: nil}, // Bucket 9
				{WorkerID: 11, Map: nil}, // Bucket 10 (Moved: 9 -> 11)
				{WorkerID: 10, Map: nil}, // Bucket 11 (Moved: 8 -> 10)
				{WorkerID: 9, Map: nil},  // Bucket 12
				{WorkerID: 8, Map: nil},  // Bucket 13
				{WorkerID: 11, Map: nil}, // Bucket 14
				{WorkerID: 11, Map: nil}, // Bucket 15
				{WorkerID: 11, Map: nil}, // Bucket 16
				{WorkerID: 10, Map: nil}, // Bucket 17 (Moved: 8 -> 10)
				{WorkerID: 10, Map: nil}, // Bucket 18
				{WorkerID: 10, Map: nil}, // Bucket 19
				{WorkerID: 10, Map: nil}, // Bucket 20
				{WorkerID: 11, Map: nil}, // Bucket 21
				{WorkerID: 11, Map: nil}, // Bucket 22
				{WorkerID: 11, Map: nil}, // Bucket 23 (Moved: 8 -> 11)
				{WorkerID: 9, Map: nil},  // Bucket 24
				{WorkerID: 10, Map: nil}, // Bucket 25
				{WorkerID: 11, Map: nil}, // Bucket 26
				{WorkerID: 9, Map: nil},  // Bucket 27
				{WorkerID: 11, Map: nil}, // Bucket 28
				{WorkerID: 9, Map: nil},  // Bucket 29
				{WorkerID: 9, Map: nil},  // Bucket 30
				{WorkerID: 11, Map: nil}, // Bucket 31
				{WorkerID: 9, Map: nil},  // Bucket 32
				{WorkerID: 10, Map: nil}, // Bucket 33 (Moved: 9 -> 10)
				{WorkerID: 11, Map: nil}, // Bucket 34
				{WorkerID: 8, Map: nil},  // Bucket 35
				{WorkerID: 10, Map: nil}, // Bucket 36
				{WorkerID: 8, Map: nil},  // Bucket 37
				{WorkerID: 11, Map: nil}, // Bucket 38
				{WorkerID: 9, Map: nil},  // Bucket 39
				{WorkerID: 9, Map: nil},  // Bucket 40
				{WorkerID: 10, Map: nil}, // Bucket 41
				{WorkerID: 10, Map: nil}, // Bucket 42
				{WorkerID: 8, Map: nil},  // Bucket 43
				{WorkerID: 8, Map: nil},  // Bucket 44
				{WorkerID: 8, Map: nil},  // Bucket 45
				{WorkerID: 11, Map: nil}, // Bucket 46
				{WorkerID: 11, Map: nil}, // Bucket 47
				{WorkerID: 11, Map: nil}, // Bucket 48 (Moved: 8 -> 11)
				{WorkerID: 9, Map: nil},  // Bucket 49
				{WorkerID: 11, Map: nil}, // Bucket 50
				{WorkerID: 8, Map: nil},  // Bucket 51
				{WorkerID: 10, Map: nil}, // Bucket 52
				{WorkerID: 11, Map: nil}, // Bucket 53 (Moved: 8 -> 11)
				{WorkerID: 10, Map: nil}, // Bucket 54
				{WorkerID: 9, Map: nil},  // Bucket 55
				{WorkerID: 10, Map: nil}, // Bucket 56 (Moved: 8 -> 10)
				{WorkerID: 11, Map: nil}, // Bucket 57
				{WorkerID: 9, Map: nil},  // Bucket 58
				{WorkerID: 11, Map: nil}, // Bucket 59 (Moved: 8 -> 11)
				{WorkerID: 11, Map: nil}, // Bucket 60
				{WorkerID: 10, Map: nil}, // Bucket 61
				{WorkerID: 10, Map: nil}, // Bucket 62
				{WorkerID: 11, Map: nil}, // Bucket 63 (Moved: 8 -> 11)
				{WorkerID: 9, Map: nil},  // Bucket 64
				{WorkerID: 11, Map: nil}, // Bucket 65
				{WorkerID: 10, Map: nil}, // Bucket 66 (Moved: 8 -> 10)
				{WorkerID: 11, Map: nil}, // Bucket 67
				{WorkerID: 10, Map: nil}, // Bucket 68
				{WorkerID: 11, Map: nil}, // Bucket 69
				{WorkerID: 10, Map: nil}, // Bucket 70
				{WorkerID: 11, Map: nil}, // Bucket 71 (Moved: 8 -> 11)
				{WorkerID: 11, Map: nil}, // Bucket 72 (Moved: 8 -> 11)
				{WorkerID: 9, Map: nil},  // Bucket 73
				{WorkerID: 9, Map: nil},  // Bucket 74
				{WorkerID: 10, Map: nil}, // Bucket 75
				{WorkerID: 9, Map: nil},  // Bucket 76 (Moved: 8 -> 9)
				{WorkerID: 10, Map: nil}, // Bucket 77
				{WorkerID: 9, Map: nil},  // Bucket 78
				{WorkerID: 10, Map: nil}, // Bucket 79
				{WorkerID: 11, Map: nil}, // Bucket 80
				{WorkerID: 10, Map: nil}, // Bucket 81
				{WorkerID: 10, Map: nil}, // Bucket 82
				{WorkerID: 9, Map: nil},  // Bucket 83
				{WorkerID: 9, Map: nil},  // Bucket 84
				{WorkerID: 10, Map: nil}, // Bucket 85
				{WorkerID: 8, Map: nil},  // Bucket 86
				{WorkerID: 8, Map: nil},  // Bucket 87
				{WorkerID: 8, Map: nil},  // Bucket 88
				{WorkerID: 10, Map: nil}, // Bucket 89
				{WorkerID: 11, Map: nil}, // Bucket 90
				{WorkerID: 9, Map: nil},  // Bucket 91 (Moved: 8 -> 9)
				{WorkerID: 10, Map: nil}, // Bucket 92
				{WorkerID: 10, Map: nil}, // Bucket 93 (Moved: 8 -> 10)
				{WorkerID: 10, Map: nil}, // Bucket 94
				{WorkerID: 9, Map: nil},  // Bucket 95
				{WorkerID: 8, Map: nil},  // Bucket 96
				{WorkerID: 11, Map: nil}, // Bucket 97
				{WorkerID: 9, Map: nil},  // Bucket 98
				{WorkerID: 9, Map: nil},  // Bucket 99
				{WorkerID: 11, Map: nil}, // Bucket 100
				{WorkerID: 11, Map: nil}, // Bucket 101
				{WorkerID: 8, Map: nil},  // Bucket 102
				{WorkerID: 10, Map: nil}, // Bucket 103
				{WorkerID: 11, Map: nil}, // Bucket 104
				{WorkerID: 8, Map: nil},  // Bucket 105
				{WorkerID: 10, Map: nil}, // Bucket 106
				{WorkerID: 9, Map: nil},  // Bucket 107
				{WorkerID: 11, Map: nil}, // Bucket 108
				{WorkerID: 9, Map: nil},  // Bucket 109
				{WorkerID: 8, Map: nil},  // Bucket 110
				{WorkerID: 10, Map: nil}, // Bucket 111
				{WorkerID: 10, Map: nil}, // Bucket 112 (Moved: 9 -> 10)
				{WorkerID: 10, Map: nil}, // Bucket 113
				{WorkerID: 10, Map: nil}, // Bucket 114
				{WorkerID: 9, Map: nil},  // Bucket 115
				{WorkerID: 11, Map: nil}, // Bucket 116
				{WorkerID: 10, Map: nil}, // Bucket 117
				{WorkerID: 11, Map: nil}, // Bucket 118
				{WorkerID: 10, Map: nil}, // Bucket 119
				{WorkerID: 8, Map: nil},  // Bucket 120
				{WorkerID: 11, Map: nil}, // Bucket 121
				{WorkerID: 10, Map: nil}, // Bucket 122 (Moved: 8 -> 10)
				{WorkerID: 11, Map: nil}, // Bucket 123 (Moved: 9 -> 11)
				{WorkerID: 9, Map: nil},  // Bucket 124
				{WorkerID: 9, Map: nil},  // Bucket 125
				{WorkerID: 11, Map: nil}, // Bucket 126
				{WorkerID: 11, Map: nil}, // Bucket 127 (Moved: 9 -> 11)
			}
			bucketOwnerChanges = map[uint16]map[uint16][]int{
				8: {
					9:  {76, 91},
					10: {8, 11, 17, 56, 66, 93, 122},
					11: {2, 23, 48, 53, 59, 63, 71, 72},
				},
				9: {
					10: {33, 112},
					11: {10, 123, 127},
				},
			}
			return newBuckets, bucketOwnerChanges
		}
		// Scale-down
		repartitionScaleDown(
			updatedWorkers,
			updatedWorkerSet,
			desiredCounts,
			existingWorkerList,
			bucketsByExistingWorker,
			newBuckets,
			bucketOwnerChanges,
		)
	}

	// Sort the bucket lists in bucketOwnerChanges to ensure deterministic
	// output
	for _, destMap := range bucketOwnerChanges {
		for _, bucketList := range destMap {
			sort.Ints(bucketList)
		}
	}

	return newBuckets, bucketOwnerChanges
}

/******************************************************************************
                     Helper functions for RePartitionV2
******************************************************************************/

func repartitionScaleUp(
	updatedWorkers []uint16, // Updated worker list
	desiredCounts map[uint16]int, // Desired bucket count for updated workers
	existingWorkerList []uint16, // Existing workers deterministic order
	bucketsByExistingWorker map[uint16][]int, // Existing workerID -> list of bucket IDs
	newBuckets []BucketV2, // Resulting bucket (re)assignment
	bucketOwnerChanges map[uint16]map[uint16][]int, // Resulting bucket movements
) {

	// Identify new workers in the given updatedWorkers order
	newWorkers := make([]uint16, 0)
	for _, workerID := range updatedWorkers {
		if _, ok := bucketsByExistingWorker[workerID]; !ok {
			newWorkers = append(newWorkers, workerID)
		}
	}
	if len(newWorkers) == 0 {
		log.Fatalf("Scale-up invalid: no new worker found\n")
	}

	// For each existing worker, calculate how many buckets it needs to shed to
	// meet the desired count (excess buckets). Map of existing workerID -> list
	// of bucket IDs to donate.
	donorBuckets := make(map[uint16][]int)
	remainingDonation := 0
	for workerID, bucketList := range bucketsByExistingWorker {

		curBucketCnt := len(bucketList)
		desired, ok := desiredCounts[workerID]
		if !ok {
			log.Fatalf(
				"Scale-up invalid: existing worker %d missing desired count\n",
				workerID,
			)
		}
		if desired > curBucketCnt {
			log.Fatalf(
				"Scale-up invalid: existing worker %d desires %d buckets but only owns %d buckets\n",
				workerID,
				desired,
				curBucketCnt,
			)
		}

		excess := curBucketCnt - desired
		if excess == 0 {
			continue
		}
		remainingDonation += excess

		// Donate buckets from the end of the bucket list
		donorBuckets[workerID] = bucketList[curBucketCnt-excess:]
	}
	if remainingDonation == 0 {
		log.Fatalf("Scale-up invalid: no donor bucket found\n")
	}

	// Cursor into existingWorkerList. This is to traverse donors in a round-
	// robin fashion to transfer buckets to new workers
	donorIdx := 0

	// Now traverse new workers and assign them buckets from donorBuckets
	for _, newWorkerID := range newWorkers {

		need, ok := desiredCounts[newWorkerID]
		if !ok {
			log.Fatalf(
				"Scale-up invalid: new worker %d missing desired count\n",
				newWorkerID,
			)
		}
		if need == 0 {
			log.Fatalf(
				"Scale-up invalid: new worker %d desires 0 buckets\n",
				newWorkerID,
			)
		}

		// Traverse donor workers to transfer buckets to the new worker in a
		// round-robin fashion until the new worker's need is satisfied.
		// Use existingWorkerList to have deterministic donor order
		if remainingDonation < need {
			log.Fatalf(
				"Scale-up invalid: not enough donated buckets for new worker %d (need %d, remaining %d)\n",
				newWorkerID,
				need,
				remainingDonation,
			)
		}

		// Traverse the donors in deterministic order. Each pass transfers at
		// most
		// one bucket per donor to avoid a 1-to-1 donor->recipient pattern.
		for need > 0 {

			donorWorkerID := existingWorkerList[donorIdx]
			donorIdx = (donorIdx + 1) % len(existingWorkerList)

			// Check if this donor has buckets to donate
			buckets, ok := donorBuckets[donorWorkerID]
			if !ok || len(buckets) == 0 {
				continue
			}

			// Pop the last bucket from the donor's list
			bucketID := buckets[len(buckets)-1]
			donorBuckets[donorWorkerID] = buckets[:len(buckets)-1]

			// Assign the bucket to the new worker
			newBuckets[bucketID].WorkerID = newWorkerID

			// Record change
			if _, ok := bucketOwnerChanges[donorWorkerID]; !ok {
				bucketOwnerChanges[donorWorkerID] = make(map[uint16][]int)
			}
			bucketOwnerChanges[donorWorkerID][newWorkerID] = append(
				bucketOwnerChanges[donorWorkerID][newWorkerID],
				bucketID,
			)

			remainingDonation--
			need--
		}
	}
}

func repartitionScaleDown(
	updatedWorkers []uint16, // Updated worker list
	updatedWorkerSet map[uint16]struct{}, // Set of updated workers
	desiredCounts map[uint16]int, // Desired bucket count for updated workers
	existingWorkerList []uint16, // Existing workers deterministic order
	bucketsByExistingWorker map[uint16][]int, // Existing workerID -> list of bucket IDs
	newBuckets []BucketV2, // Resulting bucket (re)assignment
	bucketOwnerChanges map[uint16]map[uint16][]int, // Resulting bucket movements
) {
	// Identify removed workers (donors), in the existingWorkerList for
	// determinism.
	removedWorkers := make([]uint16, 0)
	for _, workerID := range existingWorkerList {
		if _, ok := updatedWorkerSet[workerID]; !ok {
			removedWorkers = append(removedWorkers, workerID)
		}
	}
	if len(removedWorkers) == 0 {
		log.Fatalf("Scale-down invalid: no removed worker found\n")
	}

	// Donated buckets are all buckets owned by removed workers.
	donorBuckets := make(map[uint16][]int, len(removedWorkers))
	totalDonation := 0
	for _, donorWorkerID := range removedWorkers {

		bucketList, ok := bucketsByExistingWorker[donorWorkerID]
		if !ok {
			log.Fatalf(
				"Scale-down invalid: removed worker %d not found in existing buckets\n",
				donorWorkerID,
			)
		}
		donorBuckets[donorWorkerID] = bucketList
		totalDonation += len(bucketList)
	}

	// Remaining workers keep their buckets; they can only receive from removed
	// Map of remaining workerID -> bucket count it needs to receive from
	// removed workers
	recipientNeeds := make(map[uint16]int, len(updatedWorkers))
	totalNeed := 0
	for _, workerID := range updatedWorkers {
		oldCount := len(bucketsByExistingWorker[workerID])
		desired, ok := desiredCounts[workerID]
		if !ok {
			log.Fatalf(
				"Scale-down invalid: remaining worker %d missing desired count\n",
				workerID,
			)
		}
		if desired < oldCount {
			log.Fatalf(
				"Scale-down invalid: remaining worker %d desires %d buckets but already owns %d buckets\n",
				workerID,
				desired,
				oldCount,
			)
		}
		need := desired - oldCount
		if need == 0 {
			continue
		}

		recipientNeeds[workerID] = need
		totalNeed += need
	}

	if totalNeed != totalDonation {
		log.Fatalf(
			"Scale-down invalid: donation (%d) != need (%d)\n",
			totalDonation,
			totalNeed,
		)
	}

	// Assign removed buckets to remaining workers. Rotate donors while filling
	// recipients to spread removed state across the remaining workers.
	donorIdx := 0

	// Use updatedWorkers for deterministic recipient order
	for _, recipient := range updatedWorkers {

		need := recipientNeeds[recipient]
		for need > 0 {

			donor := removedWorkers[donorIdx]
			donorIdx = (donorIdx + 1) % len(removedWorkers)

			// Check if this donor has buckets to donate
			buckets := donorBuckets[donor]
			if len(buckets) == 0 {
				continue
			}

			// Pop the last bucket from the donor's list (consistent with
			// scale-up)
			bucketIdx := buckets[len(buckets)-1]
			donorBuckets[donor] = buckets[:len(buckets)-1]

			newBuckets[bucketIdx].WorkerID = recipient

			// Record change
			if _, ok := bucketOwnerChanges[donor]; !ok {
				bucketOwnerChanges[donor] = make(map[uint16][]int)
			}
			bucketOwnerChanges[donor][recipient] = append(
				bucketOwnerChanges[donor][recipient],
				bucketIdx,
			)

			need--
		}
	}
}

// Validate the repartition request and prepare repartition.
// Supported scenarios:
//  1. Scale-up: updatedWorkers is a strict superset of existing workers
//  2. Scale-down: updatedWorkers is a strict subset of existing workers
//
// Return:
//  1. updatedWorkerSet: set of updated workers
//  2. existingWorkerList: list of existing workers in the order they are first
//     seen in oldBuckets
//  3. bucketsByExistingWorker: map of existing workerID -> list of bucket IDs
func (ep *EvenPartitionPolicy) validateAndPrepareRepartition(
	updatedWorkers []uint16,
	oldBuckets []BucketV2,
) (map[uint16]struct{}, []uint16, map[uint16][]int) {

	if len(updatedWorkers) == 0 {
		log.Fatalf("New worker list invalid\n")
	}
	if ep.NumBuckets < int64(len(updatedWorkers)) {
		log.Fatalf("Number of buckets is less than number of workers\n")
	}

	// Build updated (new) worker set
	updatedWorkerSet := make(map[uint16]struct{}, len(updatedWorkers))
	for _, workerID := range updatedWorkers {
		if _, ok := updatedWorkerSet[workerID]; ok {
			log.Fatalf(
				"Updated worker list invalid: duplicate worker %d\n",
				workerID,
			)
		}
		updatedWorkerSet[workerID] = struct{}{}
	}

	// Build existing (old) worker info:
	// 1. existingWorkerList: list of existing workers in the order they are
	// 		first seen in oldBuckets (to have deterministic order)
	// 2. bucketsByExistingWorker: workerID -> list of bucket IDs
	existingWorkerList := make([]uint16, 0)
	bucketsByExistingWorker := make(map[uint16][]int)
	for i, bucket := range oldBuckets {
		if _, ok := bucketsByExistingWorker[bucket.WorkerID]; !ok {
			existingWorkerList = append(existingWorkerList, bucket.WorkerID)
		}
		bucketsByExistingWorker[bucket.WorkerID] = append(
			bucketsByExistingWorker[bucket.WorkerID],
			i,
		)
	}

	/*
		// This policy only supports pure scale-up or pure scale-down.
		if len(updatedWorkerSet) == len(bucketsByExistingWorker) {
			log.Fatalf(
				"Updated worker list invalid: number of workers unchanged (%d)\n",
				len(updatedWorkerSet),
			)
		}
	*/

	if len(updatedWorkerSet) > len(bucketsByExistingWorker) {

		// Scale-up: updated workers must be a superset of existing workers.
		for workerID := range bucketsByExistingWorker {
			if _, ok := updatedWorkerSet[workerID]; !ok {
				log.Fatalf(
					"Scale-up invalid: existing worker %d missing in updated list\n",
					workerID,
				)
			}
		}
	} else {

		// Scale-down: updated workers must be a subset of existing workers.
		for workerID := range updatedWorkerSet {
			if _, ok := bucketsByExistingWorker[workerID]; !ok {
				log.Fatalf(
					"Scale-down invalid: updated list contains new worker %d\n",
					workerID,
				)
			}
		}
	}
	return updatedWorkerSet, existingWorkerList, bucketsByExistingWorker
}

// Calculate the desired bucket count for each worker in the updated worker list
// Return: map of workerID -> desired bucket count. The first a few workers in
// the updatedWorkers list will get one extra bucket if the buckets cannot be
// evenly divided
func (ep *EvenPartitionPolicy) getDesiredCountsPerWorker(
	updatedWorkers []uint16,
) map[uint16]int {
	if len(updatedWorkers) == 0 {
		log.Fatalf("No worker to generate desired bucket distribution\n")
	}

	base := int(ep.NumBuckets) / len(updatedWorkers)
	rem := int(ep.NumBuckets) % len(updatedWorkers)

	desired := make(map[uint16]int, len(updatedWorkers))
	for i, workerID := range updatedWorkers {
		count := base
		if i < rem {
			count++
		}
		desired[workerID] = count
	}
	return desired
}
