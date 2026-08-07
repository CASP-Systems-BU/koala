package partition

import (
	"log"

	"github.com/CASP-Systems-BU/koala/internal/constant"
)

type Bucket struct {

	// Worker ID of the worker that owns this bucket
	WorkerID uint16

	// TODO: Pointer to the skip list for fine-grained key partitioning
	// nil by default
	// SkipList *skipList.SkipList
}

// The maximum number of buckets supported by the system
func MaxSupportedBuckets() int64 {

	var res int64
	switch constant.BucketIdxSize {
	case 4:
		// we use uint32 to represent the bucket idx info
		// the maximum number of buckets is 2^32 = 4,294,967,296
		res = 1 << 32
	default:
		log.Fatalf("Unsupported bucket idx size: %d\n", constant.BucketIdxSize)
	}

	// We reserve 1 bucket for range queries where we use [bucketIdx,
	// bucketIdx+1) to represent the key range for the bucket. We cannot
	// overflow the bucket idx
	return res - 1
}
