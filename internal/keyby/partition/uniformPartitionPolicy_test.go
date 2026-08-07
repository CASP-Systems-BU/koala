package partition_test

import (
	"testing"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

// Purpose: When NumBuckets is divisible by number of workers, each worker
// should own an equal, consecutive range of buckets in the given worker order.
func TestUniformPartitionPolicy_GenerateBuckets_Divisible(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 12

	policy := partition.NewUniformPartitionPolicy(config)
	buckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	if len(buckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Expected: 4 buckets each in order 1,2,3
	owners := []uint16{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

// Purpose: When NumBuckets is not divisible by number of workers, the first
// (NumBuckets % n) workers should each get one extra bucket.
func TestUniformPartitionPolicy_GenerateBuckets_NotDivisible(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10 // base=3, rem=1

	policy := partition.NewUniformPartitionPolicy(config)
	buckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	if len(buckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Expected: w1 gets 4 (0-3), w2 gets 3 (4-6), w3 gets 3 (7-9)
	owners := []uint16{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

// Purpose: Validate remainder distribution when remainder > 1.
func TestUniformPartitionPolicy_GenerateBuckets_NotDivisible_RemGT1(
	t *testing.T,
) {
	config := configuration.Default()
	config.NumBuckets = 11 // base=2, rem=3

	policy := partition.NewUniformPartitionPolicy(config)
	buckets := policy.GenerateBuckets([]uint16{10, 20, 30, 40})

	if len(buckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Expected: w10 gets 3 (0-2), w20 gets 3 (3-5), w30 gets 3 (6-8), w40 gets
	// 2 (9-10)
	owners := []uint16{10, 10, 10, 20, 20, 20, 30, 30, 30, 40, 40}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

// Purpose: All buckets should be assigned to the single worker.
func TestUniformPartitionPolicy_GenerateBuckets_OneWorker(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 5

	policy := partition.NewUniformPartitionPolicy(config)
	buckets := policy.GenerateBuckets([]uint16{42})

	if len(buckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	for i, b := range buckets {
		if b.WorkerID != 42 {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

// Purpose: Adding a worker should redistribute ranges uniformly and produce
// a correct migration plan mapping source->dest->bucket indices.
// This is for scale-up scenarios.
func TestUniformPartitionPolicy_RePartition_AddWorker(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10

	policy := partition.NewUniformPartitionPolicy(config)

	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})
	newBuckets, migration := policy.RePartition(
		[]uint16{1, 2, 3, 4},
		oldBuckets,
	)

	if len(newBuckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Expected new owners with 4 workers (base=2, rem=2):
	// w1:0-2, w2:3-5, w3:6-7, w4:8-9
	// 			 Old: {1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	owners := []uint16{1, 1, 1, 2, 2, 2, 3, 3, 4, 4}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Expected migration:
	// 1->2: [3], 2->3: [6], 3->4: [8,9]
	if len(migration) != 3 {
		t.Fatalf("Invalid number of migration sources: %d\n", len(migration))
	}

	if len(migration[1]) != 1 || len(migration[1][2]) != 1 ||
		migration[1][2][0] != 3 {
		t.Fatalf("Invalid migration plan for 1->2")
	}
	if len(migration[2]) != 1 || len(migration[2][3]) != 1 ||
		migration[2][3][0] != 6 {
		t.Fatalf("Invalid migration plan for 2->3")
	}
	if len(migration[3]) != 1 || len(migration[3][4]) != 2 ||
		migration[3][4][0] != 8 ||
		migration[3][4][1] != 9 {
		t.Fatalf("Invalid migration plan for 3->4")
	}
}

// Purpose: Removing a worker should redistribute ranges uniformly and produce
// expected migration plan.
// This is for scale-down scenarios.
func TestUniformPartitionPolicy_RePartition_RemoveWorker(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10

	policy := partition.NewUniformPartitionPolicy(config)

	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3, 4})
	newBuckets, migration := policy.RePartition([]uint16{1, 2, 3}, oldBuckets)

	if len(newBuckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Expected new owners with 3 workers (base=3, rem=1):
	// w1:0-3, w2:4-6, w3:7-9
	// 			 old: {1, 1, 1, 2, 2, 2, 3, 3, 4, 4}
	owners := []uint16{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Expected migration:
	// 2->1: [3], 3->2: [6], 4->3: [8,9]
	if len(migration) != 3 {
		t.Fatalf("Invalid number of migration sources: %d\n", len(migration))
	}

	if len(migration[2]) != 1 || len(migration[2][1]) != 1 ||
		migration[2][1][0] != 3 {
		t.Fatalf("Invalid migration plan for 2->1")
	}
	if len(migration[3]) != 1 || len(migration[3][2]) != 1 ||
		migration[3][2][0] != 6 {
		t.Fatalf("Invalid migration plan for 3->2")
	}
	if len(migration[4]) != 1 || len(migration[4][3]) != 2 ||
		migration[4][3][0] != 8 ||
		migration[4][3][1] != 9 {
		t.Fatalf("Invalid migration plan for 4->3")
	}
}

// Purpose: Same number of workers but different IDs should remap only the
// affected ranges according to the new worker order.
// This is for changing task placement scenarios.
func TestUniformPartitionPolicy_RePartition_MigrateWorkerIDs(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10

	policy := partition.NewUniformPartitionPolicy(config)

	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})
	newBuckets, migration := policy.RePartition([]uint16{1, 2, 4}, oldBuckets)

	if len(newBuckets) != int(config.NumBuckets) {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Expected owners with workers [1,2,4]: w1:0-3, w2:4-6, w4:7-9
	// 			 Old: {1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	owners := []uint16{1, 1, 1, 1, 2, 2, 2, 4, 4, 4}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Expected migration: 3->4: [7,8,9]
	if len(migration) != 1 {
		t.Fatalf("Invalid number of migration sources: %d\n", len(migration))
	}
	if len(migration[3]) != 1 || len(migration[3][4]) != 3 {
		t.Fatalf("Invalid migration plan for 3->4 length")
	}
	if migration[3][4][0] != 7 || migration[3][4][1] != 8 ||
		migration[3][4][2] != 9 {
		t.Fatalf("Invalid migration plan for 3->4 contents")
	}
}

// TestUniformPartitionPolicy_RePartition_NoChange
// Purpose: Repartition with the same worker list should produce an empty
// migration plan.
func TestUniformPartitionPolicy_RePartition_NoChange(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10

	policy := partition.NewUniformPartitionPolicy(config)

	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})
	_, migration := policy.RePartition([]uint16{1, 2, 3}, oldBuckets)

	if len(migration) != 0 {
		t.Fatalf("Invalid number of migration sources: %d\n", len(migration))
	}
}
