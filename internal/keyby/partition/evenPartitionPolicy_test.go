package partition_test

import (
	"reflect"
	"testing"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

func TestEvenPartitionPolicy_RePartitionV2_ScaleUp(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial: 2 workers [1, 2] -> 5 buckets each
	initialWorkers := []uint16{1, 2}
	buckets := policy.GenerateBucketsV2(initialWorkers)
	if len(buckets) != 10 {
		t.Errorf("expected 10 buckets, got %d", len(buckets))
	}

	bucketCounts := make(map[uint16]int)
	for _, b := range buckets {
		bucketCounts[b.WorkerID]++
	}
	if bucketCounts[1] != 5 {
		t.Errorf("worker 1 expected 10 buckets, got %d", bucketCounts[1])
	}
	if bucketCounts[2] != 5 {
		t.Errorf("worker 2 expected 10 buckets, got %d", bucketCounts[2])
	}

	// Scale up: 2 -> 3 workers [1, 2, 3]
	// Expected: 10 buckets / 3 workers = 3, 3, 4 (or similar)
	updatedWorkers := []uint16{1, 2, 3}
	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	// Verify counts
	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	// The implementation assigns extra buckets to first few workers in the list
	// 10 / 3 = 3 rem 1. So first worker gets 4, others get 3.
	// Updated list is [1, 2, 3]. So worker 1 gets 4, workers 2 and 3 get 3.
	if newCounts[1] != 4 {
		t.Errorf("worker 1 expected 4 buckets, got %d", newCounts[1])
	}
	if newCounts[2] != 3 {
		t.Errorf("worker 2 expected 3 buckets, got %d", newCounts[2])
	}
	if newCounts[3] != 3 {
		t.Errorf("worker 3 expected 3 buckets, got %d", newCounts[3])
	}

	// Verify changes map
	// 1 -> 3: [bucketID]
	// 2 -> 3: [bucketID, bucketID]
	if _, ok := changes[1]; !ok {
		t.Errorf("expected changes from worker 1")
	}
	if _, ok := changes[2]; !ok {
		t.Errorf("expected changes from worker 2")
	}

	// We check if changes map entries exist for new worker 3
	donatedTo3 := 0
	if m, ok := changes[1]; ok {
		donatedTo3 += len(m[3])
	}
	if m, ok := changes[2]; ok {
		donatedTo3 += len(m[3])
	}
	if donatedTo3 != 3 {
		t.Errorf("expected 3 buckets donated to worker 3, got %d", donatedTo3)
	}

	// Verify actual bucket IDs match changes
	for src, dests := range changes {
		for dest, bucketIDs := range dests {
			for _, bid := range bucketIDs {
				if newBuckets[bid].WorkerID != dest {
					t.Errorf(
						"bucket %d expected worker %d, got %d (source %d)",
						bid,
						dest,
						newBuckets[bid].WorkerID,
						src,
					)
				}
			}
		}
	}
}

func TestEvenPartitionPolicy_RePartitionV2_ScaleDown(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial: 3 workers [1, 2, 3]
	// 10 / 3 = 3 rem 1.
	// Worker 1: 4 buckets
	// Worker 2: 3 buckets
	// Worker 3: 3 buckets
	initialWorkers := []uint16{1, 2, 3}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	// Scale down: 3 -> 2 workers [1, 2]
	// Remove 3.
	updatedWorkers := []uint16{1, 2}
	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	// Expected: 10 / 2 = 5 each.
	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	if newCounts[1] != 5 {
		t.Errorf("worker 1 expected 5 buckets, got %d", newCounts[1])
	}
	if newCounts[2] != 5 {
		t.Errorf("worker 2 expected 5 buckets, got %d", newCounts[2])
	}

	// Verify changes
	// 3 -> 1: 1 bucket
	// 3 -> 2: 2 buckets
	if _, ok := changes[3]; !ok {
		t.Errorf("expected changes from worker 3")
	}
	if len(changes[3][1]) != 1 {
		t.Errorf("expected 1 bucket from 3 to 1, got %d", len(changes[3][1]))
	}
	if len(changes[3][2]) != 2 {
		t.Errorf("expected 2 buckets from 3 to 2, got %d", len(changes[3][2]))
	}
}

func TestEvenPartitionPolicy_RePartitionV2_ScaleUp_From1(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial: 1 worker [1] -> 10 buckets
	initialWorkers := []uint16{1}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	// Scale up: 1 -> 4 workers [1, 2, 3, 4]
	// 10 / 4 = 2 rem 2.
	// 1: 3 buckets
	// 2: 3 buckets
	// 3: 2 buckets
	// 4: 2 buckets
	updatedWorkers := []uint16{1, 2, 3, 4}
	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	if newCounts[1] != 3 {
		t.Errorf("worker 1 expected 3 bucket, got %d", newCounts[1])
	}
	if newCounts[2] != 3 {
		t.Errorf("worker 2 expected 3 bucket, got %d", newCounts[2])
	}
	if newCounts[3] != 2 {
		t.Errorf("worker 3 expected 2 bucket, got %d", newCounts[3])
	}
	if newCounts[4] != 2 {
		t.Errorf("worker 4 expected 2 bucket, got %d", newCounts[4])
	}

	// Verify moves
	if _, ok := changes[1]; !ok {
		t.Errorf("expected changes from worker 1")
	}
	// 1 -> 2: needs 3
	if len(changes[1][2]) != 3 {
		t.Errorf("expected 3 buckets from 1 to 2, got %d", len(changes[1][2]))
	}
	// 1 -> 3: needs 2
	if len(changes[1][3]) != 2 {
		t.Errorf("expected 2 buckets from 1 to 3, got %d", len(changes[1][3]))
	}
	// 1 -> 4: needs 2
	if len(changes[1][4]) != 2 {
		t.Errorf("expected 2 buckets from 1 to 4, got %d", len(changes[1][4]))
	}
}

func TestEvenPartitionPolicy_RePartitionV2_ScaleDown_To1(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial: 4 workers [1, 2, 3, 4]
	// 1: 3, 2: 3, 3: 2, 4: 2
	initialWorkers := []uint16{1, 2, 3, 4}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	// Scale down: 4 -> 1 worker [1]
	updatedWorkers := []uint16{1}
	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	if newCounts[1] != 10 {
		t.Errorf("worker 1 expected 10 buckets, got %d", newCounts[1])
	}

	// Verify changes
	// 2 -> 1: 3 buckets
	if len(changes[2][1]) != 3 {
		t.Errorf("expected 3 buckets from 2 to 1, got %d", len(changes[2][1]))
	}
	// 3 -> 1: 2 buckets
	if len(changes[3][1]) != 2 {
		t.Errorf("expected 2 buckets from 3 to 1, got %d", len(changes[3][1]))
	}
	// 4 -> 1: 2 buckets
	if len(changes[4][1]) != 2 {
		t.Errorf("expected 2 buckets from 4 to 1, got %d", len(changes[4][1]))
	}
}

func TestEvenPartitionPolicy_RePartitionV2_Stability(t *testing.T) {
	// Ensure that repeated repartitioning doesn't cause unnecessary moves
	// Note: implementation doesn't support equal number of workers repartition
	// currently
	// So we test Up -> Down -> Up cycles.

	config := configuration.Default()
	config.NumBuckets = 12 // Divisible by 2, 3, 4, 6
	policy := partition.NewEvenPartitionPolicy(config)

	// 1. Initial 2 workers
	w2 := []uint16{1, 2}
	b2 := policy.GenerateBucketsV2(w2) // 6, 6

	// 2. Up to 3 workers
	w3 := []uint16{1, 2, 3}
	b3, _ := policy.RePartitionV2(w3, b2) // 4, 4, 4

	// 3. Up to 4 workers
	w4 := []uint16{1, 2, 3, 4}
	b4, _ := policy.RePartitionV2(w4, b3) // 3, 3, 3, 3

	// 4. Down to 2 workers
	b2_new, _ := policy.RePartitionV2(w2, b4) // 6, 6

	// Verify final state is valid (6, 6)
	counts := make(map[uint16]int)
	for _, b := range b2_new {
		counts[b.WorkerID]++
	}
	if counts[1] != 6 {
		t.Errorf("worker 1 expected 6 buckets, got %d", counts[1])
	}
	if counts[2] != 6 {
		t.Errorf("worker 2 expected 6 buckets, got %d", counts[2])
	}
}

// Test deterministic assignment order
func TestEvenPartitionPolicy_RePartitionV2_Determinism(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 100
	policy := partition.NewEvenPartitionPolicy(config)

	initialWorkers := []uint16{1, 2, 3}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	updatedWorkers := []uint16{1, 2, 3, 4, 5} // Scale up

	// Run 1
	res1, ch1 := policy.RePartitionV2(updatedWorkers, buckets)

	// Run 2
	res2, ch2 := policy.RePartitionV2(updatedWorkers, buckets)

	if !reflect.DeepEqual(res1, res2) {
		t.Errorf("bucket result is not deterministic")
	}
	if !reflect.DeepEqual(ch1, ch2) {
		t.Errorf("change map is not deterministic")
	}
}

func TestEvenPartitionPolicy_RePartitionV2_MultipleNewWorkers(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 10
	policy := partition.NewEvenPartitionPolicy(config)

	// Start with 1 worker
	initialWorkers := []uint16{1}
	buckets := policy.GenerateBucketsV2(initialWorkers) // 1 has 10 buckets

	// Add 2 workers: 1 -> 1, 2, 3
	updatedWorkers := []uint16{1, 2, 3}
	// 10 / 3 = 3 rem 1.
	// 1: 4 buckets
	// 2: 3 buckets
	// 3: 3 buckets

	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	if newCounts[1] != 4 {
		t.Errorf("worker 1 expected 4 buckets, got %d", newCounts[1])
	}
	if newCounts[2] != 3 {
		t.Errorf("worker 2 expected 3 buckets, got %d", newCounts[2])
	}
	if newCounts[3] != 3 {
		t.Errorf("worker 3 expected 3 buckets, got %d", newCounts[3])
	}

	// 1 donates 6 buckets total.
	// 3 to worker 2.
	// 3 to worker 3.
	if len(changes[1][2]) != 3 {
		t.Errorf("expected 3 buckets from 1 to 2, got %d", len(changes[1][2]))
	}
	if len(changes[1][3]) != 3 {
		t.Errorf("expected 3 buckets from 1 to 3, got %d", len(changes[1][3]))
	}
}

// Test Scale Down with multiple removed workers
func TestEvenPartitionPolicy_RePartitionV2_ScaleDown_MultipleRemoved(
	t *testing.T,
) {
	config := configuration.Default()
	config.NumBuckets = 12
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial 4 workers [1, 2, 3, 4] -> 3 buckets each
	buckets := policy.GenerateBucketsV2([]uint16{1, 2, 3, 4})

	// Scale down to 2 workers [1, 2]. Remove 3 and 4.
	updatedWorkers := []uint16{1, 2}
	// Target: 6 buckets each.
	// 1 has 3, needs 3.
	// 2 has 3, needs 3.
	// 3 and 4 donate 3 each (total 6).

	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}
	if newCounts[1] != 6 {
		t.Errorf("worker 1 expected 6 buckets, got %d", newCounts[1])
	}
	if newCounts[2] != 6 {
		t.Errorf("worker 2 expected 6 buckets, got %d", newCounts[2])
	}

	// Check donations
	// 3 -> 1 or 2
	// 4 -> 1 or 2
	donatedFrom3 := len(changes[3][1]) + len(changes[3][2])
	donatedFrom4 := len(changes[4][1]) + len(changes[4][2])

	if donatedFrom3 != 3 {
		t.Errorf("worker 3 expected to donate 3 buckets, got %d", donatedFrom3)
	}
	if donatedFrom4 != 3 {
		t.Errorf("worker 4 expected to donate 3 buckets, got %d", donatedFrom4)
	}

	receivedBy1 := len(changes[3][1]) + len(changes[4][1])
	receivedBy2 := len(changes[3][2]) + len(changes[4][2])

	if receivedBy1 != 3 {
		t.Errorf("worker 1 expected to receive 3 buckets, got %d", receivedBy1)
	}
	if receivedBy2 != 3 {
		t.Errorf("worker 2 expected to receive 3 buckets, got %d", receivedBy2)
	}
}

func TestEvenPartitionPolicy_RePartitionV2_ScaleUp_4to8(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 16
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial 4 workers: 16 / 4 = 4 buckets each
	initialWorkers := []uint16{1, 2, 3, 4}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	// Verify initial state
	counts := make(map[uint16]int)
	for _, b := range buckets {
		counts[b.WorkerID]++
	}
	for _, w := range initialWorkers {
		if counts[w] != 4 {
			t.Fatalf("Worker %d expected 4 buckets, got %d", w, counts[w])
		}
	}

	// Scale up to 8 workers
	updatedWorkers := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	// Expected: 16 / 8 = 2 buckets each.
	// Workers 1-4 have 4, need 2. Donate 2 each.
	// Workers 5-8 need 2. Receive 2 each.

	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}

	for _, w := range updatedWorkers {
		if newCounts[w] != 2 {
			t.Errorf("Worker %d expected 2 buckets, got %d", w, newCounts[w])
		}
	}

	// verify existing workers 1-4 donated buckets
	for w := uint16(1); w <= 4; w++ {
		if _, ok := changes[w]; !ok {
			t.Errorf("Expected worker %d to donate buckets", w)
		}
		// Calculate total donated by this worker
		donated := 0
		for _, donatedBuckets := range changes[w] {
			donated += len(donatedBuckets)
		}
		if donated != 2 {
			t.Errorf(
				"Worker %d expected to donate 2 buckets, donated %d",
				w,
				donated,
			)
		}
	}

	// verify new workers 5-8 received buckets
	// They shouldn't appear as keys in changes (source), but as keys in the
	// inner map (dest)
	// We iterate all changes to count receptions for 5-8
	receivedCounts := make(map[uint16]int)
	for _, dests := range changes {
		for dest, buckets := range dests {
			receivedCounts[dest] += len(buckets)
		}
	}

	for w := uint16(5); w <= 8; w++ {
		if receivedCounts[w] != 2 {
			t.Errorf(
				"Worker %d expected to receive 2 buckets, received %d",
				w,
				receivedCounts[w],
			)
		}
	}
}

func TestEvenPartitionPolicy_RePartitionV2_ScaleDown_8to4(t *testing.T) {
	config := configuration.Default()
	config.NumBuckets = 16
	policy := partition.NewEvenPartitionPolicy(config)

	// Initial 8 workers: 16 / 8 = 2 buckets each
	initialWorkers := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	buckets := policy.GenerateBucketsV2(initialWorkers)

	// Verify initial state
	counts := make(map[uint16]int)
	for _, b := range buckets {
		counts[b.WorkerID]++
	}
	for _, w := range initialWorkers {
		if counts[w] != 2 {
			t.Fatalf("Worker %d expected 2 buckets, got %d", w, counts[w])
		}
	}

	// Scale down to 4 workers (remove 5, 6, 7, 8)
	updatedWorkers := []uint16{1, 2, 3, 4}
	// Expected: 16 / 4 = 4 buckets each.
	// Workers 1-4 have 2, need 4. Need 2 more each.
	// Workers 5-8 (removed) have 2. Donate 2 each.

	newBuckets, changes := policy.RePartitionV2(updatedWorkers, buckets)

	newCounts := make(map[uint16]int)
	for _, b := range newBuckets {
		newCounts[b.WorkerID]++
	}

	for _, w := range updatedWorkers {
		if newCounts[w] != 4 {
			t.Errorf("Worker %d expected 4 buckets, got %d", w, newCounts[w])
		}
	}

	// Verify removed workers 5-8 donated buckets
	for w := uint16(5); w <= 8; w++ {
		if _, ok := changes[w]; !ok {
			t.Errorf("Expected worker %d to donate buckets", w)
		}
		donated := 0
		for _, donatedBuckets := range changes[w] {
			donated += len(donatedBuckets)
		}
		if donated != 2 {
			t.Errorf(
				"Worker %d expected to donate 2 buckets, donated %d",
				w,
				donated,
			)
		}
	}

	// Verify remaining workers 1-4 received buckets
	receivedCounts := make(map[uint16]int)
	for _, dests := range changes {
		for dest, buckets := range dests {
			receivedCounts[dest] += len(buckets)
		}
	}

	for w := uint16(1); w <= 4; w++ {
		if receivedCounts[w] != 2 {
			t.Errorf(
				"Worker %d expected to receive 2 buckets, received %d",
				w,
				receivedCounts[w],
			)
		}
	}
}
