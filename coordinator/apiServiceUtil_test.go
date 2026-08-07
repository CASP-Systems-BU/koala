package coordinator_test

import (
	"testing"

	"github.com/CASP-Systems-BU/koala/coordinator"
)

func TestGenerateMigrationPlan(t *testing.T) {

	// The helper function GenerateMigrationPlan is used to re-format the bucket
	// owner change information for state migration purposes.

	// Input: map[uint16]map[uint16][]int
	// Map[source_worker_id] -> Map[destination_worker_id] -> List_of_bucket_ids

	// Output: map[uint16][]*pb.MigrationInfo
	// Map[destination_worker_id] -> List_of_MigrationInfo
	// Each MigrationInfo contains the source_worker_id and bucket_ranges

	// In this test, assume we have 4 workers 0 - 3, and 10 buckets 0 - 9
	// we want to migrate:
	// bucket 0, 5, 6 from worker 0 to worker 2
	// bucket 7, 8 from worker 0 to worker 3
	// bucket 1, 3, 4 from worker 1 to worker 2
	// bucket 2 from worker 1 to worker 3

	bucketOwnerChanges := make(map[uint16]map[uint16][]int)
	// bucket 0, 5, 6 from worker 0 to worker 2
	// bucket 7, 8 from worker 0 to worker 3
	bucketOwnerChanges[0] = make(map[uint16][]int)
	bucketOwnerChanges[0][2] = []int{0, 5, 6}
	bucketOwnerChanges[0][3] = []int{7, 8}
	// bucket 1, 3, 4 from worker 1 to worker 2
	// bucket 2 from worker 1 to worker 3
	bucketOwnerChanges[1] = make(map[uint16][]int)
	bucketOwnerChanges[1][2] = []int{1, 3, 4}
	bucketOwnerChanges[1][3] = []int{2}

	s := createMockAPIServer()
	migrationPlan := s.GenerateMigrationPlan(bucketOwnerChanges)

	// Check the migration plan
	if len(migrationPlan) != 2 {
		t.Errorf(
			"Expected 2 workers in the migration plan, got %d",
			len(migrationPlan),
		)
	}
	if _, ok := migrationPlan[2]; !ok {
		t.Errorf("Expected worker 2 in the migration plan\n")
	}
	if _, ok := migrationPlan[3]; !ok {
		t.Errorf("Expected worker 3 in the migration plan\n")
	}

	// worker 2
	if len(migrationPlan[2]) != 2 {
		t.Errorf(
			"Expected 2 migration info for worker 2, got %d",
			len(migrationPlan[2]),
		)
	}
	for _, mi := range migrationPlan[2] {
		switch mi.TargetWorkerAddr {
		case "addr0":
			if len(mi.BucketRanges) != 2 {
				t.Errorf(
					"Expected 2 bucket ranges for worker 2 from worker 0, got %d",
					len(mi.BucketRanges),
				)
			}
			if mi.BucketRanges[0].LowerBucketIdx != 0 ||
				mi.BucketRanges[0].UpperBucketIdx != 0 {
				t.Errorf("Unexpected range\n")
			}
			if mi.BucketRanges[1].LowerBucketIdx != 5 ||
				mi.BucketRanges[1].UpperBucketIdx != 6 {
				t.Errorf("Unexpected range\n")
			}
		case "addr1":
			if len(mi.BucketRanges) != 2 {
				t.Errorf(
					"Expected 2 bucket ranges for worker 2 from worker 1, got %d",
					len(mi.BucketRanges),
				)
			}
			if mi.BucketRanges[0].LowerBucketIdx != 1 ||
				mi.BucketRanges[0].UpperBucketIdx != 1 {
				t.Errorf("Unexpected range\n")
			}
			if mi.BucketRanges[1].LowerBucketIdx != 3 ||
				mi.BucketRanges[1].UpperBucketIdx != 4 {
				t.Errorf("Unexpected range\n")
			}
		default:
			t.Errorf("Unexpected worker address %s", mi.TargetWorkerAddr)
		}
	}

	// worker 3
	if len(migrationPlan[3]) != 2 {
		t.Errorf(
			"Expected 2 migration info for worker 3, got %d",
			len(migrationPlan[3]),
		)
	}
	for _, mi := range migrationPlan[3] {
		switch mi.TargetWorkerAddr {
		case "addr0":
			if len(mi.BucketRanges) != 1 {
				t.Errorf(
					"Expected 1 bucket range for worker 3 from worker 0, got %d",
					len(mi.BucketRanges),
				)
			}
			if mi.BucketRanges[0].LowerBucketIdx != 7 ||
				mi.BucketRanges[0].UpperBucketIdx != 8 {
				t.Errorf("Unexpected range\n")
			}
		case "addr1":
			if len(mi.BucketRanges) != 1 {
				t.Errorf(
					"Expected 1 bucket range for worker 3 from worker 1, got %d",
					len(mi.BucketRanges),
				)
			}
			if mi.BucketRanges[0].LowerBucketIdx != 2 ||
				mi.BucketRanges[0].UpperBucketIdx != 2 {
				t.Errorf("Unexpected range\n")
			}
		default:
			t.Errorf("Unexpected worker address %s", mi.TargetWorkerAddr)
		}
	}
}

func createMockAPIServer() *coordinator.APIServer {

	// Create mock coordinator
	managedWorkers := make(map[uint16]*coordinator.ManagedWorker)
	managedWorkers[0] = &coordinator.ManagedWorker{
		StateCommAddr: "addr0",
	}
	managedWorkers[1] = &coordinator.ManagedWorker{
		StateCommAddr: "addr1",
	}
	managedWorkers[2] = &coordinator.ManagedWorker{
		StateCommAddr: "addr2",
	}
	managedWorkers[3] = &coordinator.ManagedWorker{
		StateCommAddr: "addr3",
	}

	c := &coordinator.Coordinator{
		WorkerManager: &coordinator.WorkerManager{
			Workers:      managedWorkers,
			NextWorkerId: uint16(len(managedWorkers)),
		},
	}

	return &coordinator.APIServer{
		Coordinator: c,
	}
}
