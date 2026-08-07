package partition_test

import (
	"testing"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

func TestHashPartitionPolicy_GenerateBuckets1(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)
	// 3 workers, each worker has 2 tokens, totally 10 buckets
	buckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	// Token info based on config above
	// Sorted tokens by index id (sort by hash value if index id is the same)
	// Token 0 - hash value: 9944607518023373071, bucket index: 1, worker id: 1
	// Token 1 - hash value: 12056337994852197731, bucket index: 1, worker id: 2
	// Token 2 - hash value: 1895687879257350853, bucket index: 3, worker id: 3
	// Token 3 - hash value: 5019254374937295136, bucket index: 6, worker id: 2
	// Token 4 - hash value: 15270045795057849137, bucket index: 7, worker id: 1
	// Token 5 - hash value: 14809271240781532968, bucket index: 8, worker id: 3

	// The expected bucket list should be:
	// Owner: |-w3-|-w3-|-w2-|-w2-|-w3-|-w3-|-w3-|-w2-|-w1-|-w3-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |

	if len(buckets) != 10 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{3, 3, 2, 2, 3, 3, 3, 2, 1, 3}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestHashPartitionPolicy_GenerateBuckets2(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 12
	config.HashPartitionNumTokens = 3
	config.HashPartitionSeeds = []uint64{4321, 8765, 10987}

	policy := partition.NewHashPartitionPolicy(config)
	// 3 workers, each worker has 3 tokens, totally 12 buckets
	buckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	// Token info based on config above
	// Sorted tokens by index id (sort by hash value if index id is the same)
	// Token 0 - hash value: 4093219658747543640, bucket index: 0, worker id: 2
	// Token 1 - hash value: 4922310288054338581, bucket index: 1, worker id: 3
	// Token 2 - hash value: 15376555253303275658, bucket index: 2, worker id: 2
	// Token 3 - hash value: 8688631210421728875, bucket index: 3, worker id: 1
	// Token 4 - hash value: 11709468094262547701, bucket index: 5, worker id: 3
	// Token 5 - hash value: 13618598231221210145, bucket index: 5, worker id: 2
	// Token 6 - hash value: 3223531376055075415, bucket index: 7, worker id: 1
	// Token 7 - hash value: 10293331953736358914, bucket index: 10, worker id:
	// 3 Token 8 - hash value: 16775114797654650023, bucket index: 11, worker
	// id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w2-|-w3-|-w2-|-w1-|-w1-|-w2-|-w2-|-w1-|-w1-|-w1-|-w3-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 | 11 |

	if len(buckets) != 12 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 2, 3, 2, 1, 1, 2, 2, 1, 1, 1, 3}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestHashPartitionPolicy_OneTokenPerWorker(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 8
	config.HashPartitionNumTokens = 1
	config.HashPartitionSeeds = []uint64{4321}

	policy := partition.NewHashPartitionPolicy(config)
	// 3 workers, each worker has 1 token, totally 8 buckets
	buckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	// Token info based on config above
	// Sorted tokens by index id (sort by hash value if index id is the same)
	// Token 0 - hash value: 4093219658747543640, bucket index: 0, worker id: 2
	// Token 1 - hash value: 11709468094262547701, bucket index: 5, worker id: 3
	// Token 2 - hash value: 3223531376055075415, bucket index: 7, worker id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w2-|-w2-|-w2-|-w2-|-w2-|-w3-|-w3-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |

	if len(buckets) != 8 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 2, 2, 2, 2, 2, 3, 3}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestHashPartitionPolicy_OneBucketOneWorker(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 1
	config.HashPartitionNumTokens = 1
	config.HashPartitionSeeds = []uint64{4321}

	policy := partition.NewHashPartitionPolicy(config)
	// 1 worker, each worker has 1 token, totally 1 bucket
	buckets := policy.GenerateBuckets([]uint16{1})

	// The expected bucket list should be:
	// Owner: |-w1-|
	// Idx:	  |  0 |

	if len(buckets) != 1 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestHashPartitionPolicy_MultipleBucketOneWorker(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 3
	config.HashPartitionNumTokens = 4
	config.HashPartitionSeeds = []uint64{4321, 743, 35764, 124}

	policy := partition.NewHashPartitionPolicy(config)
	// 1 worker, each worker has 1 token, totally 3 buckets
	buckets := policy.GenerateBuckets([]uint16{1})

	// Token info based on config above
	// Sorted tokens by index id (sort by hash value if index id is the same)
	// Token 0 - hash value: 7584733382066271246, bucket index: 0, worker id: 1
	// Token 1 - hash value: 11172201254388418932, bucket index: 0, worker id: 1
	// Token 2 - hash value: 3223531376055075415, bucket index: 1, worker id: 1
	// Token 3 - hash value: 16009232467482619918, bucket index: 1, worker id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w1-|-w1-|
	// Idx:	  |  0 |  1 |  2 |

	if len(buckets) != 3 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 1, 1}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestHashPartitionPolicy_SameTokenHashFunction(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 4
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{4321, 4321}

	policy := partition.NewHashPartitionPolicy(config)
	// 2 workers, each worker has 2 tokens, totally 4 buckets
	buckets := policy.GenerateBuckets([]uint16{1, 2})

	// Token info based on config above
	// Sorted tokens by index id (sort by hash value if index id is the same)
	// Token 0 - hash value: 4093219658747543640, bucket index: 0, worker id: 2
	// Token 1 - hash value: 4093219658747543640, bucket index: 0, worker id: 2
	// Token 2 - hash value: 3223531376055075415, bucket index: 3, worker id: 1
	// Token 3 - hash value: 3223531376055075415, bucket index: 3, worker id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w2-|-w2-|-w2-|
	// Idx:	  |  0 |  1 |  2 |  3 |

	if len(buckets) != 4 {
		t.Fatalf("Invalid number of buckets: %d\n", len(buckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 2, 2, 2}
	for i, b := range buckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}
}

func TestRePartition_AddWorker1(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)

	// Old bucket list
	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	newBuckets, migrationPlan := policy.RePartition(
		[]uint16{1, 2, 3, 4},
		oldBuckets,
	)

	// New sorted tokens for workers 1, 2, 3, 4
	// Token 0 - hash value: 9944607518023373071, bucket index: 1, worker id: 1
	// Token 1 - hash value: 12056337994852197731, bucket index: 1, worker id: 2
	// Token 2 - hash value: 1895687879257350853, bucket index: 3, worker id: 3
	// Token 3 - hash value: 5019254374937295136, bucket index: 6, worker id: 2
	// Token 4 - hash value: 15270045795057849137, bucket index: 7, worker id: 1
	// Token 5 - hash value: 14809271240781532968, bucket index: 8, worker id: 3
	// Token 6 - hash value: 17591931051514362478, bucket index: 8, worker id: 4
	// Token 7 - hash value: 17995134439505708728, bucket index: 8, worker id: 4

	// The expected bucket list should be:
	// Owner: |-w4-|-w4-|-w2-|-w2-|-w3-|-w3-|-w3-|-w2-|-w1-|-w4-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |

	if len(newBuckets) != 10 {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Check the owner of each bucket
	owners := []uint16{4, 4, 2, 2, 3, 3, 3, 2, 1, 4}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Check the migration plan
	// Bucket 0, 1, 9 should be migrated from worker 3 to worker 4
	if len(migrationPlan) != 1 {
		t.Fatalf("Invalid number of migration plan: %d\n", len(migrationPlan))
	}

	if len(migrationPlan[3]) != 1 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3: %d\n",
			len(migrationPlan[3]),
		)
	}

	if len(migrationPlan[3][4]) != 3 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3 to 4: %d\n",
			len(migrationPlan[3][4]),
		)
	}

	if migrationPlan[3][4][0] != 0 || migrationPlan[3][4][1] != 1 ||
		migrationPlan[3][4][2] != 9 {
		t.Fatalf("Invalid migration plan for worker 3 to 4\n")
	}
}

func TestRePartition_AddWorker2(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 12
	config.HashPartitionNumTokens = 3
	config.HashPartitionSeeds = []uint64{4321, 8765, 10987}

	policy := partition.NewHashPartitionPolicy(config)

	// Old bucket list
	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	newBuckets, migrationPlan := policy.RePartition(
		[]uint16{1, 2, 3, 4},
		oldBuckets,
	)

	// New bucket list for workers 1, 2, 3, 4
	// Token 0 - hash value: 4093219658747543640, bucket index: 0, worker id: 2
	// Token 1 - hash value: 4922310288054338581, bucket index: 1, worker id: 3
	// Token 2 - hash value: 15376555253303275658, bucket index: 2, worker id: 2
	// Token 3 - hash value: 15996531386262128642, bucket index: 2, worker id: 4
	// Token 4 - hash value: 8688631210421728875, bucket index: 3, worker id: 1
	// Token 5 - hash value: 16100238851390950503, bucket index: 3, worker id: 4
	// Token 6 - hash value: 7136975443582785532, bucket index: 4, worker id: 4
	// Token 7 - hash value: 11709468094262547701, bucket index: 5, worker id: 3
	// Token 8 - hash value: 13618598231221210145, bucket index: 5, worker id: 2
	// Token 9 - hash value: 3223531376055075415, bucket index: 7, worker id: 1
	// Token 10 - hash value: 10293331953736358914, bucket index: 10, worker id:
	// 3 Token 11 - hash value: 16775114797654650023, bucket index: 11, worker
	// id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w2-|-w3-|-w4-|-w4-|-w4-|-w2-|-w2-|-w1-|-w1-|-w1-|-w3-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 | 11 |

	if len(newBuckets) != 12 {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 2, 3, 4, 4, 4, 2, 2, 1, 1, 1, 3}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Check the migration plan
	// Bucket 3 should be migrated from worker 2 to worker 4
	// Bucket 4, 5 should be migrated from worker 1 to worker 4

	if len(migrationPlan) != 2 {
		t.Fatalf("Invalid number of migration plan: %d\n", len(migrationPlan))
	}

	if len(migrationPlan[2]) != 1 {
		t.Fatalf(
			"Invalid number of migration plan for worker 2: %d\n",
			len(migrationPlan[2]),
		)
	}

	if len(migrationPlan[1]) != 1 {
		t.Fatalf(
			"Invalid number of migration plan for worker 1: %d\n",
			len(migrationPlan[1]),
		)
	}

	if len(migrationPlan[2][4]) != 1 {
		t.Fatalf(
			"Invalid number of migration plan for worker 2 to 4: %d\n",
			len(migrationPlan[2][4]),
		)
	}

	if len(migrationPlan[1][4]) != 2 {
		t.Fatalf(
			"Invalid number of migration plan for worker 1 to 4: %d\n",
			len(migrationPlan[1][4]),
		)
	}
}

func TestRePartition_RemoveWorker(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)

	// Old bucket list
	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	newBuckets, migrationPlan := policy.RePartition(
		[]uint16{1, 2},
		oldBuckets,
	)

	// Token 0 - hash value: 9944607518023373071, bucket index: 1, worker id: 1
	// Token 1 - hash value: 12056337994852197731, bucket index: 1, worker id: 2
	// Token 2 - hash value: 5019254374937295136, bucket index: 6, worker id: 2
	// Token 3 - hash value: 15270045795057849137, bucket index: 7, worker id: 1

	// The expected bucket list should be:
	// Owner: |-w1-|-w1-|-w2-|-w2-|-w2-|-w2-|-w2-|-w2-|-w1-|-w1-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |

	if len(newBuckets) != 10 {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Check the owner of each bucket
	owners := []uint16{1, 1, 2, 2, 2, 2, 2, 2, 1, 1}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Check the migration plan
	// Bucket 9, 0, 1 should be migrated from worker 3 to worker 1
	// Bucket 4, 5, 6 should be migrated from worker 3 to worker 2

	if len(migrationPlan) != 1 {
		t.Fatalf("Invalid number of migration plan: %d\n", len(migrationPlan))
	}

	if len(migrationPlan[3]) != 2 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3: %d\n",
			len(migrationPlan[3]),
		)
	}

	if len(migrationPlan[3][1]) != 3 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3 to 1: %d\n",
			len(migrationPlan[3][1]),
		)
	}

	if len(migrationPlan[3][2]) != 3 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3 to 2: %d\n",
			len(migrationPlan[3][2]),
		)
	}

	if migrationPlan[3][1][0] != 0 || migrationPlan[3][1][1] != 1 ||
		migrationPlan[3][1][2] != 9 {
		t.Fatalf("Invalid migration plan for worker 3 to 1\n")
	}

	if migrationPlan[3][2][0] != 4 || migrationPlan[3][2][1] != 5 ||
		migrationPlan[3][2][2] != 6 {
		t.Fatalf("Invalid migration plan for worker 3 to 2\n")
	}
}

func TestRePartition_MigrateWorker(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)

	// Old bucket list
	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	newBuckets, migrationPlan := policy.RePartition(
		[]uint16{1, 2, 4},
		oldBuckets,
	)

	// Token 0 - hash value: 9944607518023373071, bucket index: 1, worker id: 1
	// Token 1 - hash value: 12056337994852197731, bucket index: 1, worker id: 2
	// Token 2 - hash value: 5019254374937295136, bucket index: 6, worker id: 2
	// Token 3 - hash value: 15270045795057849137, bucket index: 7, worker id: 1
	// Token 4 - hash value: 17591931051514362478, bucket index: 8, worker id: 4
	// Token 5 - hash value: 17995134439505708728, bucket index: 8, worker id: 4

	// The expected bucket list should be:
	// Owner: |-w4-|-w4-|-w2-|-w2-|-w2-|-w2-|-w2-|-w2-|-w1-|-w4-|
	// Idx:	  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |

	if len(newBuckets) != 10 {
		t.Fatalf("Invalid number of buckets: %d\n", len(newBuckets))
	}

	// Check the owner of each bucket
	owners := []uint16{4, 4, 2, 2, 2, 2, 2, 2, 1, 4}
	for i, b := range newBuckets {
		if b.WorkerID != owners[i] {
			t.Fatalf("Invalid owner for bucket %d: %d\n", i, b.WorkerID)
		}
	}

	// Check the migration plan
	// Bucket 0, 1, 9 should be migrated from worker 3 to worker 4
	// Bucket 4, 5, 6 should be migrated from worker 3 to worker 2

	if len(migrationPlan) != 1 {
		t.Fatalf("Invalid number of migration plan: %d\n", len(migrationPlan))
	}

	if len(migrationPlan[3]) != 2 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3: %d\n",
			len(migrationPlan[3]),
		)
	}

	if len(migrationPlan[3][4]) != 3 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3 to 4: %d\n",
			len(migrationPlan[3][4]),
		)
	}

	if len(migrationPlan[3][2]) != 3 {
		t.Fatalf(
			"Invalid number of migration plan for worker 3 to 2: %d\n",
			len(migrationPlan[3][2]),
		)
	}
}

func TestRePartition_NoChange(t *testing.T) {

	config := configuration.Default()
	config.NumBuckets = 10
	config.HashPartitionNumTokens = 2
	config.HashPartitionSeeds = []uint64{1234, 5678}

	policy := partition.NewHashPartitionPolicy(config)

	// Old bucket list
	oldBuckets := policy.GenerateBuckets([]uint16{1, 2, 3})

	_, migrationPlan := policy.RePartition(
		[]uint16{1, 2, 3},
		oldBuckets,
	)

	// Migration Plan should be empty
	if len(migrationPlan) != 0 {
		t.Fatalf("Invalid number of migration plan: %d\n", len(migrationPlan))
	}
}
