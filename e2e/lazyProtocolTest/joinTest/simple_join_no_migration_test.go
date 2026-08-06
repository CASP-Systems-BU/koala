package joinTest

import (
	"context"
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
	"github.com/mus-format/mus-go/varint"
)

// Test Join operator with rescale under lazy no-migration protocol

func TestSimpleJoinRescaleNoMigration(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	config.LazyProtocolVersion = "no-migration"

	// We need extra empty workers for scale up
	numWorkers := SOURCE1_PARALLELISM + SOURCE2_PARALLELISM + 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Find which worker is the empty worker for scale up
	workerIdsForScaleup := make(map[uint16]struct{})
	for _, w := range workers {
		if w.AssignedTask == nil {
			// This is the empty worker
			workerIdsForScaleup[w.WorkerId] = struct{}{}
		}
	}

	// Wait for some time before rescaling
	time.Sleep(WAIT_TIME_BEFORE_RESCALE * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "join",
		TargetParallelism: 4,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(WAIT_TIME_AFTER_RESCALE * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	checkNoMigrationCorrectness(t, workerIdsForScaleup, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkNoMigrationCorrectness(
	t *testing.T,
	newWorkerIds map[uint16]struct{},
	workers []*worker.Worker,
) {

	// Print out the last received record by Sink for reference. 2 states should
	// be opposite numbers
	log.Println(
		"Last received record by Sink - state1:",
		lastReceived.V2,
		" state2:",
		lastReceived.V3,
	)

	// Separate new workers and old workers for join tasks
	newWorkerList := make([]*worker.Worker, 0, len(newWorkerIds))
	oldWorkerList := make([]*worker.Worker, 0, len(workers)-len(newWorkerIds)-2)
	for _, w := range workers {
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		if _, ok := newWorkerIds[w.WorkerId]; ok {
			newWorkerList = append(newWorkerList, w)
		} else {
			oldWorkerList = append(oldWorkerList, w)
		}
	}
	if len(newWorkerList) != 2 {
		t.Error(
			"Expect ",
			2,
			" new workers after rescale, got ",
			len(newWorkerList),
		)
	}
	if len(oldWorkerList) != 2 {
		t.Error(
			"Expect ",
			2,
			" old workers after rescale, got ",
			len(oldWorkerList),
		)
	}

	// All new workers should have empty state
	new_worker_iters := make(
		[]stateBackend.StateIterator,
		0,
		len(newWorkerList),
	)
	for _, w := range newWorkerList {
		new_worker_iters = append(
			new_worker_iters,
			w.StateService.StateBackendImpl.GetIterator(),
		)
	}
	stateCnt := 0
	for _, iter := range new_worker_iters {
		for iter.First(); iter.Valid(); iter.Next() {
			stateCnt++
		}
	}
	if stateCnt != 0 {
		t.Error("Expect new workers to have empty state, got ", stateCnt)
	}

	// All old workers should have all the state with expected values

	/*
		The processing logic is as follows:
		For each record from source1, it increases state1 by in.V2, and
		decreases state2 by in.V2
		For each record from source2, it increases state2 by in.V2, and
		decreases state1 by in.V2
		Each key is repeated REPEAT_SOURCE1 times in source1 and REPEAT_SOURCE2
		times in source2. Each record the value (V2) is the sequence number of
		repetition. Also consider that parallelism for source1 and source2 are
		SOURCE1_PARALLELISM and SOURCE2_PARALLELISM respectively, exactly same
		logic is applied to each source instance.

		Therefore, for each key, the final value of state1 and state2 should be:
		state1 = (0 + 1 + ... + REPEAT_SOURCE1-1) * SOURCE1_PARALLELISM
				- (0 + 1 + ... + REPEAT_SOURCE2-1) * SOURCE2_PARALLELISM
		state2 = (0 + 1 + ... + REPEAT_SOURCE2-1) * SOURCE2_PARALLELISM
				- (0 + 1 + ... + REPEAT_SOURCE1-1) * SOURCE1_PARALLELISM
	*/
	expectedState1 := (REPEAT_SOURCE1*(REPEAT_SOURCE1-1)/2)*SOURCE1_PARALLELISM -
		(REPEAT_SOURCE2*(REPEAT_SOURCE2-1)/2)*SOURCE2_PARALLELISM
	expectedState2 := (REPEAT_SOURCE2*(REPEAT_SOURCE2-1)/2)*SOURCE2_PARALLELISM -
		(REPEAT_SOURCE1*(REPEAT_SOURCE1-1)/2)*SOURCE1_PARALLELISM

	// All keys for each state have the same final value, so we only have 2
	// expected values: state_id -> expected_value. Note that state_id is
	// generated at state registration time, starting from 0.
	expectedVal := make(map[uint16]int64)
	expectedVal[0] = int64(expectedState1)
	expectedVal[1] = int64(expectedState2)

	// All old workers should have exact correct values for each key
	// Map: state_id -> key -> value
	results := make(map[uint16]map[int]int64)

	oldWorkerIters := make([]stateBackend.StateIterator, 0, len(oldWorkerList))
	for _, w := range oldWorkerList {
		oldWorkerIters = append(
			oldWorkerIters,
			w.StateService.StateBackendImpl.GetIterator(),
		)
	}

	for _, iter := range oldWorkerIters {
		for iter.First(); iter.Valid(); iter.Next() {
			serializedKey := iter.Key()
			serializedValue := iter.Value()

			// Extract the state id, key, and value
			stateIDOffset := constant.OperatorIDSize + constant.BucketIdxSize
			stateID := binary.BigEndian.Uint16(
				serializedKey[stateIDOffset : stateIDOffset+constant.StateIDSize],
			)
			key, _, _ := varint.UnmarshalInt(
				serializedKey[constant.KeyPrefixSize:],
			)
			value, _, _ := varint.UnmarshalInt64(serializedValue)

			// Store the values to the results map
			if _, ok := results[stateID]; !ok {
				results[stateID] = make(map[int]int64)
			}
			results[stateID][key] = value
		}
	}

	// Check correctness
	if len(results) != 2 {
		t.Error("Expect 2 states, got ", len(results))
	}
	for stateID, keyValMap := range results {

		if len(keyValMap) != NUM_KEYS {
			t.Error(
				"Expect ",
				NUM_KEYS,
				" keys for state ",
				stateID,
				", got ",
				len(keyValMap),
			)
		}

		for key, val := range keyValMap {
			if expectedVal[stateID] != val {
				t.Error(
					"Wrong value for state ",
					stateID,
					" key ",
					key,
					", expected ",
					expectedVal[stateID],
					", got ",
					val,
				)
			}
		}
	}
}
