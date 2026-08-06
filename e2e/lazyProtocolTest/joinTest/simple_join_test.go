package joinTest

import (
	"context"
	"encoding/binary"
	"log"
	"testing"
	"time"
	"unsafe"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/coordinator"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
	"github.com/mus-format/mus-go/varint"
)

// Basic test for Join operator with only ValueState with rescaling operation
// under lazy protocol. We verify that the results and all testing cases should
// be consistent with no-rescale test e.g. e2e/joinTest/simpleJoin_test.go

const NUM_KEYS = 1000
const REPEAT_SOURCE1 = 12000
const REPEAT_SOURCE2 = 12000
const SOURCE1_PARALLELISM = 2
const SOURCE2_PARALLELISM = 2
const WAIT_TIME_BEFORE_RESCALE = 7 // in seconds
const WAIT_TIME_AFTER_RESCALE = 30 // in seconds

// Pointer to the last received record by Sink for testing purposes
var lastReceived *tuple.Tuple3[int, int64, int64]

func TestSimpleJoinRescale(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"

	// We need extra empty workers for scale up
	numWorkers := SOURCE1_PARALLELISM + SOURCE2_PARALLELISM + 5
	client, workers, coordinator := testutils.DeployJob(
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
	checkCorrectness(t, workerIdsForScaleup, workers, coordinator)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkCorrectness(
	t *testing.T,
	newWorkerIds map[uint16]struct{},
	workers []*worker.Worker,
	coordinator *coordinator.Coordinator,
) {

	// Print out the last received record by Sink for reference. 2 states should
	// be opposite numbers
	log.Println(
		"Last received record by Sink - state1:",
		lastReceived.V2,
		" state2:",
		lastReceived.V3,
	)

	// Get all workers for join operator
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		// Get the state
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}

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

	// When iterating the state from all workers, we may see the same key twice
	// (once from old worker, once from new worker) if the key is transferred.
	// We record all values seen for each key, and test results at the end.
	// results[stateID][key] -> HashSet of values seen
	results := make(map[uint16]map[int]map[int64]struct{})

	// Iterate the state
	for _, iter := range iters {
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

			// Record the value seen for this key
			if _, ok := results[stateID]; !ok {
				results[stateID] = make(map[int]map[int64]struct{})
			}
			if _, ok := results[stateID][key]; !ok {
				results[stateID][key] = make(map[int64]struct{})
			}
			results[stateID][key][value] = struct{}{}
		}
	}

	// Check the results
	if len(results) != 2 {
		t.Error("Expect 2 states, got ", len(results))
	}
	for stateID, keyMap := range results {

		// Each state should have NUM_KEYS keys
		if len(keyMap) != NUM_KEYS {
			t.Errorf(
				"[CASE 1] Expect %d keys for state %d, got %d",
				NUM_KEYS,
				stateID,
				len(keyMap),
			)
		}

		for key, valSet := range keyMap {

			// A key can only appears once (not rescaled) or twice (rescaled)
			// in the entire state (across all workers)
			if !(len(valSet) == 1 || len(valSet) == 2) {
				t.Errorf(
					"[CASE 2] State %d key %d: expect 1 or 2 values, got %d",
					stateID,
					key,
					len(valSet),
				)
			}

			// Correct value should exist in the valSet
			if _, ok := valSet[expectedVal[stateID]]; !ok {
				t.Errorf(
					"[CASE 3] State_id %d key %d: no correct values %d",
					stateID,
					key,
					expectedVal[stateID],
				)
			}

			// If this key appears twice, it means it is rescaled. Then its
			// latest owner worker should be one of the new workers
			if newWorkerIds != nil {
				if len(valSet) == 2 {

					// Get latest owner worker of this key
					ptrToKey := unsafe.Pointer(&key)
					serializedKey := make([]byte, network.SizeInt(ptrToKey))
					network.EncodeInt(ptrToKey, serializedKey)
					workerId := coordinator.KeyPartitions["join"].KeyToWorkerID(
						serializedKey,
					)

					// Check if the owner worker is now a new worker
					if _, ok := newWorkerIds[workerId]; !ok {
						t.Errorf(
							"[CASE 4] State_id %d key %d: rescaled but latest owner worker %d is not a new worker",
							stateID,
							key,
							workerId,
						)
					}
				}
			}
		}
	}
}

// Simple query to join 2 streams
func query(joinParallelism int) *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	// Define Source 1
	source1 := dataflow.NewSource[*tuple.Tuple2[int, int64]](
		"source1",
		func(co collector.Collector) {
			for r := range REPEAT_SOURCE1 {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int, int64]{
						V1: i,
						V2: int64(r),
					})
				}
			}
			log.Println("Source1 completed")
		},
	)
	source1.SetParallelism(SOURCE1_PARALLELISM)
	dataflow.AddOperator(query, source1)

	// Define Source 2
	source2 := dataflow.NewSource[*tuple.Tuple2[int, int64]](
		"source2",
		func(co collector.Collector) {
			for r := range REPEAT_SOURCE2 {
				for i := range NUM_KEYS {
					co.Emit(&tuple.Tuple2[int, int64]{
						V1: i,
						V2: int64(r),
					})
				}
			}
			log.Println("Source2 completed")
		},
	)
	source2.SetParallelism(SOURCE2_PARALLELISM)
	dataflow.AddOperator(query, source2)

	// KeyAssigner for source1 output
	keyAssigner1 := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, int64]) int {
			return t.V1
		},
	)

	// KeyAssigner for source2 output
	keyAssigner2 := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, int64]) int {
			return t.V1
		},
	)

	// Define Join (generic type in [] is the output tuple type)
	join := dataflow.NewJoin[*tuple.Tuple3[int, int64, int64]](
		"join",
		/************************* 1st input stream **************************/
		source1,
		keyAssigner1,
		func(
			in *tuple.Tuple2[int, int64],
			state1 *stateType.ValueState[*tuple.Tuple1[int64]],
			state2 *stateType.ValueState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) time.Duration {

			// When Join receives record from source1, it increases state1 by
			// in.V2, and decreases state2 by in.V2

			// Increase state1 by in.V2
			state1Val, exist := state1.Get()
			if !exist {
				state1Val = tuple.NewTuple1(int64(0))
			}
			state1Val.V1 += in.V2
			state1.Set(state1Val)

			// Decrease state2 by in.V2
			state2Val, exist := state2.Get()
			if !exist {
				state2Val = tuple.NewTuple1(int64(0))
			}
			state2Val.V1 -= in.V2
			state2.Set(state2Val)

			co.Emit(&tuple.Tuple3[int, int64, int64]{
				V1: in.V1,
				V2: state1Val.V1,
				V3: state2Val.V1,
			})
			return time.Duration(0)
		},
		/************************* 2nd input stream **************************/
		source2,
		keyAssigner2,
		func(
			in *tuple.Tuple2[int, int64],
			state1 *stateType.ValueState[*tuple.Tuple1[int64]],
			state2 *stateType.ValueState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) time.Duration {

			// When Join receives record from source2, it increases state2 by
			// in.V2, and decreases state1 by in.V2

			// Increase state2 by in.V2
			state2Val, exist := state2.Get()
			if !exist {
				state2Val = tuple.NewTuple1(int64(0))
			}
			state2Val.V1 += in.V2
			state2.Set(state2Val)

			// Decrease state1 by in.V2
			state1Val, exist := state1.Get()
			if !exist {
				state1Val = tuple.NewTuple1(int64(0))
			}
			state1Val.V1 -= in.V2
			state1.Set(state1Val)

			co.Emit(&tuple.Tuple3[int, int64, int64]{
				V1: in.V1,
				V2: state1Val.V1,
				V3: state2Val.V1,
			})
			return time.Duration(0)
		},
	)
	join.SetParallelism(joinParallelism)
	dataflow.AddOperator(query, join)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int, int64, int64]) {
			lastReceived = t
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect the operators
	dataflow.Add2To1Stream(query, source1, source2, join)
	dataflow.Add1To1Stream(query, join, sink)

	return query
}
