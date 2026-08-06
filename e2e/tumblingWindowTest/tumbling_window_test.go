package tumblingWindowTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
)

// TestTumblingWindow is an end-to-end test that validates the behavior of a
// window. The test generates 10000 records and grouped into 2000 windows,
// with each window containing 5 records.

// tumblingResults is a global variable to store the results of the test
var tumblingResults []*tuple.Tuple1[int64]

func TestTumblingWindow(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	tumblingResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		tumblingWindowQuery,
		config,
	)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(100000)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessTumblingWindow(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func tumblingWindowQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {

			// Generate records with monotonically increasing timestamp
			// Each timeBucket spans 100 time units and generates 10 records
			// with different keys spread over this timeBucket
			// e.g. rec timestamp at [0, 10, 20, ... 90] for 1st timeBucket
			timeBucketSpan := 100
			numTimeBuckets := 1000
			for timeBucket := range numTimeBuckets {
				base := int64(timeBucket * timeBucketSpan)

				// In each timeBucket, generate 10 records with different keys
				// We only have 10 keys here: 0, 1, 2, ... 9
				for key := range 10 {
					timestamp := base + int64(key*10)
					co.Emit(&tuple.Tuple2[int64, int64]{
						V1: int64(key),
						V2: timestamp,
					})
				}
			}

			// Now send a record with ending timestamp to trigger the watermark
			// otherwise the last window will not be closed
			co.Emit(&tuple.Tuple2[int64, int64]{
				V1: 0,
				V2: int64(timeBucketSpan * numTimeBuckets),
			})
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int64]) int64 {
		return t.V1
	})

	// Define windowed counter
	windowAggregator := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[int64]] {
			tuple := tuple.NewTuple1(int64(0))
			return stateType.NewValueState(tuple)
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int64]], t *tuple.Tuple2[int64, int64]) *stateType.ValueState[*tuple.Tuple1[int64]] {
			tuple, _ := acc.Get()
			tuple.V1++
			acc.Set(tuple)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int64]]) *tuple.Tuple1[int64] {
			val, _ := acc.Get()
			return val
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple1[int64]], acc2 *stateType.ValueState[*tuple.Tuple1[int64]]) *stateType.ValueState[*tuple.Tuple1[int64]] {
			tuple1, _ := acc1.Get()
			tuple2, _ := acc2.Get()
			tuple1.V1 += tuple2.V1
			acc1.Set(tuple1)
			return acc1
		},
	)
	// Tumbling window has a size of 500 time units (nano seconds)
	window := dataflow.NewTumblingWindow(
		"window",
		keyAssigner,
		windowAggregator,
		500,
	)

	window.SetParallelism(2)
	dataflow.AddOperator(query, window)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple1[int64]) {
		log.Printf("[SINK] recieved %v\n", in)
		tumblingResults = append(tumblingResults, in)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectnessTumblingWindow(t *testing.T, workers []*worker.Worker) {

	// We have 200 tumbling window ranges and 10 keys -> totally 2000 windows
	if len(tumblingResults) != 2000 {
		t.Error("Expect 2000 resutls at sink, but got ", len(tumblingResults))
	}
	sum := 0
	for _, result := range tumblingResults {
		if result.V1 != 5 {
			t.Errorf(
				"Counter in each window should be 5, but got %v\n",
				result.V1,
			)
		}
		sum += int(result.V1)
	}
	// Total count of records should be 10000
	if sum != 10000 {
		t.Errorf("Expect total 10000 number counted, but got %v\n", sum)
	}

	// At the end, all windows should have been removed from state service
	// ecxcept the last one. This is to test DeleteManyAndFlush()
	numActiveWindows := 0
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}
		// Get the state
		stateIterator := w.StateService.StateBackendImpl.GetIterator()

		for stateIterator.First(); stateIterator.Valid(); stateIterator.Next() {
			numActiveWindows++
		}
	}

	if numActiveWindows != 1 {
		t.Errorf("Expect only 1 active window, but got %v\n", numActiveWindows)
	}
}
