package tumblingWindowTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// TestOutOfOrderTumblingWindow tests when a tumbling window has very large
// input records.

// Total number of records = 10_000_000
// Key range = [0, 10)
// Number of tumbling windows = 200
// Num Records in each window = 5000

var veryLargeTumblingWindowResults []*tuple.Tuple1[int64]

func TestVeryLargeTumblingWindow(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})
	veryLargeTumblingWindowResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 3
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		veryLargeTumblingWindow,
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
	expectedWM := int64(500 * 200)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	/**************************************************************************
								CHECK CORRECTNESS
	**************************************************************************/

	checkCorrectnessVeryLargeTumblingWindow(t)

	/**************************************************************************
										CLEAN UP
	**************************************************************************/
	testutils.CleanUpDataFolder()
}

func veryLargeTumblingWindow() *dataflow.Dataflow {

	query := dataflow.NewDataflow()

	// Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {
			timeBucketSpan := 500
			numTimeBuckets := 200
			for timeBucket := range numTimeBuckets {
				for i := range 10 {
					for range 5000 {
						co.Emit(&tuple.Tuple2[int64, int64]{
							V1: int64(i),
							V2: int64(timeBucket * timeBucketSpan),
						})
					}
				}
			}
			// Now send a record with ending timestamp to trigger the watermark
			co.Emit(&tuple.Tuple2[int64, int64]{
				V1: 1,
				V2: int64(timeBucketSpan * numTimeBuckets),
			})
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 1*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Tumbling window
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int64]) int64 {
		return t.V1
	})
	windowAggregator := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[int64]] {
			return stateType.NewValueState(tuple.NewTuple1(int64(0)))
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int64]], t *tuple.Tuple2[int64, int64]) *stateType.ValueState[*tuple.Tuple1[int64]] {
			val, _ := acc.Get()
			val.V1++
			acc.Set(val)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int64]]) *tuple.Tuple1[int64] {
			val, _ := acc.Get()
			return val
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple1[int64]], acc2 *stateType.ValueState[*tuple.Tuple1[int64]]) *stateType.ValueState[*tuple.Tuple1[int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			val1.V1 += val2.V1
			acc1.Set(val1)
			return acc1
		},
	)
	window := dataflow.NewTumblingWindow(
		"window",
		keyAssigner,
		windowAggregator,
		500,
	)
	window.SetParallelism(1)
	dataflow.AddOperator(query, window)

	// Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple1[int64]) {
		log.Printf("[SINK] recieved %v\n", in)
		veryLargeTumblingWindowResults = append(
			veryLargeTumblingWindowResults,
			in,
		)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectnessVeryLargeTumblingWindow(t *testing.T) {

	// We have 200 tumbling window ranges and 1 keys -> totally 200 windows
	if len(veryLargeTumblingWindowResults) != 2000 {
		t.Error(
			"Expect 2000 resutls at sink, but got ",
			len(veryLargeTumblingWindowResults),
		)
	}
	sum := 0
	for _, result := range veryLargeTumblingWindowResults {
		if result.V1 != 5000 {
			t.Errorf(
				"Counter in each window should be 5000, but got %v\n",
				result.V1,
			)
		}
		sum += int(result.V1)
	}
	// Total count of records should be 10000
	if sum != 10_000_000 {
		t.Errorf("Expect total 10_000_000 number counted, but got %v\n", sum)
	}

	// Check the output timestamp of the results
	// Order is not guaranteed here: many windows can be fired at the same time
	for _, result := range veryLargeTumblingWindowResults {
		ts := result.GetTimestamp()
		if ts < 500 || ts > 200*500 || ts%500 != 0 {
			t.Errorf(
				"Expect timestamp between 500 and 100000 with a step of 500, but got %v\n",
				ts,
			)
		}
	}
}
