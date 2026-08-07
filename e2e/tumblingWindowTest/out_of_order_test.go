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
	"github.com/CASP-Systems-BU/koala/worker"
	"golang.org/x/exp/rand"
)

// TestOutOfOrderTumblingWindow is an end-to-end test that validates the
// behavior of a window. Similar to TestTumblingWindow The test generates 10000
// records grouped into 2000 windows, with each window containing 5 records.
// However, the source will produce out-of-order records with allowed
// out-of-orderness of 200ms.

// outOfOrderResults is a global variable to store the results of the test
var outOfOrderResults []*tuple.Tuple1[int64]

func TestOutOfOrderTumblingWindow(t *testing.T) {

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	tumblingResults = make([]*tuple.Tuple1[int64], 0)

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, outOfOrderQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(
		1000*200*time.Millisecond+1*time.Second,
	) - int64(
		200*time.Millisecond,
	)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessOutOfOrderTumblingWindow(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func outOfOrderQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {

			// Generate data with 200 ms delay
			// within the delay, we will shuffle the keys and
			// their timestamps to simulate out-of-order data
			numTimeBuckets := int64(1000)
			timeBucketSpan := 200 * time.Millisecond
			for timeBucket := range numTimeBuckets {
				// Generate 10 records for each time bucket
				for key := range 10 {
					// Generate a random timestamp
					timestamp := int64(
						timeBucket,
					)*int64(
						timeBucketSpan,
					) + rand.Int63()%int64(
						timeBucketSpan/2,
					)
					// Emit the record
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
				V2: int64(timeBucketSpan)*numTimeBuckets + int64(1*time.Second),
			})
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(
		tsAssigner,
		200*time.Millisecond,
		200*time.Millisecond,
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int64]) int64 {
		return t.V1
	})

	// Define windowed counter
	windowAggregator := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[int64]] {
			return stateType.NewValueState(tuple.NewTuple1(int64(0)))
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
	// Tumbling window has a size of 1000 time units (nano seconds)
	window := dataflow.NewTumblingWindow(
		"window",
		keyAssigner,
		windowAggregator,
		200*time.Millisecond*5,
	)
	window.SetParallelism(2)
	dataflow.AddOperator(query, window)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple1[int64]) {
		log.Printf("[SINK] recieved %v\n", in)
		outOfOrderResults = append(outOfOrderResults, in)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectnessOutOfOrderTumblingWindow(
	t *testing.T,
	workers []*worker.Worker,
) {

	// We have 200 tumbling window ranges and 10 keys -> totally 2000 windows
	if len(outOfOrderResults) != 2000 {
		t.Error("Expect 2000 resutls at sink, but got ", len(outOfOrderResults))
	}
	sum := 0
	for _, result := range outOfOrderResults {
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
