package slidingWindowTest

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
)

type IntTuple1 = *tuple.Tuple1[int64]
type IntTuple2 = *tuple.Tuple2[int64, int64]
type IntTuple3 = *tuple.Tuple3[int64, int64, int64]

const END_WATERMARK = 100000

var slidingResults map[int64][]IntTuple2

const NUM_KEYS = 5
const NUM_EVENTS = 500
const DURATION = time.Duration(500)
const WINDOW_SLIDE = time.Duration(100)

// TestSlidingWindow tests the sliding window operator
// In this e2e test we create a sliding window of 500ns duration and 100ns slide
// We emit 500 events for each key in the range [0, 10) with a timestamp
// interval of 1ns

func TestSlidingWindowCounter(t *testing.T) {

	slidingResults = make(map[int64][]IntTuple2)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		slidingWindowCounterQuery,
		configuration.Default(),
	)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	go testutils.MonitorEndOfTest(sink, done, END_WATERMARK)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func slidingWindowCounterQuery() *dataflow.Dataflow {

	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[IntTuple2](
		"source",
		func(co collector.Collector) {

			// Generate events for keys in [0, 10)
			for key := range NUM_KEYS {

				// Each event has interval of 1.
				for i := range NUM_EVENTS {
					co.Emit(tuple.NewTuple2(
						int64(key),     // Key
						int64(10000+i), // Timestamp
					))
				}
			}

			// Now send a record with ending timestamp to trigger the watermark
			// otherwise the last window will not be closed
			co.Emit(tuple.NewTuple2(
				int64(0), // Key
				int64(END_WATERMARK),
			))
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t IntTuple2) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Create a new sliding window
	keyAssigner := ka.NewKeyAssigner(func(t IntTuple2) int64 {
		return t.V1
	})
	agg := dataflow.NewAggregator(
		func() *stateType.ValueState[IntTuple2] {
			return stateType.NewValueState(tuple.NewTuple2(int64(-1), int64(0)))
		},
		func(acc *stateType.ValueState[IntTuple2], t IntTuple2) *stateType.ValueState[IntTuple2] {

			tuple, _ := acc.Get()
			if tuple.V1 != -1 && tuple.V1 != t.V1 {
				log.Fatalf("Invalid key: %v, %v\n", tuple.V1, t.V1)
			}
			tuple.V1 = t.V1 // Set the key
			tuple.V2++
			acc.Set(tuple)
			return acc
		},
		func(acc *stateType.ValueState[IntTuple2]) IntTuple2 {
			tuple, _ := acc.Get()
			return tuple
		},
		func(acc1 *stateType.ValueState[IntTuple2], acc2 *stateType.ValueState[IntTuple2]) *stateType.ValueState[IntTuple2] {
			var key int64

			tuple1, _ := acc1.Get()
			tuple2, _ := acc2.Get()

			if tuple1.V1 != -1 {
				key = tuple1.V1
			} else {
				key = tuple2.V1
			}

			// Best practice is to modify and return the first accumulator to
			// avoid unnecessary memory allocation
			tuple1.V1 = key
			tuple1.V2 += tuple2.V2
			acc1.Set(tuple1)
			return acc1
		},
	)
	window := dataflow.NewSlidingWindow(
		"slidingWindow",
		keyAssigner,
		agg,
		DURATION,
		WINDOW_SLIDE,
	)
	window.SetParallelism(2)
	dataflow.AddOperator(query, window)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in IntTuple2) {
		log.Printf("[SINK] recieved %#v\n", in)
		if in.V1 == -1 {
			log.Fatalf("Invalid key: %v\n", in.V1)
		}
		if slidingResults[in.V1] == nil {
			slidingResults[in.V1] = make([]IntTuple2, 0)
		}
		slidingResults[in.V1] = append(slidingResults[in.V1], in)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectness(t *testing.T) {

	log.Printf("%#v\n", slidingResults)

	if len(slidingResults) != NUM_KEYS {
		t.Errorf(
			"Expect %v results at sink, but got %v\n",
			NUM_KEYS,
			len(slidingResults),
		)
	}

	correctNumberOfWindowPerKey := int(
		NUM_EVENTS/WINDOW_SLIDE + (DURATION / WINDOW_SLIDE) - 1,
	)

	for key, results := range slidingResults {
		// There should be 9 sliding window results for each key
		if len(results) != correctNumberOfWindowPerKey {
			t.Errorf(
				"Expect %d sliding window results for key %v, but got %v\n",
				correctNumberOfWindowPerKey,
				key,
				len(results),
			)
		}

		// For each timestamp of the window, the value should be the following
		correctTimestampToValue := map[int64]int64{
			10100: 100,
			10200: 200,
			10300: 300,
			10400: 400,
			10500: 500,
			10600: 400,
			10700: 300,
			10800: 200,
			10900: 100,
		}

		for _, result := range results {
			if result.V2 != correctTimestampToValue[result.GetTimestamp()] {
				t.Errorf(
					"Expect value %v at timestamp %v, but got %v\n",
					correctTimestampToValue[result.GetTimestamp()],
					result.GetTimestamp(),
					result.V2,
				)
			}
		}
	}
}
