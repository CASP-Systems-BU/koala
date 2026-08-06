package slidingWindowTest

import (
	"fmt"
	"log"
	"math"
	"math/rand"
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

// TestSlidingWindow tests the sliding window operator with a moving average
// workload.

var movingAvgResults = make([]IntTuple2, 0)

func TestSlidingWindowMovingAvg(t *testing.T) {

	correct := calculateCorrectMovingAvg(getEvents())
	fmt.Printf("%#v\n", correct)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(
		numWorkers,
		slidingWindowMovingAvgQuery,
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

	log.Printf("%#v\n", movingAvgResults)

	for _, r := range movingAvgResults {
		if correct[r.GetTimestamp()] != r.V2 {
			t.Errorf(
				"Incorrect moving average at timestamp %d. Expected %d, got %d",
				r.GetTimestamp(),
				correct[r.GetTimestamp()],
				r.V2,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func slidingWindowMovingAvgQuery() *dataflow.Dataflow {

	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[IntTuple3](
		"source",
		movingAvgEvents,
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t IntTuple3) int64 {
			return t.V3
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Moving avg window
	keyAssigner := ka.NewKeyAssigner(func(t IntTuple3) int64 {
		return t.V1
	})
	agg := dataflow.NewAggregator(
		func() *stateType.ValueState[IntTuple3] {
			return stateType.NewValueState(tuple.NewTuple3(
				int64(0), // Key
				int64(0), // Sum
				int64(0), // Count
			))
		},
		func(acc *stateType.ValueState[IntTuple3], t IntTuple3) *stateType.ValueState[IntTuple3] {

			tupleVal, _ := acc.Get()

			// Best practice is to modify and return the passed-in accumulator
			// to avoid unnecessary memory allocation.
			tupleVal.V1 = t.V1  // Key
			tupleVal.V2 += t.V2 // Sum
			tupleVal.V3 += 1    // Count
			acc.Set(tupleVal)
			return acc
		},
		func(acc *stateType.ValueState[IntTuple3]) IntTuple2 {
			tupleVal, _ := acc.Get()
			return tuple.NewTuple2(
				tupleVal.V1,
				tupleVal.V2/tupleVal.V3,
			)
		},
		func(acc1 *stateType.ValueState[IntTuple3], acc2 *stateType.ValueState[IntTuple3]) *stateType.ValueState[IntTuple3] {
			tupleVal1, _ := acc1.Get()
			tupleVal2, _ := acc2.Get()

			// Best practice is to modify and return the first accumulator to
			// avoid unnecessary memory allocation.
			tupleVal1.V2 += tupleVal2.V2
			tupleVal1.V3 += tupleVal2.V3
			acc1.Set(tupleVal1)
			return acc1
		},
	)
	window := dataflow.NewSlidingWindow(
		"slidingWindow",
		keyAssigner,
		agg,
		500,
		100,
	)
	window.SetParallelism(2)
	dataflow.AddOperator(query, window)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in IntTuple2) {
		log.Printf("[SINK] recieved %#v\n", in)
		if in.V1 == 0 {
			movingAvgResults = append(movingAvgResults, in)
		}
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Add operators to the query
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func movingAvgEvents(co collector.Collector) {
	for _, t := range getEvents() {
		co.Emit(t)
	}
}

type out struct {
	collector.Collector
	events []IntTuple3
}

func (o out) Emit(t tuple.Tuple) {
	o.events = append(o.events, t.(IntTuple3))
}

func getEvents() []IntTuple3 {

	// seeded random
	rnd := rand.New(rand.NewSource(42))

	events := make([]IntTuple3, 0)
	// Each event has interval of 1.
	for i := range 5000 {
		// Generate events for keys in [0, 10)
		for key := range 1 {
			events = append(events, tuple.NewTuple3(
				int64(key),           // Key
				int64(rnd.Intn(100)), // Value
				int64(10000+i),       // Timestamp
			))
		}
	}
	// Now send a record with ending timestamp to trigger the watermark
	// otherwise the last window will not be closed
	events = append(events, tuple.NewTuple3(
		int64(0), // Key
		int64(0), // Value
		int64(END_WATERMARK),
	))
	return events
}

// very expensive, just for correctness calculation
func calculateCorrectMovingAvg(tuples []IntTuple3) map[int64]int64 {
	maxTs := int64(0)
	minTs := int64(math.MaxInt64)
	for _, t := range tuples {
		if t.V3 == END_WATERMARK {
			continue
		}
		maxTs = max(maxTs, t.V3)
		minTs = min(minTs, t.V3)
	}
	minWindowTs := minTs - minTs%100 - 500
	maxWindowTs := maxTs - maxTs%100 + 500
	numWindows := (maxWindowTs - minWindowTs) / 100
	windows := make(map[int64][]int64)
	for i := range numWindows {
		windowStartTime := minWindowTs + int64(i)*100
		windows[windowStartTime] = make([]int64, 0)
		for _, t := range tuples {
			if t.V1 != 0 {
				continue
			}
			if t.V3 == END_WATERMARK {
				continue
			}
			if t.V3 >= windowStartTime && t.V3 < windowStartTime+500 {
				windows[windowStartTime] = append(
					windows[windowStartTime],
					t.V2,
				)
			}
		}
	}
	correct := make(map[int64]int64)
	for ts, values := range windows {
		if len(values) == 0 {
			continue
		}
		sum := int64(0)
		for _, v := range values {
			sum += v
		}
		correct[ts+500] = sum / int64(len(values))
	}

	return correct
}
