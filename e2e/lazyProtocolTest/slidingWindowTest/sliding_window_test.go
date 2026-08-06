package slidingWindowTest

import (
	"context"
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
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
)

// [Testing Note] In this test, we apply scale-up operation under lazy protocol
// for a sliding window query. We verify that the number of windows and each
// counter in the windows are correct after rescaling. The results should be
// the same as the results of the sliding window query without rescaling.

// Total number of keys in the test
const NUMKEYS = 10

// Each time bucket generates NUMKEYS records (1 key per record). Output one
// record every 10 time units.
const TIMEBUCKETSPAN = NUMKEYS * 10

// Total numer of time buckets in the test. Total time duration of the test is
// TIMEBUCKETSPAN * NUMTIMEBUCKETS time units.
const NUMTIMEBUCKETS = 400000

// Window spans duration in time units.
const WINDOWSPAN = 500

// Slide of the sliding window in time units
const SLIDE = 100

// Total number of windows expected
const NUMWINDOWS = (((TIMEBUCKETSPAN*NUMTIMEBUCKETS)-WINDOWSPAN)/SLIDE + 1) * NUMKEYS

// Expected count of records in each window
const EXPECTEDCOUNT = (WINDOWSPAN / SLIDE) * (SLIDE / TIMEBUCKETSPAN)

// tumblingResults is a global variable to store the results of the test
var slidingWindowResults = make([]*tuple.Tuple2[int64, int64], 0)

// [DEBUG] keep a map for bookkeeping the received windows at the sink
// map[window end time][key]->counter
var slidingWindowMap = make(map[int64]map[int64]int64)
var numDuplicatedWindows = 0

func TestSlidingWindowLazyProtocol(t *testing.T) {

	// Check input
	// WINDOWSPAN must be divisible by SLIDE
	if WINDOWSPAN%SLIDE != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by SLIDE",
		)
		return
	}
	// TIMEBUCKETSPAN*NUMTIMEBUCKETS must be divisible by SLIDE
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%SLIDE != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by SLIDE",
		)
		return
	}
	// SLIDE must be divisible by TIMEBUCKETSPAN
	if SLIDE%TIMEBUCKETSPAN != 0 {
		t.Error(
			"SLIDE must be divisible by TIMEBUCKETSPAN",
		)
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "lazy"
	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return slidingWindowQuery(2) },
		config,
	)

	// Wait for 10s before rescaling
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "window",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(TIMEBUCKETSPAN * NUMTIMEBUCKETS)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectnessSlidingWindow(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func slidingWindowQuery(windowParallelism int) *dataflow.Dataflow {

	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {

			// Generate records with monotonically increasing timestamp
			// Each timeBucket spans TIMEBUCKETSPAN time units and generates
			// NUMKEYS records with different keys spread over this timeBucket
			for timeBucket := range NUMTIMEBUCKETS {
				base := int64(timeBucket * TIMEBUCKETSPAN)

				// In each timeBucket, generate NUMKEYS records. One key per
				// record. Gap between each record is 10 time units.
				for key := range NUMKEYS {
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
				V2: int64(TIMEBUCKETSPAN * NUMTIMEBUCKETS),
			})

			log.Println("   SOUECE: all data emitted")
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
		// acc.V1: counter
		// acc.V2: key of the window
		func() *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			return stateType.NewValueState(tuple.NewTuple2(int64(0), int64(-1)))
		},
		func(acc *stateType.ValueState[*tuple.Tuple2[int64, int64]], t *tuple.Tuple2[int64, int64]) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			tupleVal, _ := acc.Get()
			tupleVal.V1++
			tupleVal.V2 = t.V1 // set the key into acc.V2
			acc.Set(tupleVal)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple2[int64, int64]]) *tuple.Tuple2[int64, int64] {
			tupleVal, _ := acc.Get()
			return tupleVal
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple2[int64, int64]], acc2 *stateType.ValueState[*tuple.Tuple2[int64, int64]]) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			// V1: counter, V2: key of the window
			tuple1, _ := acc1.Get()
			tuple2, _ := acc2.Get()

			var key int64
			if tuple1.V2 != -1 {
				key = tuple1.V2
			} else {
				key = tuple2.V2
			}

			// Best practice is to modify and return the first accumulator to
			// avoid unnecessary memory allocation
			tuple1.V1 += tuple2.V1
			tuple1.V2 = key
			acc1.Set(tuple1)
			return acc1
		},
	)

	// Sliding window
	window := dataflow.NewSlidingWindow(
		"window",
		keyAssigner,
		windowAggregator,
		WINDOWSPAN,
		SLIDE,
	)
	window.SetParallelism(windowParallelism)
	dataflow.AddOperator(query, window)

	// Define Sink
	// Sink input -  in.V1: counter, - in.V2: key of the window
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int64, int64]) {

		resTuple := tuple.NewTuple2(in.V1, in.V2)
		resTuple.SetTimestamp(in.GetTimestamp())

		// Skip the first a few windows that are triggered without all number
		// of slides.
		if resTuple.GetTimestamp() >= int64(WINDOWSPAN) {

			// Check duplicate windows
			windowsByEndTime, ok := slidingWindowMap[resTuple.GetTimestamp()]
			if !ok {
				windowsByEndTime = make(map[int64]int64)
				slidingWindowMap[resTuple.GetTimestamp()] = windowsByEndTime
				windowsByEndTime[resTuple.V2] = resTuple.V1
			} else {
				if oldVal, exists := windowsByEndTime[resTuple.V2]; exists {
					numDuplicatedWindows += 1
					log.Printf(
						"[SINK ERROR] Duplicate window detected! [key: %v, window ending time: %v] Total dup window: %v. [old val: %v| new val: %v]\n",
						resTuple.V2,
						resTuple.GetTimestamp(),
						numDuplicatedWindows,
						oldVal,
						resTuple.V1,
					)
				} else {
					windowsByEndTime[resTuple.V2] = resTuple.V1
				}
			}
			slidingWindowResults = append(slidingWindowResults, resTuple)
		}
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectnessSlidingWindow(t *testing.T) {
	log.Println("[E2E] Checking correctness of the results")

	if len(slidingWindowResults) != NUMWINDOWS {
		t.Errorf(
			"Expect %v results at sink, but got %v\n",
			NUMWINDOWS,
			len(slidingWindowResults),
		)
	}

	for _, result := range slidingWindowResults {
		if result.V1 != EXPECTEDCOUNT {
			t.Errorf(
				"Counter in each window should be %v, but got %v\n",
				EXPECTEDCOUNT,
				result.V1,
			)
		}
	}

	if numDuplicatedWindows > 0 {
		t.Errorf(
			"Found %v duplicated windows in the results.\n",
			numDuplicatedWindows,
		)
	}
}
