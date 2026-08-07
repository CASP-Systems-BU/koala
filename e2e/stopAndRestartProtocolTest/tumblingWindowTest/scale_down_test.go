package tumblingWindowTest

import (
	"context"
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
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
)

// Total number of keys in the test
const NUMKEYS = 10

// Each time bucket generates NUMKEYS records (1 key per record). Output one
// record every 10 time units.
const TIMEBUCKETSPAN = NUMKEYS * 10

// Total numer of time buckets in the test. Total time duration of the test is
// TIMEBUCKETSPAN * NUMTIMEBUCKETS time units.
const NUMTIMEBUCKETS = 2000000

// Window spans duration in time units. (TIMEBUCKETSPAN * NUMTIMEBUCKETS) %
// WINDOWSPAN must be 0. WINDOWSPAN must > TIMEBUCKETSPAN. WINDOWSPAN must
// be divisible by TIMEBUCKETSPAN.
const WINDOWSPAN = 500

// Total number of windows expected
const NUMWINDOWS = (TIMEBUCKETSPAN * NUMTIMEBUCKETS) / WINDOWSPAN * NUMKEYS

// Expected count of records in each window
const EXPECTEDCOUNT = WINDOWSPAN / TIMEBUCKETSPAN

// tumblingResults is a global variable to store the results of the test
var tumblingResults = make([]*tuple.Tuple2[int64, int64], 0)

// [DEBUG] keep a map for bookkeeping the received windows at the sink
// map[window end time][key]->counter
var tumblingWindowMap = make(map[int64]map[int64]int64)
var numDuplicatedWindows = 0

func TestTumblingWindowStopAndRestartScaleDown(t *testing.T) {

	// Check input
	if (TIMEBUCKETSPAN*NUMTIMEBUCKETS)%WINDOWSPAN != 0 {
		t.Error(
			"TIMEBUCKETSPAN * NUMTIMEBUCKETS must be divisible by WINDOWSPAN",
		)
		return
	}
	if (WINDOWSPAN % TIMEBUCKETSPAN) != 0 {
		t.Error(
			"WINDOWSPAN must be divisible by TIMEBUCKETSPAN",
		)
		t.Errorf(
			"WINDOWSPAN: %v, TIMEBUCKETSPAN: %v, result %d",
			WINDOWSPAN,
			TIMEBUCKETSPAN,
			WINDOWSPAN%TIMEBUCKETSPAN,
		)
		return
	}
	if WINDOWSPAN <= TIMEBUCKETSPAN {
		t.Error(
			"WINDOWSPAN must be larger than TIMEBUCKETSPAN",
		)
		return
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.ReconfigProtocol = "stop-and-restart"
	numWorkers := 5
	client, workers, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return tumblingWindowQuery(3) },
		config,
	)

	// Wait for 10s before rescaling
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "window",
		TargetParallelism: 2,
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

	checkCorrectnessTumblingWindow(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func tumblingWindowQuery(windowParallelism int) *dataflow.Dataflow {

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
			return stateType.NewValueState(tuple.NewTuple2(int64(0), int64(0)))
		},
		func(acc *stateType.ValueState[*tuple.Tuple2[int64, int64]], t *tuple.Tuple2[int64, int64]) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val, _ := acc.Get()
			val.V1++
			val.V2 = t.V1 // set the key into acc.V2
			acc.Set(val)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple2[int64, int64]]) *tuple.Tuple2[int64, int64] {
			val, _ := acc.Get()
			return val
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple2[int64, int64]], acc2 *stateType.ValueState[*tuple.Tuple2[int64, int64]]) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			val1.V1 += val2.V1
			acc1.Set(val1)
			return acc1
		},
	)

	// Tumbling window
	window := dataflow.NewTumblingWindow(
		"window",
		keyAssigner,
		windowAggregator,
		WINDOWSPAN,
	)
	window.SetParallelism(windowParallelism)
	dataflow.AddOperator(query, window)

	// Define Sink
	// Sink input -  in.V1: counter, - in.V2: key of the window
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int64, int64]) {

		// Check duplicate windows
		windowsByEndTime, ok := tumblingWindowMap[in.GetTimestamp()]
		if !ok {
			windowsByEndTime = make(map[int64]int64)
			tumblingWindowMap[in.GetTimestamp()] = windowsByEndTime
			windowsByEndTime[in.V2] = in.V1
		} else {
			if oldVal, exists := windowsByEndTime[in.V2]; exists {
				numDuplicatedWindows += 1
				log.Printf(
					"[SINK ERROR] Duplicate window detected! [key: %v, window ending time: %v] Total dup window: %v. [old val: %v| new val: %v]\n",
					in.V2,
					in.GetTimestamp(),
					numDuplicatedWindows,
					oldVal,
					in.V1,
				)
			} else {
				windowsByEndTime[in.V2] = in.V1
			}
		}
		tumblingResults = append(tumblingResults, tuple.NewTuple2(in.V1, in.V2))
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, window)
	dataflow.Add1To1Stream(query, window, sink)

	return query
}

func checkCorrectnessTumblingWindow(t *testing.T) {
	log.Println("[E2E] Checking correctness of the results")

	if len(tumblingResults) != NUMWINDOWS {
		t.Errorf(
			"Expect %v tumbling windows, but got %v\n",
			NUMWINDOWS,
			len(tumblingResults),
		)
	}
	sum := 0
	for _, result := range tumblingResults {
		if result.V1 != EXPECTEDCOUNT {
			t.Errorf(
				"Counter in each window should be %v, but got %v\n",
				EXPECTEDCOUNT,
				result.V1,
			)
		}
		sum += int(result.V1)
	}

	if sum != (NUMKEYS * NUMTIMEBUCKETS) {
		t.Errorf(
			"Expect total count of all windows to be %v, but got %v\n",
			NUMKEYS*NUMTIMEBUCKETS,
			sum,
		)
	}

	if numDuplicatedWindows > 0 {
		t.Errorf(
			"Found %v duplicated windows in the results.\n",
			numDuplicatedWindows,
		)
	}
}
