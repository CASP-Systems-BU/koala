package query5Test

import (
	"log"
	"reflect"
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
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/query/query5"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY5 LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query5, by
// comparing output of the query with expected results

// Query 5: Hot Items
// Selects the item with the most bids in the past 1 hours time period;
// the “hottest” item. The results are output every minute.

// Expected Result
// V1: auctionId
// V2: total bid count for that auctionId
// Each batch contains results triggered by the same watermark
var SampleHottestResultBatches = [][]tuple.Tuple2[int64, int64]{
	// Triggered by watermark 10600
	{
		// TumblingWindow[10000,10500)
		{V1: 1, V2: 1},
	},
	// Triggered by watermark 12800
	{
		// TumblingWindow[11000, 11500)
		{V1: 1, V2: 3},
		// TumblingWindow[11500, 12000)
		{V1: 2, V2: 2},
		// TumblingWindow[12000, 12500)
		{V1: 2, V2: 1},
	},
	// Triggered by watermark 14900
	{
		// TumblingWindow[13000,13500)
		{V1: 3, V2: 4},
		// TumblingWindow[13500,14000)
		{V1: 3, V2: 4},
	},
}

var hottestResults []tuple.Tuple2[int64, int64]

func TestQuery5Correctness(t *testing.T) {
	hottestResults = make([]tuple.Tuple2[int64, int64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************
	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 5
	_, workers, _ := testutils.DeployJob(numWorkers, Query5Test, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(14900)
	// Wait till we receive the ending watermark
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")
	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	expectedCount := 0
	for _, batch := range SampleHottestResultBatches {
		expectedCount += len(batch)
	}
	if len(hottestResults) != expectedCount {
		t.Errorf(
			"Incorrect amount of results=%d, expect=%d",
			len(hottestResults),
			expectedCount,
		)
	}

	log.Println("Actual results:", hottestResults)

	// Check batch by batch, we do this because if a watermark will trigger more
	// than one window, then the arriving sequence of those results is not fixed
	start := 0
	for batchIdx, expectedBatch := range SampleHottestResultBatches {
		end := start + len(expectedBatch)
		if end > len(hottestResults) {
			t.Fatalf("Results ended too early, batch=%d", batchIdx)
		}
		actualBatch := hottestResults[start:end]

		// Since in each batch, the sequence of results is not fixed, we use map
		// to test results' correctness
		expectedMap := make(map[tuple.Tuple2[int64, int64]]int)
		for _, e := range expectedBatch {
			expectedMap[e]++
		}
		actualMap := make(map[tuple.Tuple2[int64, int64]]int)
		for _, a := range actualBatch {
			actualMap[a]++
		}

		if !reflect.DeepEqual(expectedMap, actualMap) {
			t.Errorf(
				"Incorrect batch %d, expected %v, got %v",
				batchIdx+1,
				expectedBatch,
				actualBatch,
			)
		}

		start = end
	}
	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Query5Test() *dataflow.Dataflow {
	df := dataflow.NewDataflow()
	src := dataflow.NewSource[*models.BidEvent](
		"bidsource",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(200 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)

	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]

	// Assign V4(dateTime) as timestamp for bid event
	timestampAssigner := ta.NewTimestampAssigner(
		func(t *models.BidEvent) int64 {
			return t.V4
		},
	)
	src.AssignTimestampAndWatermark(
		timestampAssigner,
		100*time.Millisecond,
		0,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	window := query5.Query5SlidingWindow(1000, 500)
	window.SetParallelism(2)
	dataflow.AddOperator(df, window)

	// Keyby window ending timestamp
	hottestItemKeyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.GetTimestamp()
		},
	)
	hottestItemAggregator := dataflow.NewAggregator(
		// Create a new valueState to store auctionId and count
		func() *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			return stateType.NewValueState(tuple.NewTuple2(int64(0), int64(0)))
		},

		// Compare auctionId count and update the hottest one
		func(
			// acc: Stores auctionId and count
			// V1: auctionId
			// V2: count
			acc *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			in *tuple.Tuple2[int64, int64],
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val, _ := acc.Get()
			if in.V2 >= val.V2 {
				// If count is the same, we choose the auction with larger id
				if in.V2 == val.V2 && val.V1 > in.V1 {
					return acc
				}
				acc.Set(in)
			}
			return acc
		},

		// Output the hottest auction's Id and count
		// V1: auctionId
		// V2: count
		func(
			acc *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *tuple.Tuple2[int64, int64] {
			val, _ := acc.Get()
			return val
		},

		// Merge function, return the auctionId with larger count
		func(
			acc1 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			if val2.V2 >= val1.V2 {
				// If count is the same, we choose the auction with larger id
				if val1.V2 == val2.V2 && val1.V1 > val2.V1 {
					return acc1
				}
				acc1.Set(val2)
			}
			return acc1
		},
	)
	hottestItem := dataflow.NewTumblingWindow(
		"HottestItem",
		hottestItemKeyAssigner,
		hottestItemAggregator,
		500,
	)
	hottestItem.SetParallelism(1)
	dataflow.AddOperator(df, hottestItem)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, int64]) {
			result := tuple.Tuple2[int64, int64]{
				V1: t.V1,
				V2: t.V2,
			}
			hottestResults = append(hottestResults, result)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, window)
	dataflow.Add1To1Stream(df, window, hottestItem)
	dataflow.Add1To1Stream(df, hottestItem, sink)

	return df
}
