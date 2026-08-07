package query5Test

import (
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/query/query5"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY5MOD LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query5, by
// comparing output of the query with expected results

// Query 5: Hot Items
// Selects the item with the most bids in the past 1 hours time period;
// the “hottest” item. The results are output every minute. In this version
// we only calculate each item's bid count
// to avoid global aggregate

// type BidEvent = tuple.Tuple5[
// int64,  // V1: auction id (foriegn key)
// int64,  // V2: bidder id (foriegn key)
// int64,  // V3: price
// int64,  // V4: dateTime (unix nanoseconds)
// string, // V5: extra
// ]

var SampleInput []models.BidEvent = []models.BidEvent{
	{V1: 1, V2: 101, V3: 450, V4: 10450, V5: "extra"},
	{V1: 1, V2: 102, V3: 600, V4: 10600, V5: "extra"},
	{V1: 1, V2: 103, V3: 750, V4: 10750, V5: "extra"},
	{V1: 2, V2: 201, V3: 700, V4: 10700, V5: "extra"},
	{V1: 2, V2: 202, V3: 1000, V4: 11000, V5: "extra"},
	{V1: 3, V2: 301, V3: 2800, V4: 12800, V5: "extra"},
	{V1: 3, V2: 302, V3: 2820, V4: 12820, V5: "extra"},
	{V1: 3, V2: 303, V3: 2830, V4: 12830, V5: "extra"},
	{V1: 3, V2: 304, V3: 2850, V4: 12900, V5: "extra"},
	{V1: 3, V2: 304, V3: 2850, V4: 14900, V5: "ending"},
}

// Expected Result
// V1: auctionId
// V2: total bid count for that auctionId
// Each batch contains results triggered by the same watermark
var SampleResultBatches = [][]tuple.Tuple2[int64, int64]{
	// Triggered by waterMark 10600
	{
		// SlidingWindow[9500,10500)
		{V1: 1, V2: 1},
	},
	// Triggered by waterMark 11000
	{
		// SlidingWindow[10000, 11000)
		{V1: 1, V2: 3},
		{V1: 2, V2: 1},
	},
	// Triggered by waterMark 12800
	{
		// SlidingWindow[10500, 11500)
		{V1: 1, V2: 2},
		{V1: 2, V2: 2},
		// SlidingWindow[11000, 12000)
		{V1: 2, V2: 1},
	},
	// Triggered by watermark 14900
	{
		// SlingWindow[12000,13000)
		{V1: 3, V2: 4},
		// SlingWindow[12500,13500)
		{V1: 3, V2: 4},
	},
}

var results []tuple.Tuple2[int64, int64]

func TestQuery5ModCorrectness(t *testing.T) {
	results = make([]tuple.Tuple2[int64, int64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************
	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, Query5ModTest, config)

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

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	expectedCount := 0
	for _, batch := range SampleResultBatches {
		expectedCount += len(batch)
	}
	if len(results) != expectedCount {
		t.Errorf(
			"Incorrect amount of results=%d, expect=%d",
			len(results),
			expectedCount,
		)
	}

	log.Println("Actual results:", results)

	// Check batch by batch, we do this because if a watermark will trigger more
	// than one window, then the arriving sequence of those results is not fixed
	start := 0
	for batchIdx, expectedBatch := range SampleResultBatches {
		end := start + len(expectedBatch)
		if end > len(results) {
			t.Fatalf("Results ended too early, batch=%d", batchIdx)
		}
		actualBatch := results[start:end]

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

func Query5ModTest() *dataflow.Dataflow {
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

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, int64]) {
			result := tuple.Tuple2[int64, int64]{
				V1: t.V1,
				V2: t.V2,
			}
			results = append(results, result)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, window)
	dataflow.Add1To1Stream(df, window, sink)

	return df
}
