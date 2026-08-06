package query7Test

import (
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query7"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY7 LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query5, by
// comparing output of the query with expected results

// Select the bids with the highest bid price in the last period, since this
// needs global aggregate we only calculate each bidder's highest bid price

// type BidEvent = tuple.Tuple5[
// int64,  // V1: auction id (foriegn key)
// int64,  // V2: bidder id (foriegn key)
// int64,  // V3: price
// int64,  // V4: dateTime (unix nanoseconds)
// string, // V5: extra
// ]

var SampleInput []models.BidEvent = []models.BidEvent{
	// Window [0,40)
	{V1: 1, V2: 101, V3: 500, V4: 5, V5: "w1"},
	{V1: 2, V2: 101, V3: 600, V4: 10, V5: "w1"},
	{V1: 3, V2: 101, V3: 550, V4: 20, V5: "w1"},
	{V1: 4, V2: 102, V3: 300, V4: 25, V5: "w1"},
	{V1: 5, V2: 102, V3: 700, V4: 30, V5: "w1"},
	{V1: 6, V2: 103, V3: 200, V4: 35, V5: "w1"},
	// Window [40,80)
	{V1: 10, V2: 202, V3: 900, V4: 40, V5: "boundary"},
	{V1: 7, V2: 201, V3: 400, V4: 45, V5: "w2"},
	{V1: 8, V2: 201, V3: 700, V4: 50, V5: "w2"},
	{V1: 9, V2: 202, V3: 650, V4: 55, V5: "w2"},
	{V1: 11, V2: 203, V3: 1000, V4: 75, V5: "w2"},
	{V1: 12, V2: 203, V3: 500, V4: 70, V5: "w2"},
	// Window [120,160)
	{V1: 22, V2: 306, V3: 600, V4: 120, V5: "boundary"},
	{V1: 13, V2: 301, V3: 200, V4: 125, V5: "w4"},
	{V1: 14, V2: 301, V3: 800, V4: 130, V5: "w4"},
	{V1: 15, V2: 302, V3: 900, V4: 140, V5: "w4"},
	{V1: 16, V2: 302, V3: 850, V4: 150, V5: "w4"},
	{V1: 17, V2: 303, V3: 950, V4: 155, V5: "w4"},
	{V1: 18, V2: 304, V3: 400, V4: 135, V5: "w4"},
	{V1: 19, V2: 304, V3: 700, V4: 145, V5: "w4"},
	{V1: 20, V2: 305, V3: 100, V4: 150, V5: "w4"},
	{V1: 21, V2: 305, V3: 500, V4: 159, V5: "w4"},
	// Ending record
	{V1: 1000, V2: 123, V3: 600, V4: 1000, V5: "ending"},
}

// Expected results
// V1: highest bid price
// V2: auctionId of that bid
// V3: bidderId
// Each batch contains results triggered by the same watermark
var ExpectedResultBatches [][]tuple.Tuple3[int64, int64, int64] = [][]tuple.Tuple3[int64, int64, int64]{
	// Window [0,40)
	{
		{V1: 600, V2: 2, V3: 101},
		{V1: 700, V2: 5, V3: 102},
		{V1: 200, V2: 6, V3: 103},
	},
	// Window [40,80)
	{
		{V1: 700, V2: 8, V3: 201},
		{V1: 900, V2: 10, V3: 202},
		{V1: 1000, V2: 11, V3: 203},
	},
	// Window [120,160)
	{
		{V1: 800, V2: 14, V3: 301},
		{V1: 900, V2: 15, V3: 302},
		{V1: 950, V2: 17, V3: 303},
		{V1: 700, V2: 19, V3: 304},
		{V1: 500, V2: 21, V3: 305},
		{V1: 600, V2: 22, V3: 306},
	},
}

var results []tuple.Tuple3[int64, int64, int64]

func TestQuery7ModCorrectness(t *testing.T) {
	results = make([]tuple.Tuple3[int64, int64, int64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	testutils.DeployJob(numWorkers, Query7ModTest, config)

	// Wait enough time until all results are generated
	time.Sleep(16 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	expectedCount := 0
	for _, batch := range ExpectedResultBatches {
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
	for batchIdx, expectedBatch := range ExpectedResultBatches {
		end := start + len(expectedBatch)
		if end > len(results) {
			t.Fatalf("Results ended too early, batch=%d", batchIdx)
		}
		actualBatch := results[start:end]

		// Since in each batch, the sequence of results is not fixed, we use map
		// to test results' correctness
		expectedMap := make(map[tuple.Tuple3[int64, int64, int64]]int)
		for _, e := range expectedBatch {
			expectedMap[e]++
		}
		actualMap := make(map[tuple.Tuple3[int64, int64, int64]]int)
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

func Query7ModTest() *dataflow.Dataflow {
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
		100,
		0,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	tumblingWindow := query7.Query7TumblingWindow(40)
	tumblingWindow.SetParallelism(1)
	dataflow.AddOperator(df, tumblingWindow)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int64, int64, int64]) {
			result := tuple.Tuple3[int64, int64, int64]{
				V1: t.V1,
				V2: t.V2,
				V3: t.V3,
			}
			results = append(results, result)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, sink)

	return df
}
