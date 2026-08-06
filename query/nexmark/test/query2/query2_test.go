package query2Test

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query2"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY2 LOGIC CHANGES
// In this test, we use a fixed input set to test the correctness of query4, by
// comparing output of the query with expected results

// Query 2: Selection
// Selects all bids on a set of five items(In this test 1007, 1020, 2001, 2019,
// 1087).

// type BidEvent = tuple.Tuple5[
// int64,  // V1: auction id (foriegn key)
// int64,  // V2: bidder id (foriegn key)
// int64,  // V3: price
// int64,  // V4: dateTime (unix nanoseconds)
// string, // V5: extra
// ]

var SampleInput []models.BidEvent = []models.BidEvent{
	{V1: 1000, V2: 101, V3: 100, V4: 1, V5: "x"},
	{V1: 2000, V2: 201, V3: 500, V4: 2, V5: "x"},
	{V1: 1003, V2: 102, V3: 200, V4: 1, V5: "x"},
	{V1: 2001, V2: 103, V3: 300, V4: 1, V5: "x"},
	{V1: 5004, V2: 301, V3: 800, V4: 3, V5: "x"},
	{V1: 1235, V2: 202, V3: 600, V4: 2, V5: "x"},
	{V1: 1007, V2: 201, V3: 500, V4: 2, V5: "x"},
	{V1: 1566, V2: 103, V3: 300, V4: 1, V5: "x"},
	{V1: 1007, V2: 302, V3: 900, V4: 3, V5: "x"},
	{V1: 1087, V2: 203, V3: 700, V4: 2, V5: "x"},
	{V1: 2019, V2: 104, V3: 400, V4: 1, V5: "x"},
}

var SampleResult []models.BidEvent = []models.BidEvent{
	{V1: 2001, V2: 103, V3: 300, V4: 1, V5: "x"},
	{V1: 1007, V2: 201, V3: 500, V4: 2, V5: "x"},
	{V1: 1007, V2: 302, V3: 900, V4: 3, V5: "x"},
	{V1: 1087, V2: 203, V3: 700, V4: 2, V5: "x"},
	{V1: 2019, V2: 104, V3: 400, V4: 1, V5: "x"},
}

var results []models.BidEvent

func TestQuery2Correctness(t *testing.T) {
	results = make([]models.BidEvent, 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 3
	testutils.DeployJob(numWorkers, Query2Test, config)

	// Wait enough time until all results are generated
	time.Sleep(6 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != 5 {
		t.Errorf("Incorrect amount of results=%d, expect=9", len(results))
	}

	// Check if the result exists
	for i, r := range results {
		if SampleResult[i].V1 != r.V1 || SampleResult[i].V2 != r.V2 ||
			SampleResult[i].V3 != r.V3 ||
			SampleResult[i].V4 != r.V4 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				SampleResult[i],
				r,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Query2Test() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Define Source for BidEvent
	src := dataflow.NewSource[*models.BidEvent](
		"source",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(20 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]

	//Define Filter
	filter := query2.Query2Filter()
	filter.SetParallelism(1)
	dataflow.AddOperator(df, filter)

	// Define Sink (no-op)
	sink := dataflow.NewSink("sink", func(in *models.BidEvent) {
		results = append(results, *in)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Filter
	dataflow.Add1To1Stream(df, src, filter)
	// Connect Filter -> Sink
	dataflow.Add1To1Stream(df, filter, sink)

	return df
}
