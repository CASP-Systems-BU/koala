package query4Test

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/query/query4"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY4MOD LOGIC CHANGES
// In this test, we use a fixed input set to test the correctness of query4mod,
// by comparing output of the query with expected results

// Query 4: Average Price for a Category
// Select the average of the wining bid prices for all closed auctions in each
// category

// type ClosedAuction = tuple.Tuple5[
// 	int64, // V1: id
// 	int64, // V2: sellerId
// 	int64, // V3: finalPrice
// 	int64, // V4: category
// 	int64, // V5: dateTime (unix nanoseconds)
// ]

var SampleInput []models.ClosedAuctionEvent = []models.ClosedAuctionEvent{
	{V1: 1, V2: 101, V3: 100, V4: 1, V5: 1000000},
	{V1: 2, V2: 201, V3: 500, V4: 2, V5: 1000001},
	{V1: 3, V2: 102, V3: 200, V4: 1, V5: 1000002},
	{V1: 4, V2: 301, V3: 800, V4: 3, V5: 1000003},
	{V1: 5, V2: 202, V3: 600, V4: 2, V5: 1000004},
	{V1: 6, V2: 103, V3: 300, V4: 1, V5: 1000005},
	{V1: 7, V2: 302, V3: 900, V4: 3, V5: 1000006},
	{V1: 8, V2: 203, V3: 700, V4: 2, V5: 1000007},
	{V1: 9, V2: 104, V3: 400, V4: 1, V5: 1000008},
}

// Expected results:
// V1: category
// V2: average finalPrice of all closed_auctions in that category
var ExpectedResult []tuple.Tuple2[int64, float64] = []tuple.Tuple2[int64, float64]{
	{V1: 1, V2: 100},
	{V1: 2, V2: 500},
	{V1: 1, V2: 150},
	{V1: 3, V2: 800},
	{V1: 2, V2: 550},
	{V1: 1, V2: 200},
	{V1: 3, V2: 850},
	{V1: 2, V2: 600},
	{V1: 1, V2: 250},
}

var results []tuple.Tuple2[int64, float64]

func TestQuery4ModCorrectness(t *testing.T) {
	results = make([]tuple.Tuple2[int64, float64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	testutils.DeployJob(numWorkers, Query4ModTest, config)

	// Wait enough time until all results are generated
	time.Sleep(12 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != 9 {
		t.Errorf("Incorrect amount of results=%d, expect=9", len(results))
	}

	// Check if the result exists
	for i, r := range results {
		if ExpectedResult[i].V1 != r.V1 || ExpectedResult[i].V2 != r.V2 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				ExpectedResult[i],
				r,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

// Test Query4Mod (use ClosedAuction source)
func Query4ModTest() *dataflow.Dataflow {

	df := dataflow.NewDataflow()
	src := dataflow.NewSource[*models.ClosedAuctionEvent](
		"source",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(200 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	mapper := query4.Query4StatefulMapper()
	mapper.SetParallelism(2)
	dataflow.AddOperator(df, mapper)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, float64]) {
			result := tuple.Tuple2[int64, float64]{
				V1: t.V1,
				V2: t.V2,
			}
			results = append(results, result)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, mapper)
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
