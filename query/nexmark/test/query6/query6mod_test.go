package query6Test

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
	"github.com/CASP-Systems-BU/koala/query/nexmark/query/query6"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY6MOD LOGIC CHANGES
// In this test, we use a fixed input set to test the correctness of query6mod,
// by comparing output of the query with expected results.

// Query 6: Average Selling Price by Seller
// Select the average selling price over the last 10 closed auctions by the same
// seller.

// type ClosedAuction = tuple.Tuple5[
// 	int64, // V1: id
// 	int64, // V2: sellerId
// 	int64, // V3: finalPrice
// 	int64, // V4: category
// 	int64, // V5: dateTime (unix nanoseconds)
// ]

var SampleInput []models.ClosedAuctionEvent = []models.ClosedAuctionEvent{
	{V1: 1, V2: 101, V3: 100, V4: 1, V5: 1000000},
	{V1: 2, V2: 101, V3: 200, V4: 1, V5: 1000001},
	{V1: 3, V2: 101, V3: 300, V4: 1, V5: 1000002},
	{V1: 4, V2: 101, V3: 400, V4: 1, V5: 1000003},
	{V1: 5, V2: 101, V3: 500, V4: 1, V5: 1000004},
	{V1: 6, V2: 101, V3: 600, V4: 1, V5: 1000005},
	{V1: 7, V2: 101, V3: 700, V4: 1, V5: 1000006},
	{V1: 8, V2: 101, V3: 800, V4: 1, V5: 1000007},
	{V1: 9, V2: 101, V3: 900, V4: 1, V5: 1000008},
	{V1: 10, V2: 101, V3: 1000, V4: 1, V5: 1000009},
	{V1: 11, V2: 101, V3: 1100, V4: 1, V5: 1000010},
	{V1: 12, V2: 202, V3: 500, V4: 2, V5: 1000011},
	{V1: 13, V2: 202, V3: 600, V4: 2, V5: 1000012},
}

// SampleResult:
// V1: sellerId
// V2: average finalPrice of last 10 closed_auctions
var SampleResult []tuple.Tuple2[int64, float64] = []tuple.Tuple2[int64, float64]{
	{V1: 101, V2: 100},
	{V1: 101, V2: 150},
	{V1: 101, V2: 200},
	{V1: 101, V2: 250},
	{V1: 101, V2: 300},
	{V1: 101, V2: 350},
	{V1: 101, V2: 400},
	{V1: 101, V2: 450},
	{V1: 101, V2: 500},
	{V1: 101, V2: 550},
	{V1: 101, V2: 650},
	{V1: 202, V2: 500},
	{V1: 202, V2: 550},
}

var results []tuple.Tuple2[int64, float64]

func TestQuery6ModCorrectness(t *testing.T) {
	results = make([]tuple.Tuple2[int64, float64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	testutils.DeployJob(numWorkers, Query6ModTest, config)

	// Wait enough time until all results are generated
	time.Sleep(10 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != 13 {
		t.Errorf("Incorrect amount of results=%d, expect=13", len(results))
	}

	// Check if the result exists
	for i, r := range results {
		if SampleResult[i].V1 != r.V1 || SampleResult[i].V2 != r.V2 {
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

// Same query logic as in query6mod.go
func Query6ModTest() *dataflow.Dataflow {

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

	mapper := query6.Query6StatefulMapper(0)
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
