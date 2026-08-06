package query7Test

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
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query7"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY7 LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query5, by
// comparing output of the query with expected  highestResults

// Select the bids with the highest bid price in the last period

// Expected  highestResults
// V1: highest bid price
// V2: auctionId of that bid
// V3: bidderId
var ExpectedHighestResult []tuple.Tuple3[int64, int64, int64] = []tuple.Tuple3[int64, int64, int64]{
	// Window [40,80)
	{V1: 700, V2: 5, V3: 102},
	// Window [80,120)
	{V1: 1000, V2: 11, V3: 203},
	// Window [160,200)
	{V1: 950, V2: 17, V3: 303},
}

var highestResults []tuple.Tuple3[int64, int64, int64]

func TestQuery7Correctness(t *testing.T) {
	highestResults = make([]tuple.Tuple3[int64, int64, int64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************
	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, Query7Test, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(1000)
	// Wait till we receive the ending watermark
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")
	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(highestResults) != len(ExpectedHighestResult) {
		t.Errorf(
			"Incorrect amount of  highestResults=%d, expect=%d",
			len(highestResults),
			len(ExpectedHighestResult),
		)
	}

	log.Println("Actual  highestResults:", highestResults)

	// Check if the result exists
	for i, r := range highestResults {
		if ExpectedHighestResult[i].V1 != r.V1 ||
			ExpectedHighestResult[i].V2 != r.V2 ||
			ExpectedHighestResult[i].V3 != r.V3 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				ExpectedHighestResult[i],
				r,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Query7Test() *dataflow.Dataflow {
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

	// Keyby window ending timestamp
	highestBidKeyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[int64, int64, int64]) int64 {
			return t.GetTimestamp()
		},
	)

	highestBidAggregator := dataflow.NewAggregator(
		// Create a new valueState to store auctionId, price and bidder
		func() *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			return stateType.NewValueState(
				tuple.NewTuple3(int64(0), int64(0), int64(0)),
			)
		},

		// Update the highest bid's info
		func(
			acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			in *tuple.Tuple3[int64, int64, int64],
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			val, _ := acc.Get()
			if val.V1 <= in.V1 {
				// If price is the same, we choose the bid with larger auctionId
				if val.V1 == in.V1 && val.V2 > in.V2 {
					return acc
				}
				acc.Set(in)
			}
			return acc
		},

		// Output: auctionId, price and bidderId of the highest bid
		func(
			acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
		) *tuple.Tuple3[int64, int64, int64] {
			val, _ := acc.Get()
			return val
		},

		// Merge func, only keeps info about the highest bid
		func(
			acc1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			if val1.V1 <= val2.V1 {
				if val1.V1 == val2.V1 && val1.V2 > val2.V2 {
					return acc1
				}
				acc1.Set(val2)
			}
			return acc1
		},
	)
	// Calculate Highest bid
	highestBid := dataflow.NewTumblingWindow(
		"highestBid",
		highestBidKeyAssigner,
		highestBidAggregator,
		40,
	)
	highestBid.SetParallelism(1)
	dataflow.AddOperator(df, highestBid)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int64, int64, int64]) {
			result := tuple.Tuple3[int64, int64, int64]{
				V1: t.V1,
				V2: t.V2,
				V3: t.V3,
			}
			highestResults = append(highestResults, result)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, highestBid)
	dataflow.Add1To1Stream(df, highestBid, sink)

	return df
}
