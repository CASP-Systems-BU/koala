package closedAuctionTest

import (
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	closedauction "github.com/CASP-Systems-BU/koala/query/nexmark/query/closedAuction"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/mus-format/mus-go/varint"
)

// In this test, we use a fixed input set to test the correctness of the way we
// calculate closedAuction, by
// comparing output with expected results

// type AuctionEvent = tuple.Tuple10[
// int64,  // V1:auction id
// string, // V2:item name
// string, // V3:description
// int64,  // V4:initial bid
// int64,  // V5:reserve
// int64,  // V6:dateTime (unix nanoseconds)
// int64,  // V7:expires (unix nanoseconds)
// int64,  // V8:seller
// int64,  // V9:category
// string, // V10:extra
// ]
var SampleAuctionInput []models.AuctionEvent = []models.AuctionEvent{
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  100,
		V7:  300,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
	// For this auction, till it expires, we won't have any bid related to it
	{
		V1:  1001,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  500,
		V7:  600,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  9000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  1000000,
		V7:  1100000,
		V8:  102,
		V9:  5,
		V10: "Ending",
	},
}

// type BidEvent = tuple.Tuple5[
// int64,  // V1: auction id (foriegn key)
// int64,  // V2: bidder id (foriegn key)
// int64,  // V3: price
// int64,  // V4: dateTime (unix nanoseconds)
// string, // V5: extra
// ]
var SampleBidInput []models.BidEvent = []models.BidEvent{
	{V1: 1000, V2: 101, V3: 40, V4: 110, V5: "XXXX"},
	{V1: 1000, V2: 102, V3: 60, V4: 115, V5: "XXXX"},
	{V1: 1000, V2: 103, V3: 90, V4: 200, V5: "XXXX"},
	{V1: 1000, V2: 101, V3: 150, V4: 260, V5: "XXXX"},
	{V1: 1000, V2: 100, V3: 450, V4: 1000, V5: "XXXX"},
	{V1: 9000, V2: 10000, V3: 46532, V4: 10000, V5: "XXXX"},
}

// Expected Result
// type ClosedAuction = tuple.Tuple5[
// int64, // V1: id
// int64, // V2: sellerId
// int64, // V3: finalPrice
// int64, // V4: category
// int64, // V5: dateTime (unix nanoseconds)
// ]
var ExpectedResult []models.ClosedAuctionEvent = []models.ClosedAuctionEvent{
	{V1: 1000, V2: 102, V3: 150, V4: 5, V5: 300},
}

var results []models.ClosedAuctionEvent

func TestClosedAuctionCorrectness(t *testing.T) {
	results = make([]models.ClosedAuctionEvent, 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, ClosedAuction, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(10000)
	// Wait till we receive the ending watermark
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != len(ExpectedResult) {
		t.Errorf(
			"Incorrect amount of results=%d, expect=%d",
			len(results),
			len(ExpectedResult),
		)
	}

	// Check if the result exists
	for i, r := range results {
		if ExpectedResult[i].V1 != r.V1 || ExpectedResult[i].V2 != r.V2 ||
			ExpectedResult[i].V3 != r.V3 ||
			ExpectedResult[i].V4 != r.V4 ||
			ExpectedResult[i].V5 != r.V5 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				ExpectedResult[i],
				r,
			)
		}
	}

	var iter stateBackend.StateIterator
	// Get the customWindowJoin
	for _, w := range workers {
		// Skip the sink and source
		if !w.AssignedTask.IsStatefulOperator() {
			continue
		}

		// Get the state
		iter = w.StateService.StateBackendImpl.GetIterator()
	}
	// Expected value for auction state(state0)
	expectedValAuction := make(
		map[int64]tuple.Tuple3[int64, int64, int64],
	)
	expectedValAuction[9000] = tuple.Tuple3[int64, int64, int64]{
		V1: 9000,
		V2: 102,
		V3: 5,
	}
	// Expected value for bid state(state1)
	expectedValBid := make(map[int64]tuple.Tuple2[int64, int64])
	expectedValBid[9000] = tuple.Tuple2[int64, int64]{
		V1: 46532,
		V2: 10000,
	}

	// Check the state
	numKeys := 0
	// In this test, if our query runs correctly, we will only have 1
	// stateiterator
	for iter.First(); iter.Valid(); iter.Next() {
		serializedKey := iter.Key()
		serializedValue := iter.Value()

		// Extract the state id, key, and value
		stateIDOffset := constant.OperatorIDSize + constant.BucketIdxSize
		stateID := binary.BigEndian.Uint16(
			serializedKey[stateIDOffset : stateIDOffset+constant.StateIDSize],
		)
		key, _, _ := varint.UnmarshalInt64(
			serializedKey[constant.KeyPrefixSize:],
		)
		// This state is auctionState
		if stateID == 0 {
			auctionId, offset, _ := varint.UnmarshalInt64(serializedValue)
			sellerId, offset1, _ := varint.UnmarshalInt64(
				serializedValue[offset:],
			)
			category, _, _ := varint.UnmarshalInt64(
				serializedValue[offset+offset1:],
			)
			value := tuple.Tuple3[int64, int64, int64]{
				V1: auctionId,
				V2: sellerId,
				V3: category,
			}
			if value != expectedValAuction[key] {
				t.Error(
					"Expect ",
					expectedValAuction[key],
					" for key ",
					key,
					", but got ",
					value,
				)
			}
		}
		// This state is bidState
		if stateID == 1 {
			price, offset, _ := varint.UnmarshalInt64(serializedValue)
			dateTime, _, _ := varint.UnmarshalInt64(serializedValue[offset:])
			value := tuple.Tuple2[int64, int64]{
				V1: price,
				V2: dateTime,
			}
			if value != expectedValBid[key] {
				t.Error(
					"Expect ",
					expectedValBid[key],
					" for key ",
					key,
					", but got ",
					value,
				)
			}
		}
		numKeys++
	}
	if numKeys != 2 {
		t.Errorf("Incorrect number of keys, expected %d, got %d", 2, numKeys)
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func ClosedAuction() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Define Source for AuctionEvent
	auctionSource := dataflow.NewSource[*models.AuctionEvent](
		"auctionSource",
		func(co collector.Collector) {
			for index, event := range SampleAuctionInput {
				if index == 0 {
					time.Sleep(3 * time.Second)
				}
				time.Sleep(300 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	auctionSource.SetParallelism(1)
	// type AuctionEvent = tuple.Tuple10[
	//  int64,  // V1:auction id
	//  string, // V2:item name
	//  string, // V3:description
	//  int64,  // V4:initial bid
	//  int64,  // V5:reserve
	//  int64,  // V6:dateTime (unix nanoseconds)
	//  int64,  // V7:expires (unix nanoseconds)
	//  int64,  // V8:seller
	//  int64,  // V9:category
	//  string, // V10:extra
	// ]
	// Assign V6(dateTime) as timestamp for auction event
	auctionTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.AuctionEvent) int64 {
			return t.V6
		},
	)
	auctionSource.AssignTimestampAndWatermark(
		auctionTimestampAssigner,
		200,
		0,
	)
	dataflow.AddOperator(df, auctionSource)

	// Define Source for BidEvent
	bidSource := dataflow.NewSource[*models.BidEvent](
		"bidSource",
		func(co collector.Collector) {
			for _, event := range SampleBidInput {
				time.Sleep(300 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	bidSource.SetParallelism(1)
	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]
	// Assign V4(dateTime) as timestamp for bid event
	bidTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.BidEvent) int64 {
			return t.V4
		},
	)
	bidSource.AssignTimestampAndWatermark(
		bidTimestampAssigner,
		200,
		0,
	)
	dataflow.AddOperator(df, bidSource)

	closedAuction := closedauction.ClosedAuction(auctionSource, bidSource)
	closedAuction.SetParallelism(1)
	dataflow.AddOperator(df, closedAuction)

	// Collect output
	sink := dataflow.NewSink(
		"sink",
		func(t *models.ClosedAuctionEvent) {
			results = append(results, *t)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add2To1Stream(df, auctionSource, bidSource, closedAuction)
	dataflow.Add1To1Stream(df, closedAuction, sink)

	return df
}
