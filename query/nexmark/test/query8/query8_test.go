package query8Test

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
	"github.com/CASP-Systems-BU/koala/query/nexmark/query/query8"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/varint"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY8 LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query8, by
// comparing output of the query with expected results

// Finds people who put something up for sale within twelve hours of registering
// to use the auction service

// type PersonEvent = tuple.Tuple8[
// int64,  // V1:id
// string, // V2:name
// string, // V3:email
// string, // V4:creditCard
// string, // V5:city
// string, // V6:state
// int64,  // V7:dateTime (unix nanoseconds)
// string, // V8:extra
// ]
var SamplePersonInput []models.PersonEvent = []models.PersonEvent{
	// In this sampleInput, all timers set by personEvents will be triggered
	// except the last one
	{
		V1: 100,
		V2: "Alice",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 0,
		V8: "XXXX",
	},
	{
		V1: 101,
		V2: "Bob",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 100,
		V8: "XXXX",
	},
	{
		V1: 102,
		V2: "Cate",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 200,
		V8: "XXXX",
	},
	{
		V1: 103,
		V2: "Dave",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 360,
		V8: "XXXX",
	},
	{
		V1: 10002,
		V2: "Emily",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 1800,
		V8: "XXXX",
	},
	{
		V1: 100000,
		V2: "END",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "MA",
		V7: 3600000,
		V8: "XXXX",
	},
}

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
		V6:  50,
		V7:  12133,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  350,
		V6:  250,
		V7:  12133,
		V8:  100,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  500,
		V6:  500,
		V7:  12133,
		V8:  100,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  600,
		V6:  550,
		V7:  12133,
		V8:  101,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  2000,
		V6:  700,
		V7:  12133,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  900,
		V6:  900,
		V7:  12133,
		V8:  100,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  2000,
		V6:  2000,
		V7:  12133,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
	{
		V1:  1000,
		V2:  "ENDING",
		V3:  "S",
		V4:  1000,
		V5:  2000,
		V6:  20000,
		V7:  12133,
		V8:  102,
		V9:  5,
		V10: "XXXXX",
	},
}

// Expected results
// V1: personId
// V2: person name
var ExpectedResult []tuple.Tuple2[int64, string] = []tuple.Tuple2[int64, string]{
	{V1: 100, V2: "Alice"},
	{V1: 101, V2: "Bob"},
	{V1: 102, V2: "Cate"},
}

var results []tuple.Tuple2[int64, string]

func TestQuery8Correctness(t *testing.T) {
	results = make([]tuple.Tuple2[int64, string], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, Query8Test, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}

	// Wait till we receive the ending watermark
	expectedWM := int64(20000)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
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
		if ExpectedResult[i].V1 != r.V1 || ExpectedResult[i].V2 != r.V2 {
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
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get the state
		iter = w.StateService.StateBackendImpl.GetIterator()
	}

	expectedVal := make(map[uint16]tuple.Tuple2[int64, string])
	expectedVal[0] = tuple.Tuple2[int64, string]{
		V1: 100000,
		V2: "END",
	}

	// Check the state
	numKeys := 0
	// In this test, if our query runs correctly, we will only have 1 state
	// (key) left
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
		personId, offset1, _ := varint.UnmarshalInt64(serializedValue)
		personName, _, _ := ord.UnmarshalString(
			nil,
			serializedValue[offset1:],
		)
		value := tuple.Tuple2[int64, string]{
			V1: personId,
			V2: personName,
		}

		numKeys++
		// Compare stateValue with expectedValue
		if value != expectedVal[stateID] {
			t.Error(
				"Expect ",
				expectedVal[stateID],
				" for key ",
				key,
				", but got ",
				value,
			)
		}
	}
	if numKeys != 1 {
		t.Errorf("Incorrect number of keys, expected %d, got %d", 1, numKeys)
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Query8Test() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Define the window size we want to use in the query
	customWindowSize := int64(500)
	personSource := dataflow.NewSource[*models.PersonEvent](
		"personSource",
		func(co collector.Collector) {
			for _, event := range SamplePersonInput {
				time.Sleep(300 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	personSource.SetParallelism(1)
	// Assign V4(dateTime) as timestamp for bid event
	// type PersonEvent = tuple.Tuple8[
	//  int64,  // V1:id
	//  string, // V2:name
	//  string, // V3:email
	//  string, // V4:creditCard
	//  string, // V5:city
	//  string, // V6:state
	//  int64,  // V7:dateTime (unix nanoseconds)
	//  string, // V8:extra
	// ]
	personTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.PersonEvent) int64 {
			return t.V7
		},
	)
	personSource.AssignTimestampAndWatermark(
		personTimestampAssigner,
		200,
		0,
	)
	dataflow.AddOperator(df, personSource)

	auctionSource := dataflow.NewSource[*models.AuctionEvent](
		"auctionSource",
		func(co collector.Collector) {
			for _, event := range SampleAuctionInput {
				time.Sleep(100 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	auctionSource.SetParallelism(1)
	// Assign V6(dateTime) as timestamp for auction event
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

	windowJoin := query8.Query8CustomWindowJoin(
		personSource,
		auctionSource,
		customWindowSize,
	)
	windowJoin.SetParallelism(1)
	dataflow.AddOperator(df, windowJoin)

	// Collect results
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, string]) {
			result := tuple.Tuple2[int64, string]{
				V1: t.V1,
				V2: t.V2,
			}
			results = append(results, result)
		},
	)

	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect each operator to their upstream
	dataflow.Add2To1Stream(df, personSource, auctionSource, windowJoin)
	dataflow.Add1To1Stream(df, windowJoin, sink)

	return df
}
