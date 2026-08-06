package query3Test

import (
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constants"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query3"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/varint"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF QUERY3 LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of query3, by
// comparing output of the query with expected results

// Query 3: Local Item Suggestion
// Who is selling in OR in category 10, and for what auction ids?

// Stream SQL:
// SELECT person.name, person.city,
// person.state, open auction.id
// FROM auction, person,
// WHERE auction.sellerId = person.id
// AND person.state = ‘OR’
// AND auction.categoryId = 10;

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
	// This personEvent will be filtered out
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
	// These personEvents will arrive after some auctionEvents
	{
		V1: 101,
		V2: "Bob",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "OR",
		V7: 0,
		V8: "XXXX",
	},
	{
		V1: 102,
		V2: "Cate",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "OR",
		V7: 0,
		V8: "XXXX",
	},
	{
		V1: 103,
		V2: "Dave",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "OR",
		V7: 0,
		V8: "XXXX",
	},
	// This personEvent will not have related auctions in this test
	{
		V1: 110,
		V2: "Even",
		V3: "email",
		V4: "creaditCard",
		V5: "BOS",
		V6: "OR",
		V7: 0,
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
	// These auctionEvents will be filtered out
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  101,
		V9:  15,
		V10: "XXXXX",
	},
	{
		V1:  1001,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  102,
		V9:  14,
		V10: "XXXXX",
	},
	{
		V1:  1002,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  103,
		V9:  8,
		V10: "XXXXX",
	},
	// We will make sure that when these 3 auctionEvent arrives, there's no
	// related personState
	{
		V1:  1000,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  101,
		V9:  10,
		V10: "XXXXX",
	},
	{
		V1:  1001,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  102,
		V9:  10,
		V10: "XXXXX",
	},
	{
		V1:  1002,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  103,
		V9:  10,
		V10: "XXXXX",
	},
	// For this auctionEvent, there will not be a related personState during the
	// whole test, so it will remain in the state when test ends
	{
		V1:  1003,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  200,
		V9:  10,
		V10: "XXXXX",
	},
	// For these auctionEvents, when it arrives, there will be related
	// personState, it will output immediately
	{
		V1:  1004,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  102,
		V9:  10,
		V10: "XXXXX",
	},
	{
		V1:  1005,
		V2:  "A",
		V3:  "S",
		V4:  1000,
		V5:  50,
		V6:  50,
		V7:  12133,
		V8:  101,
		V9:  10,
		V10: "XXXXX",
	},
}

// Expected Result
// V1: person name
// V2: person city
// V3: person state
// V4: auctionId
var SampleResult []tuple.Tuple4[string, string, string, int64] = []tuple.Tuple4[string, string, string, int64]{
	{V1: "Bob", V2: "BOS", V3: "OR", V4: 1000},
	{V1: "Cate", V2: "BOS", V3: "OR", V4: 1001},
	{V1: "Dave", V2: "BOS", V3: "OR", V4: 1002},
	{V1: "Cate", V2: "BOS", V3: "OR", V4: 1004},
	{V1: "Bob", V2: "BOS", V3: "OR", V4: 1005},
}

var results []tuple.Tuple4[string, string, string, int64]

func TestQuery3Correctness(t *testing.T) {
	results = make([]tuple.Tuple4[string, string, string, int64], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 6
	_, workers, _ := testutils.DeployJob(numWorkers, Query3Test, config)

	// Wait enough time until all results are generated
	time.Sleep(11 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != len(SampleResult) {
		t.Errorf(
			"Incorrect amount of results=%d, expect=%d",
			len(results),
			len(SampleResult),
		)
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

	expectedValPerson := make(map[int64]tuple.Tuple3[string, string, string])
	expectedValPerson[101] = tuple.Tuple3[string, string, string]{
		V1: "Bob",
		V2: "BOS",
		V3: "OR",
	}
	expectedValPerson[102] = tuple.Tuple3[string, string, string]{
		V1: "Cate",
		V2: "BOS",
		V3: "OR",
	}
	expectedValPerson[103] = tuple.Tuple3[string, string, string]{
		V1: "Dave",
		V2: "BOS",
		V3: "OR",
	}
	expectedValPerson[110] = tuple.Tuple3[string, string, string]{
		V1: "Even",
		V2: "BOS",
		V3: "OR",
	}

	expectedValAuction := make(map[int64]tuple.Tuple1[int64])
	// In our test input, we have auction with sellerId 200, but we don't have
	// PersonEvent with id 200, so this auctionState will never be used and
	// cleared.
	expectedValAuction[200] = tuple.Tuple1[int64]{
		V1: 1003,
	}

	// Check the state
	numKeys := 0
	// In this test, if our query runs correctly, we will only have 1 state
	// iterator
	for iter.First(); iter.Valid(); iter.Next() {
		serializedKey := iter.Key()
		serializedValue := iter.Value()

		// Extract the state id, key, and value
		stateIDOffset := constants.OperatorIDSize + constants.BucketIdxSize
		stateID := binary.BigEndian.Uint16(
			serializedKey[stateIDOffset : stateIDOffset+constants.StateIDSize],
		)
		key, _, _ := varint.UnmarshalInt64(
			serializedKey[constants.KeyPrefixSize:],
		)
		// This state iterator is for personState
		if stateID == 0 {
			personName, offset1, _ := ord.UnmarshalString(
				nil,
				serializedValue[0:],
			)
			personCity, offset2, _ := ord.UnmarshalString(
				nil,
				serializedValue[offset1:],
			)
			personState, _, _ := ord.UnmarshalString(
				nil,
				serializedValue[offset1+offset2:],
			)
			value := tuple.Tuple3[string, string, string]{
				V1: personName,
				V2: personCity,
				V3: personState,
			}
			if value != expectedValPerson[key] {
				t.Error(
					"Expect ",
					expectedValPerson[key],
					" for key ",
					key,
					", but got ",
					value,
				)
			}
		}
		// This state iterator is for auctionState
		if stateID == 1 {
			auctionId, _, _ := varint.UnmarshalInt64(serializedValue)
			value := tuple.Tuple1[int64]{
				V1: auctionId,
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
		numKeys++
	}
	if numKeys != 5 {
		t.Errorf("Incorrect number of keys, expected %d, got %d", 5, numKeys)
	}
	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Query3Test() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	personSource := dataflow.NewSource[*models.PersonEvent](
		"personSource",
		func(co collector.Collector) {
			for index, event := range SamplePersonInput {
				if index == 0 {
					time.Sleep(3 * time.Second)
				}
				time.Sleep(300 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	personSource.SetParallelism(1)
	dataflow.AddOperator(df, personSource)

	auctionSource := dataflow.NewSource[*models.AuctionEvent](
		"auctionSource",
		func(co collector.Collector) {
			for index, event := range SampleAuctionInput {
				time.Sleep(100 * time.Millisecond)
				co.Emit(&event)
				if index == 2 {
					time.Sleep(5 * time.Second)
				}
			}
		},
	)
	auctionSource.SetParallelism(1)
	dataflow.AddOperator(df, auctionSource)

	// We only need personEvent with state "OR"
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
	personFilter := dataflow.NewFilter(
		"personFilter",
		func(t *models.PersonEvent) bool {
			return t.V6 == "OR"
		},
	)
	personFilter.SetParallelism(1)
	dataflow.AddOperator(df, personFilter)
	// We only need auctionEvent with category 10
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
	auctionFilter := dataflow.NewFilter(
		"auctionFilter",
		func(t *models.AuctionEvent) bool {
			return t.V9 == 10
		},
	)
	auctionFilter.SetParallelism(1)
	dataflow.AddOperator(df, auctionFilter)

	// Define join operator
	join := query3.Query3Join(
		personFilter,
		auctionFilter,
		false,
		0,
	)
	join.SetParallelism(1)
	dataflow.AddOperator(df, join)

	// Do nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple4[string, string, string, int64]) {
			results = append(results, *t)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect each operator to their upstream
	dataflow.Add1To1Stream(df, personSource, personFilter)
	dataflow.Add1To1Stream(df, auctionSource, auctionFilter)
	dataflow.Add2To1Stream(df, personFilter, auctionFilter, join)
	dataflow.Add1To1Stream(df, join, sink)

	return df
}
