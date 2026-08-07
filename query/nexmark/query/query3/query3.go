package query3

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 3: Local Item Suggestion
// Find all auctions in category 10 that are being sold by people located in
// Oregon

// Stream SQL:
// SELECT person.name, person.city,
// person.state, open auction.id
// FROM auction, person,
// WHERE auction.sellerId = person.id
// AND person.state = ‘OR’
// AND auction.categoryId = 10;

func Query3() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	personSourceConfig := config.DefaultNexmarkSourceConfig()

	// Define Source for PersonEvent
	personSource := source.NewNexmarkPersonSource(
		"personSource",
		personSourceConfig,
	)
	personSource.SetParallelism(1)
	dataflow.AddOperator(df, personSource)

	// Define Source for AuctionEvent
	auctionSourceConfig := config.DefaultNexmarkSourceConfig()

	auctionSource := source.NewNexmarkAuctionSource(
		"auctionSource",
		auctionSourceConfig,
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
	join := Query3Join(
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
		func(t *tuple.Tuple4[string, string, string, int64]) {},
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
