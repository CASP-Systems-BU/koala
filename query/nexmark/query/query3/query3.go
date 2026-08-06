package query3

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
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

	// Define join operator
	join := Query3Join(
		personSource,
		auctionSource,
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
	dataflow.Add2To1Stream(df, personSource, auctionSource, join)
	dataflow.Add1To1Stream(df, join, sink)

	return df
}
