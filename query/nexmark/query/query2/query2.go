package query2

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 2: Selection
// Selects all bids on a set of five items.

// Stream SQL:
// SELECT itemid, price
// FROM bid
// WHERE itemid = 1007 OR
// itemid = 1020 OR
// itemid = 2001 OR
// itemid = 2019 OR
// itemid = 1087;

func Query2() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Define Source for BidEvent
	src := source.NewNexmarkBidSource(
		"source",
		cfg,
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
	filter := Query2Filter()
	filter.SetParallelism(1)
	dataflow.AddOperator(df, filter)

	// Define Sink (no-op)
	sink := dataflow.NewSink("sink", func(in *models.BidEvent) {})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Filter
	dataflow.Add1To1Stream(df, src, filter)
	// Connect Filter -> Sink
	dataflow.Add1To1Stream(df, filter, sink)

	return df
}
