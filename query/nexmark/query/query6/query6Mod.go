package query6

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

// Query 6: Average Selling Price by Seller
// Select the average selling price over the last 10 closed auctions by the
// same seller. This is a simplified version of Query 6 that does not require
// calculating closed_auction by joining auction and bid streams - assume we
// start from a closed_auction source directly

// Stream SQL:
// SELECT AVG(CA.price), CA.sellerId
// FROM closed auction CA
// [PARTITION BY CA.sellerId
// ROWS 10 PRECEDING];

func Query6Mod() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Define Source for ClosedAuctionEvent
	src := source.NewNexmarkClosedAuctionSource(
		"closedAuctionSource",
		cfg,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	mapper := Query6StatefulMapper(0)
	mapper.SetParallelism(1)
	dataflow.AddOperator(df, mapper)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, float64]) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, mapper)
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
