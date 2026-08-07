package query4

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 4: Average Price for a Category
// Select the average of the wining bid prices for all closed auctions in each
// category. This is a simplified version of Query 4 that does not require
// calculating closed_auction by joining auction and bid streams - assume we
// start from a closed_auction source directly

// Stream SQL:
// SELECT C.id, AVG(CA.price)
// FROM category C, item I, closed auction CA
// WHERE C.id = I.categoryId
// AND I.id = CA.itemid
// GROUP BY C.id;

func Query4Mod() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Define Source for AuctionEvent
	src := source.NewNexmarkClosedAuctionSource(
		"closedAuctionSource",
		cfg,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	mapper := Query4StatefulMapper()
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
