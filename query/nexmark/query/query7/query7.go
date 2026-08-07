package query7

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 7: Highest Bid
// Select the bids with the highest bid price in the last configured period, we
// only calculate each bidder's highest bid price to avoid global aggregation.

// Stream SQL:
// SELECT bid.auctoin, bid.price, bid.bidder
// FROM bid where bid.price =
// (SELECT MAX(bid.price)
// FROM bid [FIXEDRANGE
// 10 MINUTES PRECEDING]);

func Query7() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Define Source for BidEvent
	src := source.NewNexmarkBidSource(
		"source",
		cfg,
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
		time.Millisecond,
		0,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	tumblingWindow := Query7TumblingWindow(50 * time.Millisecond)
	tumblingWindow.SetParallelism(1)
	dataflow.AddOperator(df, tumblingWindow)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int64, int64, int64]) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, sink)

	return df
}
