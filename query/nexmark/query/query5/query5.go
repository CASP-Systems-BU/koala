package query5

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 5: Hot Items
// Selects the item with the most bids in the past configured time period;
// the “hottest” item. In this version we only calculate each item's bid
// number to avoid global aggregate.

// Stream SQL:
// SELECT bid.itemid
// FROM bid [RANGE 60 MINUTES PRECEDING]
// WHERE (SELECT COUNT(bid.itemid)
//     FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING])
//     >= ALL (SELECT COUNT(bid.itemid)
//             FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING];

func Query5() *dataflow.Dataflow {
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

	window := Query5SlidingWindow(100*time.Millisecond, 10*time.Millisecond)
	window.SetParallelism(1)
	dataflow.AddOperator(df, window)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, int64]) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, window)
	dataflow.Add1To1Stream(df, window, sink)

	return df
}
