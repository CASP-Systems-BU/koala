package query4

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	closedauction "github.com/CASP-Systems-BU/koala/query/nexmark/query/closedAuction"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// Query 4: Average Price for a Category
// Select the average of the wining bid prices for all closed auctions in each
// category

// Stream SQL:
// SELECT C.id, AVG(CA.price)
// FROM category C, item I, closed auction CA
// WHERE C.id = I.categoryId
// AND I.id = CA.itemid
// GROUP BY C.id;

func Query4() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	auctionSourceConfig := config.DefaultNexmarkSourceConfig()

	// Define Source for AuctionEvent
	auctionSource := source.NewNexmarkAuctionSource(
		"auctionSource",
		auctionSourceConfig,
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

	bidSourceConfig := config.DefaultNexmarkSourceConfig()

	// Define Source for BidEvent
	bidSource := source.NewNexmarkBidSource(
		"bidSource",
		bidSourceConfig,
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
		200*time.Millisecond,
		0,
	)
	dataflow.AddOperator(df, bidSource)

	// Calculate closedAuction based on auction and bid streams
	closedAuction := closedauction.ClosedAuction(auctionSource, bidSource)
	closedAuction.SetParallelism(1)
	dataflow.AddOperator(df, closedAuction)

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

	dataflow.Add2To1Stream(
		df,
		auctionSource,
		bidSource,
		closedAuction,
	)
	dataflow.Add1To1Stream(df, closedAuction, mapper)
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
