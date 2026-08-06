package query8

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

// Query 8: Monitor New Users
// Finds people who put something up for sale within twelve hours of registering
// to use the auction service

// Stream SQL
// SELECT person.id, person.name
// FROM person [RANGE 12 HOURS PRECEDING]
// open auction [RANGE 12 HOURS PRECEDING]
// WHERE person.id = open auction.sellerId;

func Query8() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Time period for new user monitoring (1s in unit of ns)
	newUserPeriod := int64(1000000000)

	personSourceConfig := config.DefaultNexmarkSourceConfig()

	// Define Source for PersonEvent
	personSource := source.NewNexmarkPersonSource(
		"personSource",
		personSourceConfig,
	)

	personSource.SetParallelism(1)
	// Assign V7(dateTime) as timestamp for person event
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

	auctionSourceConfig := config.DefaultNexmarkSourceConfig()

	// Define Source for AuctionEvent
	auctionSource := source.NewNexmarkAuctionSource(
		"auctionSource",
		auctionSourceConfig,
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

	windowJoin := Query8CustomWindowJoin(
		personSource,
		auctionSource,
		newUserPeriod,
	)
	windowJoin.SetParallelism(1)
	dataflow.AddOperator(df, windowJoin)

	// Do nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, string]) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect each operator to their upstream
	dataflow.Add2To1Stream(
		df,
		personSource,
		auctionSource,
		windowJoin,
	)
	dataflow.Add1To1Stream(df, windowJoin, sink)

	return df
}
