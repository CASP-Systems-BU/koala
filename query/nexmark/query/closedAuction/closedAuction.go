package closedauction

import (
	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

// Define ClosedAuction join operator: calculate closedAuction by joining
// auction and bid streams based on CustomWindowJoin operator

func ClosedAuction(
	auctionSource dataflow.OperatorWith1OutputStream[*models.AuctionEvent],
	bidSource dataflow.OperatorWith1OutputStream[*models.BidEvent],
) *dataflow.CustomWindowJoin[
	*models.ClosedAuctionEvent,
	*models.AuctionEvent,
	*models.BidEvent,
	int64,
	*stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
	*stateType.ListState[*tuple.Tuple2[int64, int64]],
] {
	// Key by V1 (auction id) for auction event
	keyAssignerAuction := ka.NewKeyAssigner(
		func(t *models.AuctionEvent) int64 {
			return t.V1
		},
	)

	// Key by V1 (auction id) for bid event
	keyAssignerBid := ka.NewKeyAssigner(
		func(t *models.BidEvent) int64 {
			return t.V1
		},
	)

	// Calculate closedAuction from auction and bid streams
	closedAuction := dataflow.NewCustomWindowJoin[*models.ClosedAuctionEvent](
		"closedAuction",
		auctionSource,
		keyAssignerAuction,
		func(
			in *models.AuctionEvent,
			// Store auctionId, sellerId, and category for each auctionEvent
			// V1: auctionId
			// V2: sellerId
			// V3: category
			state1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			// Store price and dateTime for each bidEvent
			// V1: price
			// V2: dateTime
			state2 *stateType.ListState[*tuple.Tuple2[int64, int64]],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {

			// Register a timer to be triggered at the auction's expire time
			timerService.RegisterTimer(in.V7)
			state1.Set(&tuple.Tuple3[int64, int64, int64]{
				V1: in.V1,
				V2: in.V8,
				V3: in.V9,
			})
		},
		bidSource,
		keyAssignerBid,
		func(
			in *models.BidEvent,
			// Store auctionId, sellerId and category for eachauctionEvent
			// V1: auctionId
			// V2: sellerId
			// V3: category
			state1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			// Store price and dateTime for each bidEvent
			// V1: price
			// V2: dateTime
			state2 *stateType.ListState[*tuple.Tuple2[int64, int64]],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {

			// Append the bid info to list of bids for this auction
			state2.Add(&tuple.Tuple2[int64, int64]{
				V1: in.V3,
				V2: in.V4,
			})
		},
		/****************************** OnTimer ******************************/
		func(
			timestamp int64,
			state1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			state2 *stateType.ListState[*tuple.Tuple2[int64, int64]],
			co collector.Collector,
		) {
			bidState := state2.Get()
			winningPrice := int64(-1)

			// Traverse all recorded bids for this auction and find the highest
			// bid that was placed before the auction's expiration time
			for _, bid := range bidState {
				if bid.V2 <= timestamp && bid.V1 > winningPrice {
					winningPrice = bid.V1
				}
			}

			// If we do have a winning bid (at least one bid was placed), emit
			// the closed auction event with the winning price
			if winningPrice > -1 {
				auctionState, _ := state1.Get()
				// type ClosedAuction = tuple.Tuple5[
				// 	int64, // V1: id
				// 	int64, // V2: sellerId
				// 	int64, // V3: finalPrice
				// 	int64, // V4: category
				// 	int64, // V5: dateTime (unix nanoseconds)
				// ]
				co.Emit(&models.ClosedAuctionEvent{
					V1: auctionState.V1,
					V2: auctionState.V2,
					V3: winningPrice,
					V4: auctionState.V3,
					V5: timestamp,
				})
			}
			state1.Clear()
			state2.Clear()
		},
	)
	return closedAuction
}
