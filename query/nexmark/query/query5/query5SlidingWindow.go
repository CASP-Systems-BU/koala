package query5

import (
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

func Query5SlidingWindow(
	duration time.Duration,
	slide time.Duration,
) *dataflow.SlidingWindow[
	*models.BidEvent,
	*tuple.Tuple2[int64, int64],
	int64,
	*stateType.ValueState[*tuple.Tuple2[int64, int64]],
] {
	// Keyby source stream by auctionID (V1)
	keyAssigner := ka.NewKeyAssigner(func(t *models.BidEvent) int64 {
		return t.V1
	})

	// Aggregator to calculate bids number for each auctionId
	agg := dataflow.NewAggregator(
		// Newfunc, creates a state to store AuctionId and count for that
		// auctionId
		func() *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			return stateType.NewValueState(tuple.NewTuple2(
				int64(0), // auctionId
				int64(0), // count
			))
		},

		// Addfunc, every time we receive an event with a specific auctionId,
		// add its related count by 1
		func(
			acc *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			t *models.BidEvent,
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {

			// val
			// V1: auctionId
			// V2: total bid count for that auctionId
			val, _ := acc.Get()
			val.V1 = t.V1
			val.V2 += 1
			acc.Set(val)
			return acc
		},

		// Output:
		// V1: auctionId
		// V2: average price of last 10 closed_auctions for that seller
		func(acc *stateType.ValueState[*tuple.Tuple2[int64, int64]]) *tuple.Tuple2[int64, int64] {

			val, _ := acc.Get()
			return val
		},

		// Mergefunc, add two count for the same auctionId
		func(
			acc1 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {

			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			val1.V1 = val2.V1 // optional since val1.V1 and val2.V1 are already the same
			val1.V2 += val2.V2
			acc1.Set(val1)
			return acc1
		},
	)

	window := dataflow.NewSlidingWindow(
		"slidingWindow",
		keyAssigner,
		agg,
		duration,
		slide,
	)

	return window
}
