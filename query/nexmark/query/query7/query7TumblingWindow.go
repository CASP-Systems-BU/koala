package query7

import (
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

func Query7TumblingWindow(
	duration time.Duration,
) *dataflow.TumblingWindow[
	*models.BidEvent,
	*tuple.Tuple3[int64, int64, int64],
	int64,
	*stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
] {
	// Keyby source stream by bidderID (V2)
	keyAssigner := ka.NewKeyAssigner(
		func(t *models.BidEvent) int64 {
			return t.V2
		},
	)

	// Only return the highest bid per bidder
	windowAggregator := dataflow.NewAggregator(
		// Newfunc, creates a state to store bid price, auctionId and bidderId
		// for that bidderID
		func() *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			return stateType.NewValueState(
				tuple.NewTuple3(int64(0), int64(0), int64(0)),
			)
		},

		// Add func, update the bidInfo with the highest bid price for each
		// bidder
		func(
			acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			t *models.BidEvent,
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			// val
			// V1: highest bid so far
			// V2: auctionId of the bid with highest bid price so far
			// V3: bidderId
			val, _ := acc.Get()
			if val.V1 < t.V3 {
				val.V1 = t.V3
				val.V2 = t.V1
				val.V3 = t.V2
			}
			acc.Set(val)
			return acc
		},

		// Output:
		// V1: highest bid price
		// V2: auctionId of that bid
		// V3: bidderId
		func(acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]]) *tuple.Tuple3[int64, int64, int64] {
			val, _ := acc.Get()
			return val
		},

		// Mergefunc, choose the state with higher bid price
		func(
			acc1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			if val1.V1 >= val2.V1 {
				return acc1
			}
			return acc2
		},
	)

	tumblingWindow := dataflow.NewTumblingWindow(
		"tumblingWindow",
		keyAssigner,
		windowAggregator,
		duration,
	)

	return tumblingWindow
}
