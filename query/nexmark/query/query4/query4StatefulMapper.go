package query4

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

func Query4StatefulMapper() *dataflow.StatefulMapper[
	*models.ClosedAuctionEvent,
	*tuple.Tuple2[int64, float64],
	int64,
	*stateType.ValueState[*tuple.Tuple2[int64, int64]],
] {
	// type ClosedAuctionEvent = tuple.Tuple5[
	// 	int64, // V1: id
	// 	int64, // V2: sellerId
	// 	int64, // V3: finalPrice
	// 	int64, // V4: category
	// 	int64, // V5: dateTime (unix nanoseconds)
	// ]

	// Keyby source stream by categoryID (V4)
	keyAssigner := ka.NewKeyAssigner(
		func(t *models.ClosedAuctionEvent) int64 {
			return t.V4
		},
	)
	statefulMapper := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *models.ClosedAuctionEvent,
			// State:
			// V1: number of closed_auction for this category
			// V2: total closing price for this category
			state *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *tuple.Tuple2[int64, float64] {

			currentState, exist := state.Get()
			if !exist {
				currentState = tuple.NewTuple2(int64(0), int64(0))
			}

			// Increment the counter (state.V1) and total price (state.V2)
			currentState.V1++
			currentState.V2 += in.V3
			state.Set(currentState)

			// Output:
			// V1: categoryID
			// V2: Average closing price for that category so far
			return &tuple.Tuple2[int64, float64]{
				V1: in.V4,
				V2: float64(currentState.V2) / float64(currentState.V1),
			}
		},
	)
	return statefulMapper
}
