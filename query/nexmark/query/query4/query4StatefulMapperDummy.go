package query4

import (
	"fmt"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	dummyfieldgenerator "github.com/CASP-Systems-BU/koala/query/nexmark/query/dummyFieldGenerator"
)

func Query4StatefulMapperDummy(dummyFieldSize int) *dataflow.StatefulMapper[
	*models.ClosedAuctionEvent,
	*tuple.Tuple2[int64, float64],
	int64,
	*stateType.ValueState[*tuple.Tuple3[int64, int64, string]],
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

	dummyFieldGenerator := dummyfieldgenerator.NewDummyFieldGenerator(dummyFieldSize)
	dummyFieldContent := ""

	statefulMapper := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *models.ClosedAuctionEvent,
			// State:
			// V1: number of closed_auction for this category
			// V2: total closing price for this category
			state *stateType.ValueState[*tuple.Tuple3[int64, int64, string]],
		) *tuple.Tuple2[int64, float64] {

			currentState, exist := state.Get()
			if !exist {
				fmt.Println("new key", in.V4)
				dummyFieldContent = dummyFieldGenerator.GenerateDummyField()
				currentState = tuple.NewTuple3(int64(0), int64(0), dummyFieldContent)
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
