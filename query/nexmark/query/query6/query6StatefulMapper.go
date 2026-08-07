package query6

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	dummyfieldgenerator "github.com/CASP-Systems-BU/koala/query/nexmark/query/dummyFieldGenerator"
)

func Query6StatefulMapper(
	dummyFieldSize int,
) *dataflow.StatefulMapper[
	*models.ClosedAuctionEvent,
	*tuple.Tuple2[int64, float64],
	int64,
	*stateType.ValueState[*tuple.Tuple12[
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		int64,
		string,
	]],
] {
	// type ClosedAuctionEvent = tuple.Tuple5[
	// 	int64, // V1: id
	// 	int64, // V2: sellerId
	// 	int64, // V3: finalPrice
	// 	int64, // V4: category
	// 	int64, // V5: dateTime (unix nanoseconds)
	// ]

	// Keyby source stream by sellerID (V2)
	keyAssigner := ka.NewKeyAssigner(
		func(t *models.ClosedAuctionEvent) int64 {
			return t.V2
		},
	)

	dummyFieldGenerator := dummyfieldgenerator.NewDummyFieldGenerator(dummyFieldSize)
	dummyFieldContent := ""

	mapper := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *models.ClosedAuctionEvent,
			// State:
			// V1: Counter of closed_auctions for this seller received so far
			// V2 - V11: value for the last 10 closed_auction (FIFO window)
			state *stateType.ValueState[*tuple.Tuple12[int64, int64, int64, int64, int64, int64, int64, int64, int64, int64, int64, string]],
		) *tuple.Tuple2[int64, float64] {

			currentState, exist := state.Get()
			// Initialize the last-10-closed-auctions if not exist
			if !exist {
				dummyFieldContent = dummyFieldGenerator.GenerateDummyField()
				currentState = tuple.NewTuple12(
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					dummyFieldContent,
				)
			}

			// Identify which of the last 10 values to overwrite
			index := currentState.V1 % 10
			switch index {
			case 0:
				currentState.V2 = in.V3
			case 1:
				currentState.V3 = in.V3
			case 2:
				currentState.V4 = in.V3
			case 3:
				currentState.V5 = in.V3
			case 4:
				currentState.V6 = in.V3
			case 5:
				currentState.V7 = in.V3
			case 6:
				currentState.V8 = in.V3
			case 7:
				currentState.V9 = in.V3
			case 8:
				currentState.V10 = in.V3
			case 9:
				currentState.V11 = in.V3
			}

			currentState.V1++
			state.Set(currentState)

			// Calculate average price so far
			var sum int64 = 0
			var count int64 = 10
			if currentState.V1 < 10 {
				count = currentState.V1
			}
			for i := int64(0); i < count; i++ {
				switch i {
				case 0:
					sum += currentState.V2
				case 1:
					sum += currentState.V3
				case 2:
					sum += currentState.V4
				case 3:
					sum += currentState.V5
				case 4:
					sum += currentState.V6
				case 5:
					sum += currentState.V7
				case 6:
					sum += currentState.V8
				case 7:
					sum += currentState.V9
				case 8:
					sum += currentState.V10
				case 9:
					sum += currentState.V11
				}
			}
			avg := float64(sum) / float64(count)

			// Output:
			// V1: sellerId
			// V2: average price of last 10 closed_auctions for that seller
			return &tuple.Tuple2[int64, float64]{
				V1: in.V2,
				V2: avg,
			}
		},
	)

	return mapper
}
