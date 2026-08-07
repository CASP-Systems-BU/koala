package query8

import (
	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

func Query8CustomWindowJoin(
	personSource dataflow.OperatorWith1OutputStream[*models.PersonEvent],
	auctionSource dataflow.OperatorWith1OutputStream[*models.AuctionEvent],
	newUserPeriod int64,
) *dataflow.CustomWindowJoin[
	*tuple.Tuple2[int64, string],
	*models.PersonEvent,
	*models.AuctionEvent,
	int64,
	*stateType.ValueState[*tuple.Tuple2[int64, string]],
	*stateType.ListState[*models.AuctionEvent],
] {
	// For PersonEvent, keyby person id
	personKeyAssigner := ka.NewKeyAssigner(
		func(t *models.PersonEvent) int64 {
			return t.V1
		},
	)
	// For AuctionEvent, keyby sellerId
	auctionKeyAssigner := ka.NewKeyAssigner(
		func(t *models.AuctionEvent) int64 {
			return t.V8
		},
	)

	windowJoin := dataflow.NewCustomWindowJoin[*tuple.Tuple2[int64, string]](
		"windowJoin",
		personSource,
		personKeyAssigner,
		// Processing personEvent, we simply store its id and name for this
		// person
		func(
			in *models.PersonEvent,
			// Store personId, name for each personEvent
			// V1: personId
			// V2: personName
			state1 *stateType.ValueState[*tuple.Tuple2[int64, string]],
			// State2: Store a list of auctionEvent
			state2 *stateType.ListState[*models.AuctionEvent],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {
			// Set the duration of the timer
			firing_timestamp := in.V7 + newUserPeriod
			// Register the timer
			timerService.RegisterTimer(firing_timestamp)
			state1.Set(&tuple.Tuple2[int64, string]{
				V1: in.V1,
				V2: in.V2,
			})
		},
		auctionSource,
		auctionKeyAssigner,
		func(
			in *models.AuctionEvent,
			// Store personId, name for each personEvent
			// V1: personId
			// V2: personName
			state1 *stateType.ValueState[*tuple.Tuple2[int64, string]],
			// State2: Store whether this personId have related auction
			state2 *stateType.ListState[*models.AuctionEvent],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {
			state2.Add(in)
		},
		/****************************** OnTimer ******************************/
		func(
			timestamp int64,
			state1 *stateType.ValueState[*tuple.Tuple2[int64, string]],
			state2 *stateType.ListState[*models.AuctionEvent],
			co collector.Collector,
		) {
			// Get auctionState, in this query we only care whether there's the
			// state
			AuctionState := state2.Get()
			// Check if the personId has related AuctionEvent
			if len(AuctionState) > 0 {

				// Get personState - must exist since we only set timer when
				// person event arrives
				personState, _ := state1.Get()

				// Check if the person has created an auction within the
				// setting period of time
				hasAuctionInPeriod := false
				for _, auction := range AuctionState {
					if auction.V6 <= timestamp &&
						auction.V6 >= timestamp-newUserPeriod {
						hasAuctionInPeriod = true
						break
					}
				}

				// The person did put something up for sale within twelve
				// hours of registering
				if hasAuctionInPeriod {
					// Output:
					// V1: personId
					// V2: personName
					co.Emit(&tuple.Tuple2[int64, string]{
						V1: personState.V1,
						V2: personState.V2,
					})
				}
			}
			// Clear states
			state1.Clear()
			state2.Clear()
		},
	)

	return windowJoin
}
