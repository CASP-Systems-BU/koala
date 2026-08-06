package query3

import (
	"log"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	dummyfieldgenerator "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/dummyFieldGenerator"
)

func Query3Join(
	personSource dataflow.OperatorWith1OutputStream[*models.PersonEvent],
	auctionSource dataflow.OperatorWith1OutputStream[*models.AuctionEvent],
	useDummyField bool,
	dummyFieldSize int,
) *dataflow.Join[
	*tuple.Tuple4[string, string, string, int64],
	*models.PersonEvent,
	*models.AuctionEvent,
	int64,
	*stateType.ValueState[*tuple.Tuple4[string, string, string, string]],
	*stateType.ListState[*tuple.Tuple1[int64]],
] {
	// Define keyAssigners for both upstreams
	// For PersonEvent, keyby personId
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

	dummyFieldGenerator := dummyfieldgenerator.NewDummyFieldGenerator(dummyFieldSize)
	dummyFieldContent := ""

	join := dataflow.NewJoin[*tuple.Tuple4[string, string, string, int64]](
		"join",
		personSource,
		personKeyAssigner,
		// Processing PersonEvent
		func(
			in *models.PersonEvent,
			// In state1, we store personName, city and state info
			state1 *stateType.ValueState[*tuple.Tuple4[string, string, string, string]],
			// In state2, we only store the auctionId
			state2 *stateType.ListState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) time.Duration {
			generationTime := time.Duration(0)
			if useDummyField {
				startGeneration := time.Now()
				dummyFieldContent = dummyFieldGenerator.GenerateDummyField()
				generationTime = time.Since(startGeneration)
				state1.Set(&tuple.Tuple4[string, string, string, string]{
					V1: in.V2,
					V2: in.V5,
					V3: in.V6,
					V4: dummyFieldContent,
				})
			} else {
				state1.Set(&tuple.Tuple4[string, string, string, string]{
					V1: in.V2,
					V2: in.V5,
					V3: in.V6,
				})
			}

			auctionState := state2.Get()
			for i := range auctionState {
				// Output:
				// V1: person name
				// V2: person city
				// V3: person state
				// V4: auctionId
				co.Emit(&tuple.Tuple4[string, string, string, int64]{
					V1: in.V2,
					V2: in.V5,
					V3: in.V6,
					V4: auctionState[i].V1,
				})
			}
			state2.Clear()
			return generationTime
		},
		auctionSource,
		auctionKeyAssigner,
		// Processing AuctionEvent
		func(
			in *models.AuctionEvent,
			// In state1, we store personName, city and state info
			state1 *stateType.ValueState[*tuple.Tuple4[string, string, string, string]],
			// In state2, we only store the auctionId
			state2 *stateType.ListState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) time.Duration {
			personState, hasPersonState := state1.Get()
			if hasPersonState {
				// Output:
				// V1: person name
				// V2: person city
				// V3: person state
				// V4: auctionId
				co.Emit(&tuple.Tuple4[string, string, string, int64]{
					V1: personState.V1,
					V2: personState.V2,
					V3: personState.V3,
					V4: in.V1,
				})
			} else {
				log.Fatal("AuctionEvent arrives before the corresponding PersonEvent, which should not happen in Nexmark!")
				state2.Add(&tuple.Tuple1[int64]{
					V1: in.V1,
				})
			}
			return 0
		},
	)

	return join
}
