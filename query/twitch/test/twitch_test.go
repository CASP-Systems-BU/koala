package test

import (
	dummyfieldgenerator "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/dummyFieldGenerator"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/models"

	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF TWITCH LOGIC CHANGES
// In this test, we use a fixed input set to test the correctness of twitch, by
// comparing output of the query with expected results

// type TwitchEvent = tuple.Tuple4[
// string, //userId
// string, //streamerName
// int64,  //eventTime
// string, //eventType
// ]

var SampleInput []models.TwitchEvent = []models.TwitchEvent{
	{V1: "user1", V2: "1", V3: "streamerA", V4: 1000, V5: "ENTER"},
	{V1: "user1", V2: "1", V3: "streamerA", V4: 2000, V5: "LEAVE"},
	{V1: "user2", V2: "1", V3: "streamerA", V4: 3000, V5: "ENTER"},
	{V1: "user2", V2: "1", V3: "streamerA", V4: 4000, V5: "LEAVE"},
	{V1: "user3", V2: "3", V3: "streamerB", V4: 5000, V5: "ENTER"},
	{V1: "user3", V2: "3", V3: "streamerB", V4: 6000, V5: "LEAVE"},
	{V1: "user4", V2: "3", V3: "streamerB", V4: 7000, V5: "ENTER"},
	{V1: "user4", V2: "3", V3: "streamerB", V4: 8000, V5: "LEAVE"},
	{V1: "user5", V2: "5", V3: "streamerC", V4: 9000, V5: "ENTER"},
	{V1: "user5", V2: "5", V3: "streamerC", V4: 10000, V5: "LEAVE"},
}

var SampleResult []tuple.Tuple4[string, float64, float64, int] = []tuple.Tuple4[string, float64, float64, int]{
	{V1: "streamerA", V2: 5.003, V3: 1.5009, V4: 1},
	{V1: "streamerA", V2: 8.39338888888889, V3: 2.2928816666666663, V4: 2},
	{V1: "streamerA", V2: 12.228222222222222, V3: 3.059800333333333, V4: 3},
	{V1: "streamerB", V2: 5.003, V3: 1.5009, V4: 1},
	{V1: "streamerB", V2: 8.39338888888889, V3: 2.2928816666666663, V4: 2},
	{V1: "streamerB", V2: 12.228222222222222, V3: 3.059800333333333, V4: 3},
	{V1: "streamerC", V2: 5.003, V3: 1.5009, V4: 1},
}

var results []tuple.Tuple4[string, float64, float64, int]

func TestTwitchCorrectness(t *testing.T) {
	results = make([]tuple.Tuple4[string, float64, float64, int], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************
	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 5
	_, workers, _ := testutils.DeployJob(numWorkers, TwitchPipeline, config)
	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	// Wait till we receive the ending watermark
	expectedWM := int64(10000)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	if len(results) != 7 {
		t.Errorf("Incorrect amount of results=%d, expect=7", len(results))
	}

	// Check if the result exists
	for i, r := range results {
		if SampleResult[i].V1 != r.V1 || SampleResult[i].V2 != r.V2 ||
			SampleResult[i].V3 != r.V3 ||
			SampleResult[i].V4 != r.V4 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				SampleResult[i],
				r,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func TwitchPipeline() *dataflow.Dataflow {
	df := dataflow.NewDataflow()
	src := dataflow.NewSource[*models.TwitchEvent](
		"source",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(20 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	src.SetParallelism(1)
	// type TwitchEvent = tuple.Tuple4[
	// 	 string, //userId
	// 	 string, //streamerName
	//	 int64,  //eventTime
	//	 string, //eventType
	//]
	// Assign eventTime as timestamp
	twitchTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.TwitchEvent) int64 {
			return t.V4
		},
	)
	src.AssignTimestampAndWatermark(
		twitchTimestampAssigner,
		200*time.Millisecond,
		0,
	)
	dataflow.AddOperator(df, src)

	// Combine origin opeartors into a single stateful flat mapper
	// Keyby streamerID
	keyAssignerForRetentionAnalyzer := ka.NewKeyAssigner(
		func(t *models.TwitchEvent) string {
			return t.V2
		},
	)

	retentionAnalyzer := dataflow.NewStatefulFlatmap[
		*models.TwitchEvent,
		*tuple.Tuple10[string, float64, int64, string, int, string, float64, string, float64, string],
	](
		"retention-Analyzer",
		keyAssignerForRetentionAnalyzer,
		func(in *models.TwitchEvent,
			// V1 -V2:stores viewerCount and totalEvent for a streamer
			// V1: viewerCount
			// V2: totalEvent
			// V3-V4:stores avgRatio and updateCount for a streamer
			// V3: avgRatio
			// V4: updateCount
			// V5-V6:stores currentTime and retentionScore
			// V5: currentTime
			// V6: retentionScore
			state *stateType.ValueState[*tuple.Tuple6[int, int, float64, int, int64, float64]],
			co collector.Collector,
		) {
			// Map twitch event to tuple2
			// Result: streamerName and count(enter(1) or leave(-1))
			// V1: streamerName
			// V2: count
			var count int
			if in.V5 == "ENTER" {
				count = 1
			} else {
				count = -1
			}
			baseEvent := tuple.Tuple2[string, int]{
				V1: in.V3,
				V2: count,
			}

			// Calculate viewer ratio for each streamer
			// Result : streamerName and viewerRatio
			// V1: streamerName
			// V2: viewerRatio
			currentState, exist := state.Get()
			if !exist {
				currentState = tuple.NewTuple6(0, 0, 0.0, 0, int64(-1), 0.0)
			}
			currentState.V1 += baseEvent.V2
			currentState.V2++
			ratio := float64(currentState.V1) / float64(currentState.V2)
			viewerRatio := tuple.Tuple2[string, float64]{
				V1: baseEvent.V1,
				V2: ratio,
			}

			// Calculate Score engagement based on viewer ratio
			// Result: streamerName and avgRatio and count
			// V1: streamerName
			// V2: avgRatio
			currentAvg := currentState.V3
			engagementScorerCount := currentState.V4
			newAvg := (currentAvg*float64(engagementScorerCount) + viewerRatio.V2) / (float64(engagementScorerCount + 1))
			currentState.V3 = newAvg
			currentState.V4++
			engagementScorer := tuple.Tuple2[string, float64]{
				V1: viewerRatio.V1,
				V2: newAvg,
			}

			currentTime := in.V4
			previousTime := currentState.V5
			if previousTime != int64(-1) {
				timeDiff := currentTime - previousTime
				newScore := currentState.V6 + engagementScorer.V2*float64(
					timeDiff,
				)
				currentState.V6 = newScore
				out := &tuple.Tuple10[string, float64, int64, string, int, string, float64, string, float64, string]{
					V1:  engagementScorer.V1,
					V2:  newScore,
					V3:  timeDiff,
					V4:  in.V2,
					V5:  count,
					V6:  baseEvent.V1,
					V7:  ratio,
					V8:  viewerRatio.V1,
					V9:  newAvg,
					V10: in.V2,
				}
				co.Emit(out)
			}
			currentState.V5 = currentTime

			state.Set(currentState)

		},
	)
	retentionAnalyzer.SetParallelism(1)
	dataflow.AddOperator(df, retentionAnalyzer)

	dummyFieldGenerator := dummyfieldgenerator.NewDummyFieldGenerator(1)
	dummyFieldContent := ""
	// Keyby streamerName to shuffle input
	keyAssignerForViewerLoyaltyCalculator := ka.NewKeyAssigner(
		func(t *tuple.Tuple10[string, float64, int64, string, int, string, float64, string, float64, string]) string {
			return t.V1
		},
	)
	// Calculate loyaltyScore, totalWatchTime and eventCount for a streamer
	// Output: streamerName, avgLoyalty, newTrend and count
	// V1: streamerName
	// V2: avgLoyalty
	// V3: newTrend
	// V4: count
	viewerLoyaltyCalculator := dataflow.NewStatefulMapper2(
		"viewer-Loyalty-Calculator",
		keyAssignerForViewerLoyaltyCalculator,
		func(
			in *tuple.Tuple10[string, float64, int64, string, int, string, float64, string, float64, string],
			// stores engagementScore, totalWatchTime, eventCount
			// V1: engagementScore
			// V2: totalWatchTime
			// V3: eventCount
			state *stateType.ValueState[*tuple.Tuple3[float64, int64, int]],
			dummyState *stateType.ValueState[*tuple.Tuple1[string]],
		) *tuple.Tuple5[string, float64, int64, int, string] {
			currentState, exist := state.Get()
			if !exist {
				//log.Fatalln("New key", in.V1)
				currentState = tuple.NewTuple3(0.0, int64(0), 0)
				dummyFieldContent = dummyFieldGenerator.GenerateDummyField()
				dummyState.Set(tuple.NewTuple1(dummyFieldContent))
			}
			currentState.V1 += in.V2
			currentState.V2 += in.V3
			currentState.V3++
			state.Set(currentState)
			loyaltyScore := (currentState.V1*0.4 +
				float64(currentState.V2)*0.3 +
				float64(currentState.V3)*0.3) / 100.0
			return &tuple.Tuple5[string, float64, int64, int, string]{
				V1: in.V1,
				V2: loyaltyScore,
				V3: currentState.V2,
				V4: currentState.V3,
				V5: in.V10,
			}
		},
	)
	viewerLoyaltyCalculator.SetParallelism(1)
	dataflow.AddOperator(df, viewerLoyaltyCalculator)

	// Keyby streamerID
	keyAssignerForLoyaltyTrendAnalyzer := ka.NewKeyAssigner(
		func(t *tuple.Tuple5[string, float64, int64, int, string]) string {
			return t.V5
		},
	)
	// Analyze loyalty trends and aggregate Metric
	// Output: streamerName, avgLoyalty, newTrend and count
	// V1: streamerName
	// V2: avgLoyalty
	// V3: newTrend
	// V4: count

	loyaltyTrendAnalyzer := dataflow.NewStatefulMapper(
		"loyalty-Trend-Analyzer",
		keyAssignerForLoyaltyTrendAnalyzer,
		func(
			in *tuple.Tuple5[string, float64, int64, int, string],
			// stores prevLoyalty and loyaltyTrend
			// V1: preLoyalty
			// V2: loyaltyTrend
			// V3: avgLoyalty
			// V4: trendSum
			// V5: updateCount
			state *stateType.ValueState[*tuple.Tuple5[float64, float64, float64, float64, int]],
		) *tuple.Tuple4[string, float64, float64, int] {
			currentState, exist := state.Get()
			if !exist {
				currentState = tuple.NewTuple5(0.0, 0.0, 0.0, 0.0, 0)
			}
			trend := in.V2 - currentState.V1
			smoothedTrend := currentState.V2*0.7 + trend*0.3
			currentState.V1 = in.V2
			currentState.V2 = smoothedTrend
			state.Set(currentState)

			currentAvg := currentState.V3
			currentTrendSum := currentState.V4
			count := currentState.V5
			newAvg := (currentAvg*float64(count) + in.V2) / float64(count+1)
			newTrendSum := currentTrendSum + smoothedTrend
			currentState.V3 = newAvg
			currentState.V4 = newTrendSum
			currentState.V5++
			state.Set(currentState)
			return &tuple.Tuple4[string, float64, float64, int]{
				V1: in.V1,
				V2: newAvg,
				V3: newTrendSum / float64(count+1),
				V4: count + 1,
			}
		},
	)
	loyaltyTrendAnalyzer.SetParallelism(1)
	dataflow.AddOperator(df, loyaltyTrendAnalyzer)

	// Do-nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple4[string, float64, float64, int]) {
			results = append(results, *t)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> retentionAnalyzer
	dataflow.Add1To1Stream(df, src, retentionAnalyzer)
	// Connect retentionAnalyzer -> viewerLoyaltyCalculator
	dataflow.Add1To1Stream(df, retentionAnalyzer, viewerLoyaltyCalculator)
	// Connect viewerLoyaltyCalculator -> sink
	dataflow.Add1To1Stream(df, viewerLoyaltyCalculator, loyaltyTrendAnalyzer)
	dataflow.Add1To1Stream(df, loyaltyTrendAnalyzer, sink)

	return df
}
