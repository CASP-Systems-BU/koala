package twitch

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	dummyfieldgenerator "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/dummyFieldGenerator"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/models"
)

type TwitchConfig struct {
	// We use the number of producers to be the sourceParallelism to gurantee
	// that each source task will only read from a single
	ProducerIPs                        []string
	BasicEventCounterParallelism       int
	ViewerRatioCalculatorParallelism   int
	EngagementScorerParallelism        int
	RetentionAnalyzerParallelism       int
	ViewerLoyaltyCalculatorParallelism int
	LoyaltyTrendAnalyzerParallelism    int
	MetricsAggregatorParallelism       int
	SinkParallelism                    int
	KafkaClusterIPs                    []string
	WatermarkInterval                  utils.Duration
	MaxAllowedDelay                    utils.Duration
	DummyFieldSize                     int
}

// Override the UnmarshalJSON function to set default values
func (cfg *TwitchConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = TwitchConfig{
		ProducerIPs:                        []string{},
		BasicEventCounterParallelism:       1,
		ViewerRatioCalculatorParallelism:   1,
		EngagementScorerParallelism:        1,
		RetentionAnalyzerParallelism:       1,
		ViewerLoyaltyCalculatorParallelism: 1,
		LoyaltyTrendAnalyzerParallelism:    1,
		MetricsAggregatorParallelism:       1,
		SinkParallelism:                    1,
		KafkaClusterIPs:                    []string{"localhost"},
		WatermarkInterval: utils.Duration(
			100 * time.Millisecond,
		),
		MaxAllowedDelay: utils.Duration(0),
		DummyFieldSize:  0,
	}

	// Use alias to avoid infinite recursion
	type alias TwitchConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

func TwitchPipeline(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config TwitchConfig
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatal(err)
	}
	// Make source parallelism the same as number of producers(partitions)
	sourceParallelism := len(config.ProducerIPs)
	src := dataflow.NewKafkaSource[*models.TwitchEvent](
		"twitchFileSource",
		"twitch",
		kafkaConfig,
	)
	src.SetParallelism(sourceParallelism)

	// type TwitchEvent = tuple.Tuple4[
	// 	 string, //userId
	// 	 string, //streamerName
	//	 int64,  //eventTime
	//	 string, //eventType
	//]
	dataflow.AddOperator(df, src)

	// Combine origin opeartors into a single stateful flat mapper
	// Keyby streamerID + streamerName
	keyAssignerForRetentionAnalyzer := ka.NewKeyAssigner(
		func(t *models.TwitchEvent) string {
			return t.V2 + t.V3
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
	retentionAnalyzer.SetParallelism(config.RetentionAnalyzerParallelism)
	dataflow.AddOperator(df, retentionAnalyzer)

	dummyFieldGenerator := dummyfieldgenerator.NewDummyFieldGenerator(config.DummyFieldSize)
	dummyFieldContent := ""
	// Keyby streamerID to shuffle input
	keyAssignerForViewerLoyaltyCalculator := ka.NewKeyAssigner(
		func(t *tuple.Tuple10[string, float64, int64, string, int, string, float64, string, float64, string]) string {
			return t.V10
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
	viewerLoyaltyCalculator.SetParallelism(
		config.ViewerLoyaltyCalculatorParallelism,
	)
	dataflow.AddOperator(df, viewerLoyaltyCalculator)

	// Keyby streamerName
	keyAssignerForLoyaltyTrendAnalyzer := ka.NewKeyAssigner(
		func(t *tuple.Tuple5[string, float64, int64, int, string]) string {
			return t.V1
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
	loyaltyTrendAnalyzer.SetParallelism(config.LoyaltyTrendAnalyzerParallelism)
	dataflow.AddOperator(df, loyaltyTrendAnalyzer)

	// Do-nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple4[string, float64, float64, int]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	// Connect Source -> retentionAnalyzer
	dataflow.Add1To1Stream(df, src, retentionAnalyzer)
	// Connect retentionAnalyzer -> viewerLoyaltyCalculator
	dataflow.Add1To1Stream(df, retentionAnalyzer, viewerLoyaltyCalculator)
	// Connect viewerLoyaltyCalculator ->
	dataflow.Add1To1Stream(df, viewerLoyaltyCalculator, loyaltyTrendAnalyzer)
	dataflow.Add1To1Stream(df, loyaltyTrendAnalyzer, sink)

	return df
}
