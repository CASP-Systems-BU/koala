package query5

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

type Q5ModConfig struct {
	SlidingWindowParallelism int
	SinkParallelism          int
	ProducerIPs              []string
	KafkaClusterIPs          []string
	WatermarkInterval        utils.Duration
	MaxAllowedDelay          utils.Duration
	WindowDuration           utils.Duration
	WindowSlide              utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q5ModConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q5ModConfig{
		SlidingWindowParallelism: 1,
		SinkParallelism:          1,
		ProducerIPs:              []string{"localhost"},
		KafkaClusterIPs:          []string{"localhost:"},
		WatermarkInterval:        utils.Duration(100 * time.Millisecond),
		MaxAllowedDelay:          utils.Duration(0),
		WindowDuration:           utils.Duration(100 * time.Millisecond),
		WindowSlide:              utils.Duration(10 * time.Millisecond),
	}

	// Use alias to avoid infinite recursion
	type alias Q5ModConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 5: Hot Items
// Selects the item with the most bids in the past configured time period;
// the “hottest” item. The results are output every configured slide. In
// this version we only calculate each item's bid
// number to avoid global aggregate.

// Stream SQL:
// SELECT bid.itemid
// FROM bid [RANGE 60 MINUTES PRECEDING]
// WHERE (SELECT COUNT(bid.itemid)
//     FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING])
//     >= ALL (SELECT COUNT(bid.itemid)
//             FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING];

func Query5ModKafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	var config Q5ModConfig
	// Reading config info from json file
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalln("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatalln("Error during Unmarshal(): ", err)
	}
	watermarkInterval := time.Duration(config.WatermarkInterval)
	maxAllowedDelay := time.Duration(config.MaxAllowedDelay)
	duration := time.Duration(config.WindowDuration)
	slide := time.Duration(config.WindowSlide)
	// Replace default bootstrap servers
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatalln("Fail to set key in kafkaConfig", err)
	}

	// Using kafka source for BidEvent
	src := dataflow.NewKafkaSource[*models.BidEvent](
		"source",
		"nexmark-bid",
		kafkaConfig,
	)
	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]

	// Assign V4(dateTime) as timestamp for bid event
	timestampAssigner := ta.NewTimestampAssigner(
		func(t *models.BidEvent) int64 {
			return t.V4
		},
	)
	src.AssignTimestampAndWatermark(
		timestampAssigner,
		watermarkInterval,
		maxAllowedDelay,
	)
	src.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, src)

	window := Query5SlidingWindow(duration, slide)
	window.SetParallelism(config.SlidingWindowParallelism)
	dataflow.AddOperator(df, window)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, int64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, window)
	dataflow.Add1To1Stream(df, window, sink)

	return df
}
