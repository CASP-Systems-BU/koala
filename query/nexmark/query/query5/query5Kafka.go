package query5

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/nexmarkKafkaProducer"
)

type Q5Config struct {
	SlidingWindowParallelism int
	HottestItemParallelism   int
	SinkParallelism          int
	ProducerIPs              []string
	KafkaClusterIPs          []string
	WatermarkInterval        utils.Duration
	MaxAllowedDelay          utils.Duration
	WindowDuration           utils.Duration
	WindowSlide              utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q5Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q5Config{
		SlidingWindowParallelism: 1,
		HottestItemParallelism:   1,
		SinkParallelism:          1,
		ProducerIPs:              []string{"localhost"},
		KafkaClusterIPs:          []string{"localhost"},
		WatermarkInterval:        utils.Duration(100 * time.Millisecond),
		MaxAllowedDelay:          utils.Duration(0),
		WindowDuration:           utils.Duration(100 * time.Millisecond),
		WindowSlide:              utils.Duration(10 * time.Millisecond),
	}

	// Use alias to avoid infinite recursion
	type alias Q5Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 5: Hot Items
// Selects the item with the most bids in the past configured time period;
// the “hottest” item. In this version we only calculate each item's bid
// number to avoid global aggregate.

// Stream SQL:
// SELECT bid.itemid
// FROM bid [RANGE 60 MINUTES PRECEDING]
// WHERE (SELECT COUNT(bid.itemid)
//     FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING])
//     >= ALL (SELECT COUNT(bid.itemid)
//             FROM bid [PARTITION BY bid.itemid RANGE 60 MINUTES PRECEDING];

func Query5Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q5Config
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
	slide := time.Duration(config.WindowDuration)

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
	bidEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Bid]
	src := dataflow.NewKafkaSource[*models.BidEvent](
		"source",
		bidEventKafkaTopic,
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

	// Global aggregation: keyby window ending timestamp
	hottestItemKeyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.GetTimestamp()
		},
	)
	hottestItemAggregator := dataflow.NewAggregator(
		// Create a new valueState to store auctionId and count
		func() *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			return stateType.NewValueState(tuple.NewTuple2(int64(0), int64(0)))
		},

		// Compare auctionId count and update the hottest one
		func(
			// acc: Stores auctionId and count
			// V1: auctionId
			// V2: count
			acc *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			in *tuple.Tuple2[int64, int64],
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val, _ := acc.Get()
			if in.V2 >= val.V2 {
				// If count is the same, we choose the auction with larger id
				if in.V2 == val.V2 && val.V1 > in.V1 {
					return acc
				}
				acc.Set(in)
			}
			return acc
		},

		// Output the hottest auction's Id and count
		// V1: auctionId
		// V2: count
		func(
			acc *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *tuple.Tuple2[int64, int64] {
			val, _ := acc.Get()
			return val
		},

		// Merge function, return the auctionId with larger count
		func(
			acc1 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple2[int64, int64]],
		) *stateType.ValueState[*tuple.Tuple2[int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			if val2.V2 >= val1.V2 {
				// If count is the same, we choose the auction with larger id
				if val1.V2 == val2.V2 && val1.V1 > val2.V1 {
					return acc1
				}
				acc1.Set(val2)
			}
			return acc1
		},
	)
	hottestItem := dataflow.NewTumblingWindow(
		"HottestItem",
		hottestItemKeyAssigner,
		hottestItemAggregator,
		slide,
	)
	hottestItem.SetParallelism(config.HottestItemParallelism)
	dataflow.AddOperator(df, hottestItem)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, int64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, window)
	dataflow.Add1To1Stream(df, window, hottestItem)
	dataflow.Add1To1Stream(df, hottestItem, sink)

	return df
}
