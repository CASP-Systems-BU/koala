package query7

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

type Q7ModConfig struct {
	TumblingWindowParallelism int
	SinkParallelism           int
	ProducerIPs               []string
	KafkaClusterIPs           []string
	WatermarkInterval         utils.Duration
	MaxAllowedDelay           utils.Duration
	WindowDuration            utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q7ModConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q7ModConfig{
		TumblingWindowParallelism: 1,
		SinkParallelism:           1,
		ProducerIPs:               []string{"localhost"},
		KafkaClusterIPs:           []string{"localhost"},
		WatermarkInterval:         utils.Duration(200 * time.Millisecond),
		MaxAllowedDelay:           utils.Duration(0),
		WindowDuration:            utils.Duration(100 * time.Millisecond),
	}

	// Use alias to avoid infinite recursion
	type alias Q7ModConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 7: Highest Bid
// Select the bids with the highest bid price in the last configured period, we
// only calculate each bidder's highest bid price to avoid global aggregation.

// Stream SQL:
// SELECT bid.auctoin, bid.price, bid.bidder
// FROM bid where bid.price =
// (SELECT MAX(bid.price)
// FROM bid [FIXEDRANGE
// 10 MINUTES PRECEDING]);

func Query7ModKafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	var config Q7ModConfig
	// Reading config info from json file
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}
	watermarkInterval := time.Duration(config.WatermarkInterval)
	maxAllowedDelay := time.Duration(config.MaxAllowedDelay)
	duration := time.Duration(config.WindowDuration)
	// Replace default bootstrap servers
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatal(err)
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

	tumblingWindow := Query7TumblingWindow(duration)
	tumblingWindow.SetParallelism(config.TumblingWindowParallelism)
	dataflow.AddOperator(df, tumblingWindow)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int64, int64, int64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, sink)

	return df
}
