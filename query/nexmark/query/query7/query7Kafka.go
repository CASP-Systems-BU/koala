package query7

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"github.com/CASP-Systems-BU/koala/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/nexmarkKafkaProducer"
)

type Q7Config struct {
	TumblingWindowParallelism int
	HighestBidParallelism     int
	SinkParallelism           int
	ProducerIPs               []string
	KafkaClusterIPs           []string
	WatermarkInterval         utils.Duration
	MaxAllowedDelay           utils.Duration
	WindowDuration            utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q7Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q7Config{
		TumblingWindowParallelism: 1,
		HighestBidParallelism:     1,
		SinkParallelism:           1,
		ProducerIPs:               []string{"localhost"},
		KafkaClusterIPs:           []string{"localhost"},
		WatermarkInterval:         utils.Duration(100 * time.Millisecond),
		MaxAllowedDelay:           utils.Duration(0),
		WindowDuration:            utils.Duration(100 * time.Millisecond),
	}

	// Use alias to avoid infinite recursion
	type alias Q7Config

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

func Query7Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q7Config
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

	tumblingWindow := Query7TumblingWindow(duration)
	tumblingWindow.SetParallelism(config.TumblingWindowParallelism)
	dataflow.AddOperator(df, tumblingWindow)

	// Keyby window ending timestamp
	highestBidKeyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[int64, int64, int64]) int64 {
			return t.GetTimestamp()
		},
	)

	highestBidAggregator := dataflow.NewAggregator(
		// Create a new valueState to store auctionId, price and bidder
		func() *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			return stateType.NewValueState(
				tuple.NewTuple3(int64(0), int64(0), int64(0)),
			)
		},

		// Update the highest bid's info
		func(
			acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			in *tuple.Tuple3[int64, int64, int64],
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			val, _ := acc.Get()
			if val.V1 <= in.V1 {
				// If price is the same, we choose the bid with larger auctionId
				if val.V1 == in.V1 && val.V2 > in.V2 {
					return acc
				}
				acc.Set(in)
			}
			return acc
		},

		// Output: auctionId, price and bidderId of the highest bid
		func(
			acc *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
		) *tuple.Tuple3[int64, int64, int64] {
			val, _ := acc.Get()
			return val
		},

		// Merge func, only keeps info about the highest bid
		func(
			acc1 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
			acc2 *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]],
		) *stateType.ValueState[*tuple.Tuple3[int64, int64, int64]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			if val1.V1 <= val2.V1 {
				if val1.V1 == val2.V1 && val1.V2 > val2.V2 {
					return acc1
				}
				acc1.Set(val2)
			}
			return acc1
		},
	)
	// Calculate Highest bid
	highestBid := dataflow.NewTumblingWindow(
		"highestBid",
		highestBidKeyAssigner,
		highestBidAggregator,
		duration,
	)
	highestBid.SetParallelism(config.HighestBidParallelism)
	dataflow.AddOperator(df, highestBid)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[int64, int64, int64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, highestBid)
	dataflow.Add1To1Stream(df, highestBid, sink)

	return df
}
