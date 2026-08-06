package query6

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
	nexmarkSrcCfg "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/nexmarkKafkaProducer"
	closedauction "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/closedAuction"
)

type Q6Config struct {
	ClosedAuctionParallelism  int
	StatefulMapperParallelism int
	SinkParallelism           int
	ProducerIPs               []string
	KafkaClusterIPs           []string
	WatermarkInterval         utils.Duration
	MaxAllowedDelay           utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q6Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q6Config{
		ClosedAuctionParallelism:  1,
		StatefulMapperParallelism: 1,
		SinkParallelism:           1,
		ProducerIPs:               []string{"localhost"},
		KafkaClusterIPs:           []string{"localhost"},
		WatermarkInterval:         utils.Duration(100 * time.Millisecond),
		MaxAllowedDelay:           utils.Duration(0),
	}

	// Use alias to avoid infinite recursion
	type alias Q6Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 6, 'Average Selling Price by Seller'.
// Select the average selling price over the last 10
// closed auctions by the same seller.

// Stream SQL:
// SELECT AVG(CA.price), CA.sellerId
// FROM closed auction CA
// [PARTITION BY CA.sellerId
// ROWS 10 PRECEDING];

func Query6Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q6Config
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

	// Replace default bootstrap servers
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Using kafka source for AuctionEvent
	auctionEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Auction]
	auctionSource := dataflow.NewKafkaSource[*models.AuctionEvent](
		"auctionSource",
		auctionEventKafkaTopic,
		kafkaConfig,
	)
	auctionSource.SetParallelism(len(config.ProducerIPs))
	// type AuctionEvent = tuple.Tuple10[
	//  int64,  // V1:auction id
	//  string, // V2:item name
	//  string, // V3:description
	//  int64,  // V4:initial bid
	//  int64,  // V5:reserve
	//  int64,  // V6:dateTime (unix nanoseconds)
	//  int64,  // V7:expires (unix nanoseconds)
	//  int64,  // V8:seller
	//  int64,  // V9:category
	//  string, // V10:extra
	// ]
	// Assign V6(dateTime) as timestamp for auction event
	auctionTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.AuctionEvent) int64 {
			return t.V6
		},
	)
	auctionSource.AssignTimestampAndWatermark(
		auctionTimestampAssigner,
		watermarkInterval,
		maxAllowedDelay,
	)
	dataflow.AddOperator(df, auctionSource)

	// Using kafka source for BidEvent
	bidEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Bid]
	bidSource := dataflow.NewKafkaSource[*models.BidEvent](
		"bidSource",
		bidEventKafkaTopic,
		kafkaConfig,
	)
	bidSource.SetParallelism(len(config.ProducerIPs))
	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]
	// Assign V4(dateTime) as timestamp for bid event
	bidTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.BidEvent) int64 {
			return t.V4
		},
	)
	bidSource.AssignTimestampAndWatermark(
		bidTimestampAssigner,
		watermarkInterval,
		maxAllowedDelay,
	)
	dataflow.AddOperator(df, bidSource)

	// Calculate closedAuction based on auction and bid streams
	closedAuction := closedauction.ClosedAuction(auctionSource, bidSource)
	closedAuction.SetParallelism(config.ClosedAuctionParallelism)
	dataflow.AddOperator(df, closedAuction)

	mapper := Query6StatefulMapper(0)
	mapper.SetParallelism(config.StatefulMapperParallelism)
	dataflow.AddOperator(df, mapper)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, float64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add2To1Stream(
		df,
		auctionSource,
		bidSource,
		closedAuction,
	)
	dataflow.Add1To1Stream(df, closedAuction, mapper)
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
