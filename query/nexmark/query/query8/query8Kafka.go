package query8

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"github.com/CASP-Systems-BU/koala/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/nexmarkKafkaProducer"
)

type Q8Config struct {
	CustomWindowJoinParallelism int
	SinkParallelism             int
	ProducerIPs                 []string
	KafkaClusterIPs             []string
	WatermarkInterval           utils.Duration
	MaxAllowedDelay             utils.Duration
	NewUserPeriod               utils.Duration
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q8Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q8Config{
		CustomWindowJoinParallelism: 1,
		SinkParallelism:             1,
		ProducerIPs:                 []string{"localhost"},
		KafkaClusterIPs:             []string{"localhost"},
		WatermarkInterval:           utils.Duration(100 * time.Millisecond),
		MaxAllowedDelay:             utils.Duration(0),
		NewUserPeriod:               utils.Duration(100 * time.Millisecond),
	}

	// Use alias to avoid infinite recursion
	type alias Q8Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 8: Monitor New Users
// Finds people who put something up for sale within twelve hours of registering
// to use the auction service

// Stream SQL
// SELECT person.id, person.name
// FROM person [RANGE 12 HOURS PRECEDING]
// open auction [RANGE 12 HOURS PRECEDING]
// WHERE person.id = open auction.sellerId;

func Query8Kafka(configFile string) *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q8Config
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
	newUserPeriod := time.Duration(config.NewUserPeriod)

	// Replace default bootstrap servers
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Using kafka source for PersonEvent
	personEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Person]
	personSource := dataflow.NewKafkaSource[*models.PersonEvent](
		"personSource",
		personEventKafkaTopic,
		kafkaConfig,
	)
	personSource.SetParallelism(len(config.ProducerIPs))
	// Assign V7(dateTime) as timestamp for person event
	// type PersonEvent = tuple.Tuple8[
	//  int64,  // V1:id
	//  string, // V2:name
	//  string, // V3:email
	//  string, // V4:creditCard
	//  string, // V5:city
	//  string, // V6:state
	//  int64,  // V7:dateTime (unix nanoseconds)
	//  string, // V8:extra
	// ]
	personTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.PersonEvent) int64 {
			return t.V7
		},
	)
	personSource.AssignTimestampAndWatermark(
		personTimestampAssigner,
		watermarkInterval,
		maxAllowedDelay,
	)
	dataflow.AddOperator(df, personSource)

	// Using kafka source for AuctionEvent
	auctionEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Auction]
	auctionSource := dataflow.NewKafkaSource[*models.AuctionEvent](
		"auctionSource",
		auctionEventKafkaTopic,
		kafkaConfig,
	)
	auctionSource.SetParallelism(len(config.ProducerIPs))
	// Assign V6(dateTime) as timestamp for auction event
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

	windowJoin := Query8CustomWindowJoin(
		personSource,
		auctionSource,
		int64(newUserPeriod),
	)
	windowJoin.SetParallelism(config.CustomWindowJoinParallelism)
	dataflow.AddOperator(df, windowJoin)

	// Do nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, string]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	// Connect each operator to their upstream
	dataflow.Add2To1Stream(
		df,
		personSource,
		auctionSource,
		windowJoin,
	)
	dataflow.Add1To1Stream(df, windowJoin, sink)

	return df
}
