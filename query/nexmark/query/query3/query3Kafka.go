package query3

import (
	"encoding/json"
	"log"
	"os"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/nexmarkKafkaProducer"
)

type Q3Config struct {
	JoinParallelism int
	SinkParallelism int
	ProducerIPs     []string
	KafkaClusterIPs []string
	DummyFieldSize  int
	UseDummyField   bool
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q3Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q3Config{
		JoinParallelism: 1,
		SinkParallelism: 1,
		ProducerIPs:     []string{"localhost"},
		KafkaClusterIPs: []string{"localhost"},
		DummyFieldSize:  0,
		UseDummyField:   false,
	}

	// Use alias to avoid infinite recursion
	type alias Q3Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 3: Local Item Suggestion
// Find all auctions in category 10 that are being sold by people located in
// Oregon

// Stream SQL:
// SELECT person.name, person.city,
// person.state, open auction.id
// FROM auction, person,
// WHERE auction.sellerId = person.id
// AND person.state = ‘OR’
// AND auction.categoryId = 10;

func Query3Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q3Config
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}

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
	dataflow.AddOperator(df, personSource)

	// Using kafka source for AuctionEvent
	auctionEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Auction]
	auctionSource := dataflow.NewKafkaSource[*models.AuctionEvent](
		"auctionSource",
		auctionEventKafkaTopic,
		kafkaConfig,
	)
	auctionSource.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, auctionSource)

	// Define join operator
	join := Query3Join(
		personSource,
		auctionSource,
		config.UseDummyField,
		config.DummyFieldSize,
	)
	join.SetParallelism(config.JoinParallelism)
	dataflow.AddOperator(df, join)

	// Do nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple4[string, string, string, int64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	// Connect each operator to their upstream
	dataflow.Add2To1Stream(df, personSource, auctionSource, join)
	dataflow.Add1To1Stream(df, join, sink)

	return df
}
