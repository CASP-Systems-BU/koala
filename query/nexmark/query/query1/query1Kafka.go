package query1

import (
	"encoding/json"
	"log"
	"os"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/nexmarkKafkaProducer"
)

type Q1Config struct {
	MapperParallelism int
	SinkParallelism   int
	ProducerIPs       []string
	KafkaClusterIPs   []string
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q1Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q1Config{
		MapperParallelism: 1,
		SinkParallelism:   1,
		ProducerIPs:       []string{"localhost"},
		KafkaClusterIPs:   []string{"localhost"},
	}

	// Use alias to avoid infinite recursion
	type alias Q1Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 1: Currency Conversion
// Convert each bid value from dollars to euros. Illustrates a simple
// transformation.

// Stream SQL:
// SELECT itemid, DOLTOEUR(price),
//    bidderId, bidTime
// FROM bid;

func Query1Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q1Config
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

	// Using kafka source for BidEvent
	bidEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Bid]
	src := dataflow.NewKafkaSource[*models.BidEvent](
		"source",
		bidEventKafkaTopic,
		kafkaConfig,
	)
	src.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, src)

	// Define Map
	mapper := Query1Mapper()
	mapper.SetParallelism(config.MapperParallelism)
	dataflow.AddOperator(df, mapper)

	// Define Sink (no-op)
	sink := dataflow.NewSink("sink", func(in *models.BidEvent) {})
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Mapper
	dataflow.Add1To1Stream(df, src, mapper)
	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, mapper, sink)
	return df
}
