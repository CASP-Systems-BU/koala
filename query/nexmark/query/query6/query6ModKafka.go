package query6

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

type Q6ModConfig struct {
	StatefulMapperParallelism int
	SinkParallelism           int
	ProducerIPs               []string
	KafkaClusterIPs           []string
	DummyFieldSize            int
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q6ModConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q6ModConfig{
		StatefulMapperParallelism: 1,
		SinkParallelism:           1,
		ProducerIPs:               []string{"localhost"},
		KafkaClusterIPs:           []string{"localhost"},
		DummyFieldSize:            0,
	}

	// Use alias to avoid infinite recursion
	type alias Q6ModConfig

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

func Query6ModKafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q6ModConfig
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalln("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatalln("Error during Unmarshal(): ", err)
	}

	// Replace default bootstrap servers
	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatalln(err)
	}

	// Using kafka source for ClosedAuction
	closedAuctionKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.ClosedAuction]
	src := dataflow.NewKafkaSource[*models.ClosedAuctionEvent](
		"source",
		closedAuctionKafkaTopic,
		kafkaConfig,
	)
	src.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, src)

	mapper := Query6StatefulMapper(config.DummyFieldSize)
	mapper.SetParallelism(config.StatefulMapperParallelism)
	dataflow.AddOperator(df, mapper)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int64, float64]) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, mapper)
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
