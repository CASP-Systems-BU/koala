package query4

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

type Q4ModConfig struct {
	StatefulMapperParallelism int
	SinkParallelism           int
	DummyFieldSize            int
	ProducerIPs               []string
	KafkaClusterIPs           []string
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q4ModConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q4ModConfig{
		StatefulMapperParallelism: 1,
		SinkParallelism:           1,
		DummyFieldSize:            0,
		ProducerIPs:               []string{"localhost"},
		KafkaClusterIPs:           []string{"localhost"},
	}

	// Use alias to avoid infinite recursion
	type alias Q4ModConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 4: Average Price for a Category
// Select the average of the wining bid prices for all closed auctions in each
// category

// Stream SQL:
// SELECT C.id, AVG(CA.price)
// FROM category C, item I, closed auction CA
// WHERE C.id = I.categoryId
// AND I.id = CA.itemid
// GROUP BY C.id;

// We use a closed_auction source to imply that an auction is over
func Query4ModKafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q4ModConfig
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
	closedAuctionEventKafkaTopic :=
		nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.ClosedAuction]
	src := dataflow.NewKafkaSource[*models.ClosedAuctionEvent](
		"source",
		closedAuctionEventKafkaTopic,
		kafkaConfig,
	)
	src.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, src)

	mapper := Query4StatefulMapperDummy(config.DummyFieldSize)
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
