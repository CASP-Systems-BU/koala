package query2

import (
	"encoding/json"
	"log"
	"os"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/kafka"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/nexmarkKafkaProducer"
)

type Q2Config struct {
	FilterParallelism int
	SinkParallelism   int
	ProducerIPs       []string
	KafkaClusterIPs   []string
}

// Override the UnmarshalJSON function to set default values
func (cfg *Q2Config) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = Q2Config{
		FilterParallelism: 1,
		SinkParallelism:   1,
		ProducerIPs:       []string{"localhost"},
		KafkaClusterIPs:   []string{"localhost"},
	}

	// Use alias to avoid infinite recursion
	type alias Q2Config

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Query 2: Selection
// Selects all bids on a set of five items.

// Stream SQL:
// SELECT itemid, price
// FROM bid
// WHERE itemid = 1007 OR
// itemid = 1020 OR
// itemid = 2001 OR
// itemid = 2019 OR
// itemid = 1087;

func Query2Kafka(configFile string) *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config Q2Config
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

	// Using kafka source for BidEvent
	bidEventKafkaTopic := nexmarkKafkaProducer.NexmarkEventTypeToTopic[nexmarkSrcCfg.Bid]
	src := dataflow.NewKafkaSource[*models.BidEvent](
		"source",
		bidEventKafkaTopic,
		kafkaConfig,
	)
	src.SetParallelism(len(config.ProducerIPs))
	dataflow.AddOperator(df, src)

	// type BidEvent = tuple.Tuple5[
	// int64,  // V1: auction id (foriegn key)
	// int64,  // V2: bidder id (foriegn key)
	// int64,  // V3: price
	// int64,  // V4: dateTime (unix nanoseconds)
	// string, // V5: extra
	// ]

	//Define Filter
	filter := dataflow.NewFilter(
		"filter",
		func(in *models.BidEvent) bool {
			return in.V1 == 1007 ||
				in.V1 == 1020 ||
				in.V1 == 2001 ||
				in.V1 == 2019 ||
				in.V1 == 1087
		},
	)
	filter.SetParallelism(config.FilterParallelism)
	dataflow.AddOperator(df, filter)

	// Define Sink (no-op)
	sink := dataflow.NewSink("sink", func(in *models.BidEvent) {})
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Filter
	dataflow.Add1To1Stream(df, src, filter)
	// Connect Filter -> Sink
	dataflow.Add1To1Stream(df, filter, sink)
	return df
}
