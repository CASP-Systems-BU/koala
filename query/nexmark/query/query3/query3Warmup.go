package query3

import (
	"encoding/json"
	"log"
	"os"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/kafka"
	"github.com/CASP-Systems-BU/koala/query"
	nexmarkSrcCfg "github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/nexmarkKafkaProducer"
)

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

// Notice: This query is just for query3's warm-up purpose.

func Query3WarmUp(configFile string) *dataflow.Dataflow {
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

	// We only need personEvent with state "OR"
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
	personFilter := dataflow.NewFilter(
		"personFilter",
		func(t *models.PersonEvent) bool {
			return t.V6 != "OR"
		},
	)
	personFilter.SetParallelism(config.PersonFilterParallelism)
	dataflow.AddOperator(df, personFilter)
	// We only need auctionEvent with category 10
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
	auctionFilter := dataflow.NewFilter(
		"auctionFilter",
		func(t *models.AuctionEvent) bool {
			return t.V9 != 10
		},
	)
	auctionFilter.SetParallelism(config.AuctionFilterParallelism)
	dataflow.AddOperator(df, auctionFilter)

	// Define keyAssigners for both upstreams
	// For PersonEvent, keyby personId
	personKeyAssigner := ka.NewKeyAssigner(
		func(t *models.PersonEvent) int64 {
			return t.V1
		},
	)
	// For AuctionEvent, keyby sellerId
	auctionKeyAssigner := ka.NewKeyAssigner(
		func(t *models.AuctionEvent) int64 {
			return t.V8
		},
	)

	join := dataflow.NewJoin[*tuple.Tuple4[string, string, string, int64]](
		"join",
		personFilter,
		personKeyAssigner,
		func(
			in *models.PersonEvent,
			// In state1, we store personName, city and state info
			state1 *stateType.ValueState[*tuple.Tuple4[string, string, string, string]],
			// In state2, we only store the auctionId
			state2 *stateType.ListState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) {
			if config.UseDummyField {
				dummyFieldContent := query.RandString(config.DummyFieldSize)
				state1.Set(&tuple.Tuple4[string, string, string, string]{
					V1: in.V2,
					V2: in.V5,
					V3: in.V6,
					V4: dummyFieldContent,
				})
			} else {
				state1.Set(&tuple.Tuple4[string, string, string, string]{
					V1: in.V2,
					V2: in.V5,
					V3: in.V6,
				})
			}

			co.Emit(&tuple.Tuple4[string, string, string, int64]{
				V1: "a",
				V2: "b",
				V3: "c",
				V4: 1,
			})
		},
		auctionFilter,
		auctionKeyAssigner,
		func(
			in *models.AuctionEvent,
			// In state1, we store personName, city and state info
			state1 *stateType.ValueState[*tuple.Tuple4[string, string, string, string]],
			// In state2, we only store the auctionId
			state2 *stateType.ListState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) {
		},
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
	dataflow.Add1To1Stream(df, personSource, personFilter)
	dataflow.Add1To1Stream(df, auctionSource, auctionFilter)
	dataflow.Add2To1Stream(df, personFilter, auctionFilter, join)
	dataflow.Add1To1Stream(df, join, sink)

	return df
}
