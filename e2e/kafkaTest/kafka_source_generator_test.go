package kafkatest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/kafka"
	kk "github.com/CASP-Systems-BU/koala/kafka"
)

var recordsReceived = make(map[int]int)
var REPEAT = 50

// In this test, we define a dataflow to test the kafka source generator.

// SETUP before the test: run ./deployKafka.sh to deploy the kafka
// cluster first before running the test.

func TestKafka(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	numWorkers := 5

	// Define data generating function in kafka
	sourceFunc := func(out collector.Collector) {
		for i := 0; i < REPEAT; i++ {
			out.Emit(&tuple.Tuple2[int, int]{V1: i, V2: i})
		}
	}

	// Start Kafka producer to generate data (this also creates the topic if
	// it's not present)
	partitionIdx := int32(0)
	numPartitions := 1
	producer := kafka.NewKafkaProducer[*tuple.Tuple2[int, int]](
		"s1",
		partitionIdx,
		numPartitions,
		kk.DefaultKafkaProducerConfig(),
		sourceFunc,
	)

	config := configuration.Default()
	testutils.DeployJob(numWorkers, query1, config)

	log.Println("Kafka producer producing data ...")
	producer.Run()

	time.Sleep(10 * time.Second)

	if len(recordsReceived) != REPEAT {
		log.Fatalf("Expected %d, got %d\n", REPEAT, recordsReceived)
	}
	// Verify that all integers were received
	for i := 0; i < REPEAT; i++ {
		if recordsReceived[i] != 1 {
			log.Fatalf(
				"Incorrect integer: %d, received: %d\n",
				i,
				recordsReceived[i],
			)
		}
	}
	log.Println("[E2E] Test completed")

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func query1() *dataflow.Dataflow {

	// Kafka consumer configuration
	kafkaConsumerConfig := kk.DefaultKafkaConsumerConfig()

	// Define query
	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewKafkaSource[*tuple.Tuple2[int, int]](
		"s1",
		"s1",
		kafkaConsumerConfig,
	)

	// increase source parallelism to check if we receive the message only once.
	source.SetParallelism(2)
	dataflow.AddOperator(query, source)

	// Define flatmap
	flatmap := dataflow.NewFlatmap[*tuple.Tuple2[int, int], *tuple.Tuple1[int]](
		"flatmap",
		func(t *tuple.Tuple2[int, int], co collector.Collector) {
			co.Emit(&tuple.Tuple1[int]{V1: t.V1})
		},
	)
	flatmap.SetParallelism(1)
	dataflow.AddOperator(query, flatmap)

	// Define sink

	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple1[int]) {
			recordsReceived[t.V1] += 1
		},
	)
	sink.SetParallelism(2)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, flatmap)
	dataflow.Add1To1Stream(query, flatmap, sink)

	return query
}
