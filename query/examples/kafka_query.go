package examples

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Example_Kafka_Query() *dataflow.Dataflow {
	// Define query
	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "disaggregated-streaming",
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 6000,
		"fetch.min.bytes":    1,
		"fetch.wait.max.ms":  10,
	}

	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewKafkaSource[*tuple.Tuple2[int, string]](
		"s1",
		"s1",
		kafkaConsumerConfig,
	)

	// increase source parallelism to check if we receive the message only once.
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, string]) int {
			return t.V1
		},
	)

	// Define flatmap
	mapper := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int, string],
			state *stateType.ValueState[*tuple.Tuple2[int, string]],
		) *tuple.Tuple2[int, string] {

			// Read the state
			curCount, exist := state.Get()
			if !exist {
				curCount = tuple.NewTuple2(0, in.V2)
			}

			// Increment the counter
			curCount.V1++

			// Write the state
			state.Set(curCount)

			return &tuple.Tuple2[int, string]{
				V1: curCount.V1,
				V2: curCount.V2,
			}
		},
	)
	mapper.SetParallelism(1)
	dataflow.AddOperator(query, mapper)

	// Define sink

	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int, string]) {
			log.Println("Received:", t.V1)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, mapper)
	dataflow.Add1To1Stream(query, mapper, sink)

	return query
}
