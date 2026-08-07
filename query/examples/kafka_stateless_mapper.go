package examples

import (
	"math"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Example_Kafka_Query1() *dataflow.Dataflow {
	// Define query
	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "koala",
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 6000,
		"fetch.min.bytes":    1,
		"fetch.wait.max.ms":  10,
	}

	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewKafkaSource[*tuple.Tuple2[int, int]](
		"q1k",
		"s1",
		kafkaConsumerConfig,
	)

	// increase source parallelism to check if we receive the message only once.
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Define flatmap
	mapper := dataflow.NewMapper(
		"mapper",
		func(in *tuple.Tuple2[int, int]) *tuple.Tuple2[int, float64] {
			// Expansive computation
			res := 0.0
			for i := 0; i < 100; i++ {
				res = math.Pow(float64(in.V1), float64(in.V2))
			}
			return &tuple.Tuple2[int, float64]{
				V1: in.V1,
				V2: res,
			}
		},
	)
	mapper.SetParallelism(1)
	dataflow.AddOperator(query, mapper)

	// Define sink

	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[int, float64]) {
			//log.Println("Received:", t.V1)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, mapper)
	dataflow.Add1To1Stream(query, mapper, sink)

	return query
}
