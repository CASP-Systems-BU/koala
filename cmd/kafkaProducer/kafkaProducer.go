package main

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/kafka"
	kk "github.com/CASP-Systems-BU/koala/kafka"
)

// This is a basic example of Kafka producer executable that produces tuples
// to a specified Kafka topic at a defined rate

const RATE = 10000

func main() {

	// Kafka consumer configuration
	cfg := kk.DefaultKafkaProducerConfig()

	// Define the record generation function
	sourceFunc := func(out collector.Collector) {
		i := 0
		expStart := time.Now()
		for time.Since(expStart) < 5*time.Minute {
			start := time.Now()
			for range RATE {
				out.Emit(&tuple.Tuple2[int, int]{V1: i,
					V2: i})
				i++
			}

			if time.Since(start) < time.Second {
				time.Sleep(time.Second - time.Since(start))
			}
		}
	}

	// Initialize the Kafka producer with output message type of Tuple2.
	// Producer is responsible for creating the Kafka topic if it does not
	// already exist.
	kafkaTopic := "s1"
	partitionIdx := int32(0)
	numPartitions := 1
	p := kafka.NewKafkaProducer[*tuple.Tuple2[int, int]](
		kafkaTopic,
		partitionIdx,
		numPartitions,
		cfg,
		sourceFunc,
	)
	p.Run()
}
