package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaSource default metric sampling interval in number of messages.
// Collect a data point every 1000 messages
const DefaultMetricSamplingInterval = 1000

// Overwrite the default configuration as needed. Example:
// cfg := DefaultKafkaProducerConfig()
// cfg.SetKey("bootstrap.servers", "localhost:9093")

func DefaultKafkaProducerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":            "localhost:9092",
		"queue.buffering.max.messages": 10000000,
	}
}

/*
Notes on some important consumer configs:

1. fetch.min.bytes: if no data available in kafka, need to wait until this
   amount of data available before return (lower bound), default 1 byte.
   but if available data > this value, this is not effective
2. fetch.wait.max.ms: wait up to this time, we have to return whatever
   available, even available data is < fetch.min.bytes. If available data is
   > fetch.min.bytes, we don’t need to wait this time. default 500ms.
3. max.partition.fetch.bytes: max size in return value per partition, default
   1MB. We change it to 10MB to allow larger batch size for high throughput.
4. fetch.max.bytes: if a request read data from multiple partitions, total
   return value size cannot exceed this. default 50MB.
   It doesn't make sense to make fetch.max.bytes < max.partition.fetch.bytes
*/

func DefaultKafkaConsumerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":         "localhost:9092",
		"group.id":                  "disaggregated-streaming",
		"auto.offset.reset":         "earliest",
		"max.partition.fetch.bytes": 10485760, // 10MB
	}
}

/******************************************************************************
					Update fields of an existing ConfigMap
******************************************************************************/

func SetKafkaServerAddress(
	config *kafka.ConfigMap,
	serverAddr string,
) {
	err := config.SetKey(
		"bootstrap.servers",
		serverAddr,
	)
	if err != nil {
		log.Fatalln(err)
	}
}
