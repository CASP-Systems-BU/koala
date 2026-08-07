package dataflow

import (
	"log"
	"reflect"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/network"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	kk "github.com/CASP-Systems-BU/koala/kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Source operator that consumes data from kafka
type KafkaSource[T tuple.Tuple] struct {
	*Source[T]

	// Decoder for deserializing received messages from Kafka
	decoder network.TupleDecoder

	// Kafka consumer
	consumer *kafka.Consumer

	// Sampling interval - this is used to sample the data for kafka based
	// metric collection
	samplingInterval int64

	// kafka topic
	kafkaTopic string

	config *kafka.ConfigMap
}

func NewKafkaSource[T tuple.Tuple](
	id string,
	topic string,
	config *kafka.ConfigMap,
) *KafkaSource[T] {

	source := &KafkaSource[T]{}

	// Create Tuple decoder
	var zero T
	source.decoder = network.CreateDecoder(reflect.TypeOf(zero).Elem())

	source.samplingInterval = kk.DefaultMetricSamplingInterval
	source.config = config

	// Init the base Source operator
	source.Source = NewSource[T](id, source.produceSourceFunc())

	// Set the Kafka topic
	source.kafkaTopic = topic

	return source
}

// Override Setup() to subscribe to Kafka topic during runtime
func (source *KafkaSource[T]) Setup(paras *utils.OperatorSetupParas) {

	// Init Kafka consumer
	consumer, e := kafka.NewConsumer(source.config)
	if e != nil {
		log.Fatalf("Error creating consumer: %s", e)
	}
	source.consumer = consumer

	// Call the base Setup() method
	source.Source.Setup(paras)

	// Subscribe to Kafka topic
	err := source.consumer.SubscribeTopics([]string{source.kafkaTopic}, nil)
	if err != nil {
		log.Fatalf("Error subscribing to topic: %s \n", err)
	}
}

// User API to configure the metric sampling interval - overwrite the default
func (source *KafkaSource[T]) SetMetricSamplingInterval(interval int64) {
	source.samplingInterval = interval
}

/******************************************************************************
						Util functions for KafkaSource
******************************************************************************/

// Generate the source function dataflow.Source
func (source *KafkaSource[T]) produceSourceFunc() func(collector.Collector) {

	return func(co collector.Collector) {

		// Main loop to read messages from Kafka topic
		var metricSamplingCounter int64
		for {

			// Read a message from tail of the Kafka topic. Timeout -1 indicates
			// indefinite blocking until a message is received. We update the
			// IdleTime metric in KafkaSrouce to caputre the time spent waiting
			// for messages - equivalent to waiting for input buffer in other
			// operators.
			now := time.Now()
			msg, err := source.consumer.ReadMessage(-1)
			source.MetricCollector.UpdateIdleTime(time.Since(now))
			if err != nil {

				// We don't want to exit the loop on error, just log it and
				// continue to read messages since it is possible that the
				// topic hasn't started producing messages yet.
				// TODO: add more robust error handling
				log.Printf(
					"Kafka Source error (keep trying without exiting): %s\n",
					err,
				)
				time.Sleep(100 * time.Millisecond)
			} else {

				if msg != nil {
					// To avoid frequent metric updates, we sample the Kafka
					// input messages for metric collection.
					metricSamplingCounter++
					if metricSamplingCounter == source.samplingInterval {

						// Update KafkaQueueTime: time spent in Kafka queue
						source.MetricCollector.UpdateKafkaQueueTime(
							time.Since(msg.Timestamp).Nanoseconds(),
						)
						metricSamplingCounter = 0
					}

					msgVal := source.deserializeTuple(msg.Value)
					if msgVal != nil {
						co.Emit(msgVal)
					}
				} else {
					log.Printf("No message\n")
					// Sleep for a while to avoid busy waiting
					// TODO: add more robust error handling
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}

// Deserialize the tuple from Kafka message
func (source *KafkaSource[T]) deserializeTuple(buf []byte) tuple.Tuple {

	// Allocate an empty tuple for decoding
	var newRecord T
	newRecord = newRecord.New().(T)

	_, err := source.decoder(newRecord, buf)
	if err != nil {
		log.Fatalf("Error decoding Kafka tuple: %v", err)
	}

	return newRecord
}
