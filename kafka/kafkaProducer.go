package kafka

import (
	"context"
	"log"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaProducer interface for any output data type
type KafkaProducerInstance interface {
	Run()
}

type KafkaProducer[OUT tuple.Tuple] struct {

	// Underlying Kafka producer to send messages
	collector *KafkaCollector[OUT]

	// User-defined function to generate data
	sourceFunc func(collector.Collector)
}

func NewKafkaProducer[OUT tuple.Tuple](
	topic string,
	partitionIdx int32,
	numPartitions int,
	producerConfig *kafka.ConfigMap,
	sourceFunc func(collector.Collector),
) *KafkaProducer[OUT] {

	// Set up topic configuration - update topic config to record LogAppendTime
	// instead of CreateTime
	if partitionIdx != 0 {
		time.Sleep(500 * time.Millisecond)
	}
	updateTopic(topic, numPartitions, producerConfig)
	collector := NewKafkaCollector[OUT](producerConfig, topic, partitionIdx)
	return &KafkaProducer[OUT]{
		collector:  collector,
		sourceFunc: sourceFunc,
	}
}

func (kp *KafkaProducer[OUT]) Run() {

	// Background routine to capture error messages
	go kp.captureErrorMessage()

	// Report kafka producer output rate
	go kp.collector.ReportOutputRate()

	// Main routine executes the user-defined source function
	kp.sourceFunc(kp.collector)

	// Ensure messages are flushed
	kp.collector.producer.Flush(15 * 1000)
	kp.collector.producer.Close()
}

/******************************************************************************
						Helper functions for KafkaProducer
******************************************************************************/

// Update the topic configuration to record LogAppendTime instead of CreateTime
// in kafka
func updateTopic(topic string, numPartitions int, config *kafka.ConfigMap) {
	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		log.Fatalf("Failed to create Admin client: %v", err)
	}
	defer adminClient.Close()

	// create topic if it does not exist
	if !topicExists(adminClient, topic) {
		if err := createTopic(adminClient, topic, numPartitions); err != nil {
			log.Printf("[Kafka] Failed to create topic %s: %v\n", topic, err)
		} else {
			log.Printf("[Kafka] Successfully created topic %s.", topic)
		}
	}
}

func createTopic(
	adminClient *kafka.AdminClient,
	topic string,
	numPartitions int,
) error {
	topicSpecification := kafka.TopicSpecification{
		Topic:         topic,
		NumPartitions: numPartitions,
		Config: map[string]string{
			"message.timestamp.type": "LogAppendTime",
		},
	}

	// Alter topic configuration
	r, err := adminClient.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{topicSpecification},
	)
	if err != nil {
		return err
	}
	for _, result := range r {
		if result.Error.Code() != kafka.ErrNoError {
			return result.Error
		}
	}
	return nil
}

func topicExists(adminClient *kafka.AdminClient, topic string) bool {
	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		log.Printf("Failed to fetch metadata: %v", err)
	}

	for t := range metadata.Topics {
		if t == topic {
			return true
		}
	}
	return false
}

func (kp *KafkaProducer[OUT]) captureErrorMessage() {

	for ev := range kp.collector.producer.Events() {
		switch m := ev.(type) {
		case *kafka.Message:
			if m.TopicPartition.Error != nil {
				log.Fatalf("delivery failed: %v",
					m.TopicPartition.Error)
			}
		}
	}
}
