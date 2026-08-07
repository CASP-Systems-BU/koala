package nexmarkKafkaProducer

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	kk "github.com/CASP-Systems-BU/koala/kafka"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/randGenerator"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
	"github.com/CASP-Systems-BU/koala/query/rateLimiter"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type NexmarkKafkaProducer struct {

	// List of initialized KafkaProducer instances
	kafkaProducers []kk.KafkaProducerInstance
}

func NewNexmarkKafkaProducer(
	configJsonPath string,
	replicaID int64,
) *NexmarkKafkaProducer {

	// Read and parse the producer config file
	cfg := getNexmarkKafkaProducerConfig(configJsonPath)

	// Create Kafka producer client config
	kafkaConfig := kk.DefaultKafkaProducerConfig()

	// Use the 1st kafka server as the bootstrap server
	if len(cfg.KafkaClusterIPs) == 0 {
		log.Fatalln(
			"Error: empty KafkaClusterIPs in NexmarkKafkaProducerConfig",
		)
	}
	kk.SetKafkaServerAddress(kafkaConfig, cfg.KafkaClusterIPs[0]+":9092")

	// Validate the number of producers
	producerParallelism := len(cfg.ProducerIPs)
	if producerParallelism == 0 {
		log.Fatalln("Error: empty ProducerIPs in NexmarkKafkaProducerConfig")
	}

	// Create producer instances for each logical source
	kafkaProducers := make([]kk.KafkaProducerInstance, 0)
	for _, logicalSourceConfig := range cfg.NexmarkLogicalSourceConfigs {

		var producerInstance kk.KafkaProducerInstance
		switch logicalSourceConfig.EventType {
		case config.Person:
			producerInstance = newKafkaProducerInstance[*models.PersonEvent](
				replicaID,
				producerParallelism,
				kafkaConfig,
				&logicalSourceConfig,
			)
		case config.Auction:
			producerInstance = newKafkaProducerInstance[*models.AuctionEvent](
				replicaID,
				producerParallelism,
				kafkaConfig,
				&logicalSourceConfig,
			)
		case config.Bid:
			producerInstance = newKafkaProducerInstance[*models.BidEvent](
				replicaID,
				producerParallelism,
				kafkaConfig,
				&logicalSourceConfig,
			)
		case config.ClosedAuction:
			producerInstance = newKafkaProducerInstance[*models.ClosedAuctionEvent](
				replicaID,
				producerParallelism,
				kafkaConfig,
				&logicalSourceConfig,
			)
		}
		kafkaProducers = append(kafkaProducers, producerInstance)
	}

	return &NexmarkKafkaProducer{
		kafkaProducers: kafkaProducers,
	}
}

// Main routine for NexmarkKafkaProducer. Start a separate routine for each
// specified logical nexmark source
func (nkp *NexmarkKafkaProducer) Run() {

	var wg sync.WaitGroup
	for _, producer := range nkp.kafkaProducers {
		wg.Add(1)
		go func(p kk.KafkaProducerInstance) {
			defer wg.Done()
			p.Run()
		}(producer)
	}
	wg.Wait()

	// Prevent producer to exit after generting all events if configured to run
	// with bound - our experiment script monitors the health of producers
	select {}
}

/******************************************************************************
					Helper functions for NexmarkKafkaProducer
******************************************************************************/

// Read and parse the producer config file
func getNexmarkKafkaProducerConfig(
	configJsonPath string,
) *NexmarkKafkaProducerConfig {

	jsonFile, err := os.ReadFile(configJsonPath)
	if err != nil {
		log.Fatalln("Error reading config json file: ", err)
	}
	var config NexmarkKafkaProducerConfig
	err = json.Unmarshal(jsonFile, &config)
	if err != nil {
		log.Fatalln("Error unmarshalling query json file: ", err)
	}
	return &config
}

// Helper function to initialize a nexmark KafkaProducer instance
func newKafkaProducerInstance[OUT tuple.Tuple](
	replicaID int64,
	totalNumParallelism int,
	kafkaConfig *kafka.ConfigMap,
	logicalSourceConfig *NexmarkLogicalSourceConfig,
) kk.KafkaProducerInstance {

	eventType := logicalSourceConfig.EventType
	nexmarkConfig := logicalSourceConfig.NexmarkSourceConfig
	nexmarkGenerator := randGenerator.NewRandGenerator(nexmarkConfig)
	rateLimiter := rateLimiter.NewRateLimiter(
		nexmarkConfig.RateLimiterConfig.RateLimit,
		nexmarkConfig.RateLimiterConfig.RateChangeInterval,
	)

	sourceFunc := source.CreateSourceFunc[OUT](
		eventType,
		nexmarkGenerator,
		nexmarkConfig.RateLimiterConfig.UseRateLimit,
		rateLimiter,
		nexmarkConfig.NumEvents,
		nexmarkConfig.RateLimiterConfig.GeneratorInterval,
		replicaID,
		totalNumParallelism,
		nil, // not used in KafkaProducer
	)

	// ReplicaID is also used for identifying which Kafka topic partition this
	// producer will send messages to
	return kk.NewKafkaProducer[OUT](
		NexmarkEventTypeToTopic[eventType],
		int32(replicaID),
		totalNumParallelism,
		kafkaConfig,
		sourceFunc,
	)
}
