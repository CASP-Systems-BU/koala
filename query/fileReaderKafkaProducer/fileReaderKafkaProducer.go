package fileReaderKafkaProducer

import (
	"encoding/json"
	"log"
	"os"
	"time"

	kk "github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	azure "github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/fileSourceFunc"
	azureModels "github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/models"
	borg "github.com/CASP-Systems-BU/disaggregated-streaming/query/borg/fileSourceFunc"
	borgModels "github.com/CASP-Systems-BU/disaggregated-streaming/query/borg/models"
	ratelimiter "github.com/CASP-Systems-BU/disaggregated-streaming/query/rateLimiter"
	taxi "github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/fileSourceFunc"
	taxiModels "github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/models"
	twitch "github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/fileSourceFunc"
	twitchModels "github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/models"
)

type FileReaderKafkaProducer struct {

	// Initialized KafkaProducer instances
	kafkaProducer kk.KafkaProducerInstance
}

func NewFileReaderKafkaProducer(
	configJsonPath string,
	replicaID int64,
) *FileReaderKafkaProducer {

	// Read and parse the producer config file
	cfg := getFileReaderKafkaProducerConfig(configJsonPath)

	// Create Kafka producer config
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

	// Create producer instance
	var kafkaProducer kk.KafkaProducerInstance
	ratelimiter := ratelimiter.NewRateLimiter(
		cfg.RateLimiterConfig.RateLimit,
		cfg.RateLimiterConfig.RateChangeInterval,
	)
	switch cfg.EventType {
	case Taxi:
		sourceFunc := taxi.TaxiFileSourceFunc[*taxiModels.TaxiTrip](
			ratelimiter,
			cfg.NumEvents,
			cfg.OutputEventNumber,
			cfg.RateLimiterConfig.UseRateLimit,
			time.Duration(cfg.RateLimiterConfig.GeneratorInterval),
			cfg.FilePath[replicaID],
		)
		kafkaProducer = kk.NewKafkaProducer[*taxiModels.TaxiTrip](
			FileReaderEventTypeToTopic[cfg.EventType],
			int32(replicaID),
			len(cfg.ProducerIPs),
			kafkaConfig,
			sourceFunc,
		)
	case Twitch:
		sourceFunc := twitch.TwitchFileSourceFunc[*twitchModels.TwitchEvent](
			ratelimiter,
			cfg.NumEvents,
			cfg.OutputEventNumber,
			cfg.RateLimiterConfig.UseRateLimit,
			time.Duration(cfg.RateLimiterConfig.GeneratorInterval),
			cfg.FilePath[replicaID],
		)
		kafkaProducer = kk.NewKafkaProducer[*twitchModels.TwitchEvent](
			FileReaderEventTypeToTopic[cfg.EventType],
			int32(replicaID),
			len(cfg.ProducerIPs),
			kafkaConfig,
			sourceFunc,
		)
	case Borg:
		sourceFunc := borg.BorgFileSourceFunc[*borgModels.TaskEvent](
			ratelimiter,
			cfg.NumEvents,
			cfg.OutputEventNumber,
			cfg.RateLimiterConfig.UseRateLimit,
			time.Duration(cfg.RateLimiterConfig.GeneratorInterval),
			cfg.FilePath[replicaID],
			cfg.IsWarmUp,
		)
		kafkaProducer = kk.NewKafkaProducer[*borgModels.TaskEvent](
			FileReaderEventTypeToTopic[cfg.EventType],
			int32(replicaID),
			len(cfg.ProducerIPs),
			kafkaConfig,
			sourceFunc,
		)
	case Azure:
		sourceFunc := azure.AzureFileSourceFunc[*azureModels.AzureEvent](
			ratelimiter,
			cfg.NumEvents,
			cfg.OutputEventNumber,
			cfg.RateLimiterConfig.UseRateLimit,
			time.Duration(cfg.RateLimiterConfig.GeneratorInterval),
			cfg.FilePath[replicaID],
			cfg.IsWarmUp,
		)
		kafkaProducer = kk.NewKafkaProducer[*azureModels.AzureEvent](
			FileReaderEventTypeToTopic[cfg.EventType],
			int32(replicaID),
			len(cfg.ProducerIPs),
			kafkaConfig,
			sourceFunc,
		)
	}

	return &FileReaderKafkaProducer{
		kafkaProducer: kafkaProducer,
	}
}

// Main routine for FileReaderKafkaProducer.
func (rkp *FileReaderKafkaProducer) Run() {
	rkp.kafkaProducer.Run()
	select {}
}

/******************************************************************************
					Helper functions for FileReaderKafkaProducer
******************************************************************************/

// Read and parse the producer config file
func getFileReaderKafkaProducerConfig(
	configJsonPath string,
) *FileReaderKafkaProducerConfig {

	jsonFile, err := os.ReadFile(configJsonPath)
	if err != nil {
		log.Fatalln("Error reading config json file: ", err)
	}
	var config FileReaderKafkaProducerConfig
	err = json.Unmarshal(jsonFile, &config)
	if err != nil {
		log.Fatalln("Error unmarshalling query json file: ", err)
	}

	// TODO: validate the input parameters
	return &config
}
