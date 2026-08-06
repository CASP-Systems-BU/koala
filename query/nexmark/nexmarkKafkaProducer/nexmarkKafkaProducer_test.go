package nexmarkKafkaProducer_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	cfg "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/nexmarkKafkaProducer"
)

// Test JSON reading of NexmarkProducerConfig

func TestNexmarkProducerConfig(t *testing.T) {

	jsonFile, err := os.ReadFile("test.json")
	if err != nil {
		log.Fatalln("Error reading json file: ", err)
	}

	var config nexmarkKafkaProducer.NexmarkKafkaProducerConfig
	err = json.Unmarshal(jsonFile, &config)
	if err != nil {
		log.Fatalln("Error unmarshalling query json file: ", err)
	}

	// Check the overwritten config values
	if config.KafkaClusterIPs[0] != "localhost" {
		t.Errorf(
			"Expected KafkaClusterIPs[0] to be localhost, got %s",
			config.KafkaClusterIPs[0],
		)
	}
	if len(config.ProducerIPs) != 3 {
		t.Errorf(
			"Expected num ProducerIPs to be 3, got %d",
			len(config.ProducerIPs),
		)
	}
	if len(config.NexmarkLogicalSourceConfigs) != 2 {
		t.Errorf(
			"Expected 2 NexmarkLogicalSourceConfigs, got %d",
			len(config.NexmarkLogicalSourceConfigs),
		)
	}
	if config.NexmarkLogicalSourceConfigs[0].EventType != cfg.Auction {
		t.Errorf(
			"Expected first EventType to be Auction, got %d",
			config.NexmarkLogicalSourceConfigs[0].EventType,
		)
	}
	if config.NexmarkLogicalSourceConfigs[0].NexmarkSourceConfig.NumCategories != 1000 {
		t.Errorf(
			"Expected first NumCategories to be 1000, got %d",
			config.NexmarkLogicalSourceConfigs[0].NexmarkSourceConfig.NumCategories,
		)
	}
	if config.NexmarkLogicalSourceConfigs[1].EventType != cfg.Bid {
		t.Errorf(
			"Expected second EventType to be Bid, got %d",
			config.NexmarkLogicalSourceConfigs[1].EventType,
		)
	}
	if config.NexmarkLogicalSourceConfigs[1].NexmarkSourceConfig.NumActivePeople != 200 {
		t.Errorf(
			"Expected second NumActivePeople to be 200, got %d",
			config.NexmarkLogicalSourceConfigs[1].NexmarkSourceConfig.NumActivePeople,
		)
	}

	// Check the default config values
	if config.NexmarkLogicalSourceConfigs[0].NexmarkSourceConfig.PersonProportion != 1 {
		t.Errorf(
			"Expected default PersonProportion to be 1, got %d",
			config.NexmarkLogicalSourceConfigs[0].NexmarkSourceConfig.PersonProportion,
		)
	}
	if config.NexmarkLogicalSourceConfigs[1].NexmarkSourceConfig.RateLimiterConfig.GeneratorInterval != utils.Duration(
		200*time.Millisecond,
	) {
		t.Errorf(
			"Expected default GeneratorInterval to be 200ms, got %s",
			time.Duration(
				config.NexmarkLogicalSourceConfigs[1].NexmarkSourceConfig.RateLimiterConfig.GeneratorInterval,
			),
		)
	}
}
