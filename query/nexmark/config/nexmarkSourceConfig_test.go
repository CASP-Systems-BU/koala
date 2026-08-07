package config_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
)

// Test customized UnmarshalJSON method of NexmarkSourceConfig
func TestNexmarkSourceConfigJSONUnmarshal(t *testing.T) {

	jsonFile, err := os.ReadFile("test.json")
	if err != nil {
		log.Fatalln("Error reading json file: ", err)
	}

	var nexmarkSourceConfig config.NexmarkSourceConfig
	err = json.Unmarshal(jsonFile, &nexmarkSourceConfig)
	if err != nil {
		log.Fatalln("Error unmarshalling query json file: ", err)
	}

	// We overwrite the BidProportion to 111, InterEventDelay to 1s, and
	// RateLimit to [1000,2000,3000]

	if nexmarkSourceConfig.BidProportion != 111 {
		t.Errorf("BidProportion unmarshal error, expected 111, got %d",
			nexmarkSourceConfig.BidProportion)
	}

	if time.Duration(nexmarkSourceConfig.InterEventDelay) != time.Second {
		t.Errorf("InterEventDelay unmarshal error, expected 1s, got %s",
			time.Duration(nexmarkSourceConfig.InterEventDelay))
	}

	if nexmarkSourceConfig.RateLimiterConfig.RateLimit[0] != 1000 ||
		nexmarkSourceConfig.RateLimiterConfig.RateLimit[1] != 2000 ||
		nexmarkSourceConfig.RateLimiterConfig.RateLimit[2] != 3000 {
		t.Errorf("RateLimit unmarshal error, expected [1000,2000,3000], got %v",
			nexmarkSourceConfig.RateLimiterConfig.RateLimit)
	}

	// Check some default values to ensure they are set correctly
	if nexmarkSourceConfig.PersonProportion != 1 {
		t.Errorf("PersonProportion default value error, expected 1, got %d",
			nexmarkSourceConfig.PersonProportion)
	}

	if nexmarkSourceConfig.AuctionProportion != 3 {
		t.Errorf("AuctionProportion default value error, expected 3, got %d",
			nexmarkSourceConfig.AuctionProportion)
	}

	if time.Duration(
		nexmarkSourceConfig.RateLimiterConfig.GeneratorInterval,
	) != 200*time.Millisecond {
		t.Errorf(
			"GeneratorInterval default value error, expected 200ms, got %s",
			time.Duration(
				nexmarkSourceConfig.RateLimiterConfig.GeneratorInterval,
			),
		)
	}
}
