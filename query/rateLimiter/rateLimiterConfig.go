package rateLimiter

import (
	"encoding/json"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
)

type RateLimiterConfig struct {

	// Use rate limiter in this source
	UseRateLimit bool

	// RateLimit pattern
	RateLimit []int

	// After how many Rate Intervals to change rate
	RateChangeInterval []int

	// Wall clock time interval for a rate limit period. For example, if rate
	// limit is 1000 records per interval, and GeneratorInterval is 200ms, then
	// we generate at most 1000 records in each 200ms and sleep for the rest of
	// the 200ms if we finish early
	GeneratorInterval utils.Duration
}

// Return the default RateLimiterConfig
func DefaultRateLimiterConfig() *RateLimiterConfig {
	return &RateLimiterConfig{
		UseRateLimit:       false,
		RateLimit:          []int{},
		RateChangeInterval: []int{},
		GeneratorInterval:  utils.Duration(200 * time.Millisecond),
	}
}

/******************************************************************************
						 Custom JSON unmarshal methods
******************************************************************************/

// Override the UnmarshalJSON function to set default values
func (cfg *RateLimiterConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = *DefaultRateLimiterConfig()

	// Use alias to avoid infinite recursion
	type alias RateLimiterConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}
