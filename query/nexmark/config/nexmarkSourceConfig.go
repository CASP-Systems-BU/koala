package config

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/rateLimiter"
)

const (
	// Start the ids at a specific value to ensure the quries find a match
	// even on small synthesized dataset sizes.
	FIRST_AUCTION_ID  int64 = 1000
	FIRST_PERSON_ID   int64 = 1000
	FIRST_CATEGORY_ID int64 = 10
)

type EventType int

const (
	Person EventType = iota
	Auction
	Bid
	ClosedAuction
)

type NexmarkSourceConfig struct {

	/******************************************************************************
				 General configs shared by all kinds of event
	******************************************************************************/

	// Number of events to generate - bounded or unbounded source. If zero,
	// generate events indefinitely
	NumEvents int64

	// StartPoint of the generator, from which event to start generate
	OutputEventNumber int64

	// Event time delay between two consecutive generated events
	InterEventDelay utils.Duration

	// Proportion of the events between person/auction/bid
	// default to 1:3:46
	PersonProportion  int
	AuctionProportion int
	BidProportion     int

	// Extra payloadSize for each kind of event, defines the length of extra
	// field in each eventType
	ExtraPayloadSize int

	RateLimiterConfig *rateLimiter.RateLimiterConfig

	/******************************************************************************
		AuctionEvent related configs (shared by both Auction and ClosedAuction)
	******************************************************************************/

	// How many categories auctionEvent and ClosedAuction event will have
	// [Auction][ClosedAuction]
	NumCategories int64

	// Is this producer used for query warmup
	WarmupRun bool

	// Hot key ratio for auctionID in bid event
	// [Auction][ClosedAuction]
	HotCategoryRatio float64

	// Among how many auctionIDs in bid event will there be a hot key
	// [Auction][ClosedAuction]
	HotCategoryRange int64

	// Hot key ratio for personID in auction event
	// [Auction][ClosedAuction]
	HotSellerRatio float64

	// Among how many personIDs in auction event will there be a hot key
	// [Auction][ClosedAuction]
	HotSellerRange int64

	// Number of all kinds of events we will generate before an auction expires
	// [Auction] ClosedAuction source won't need this field
	NumInFlightEvents int

	/******************************************************************************
				              BidEvent related configs
	******************************************************************************/

	// Hot key ratio for personID in bid event
	HotBidderRatio float64

	// Among how many personIDs in bid event will there be a hot key
	HotBidderRange int64

	// Hot key ratio for auctionID in bid event
	HotAuctionRatio float64

	// Among how many auctionIDs in bid event will there be a hot key
	HotAuctionRange int64

	// Maximum number of former personIDs to consider as active in bid event.
	NumActiveAuctions int

	// Maximum number of former personIDs to consider as active in bid and
	// auctionEvent.
	NumActivePeople int
}

// Return the default NexmarkSourceConfig
func DefaultNexmarkSourceConfig() *NexmarkSourceConfig {
	return &NexmarkSourceConfig{
		NumEvents:         0,
		OutputEventNumber: 0,
		PersonProportion:  1,
		AuctionProportion: 3,
		BidProportion:     46,
		InterEventDelay:   utils.Duration(time.Second),
		ExtraPayloadSize:  0,
		RateLimiterConfig: &rateLimiter.RateLimiterConfig{
			UseRateLimit:       false,
			RateLimit:          []int{},
			RateChangeInterval: []int{},
			GeneratorInterval:  utils.Duration(200 * time.Millisecond),
		},
		NumCategories:     10,
		WarmupRun:         false,
		NumInFlightEvents: 500000,
		NumActiveAuctions: 30000,
		NumActivePeople:   100,
	}
}

/******************************************************************************
						 Custom JSON unmarshal methods
******************************************************************************/

// Override the UnmarshalJSON function to set default values
func (cfg *NexmarkSourceConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = *DefaultNexmarkSourceConfig()

	// Use alias to avoid infinite recursion
	type alias NexmarkSourceConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// Convert EventType from string to int
func (e *EventType) UnmarshalJSON(data []byte) error {

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	switch s {
	case "Person":
		*e = Person
	case "Auction":
		*e = Auction
	case "Bid":
		*e = Bid
	case "ClosedAuction":
		*e = ClosedAuction
	default:
		return errors.New("Invalid EventType string: " + s)
	}
	return nil
}
