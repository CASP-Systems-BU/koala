package sourceTest

import (
	"math/rand"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/randGenerator"
)

// Util function that generates event based on given parameters
func CalculateNextEvent(
	eventType config.EventType,
	eventNumber int64,
	random *rand.Rand,
	cfg *config.NexmarkSourceConfig,
) tuple.Tuple {

	totalProportion := cfg.PersonProportion + cfg.AuctionProportion + cfg.BidProportion
	switch eventType {
	case config.Person:
		currentEventID := randGenerator.GetEventCountSoFar(
			config.Person,
			cfg.PersonProportion,
			eventNumber,
			totalProportion,
			cfg.PersonProportion,
			cfg.AuctionProportion,
		)
		timeStamp := getEventTimeTest(currentEventID, cfg.InterEventDelay)
		return randGenerator.NextPerson(currentEventID, random, timeStamp, cfg)
	case config.Auction:
		currentEventID := randGenerator.GetEventCountSoFar(
			config.Auction,
			cfg.AuctionProportion,
			eventNumber,
			totalProportion,
			cfg.PersonProportion,
			cfg.AuctionProportion,
		)
		timeStamp := getEventTimeTest(currentEventID, cfg.InterEventDelay)
		return randGenerator.NextAuction(currentEventID, random, timeStamp, cfg)
	default:
		currentEventID := randGenerator.GetEventCountSoFar(
			config.Bid,
			cfg.BidProportion,
			eventNumber,
			totalProportion,
			cfg.PersonProportion,
			cfg.AuctionProportion,
		)
		timeStamp := getEventTimeTest(currentEventID, cfg.InterEventDelay)
		return randGenerator.NextBid(currentEventID, random, timeStamp, cfg)
	}
}

// Given the global event ID and inter-event delay, get the current event time
func getEventTimeTest(
	globalEventID int64,
	interEventDelay utils.Duration,
) int64 {
	return globalEventID * time.Duration(interEventDelay).Nanoseconds()
}
