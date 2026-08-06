package randGenerator

import (
	"math/rand"

	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

// Generate and return a random auction with next avaliable ID.
func NextAuction(
	eventID int64,
	random *rand.Rand,
	timestamp int64,
	cfg *config.NexmarkSourceConfig,
) *models.AuctionEvent {

	id := lastBase0AuctionID(eventID, cfg) + config.FIRST_AUCTION_ID

	var seller int64

	// We use random generator to decide whether
	// this auction will have a hot sellerID
	// If not, we just uniformly choose an active sellerID
	if random.Float64() < cfg.HotSellerRatio {
		seller = (lastBase0PersonID(eventID, cfg) / cfg.HotSellerRange) * cfg.HotSellerRange
	} else {
		seller = nextBase0PersonID(eventID, random, cfg)
	}

	seller += config.FIRST_PERSON_ID

	var category int64 = config.FIRST_CATEGORY_ID + random.Int63n(cfg.NumCategories)
	var initialBid int64 = nextPrice(random)
	var expires int64 = getEventTime(eventID+int64(cfg.NumInFlightEvents), cfg.InterEventDelay)
	var name string = nextString(random, 20)
	var description string = nextString(random, 100)
	var reserve int64 = initialBid + nextPrice(random)

	auction := &models.AuctionEvent{
		V1: id,
		V2: name,
		V3: description,
		V4: initialBid,
		V5: reserve,
		V6: timestamp,
		V7: expires,
		V8: seller,
		V9: category,
	}
	return auction
}

// Return the last valid auction id (ignoring FIRST_AUCTION_ID).
// Will be the curent auction id if due to generate an auction.
func lastBase0AuctionID(eventID int64, cfg *config.NexmarkSourceConfig) int64 {

	totalProportion := cfg.PersonProportion + cfg.AuctionProportion + cfg.BidProportion
	var epoch int64 = eventID / int64(totalProportion)
	var offset int64 = eventID % int64(totalProportion)

	if offset < int64(cfg.PersonProportion) {
		epoch -= 1
		offset = int64(cfg.AuctionProportion) - 1
	} else if offset >= int64(cfg.PersonProportion+cfg.AuctionProportion) {
		offset = int64(cfg.AuctionProportion) - 1
	} else {
		offset -= int64(cfg.AuctionProportion)
	}

	return epoch*int64(cfg.AuctionProportion) + offset
}

// Return a random auction id (base 0) from last
// cfg.NumActiveAuctions active auction id
func nextBase0AuctionID(
	nextEventID int64,
	random *rand.Rand,
	cfg *config.NexmarkSourceConfig,
) int64 {
	// Choose a random auction that is still likely active
	maxAuctionID := lastBase0AuctionID(nextEventID, cfg)
	minAuctionID := max(
		maxAuctionID-int64(cfg.NumActiveAuctions),
		0,
	)
	return minAuctionID + nextLong(random, maxAuctionID-minAuctionID+1)
}
