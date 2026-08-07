package randGenerator

import (
	"math/rand"

	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

// Return a random bid with next avaliable id
func NextBid(
	nextEventID int64,
	random *rand.Rand,
	timestamp int64,
	cfg *config.NexmarkSourceConfig,
) *models.BidEvent {
	var auction int64
	// We use random generator to decide whether
	// this bid will have a hot auctionID
	// If not, we just uniformly choose an active auctionID
	if random.Float64() < cfg.HotAuctionRatio {
		auction = (lastBase0AuctionID(nextEventID, cfg) / cfg.HotAuctionRange) * cfg.HotAuctionRange
	} else {
		auction = nextBase0AuctionID(nextEventID, random, cfg)
	}

	auction += config.FIRST_AUCTION_ID
	var bidder int64
	// We use random generator to decide whether
	// this bid will have a hot bidderID
	// If not, we just uniformly choose an active bidderID
	if random.Float64() < cfg.HotBidderRatio {
		bidder = (lastBase0PersonID(nextEventID, cfg) / cfg.HotBidderRange) * cfg.HotBidderRange
	} else {
		bidder = nextBase0PersonID(nextEventID, random, cfg)
	}

	bidder += config.FIRST_PERSON_ID

	price := nextPrice(random)

	return &models.BidEvent{
		V1: auction,
		V2: bidder,
		V3: price,
		V4: timestamp,
	}
}
