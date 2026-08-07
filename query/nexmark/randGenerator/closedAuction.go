package randGenerator

import (
	"math/rand"

	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

// Generate and return a random closed_Auction with next avaliable ID.
func NextClosedAuction(
	eventID int64,
	random *rand.Rand,
	timestamp int64,
	cfg *config.NexmarkSourceConfig,
) *models.ClosedAuctionEvent {

	id := lastBase0AuctionID(eventID, cfg) + config.FIRST_AUCTION_ID

	// We use random generator to decide whether this closed auction will have a
	// hot sellerID. If not, we just uniformly choose an active sellerID
	var seller int64
	if random.Float64() < cfg.HotSellerRatio {
		seller = (lastBase0PersonID(eventID, cfg) / cfg.HotSellerRange) * cfg.HotSellerRange
	} else {
		seller = nextBase0PersonID(eventID, random, cfg)
	}

	seller += config.FIRST_PERSON_ID

	// We use random generator to decide whether this closed auction will have a
	// hot categoryID If not, we just uniformly choose a categoryID because its
	// key space is fixed
	var category int64
	category = config.FIRST_CATEGORY_ID + random.Int63n(cfg.NumCategories)
	if random.Float64() < cfg.HotCategoryRatio {
		category = (category / cfg.HotCategoryRange) * cfg.HotCategoryRange
	}

	var finalPrice int64 = nextPrice(random)

	closed_Auction := &models.ClosedAuctionEvent{
		V1: id,
		V2: seller,
		V3: finalPrice,
		V4: category,
		V5: timestamp,
	}
	return closed_Auction
}

// Generate and return a closed_Auction with fixed sellerID.
func NextWarmupClosedAuction(
	eventID int64,
	random *rand.Rand,
	timestamp int64,
	cfg *config.NexmarkSourceConfig,
	totalEventGenerated int64,
	totalProportion int,
) *models.ClosedAuctionEvent {

	id := lastBase0AuctionID(eventID, cfg) + config.FIRST_AUCTION_ID

	// We use random generator to decide whether this closed auction will have a
	// hot sellerID. If not, we just uniformly choose an active sellerID
	var seller int64

	sellerEventID := GetEventCountSoFar(
		config.Person,
		cfg.PersonProportion,
		totalEventGenerated,
		totalProportion,
		cfg.PersonProportion,
		cfg.AuctionProportion,
	)
	seller = lastBase0PersonID(sellerEventID, cfg)

	seller += config.FIRST_PERSON_ID

	// We use random generator to decide whether this closed auction will have a
	// hot categoryID If not, we just uniformly choose a categoryID because its
	// key space is fixed
	var category int64
	category = config.FIRST_CATEGORY_ID + random.Int63n(cfg.NumCategories)
	if random.Float64() < cfg.HotCategoryRatio {
		category = (category / cfg.HotCategoryRange) * cfg.HotCategoryRange
	}

	var finalPrice int64 = nextPrice(random)

	closed_Auction := &models.ClosedAuctionEvent{
		V1: id,
		V2: seller,
		V3: finalPrice,
		V4: category,
		V5: timestamp,
	}
	return closed_Auction
}
