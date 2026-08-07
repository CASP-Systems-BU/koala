package source

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/randGenerator"
	"github.com/CASP-Systems-BU/koala/query/rateLimiter"
)

// NexmarkSource operator that extends the basic Dataflow Source operator

type NexmarkSource[OUT tuple.Tuple] struct {
	*dataflow.Source[OUT]

	// The generator that generates the events.
	RandGenerator *randGenerator.RandGenerator

	// Ratelimiter for this source
	RateLimiter *rateLimiter.RateLimiter
}

func NewNexmarkPersonSource(
	id string,
	cfg *config.NexmarkSourceConfig,
) *NexmarkSource[*models.PersonEvent] {

	// Create newSource for PersonEvent
	src := newSource[*models.PersonEvent](id, config.Person, cfg)
	return src
}

func NewNexmarkAuctionSource(
	id string,
	cfg *config.NexmarkSourceConfig,
) *NexmarkSource[*models.AuctionEvent] {

	// Create newSource for AuctionEvent
	src := newSource[*models.AuctionEvent](id, config.Auction, cfg)
	return src
}

func NewNexmarkBidSource(
	id string,
	cfg *config.NexmarkSourceConfig,
) *NexmarkSource[*models.BidEvent] {

	// Create newSource for BidEvent
	src := newSource[*models.BidEvent](id, config.Bid, cfg)
	return src
}

func NewNexmarkClosedAuctionSource(
	id string,
	cfg *config.NexmarkSourceConfig,
) *NexmarkSource[*models.ClosedAuctionEvent] {

	// Create newSource for ClosedAuction
	src := newSource[*models.ClosedAuctionEvent](
		id,
		config.ClosedAuction,
		cfg,
	)
	return src
}

func newSource[OUT tuple.Tuple](
	id string,
	eventType config.EventType,
	cfg *config.NexmarkSourceConfig,
) *NexmarkSource[OUT] {

	// Source
	source := &NexmarkSource[OUT]{
		RandGenerator: randGenerator.NewRandGenerator(cfg),
		RateLimiter: rateLimiter.NewRateLimiter(
			cfg.RateLimiterConfig.RateLimit,
			cfg.RateLimiterConfig.RateChangeInterval,
		),
	}

	// Set source function
	sourceF := CreateSourceFunc(
		eventType,
		source.RandGenerator,
		cfg.RateLimiterConfig.UseRateLimit,
		source.RateLimiter,
		cfg.NumEvents,
		cfg.RateLimiterConfig.GeneratorInterval,
		-1, // ReplicaID - not used by NexmarkSource
		-1, // Parallelism - not used by NexmarkSource
		source,
	)
	source.Source = dataflow.NewSource[OUT](id, sourceF)
	return source
}
