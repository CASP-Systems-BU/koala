package sourceTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
	"github.com/CASP-Systems-BU/koala/query/nexmark/source"
)

// In this test, we check whether the skewness in generated events is as
// expected. Since we use a random generator to decide whether an event should
// have a hot key, we allow up to 0.01 deviation.

func TestNewRandBidSourceSkewed(t *testing.T) {
	//Total number of generated bidEvents
	BidTotalCount = 0
	//Total number of generated auctionEvents with hotAuctionId
	BidWithHotAuction = 0
	//Total number of generated auctionEvents with hotBidderId
	BidWithHotBidder = 0
	configuration := configuration.Default()
	configuration.BufferSize = 20
	numWorkers := 6
	testutils.DeployJob(numWorkers, newBidSourceSkewedQuery, configuration)
	time.Sleep(60 * time.Second)
	log.Println("got", BidTotalCount, "results")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	hotAuctionRatio := float64(BidWithHotAuction) / float64(BidTotalCount)
	log.Println("hot auction ratio is ", hotAuctionRatio)
	// In this test we set hotAuctionRatio = 0.5
	if hotAuctionRatio < 0.49 || hotAuctionRatio > 0.51 {
		t.Errorf(
			"Hot auction ratio %.2f out of expected range",
			hotAuctionRatio,
		)
	}

	hotBidderRatio := float64(BidWithHotBidder) / float64(BidTotalCount)
	log.Println("hot bidder ratio is ", hotBidderRatio)
	// In this test we set hotBidderRatio = 0.3
	if hotBidderRatio < 0.29 || hotBidderRatio > 0.31 {
		t.Errorf("Hot bidder ratio %.2f out of expected range", hotBidderRatio)
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func newBidSourceSkewedQuery() *dataflow.Dataflow {
	query := dataflow.NewDataflow()
	cfg := config.DefaultNexmarkSourceConfig()
	// How many percentage of bid events will have hot key(auctionId)
	cfg.HotAuctionRatio = 0.5
	// Among how many auctionIds there will be a hot key
	cfg.HotAuctionRange = 1000
	// How many bid events will have hot key(bidderId)
	cfg.HotBidderRatio = 0.3
	// Among how many bidderIds there will be a hot key
	cfg.HotBidderRange = 1000
	// Total events each parallel source will generate
	cfg.NumEvents = 10000000
	// How many personIds will be considered active when generating bid event
	cfg.NumActivePeople = 100
	// How many auctionIds will be considered active when generating bid event
	cfg.NumActiveAuctions = 300

	src := source.NewNexmarkBidSource(
		"bidSource",
		cfg,
	)
	src.SetParallelism(5)
	dataflow.AddOperator(query, src)

	sink := dataflow.NewSink(
		"sink",
		func(t *models.BidEvent) {
			BidTotalCount++
			if (t.V1-config.FIRST_AUCTION_ID)%cfg.HotAuctionRange == 0 {
				BidWithHotAuction++
			}
			if (t.V2-config.FIRST_PERSON_ID)%cfg.HotBidderRange == 0 {
				BidWithHotBidder++
			}
		},
	)
	sink.SetParallelism(1)

	dataflow.AddOperator(query, sink)
	dataflow.Add1To1Stream(query, src, sink)

	return query
}
