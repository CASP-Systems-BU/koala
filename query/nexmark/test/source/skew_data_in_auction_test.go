package sourceTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

var AuctionTotalCount int64
var AuctionWithHotkey int64
var BidTotalCount int64
var BidWithHotAuction int64
var BidWithHotBidder int64

// In this test, we check whether the skewness in generated events is as
// expected. Since we use a random generator to decide whether an event should
// have a hot key, we allow up to 0.01 deviation.

func TestNewRandAuctionSourceSkewed(t *testing.T) {
	//Total number of generated auctionEvents
	AuctionTotalCount = 0
	//Total number of generated auctionEvents with hotkey
	AuctionWithHotkey = 0

	configuration := configuration.Default()
	configuration.BufferSize = 20
	numWorkers := 6
	testutils.DeployJob(numWorkers, newAuctionSourceSkewedQuery, configuration)
	time.Sleep(30 * time.Second)
	log.Println("got", AuctionTotalCount, "results")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	hotRatio := float64(AuctionWithHotkey) / float64(AuctionTotalCount)
	log.Println("hot ratio is ", hotRatio)

	// In this test we set hotRatio = 0.5
	if hotRatio < 0.49 || hotRatio > 0.51 {
		t.Errorf("Hot ratio %.2f out of expected range", hotRatio)
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()

}

func newAuctionSourceSkewedQuery() *dataflow.Dataflow {
	query := dataflow.NewDataflow()
	cfg := config.DefaultNexmarkSourceConfig()

	// How many percentage of auction events will have hot key(sellerId)
	cfg.HotSellerRatio = 0.5
	// Among how many sellerIds there will be a hot key
	cfg.HotSellerRange = 1000
	// Total events each parallel source will generate
	cfg.NumEvents = 1000000
	// How many personIds will be considered active when generating
	// auction event
	cfg.NumActivePeople = 100

	src := source.NewNexmarkAuctionSource(
		"auctionSource",
		cfg,
	)
	src.SetParallelism(5)
	dataflow.AddOperator(query, src)

	sink := dataflow.NewSink(
		"sink",
		func(t *models.AuctionEvent) {
			AuctionTotalCount++
			if (t.V8-config.FIRST_PERSON_ID)%cfg.HotSellerRange == 0 {
				AuctionWithHotkey++
			}
		},
	)
	sink.SetParallelism(1)

	dataflow.AddOperator(query, sink)
	dataflow.Add1To1Stream(query, src, sink)

	return query
}
