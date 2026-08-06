package sourceTest

import (
	"log"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

// In this test, first we generate a given number of bid events, then use
// CalculateSupposedEvent to generate deterministic expected events. The test
// compares them against the events emitted by our source under a given
// parallelism to ensure they match exactly.

var SourceResultBid []models.BidEvent

func TestNewRandBidSourceMultiple(t *testing.T) {
	configuration := configuration.Default()
	configuration.BufferSize = 20
	numWorkers := 6
	// Parallelism of the source, please make sure that it is the same as the
	// parallelism in dataflow
	parallelism := 3
	cfg := config.DefaultNexmarkSourceConfig()
	testutils.DeployJob(numWorkers, newBidSourceMultipleQuery, configuration)
	time.Sleep(10 * time.Second)
	sort.Slice(SourceResultBid, func(i, j int) bool {
		return CompareBid(SourceResultBid[i], SourceResultBid[j])
	})
	log.Println("got", len(SourceResultBid), "results")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	var random []*rand.Rand
	// Generate all random generators we need
	for i := 0; i < parallelism; i++ {
		random = append(
			random,
			rand.New(rand.NewSource(int64(i)+source.RandDrift)),
		)
	}

	// Compare the records we got from sink with deterministic expected events
	for i, record := range SourceResultBid {
		supposedResult := CalculateNextEvent(
			config.Bid,
			int64(i),
			random[i%parallelism],
			cfg,
		)
		if !record.Equal(supposedResult) {
			t.Errorf(
				"result "+strconv.Itoa(i)+" not the same",
				record,
				supposedResult,
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func newBidSourceMultipleQuery() *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Set total number of event that we want a source task to generate
	cfg.NumEvents = 46000

	src := source.NewNexmarkBidSource(
		"bidSource",
		cfg,
	)
	src.SetParallelism(3)
	dataflow.AddOperator(query, src)

	sink := dataflow.NewSink(
		"sink",
		func(t *models.BidEvent) {
			SourceResultBid = append(SourceResultBid, *t)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)
	dataflow.Add1To1Stream(query, src, sink)

	return query
}

func CompareBid(bid1 models.BidEvent, bid2 models.BidEvent) bool {
	return bid1.V4 < bid2.V4
}
