package sourceTest

import (
	"database/sql"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

// In this test we test whether the RateLimiter works correctly by setting a
// fixed rate limit and verifying whether the average input rate(every 5s) from
// sink fits the limited rate.

func TestRandSourceRateLimit(t *testing.T) {
	// Please make sure that all below parameters are the same as the parameters
	// in dataflow Defines how many events should be generated during
	// generatorInterval, in this test there will be not rate change
	rateLimit := 10000
	// Parallelism of the source
	sourceParallelism := 2

	configuration := configuration.Default()
	configuration.BufferSize = 20
	numWorkers := 3

	testutils.DeployJob(
		numWorkers,
		RatelimiterTestQuery,
		configuration,
	)
	// Wait until all events are generated
	time.Sleep(104 * time.Second)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Get sink input rate
	sinkInputRate, err := getSinkInputRates(t)
	if err != nil {
		t.Fatalf("Failed to get sinkInputRate: %v", err)
	}
	// Check if average input rate in sink per 5s fit the ratelimit
	// We skip the first 8 and last 2 data points
	for sec, rate := range sinkInputRate[8 : len(sinkInputRate)-2] {
		if float64(rate) > float64(rateLimit*sourceParallelism)*1.02 ||
			float64(rate) < float64(rateLimit*sourceParallelism)*0.98 {
			t.Fatalf(
				"period %d has average rate %f, doesn't fit rate limit %d",
				sec, rate, rateLimit,
			)
		}
	}

	t.Logf("Generated events with rate limit %d per source", rateLimit)

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func RatelimiterTestQuery() *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()
	cfg.RateLimiterConfig.UseRateLimit = true
	// RateLimit, by default is a per 200ms value, can be changed by changing
	// GeneratorInterval
	cfg.RateLimiterConfig.RateLimit = []int{10000}
	cfg.RateLimiterConfig.RateChangeInterval = []int{1}
	// We will fix GeneratorInterval to 1s in this test
	cfg.RateLimiterConfig.GeneratorInterval = utils.Duration(time.Second)
	// Total events we will generate
	cfg.NumEvents = int64(1000000)

	src := source.NewNexmarkBidSource(
		"bidSource",
		cfg,
	)
	src.SetParallelism(2)
	dataflow.AddOperator(query, src)

	sink := dataflow.NewSink(
		"sink",
		func(t *models.BidEvent) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	dataflow.Add1To1Stream(query, src, sink)
	return query
}

// GetSinkInputRates retrieves the InputRate metrics of the sink operator from
// the metricCollector.db database.
// Returns:
// - []float64: A slice of InputRate values (records/sec), one per reporting
// interval.
// - error:     Non-nil if the database query or row scan fails.

func getSinkInputRates(t *testing.T) ([]float64, error) {
	db, err := sql.Open("sqlite3", "data/metricCollector.db")
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	query := `
		SELECT metric_value
		FROM metrics
		WHERE metric_type = 'InputRate' AND operator_id LIKE 'sink:%';
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rates []float64
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		rates = append(rates, value)
	}

	return rates, nil
}
