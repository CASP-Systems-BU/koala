package test

import (
	"log"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/models"
	azurequery "github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/query"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF cpuReadings LOGIC CHANGES

// AzureEvent: timestamp, vmID, minCpu, maxCpu, avgCpu
var SampleInput = []models.AzureEvent{
	{V1: 100, V2: "vm1", V3: 0.1, V4: 0.5, V5: 0.3},
	{V1: 200, V2: "vm2", V3: 0.2, V4: 0.6, V5: 0.4},
	{V1: 280, V2: "vm3", V3: 0.3, V4: 0.7, V5: 0.5},
	{V1: 350, V2: "vm1", V3: 0.05, V4: 0.4, V5: 0.25},
	{V1: 400, V2: "vm4", V3: 0.0, V4: 0.3, V5: 0.15},
	{V1: 580, V2: "vm5", V3: 0.4, V4: 0.9, V5: 0.65},
	{V1: 900, V2: "vm6", V3: 0.02, V4: 0.2, V5: 0.1},
	{V1: 950, V2: "vm3", V3: 0.35, V4: 0.85, V5: 0.6},
	{V1: 1000, V2: "vm1", V3: 0.15, V4: 0.55, V5: 0.35},
}

// ExpectedResults: vmID, medianMinCpu, medianMaxCpu, medianAvgCpu, count
// Median uses list[n/2] for sorted values.
var ExpectedResults = []tuple.Tuple5[string, float64, float64, float64, int64]{
	{V1: "vm1", V2: 0.1, V3: 0.5, V4: 0.3, V5: 1},
	{V1: "vm2", V2: 0.2, V3: 0.6, V4: 0.4, V5: 1},
	{V1: "vm3", V2: 0.3, V3: 0.7, V4: 0.5, V5: 1},
	{V1: "vm1", V2: 0.1, V3: 0.5, V4: 0.3, V5: 2},
	{V1: "vm4", V2: 0.0, V3: 0.3, V4: 0.15, V5: 1},
	{V1: "vm5", V2: 0.4, V3: 0.9, V4: 0.65, V5: 1},
	{V1: "vm6", V2: 0.02, V3: 0.2, V4: 0.1, V5: 1},
	{V1: "vm3", V2: 0.35, V3: 0.85, V4: 0.6, V5: 2},
	{V1: "vm1", V2: 0.1, V3: 0.5, V4: 0.3, V5: 3},
}

var results []tuple.Tuple5[string, float64, float64, float64, int64]
var resultCh chan struct{}

const testListStateSize = 4

func medianFromFiltered(
	filtered []*azurequery.CpuRecord,
	getVal func(*azurequery.CpuRecord) float64,
) float64 {
	if len(filtered) == 0 {
		return 0.0
	}
	vals := make([]float64, len(filtered))
	for i, r := range filtered {
		vals[i] = getVal(r)
	}
	sort.Float64s(vals)
	return vals[len(vals)/2]
}

func TestCpuReadingsCorrectness(t *testing.T) {
	results = make([]tuple.Tuple5[string, float64, float64, float64, int64], 0)
	resultCh = make(chan struct{}, len(SampleInput))

	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 3
	_, _, _ = testutils.DeployJob(numWorkers, cpuReadingsTestDataflow, config)

	go func() {
		for i := 0; i < len(ExpectedResults); i++ {
			<-resultCh
		}
		done <- struct{}{}
	}()

	<-done
	log.Println("[E2E] Test completed")

	if len(results) != len(ExpectedResults) {
		t.Fatalf(
			"Incorrect number of results=%d, expected=%d",
			len(results),
			len(ExpectedResults),
		)
	}

	log.Println("Actual results:", results)

	if !reflect.DeepEqual(results, ExpectedResults) {
		t.Errorf(
			"Results mismatch:\n  expected: %v\n  actual:   %v",
			ExpectedResults,
			results,
		)
	}

	testutils.CleanUpDataFolder()
}

func cpuReadingsTestDataflow() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	src := dataflow.NewSource[*models.AzureEvent](
		"source",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(200 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	vmKeyAssigner := ka.NewKeyAssigner(
		func(t *models.AzureEvent) string {
			return t.V2
		},
	)

	maxListSize := testListStateSize

	getMinCpu := func(r *azurequery.CpuRecord) float64 { return r.V3 }
	getMaxCpu := func(r *azurequery.CpuRecord) float64 { return r.V4 }
	getAvgCpu := func(r *azurequery.CpuRecord) float64 { return r.V5 }

	cpuReadingStats := dataflow.NewStatefulMapper(
		"cpuReadingStats",
		vmKeyAssigner,
		func(
			in *models.AzureEvent,
			state *stateType.ListState[*azurequery.CpuRecord],
		) *azurequery.CpuRecord {
			list := state.Get()

			rec := &azurequery.CpuRecord{
				V1: in.V1,
				V2: in.V2,
				V3: in.V3,
				V4: in.V4,
				V5: in.V5,
			}
			list = append(list, rec)

			sort.Slice(list, func(i, j int) bool {
				return list[i].V1 > list[j].V1
			})
			if len(list) > maxListSize {
				list = list[:maxListSize]
			}

			var filtered []*azurequery.CpuRecord
			for _, r := range list {
				if !azurequery.IsWarmupRecord(r) {
					filtered = append(filtered, r)
				}
			}
			n := len(filtered)

			medianMin := medianFromFiltered(filtered, getMinCpu)
			medianMax := medianFromFiltered(filtered, getMaxCpu)
			medianAvg := medianFromFiltered(filtered, getAvgCpu)

			rec.V6, rec.V7, rec.V8 = medianMin, medianMax, medianAvg
			rec.V9 = int64(n)

			state.Update(list)

			return &azurequery.CpuRecord{
				V1: in.V1,
				V2: in.V2,
				V3: in.V3,
				V4: in.V4,
				V5: in.V5,
				V6: medianMin,
				V7: medianMax,
				V8: medianAvg,
				V9: int64(n),
			}
		},
	)
	cpuReadingStats.SetParallelism(1)
	dataflow.AddOperator(df, cpuReadingStats)

	sink := dataflow.NewSink(
		"sink",
		func(t *azurequery.CpuRecord) {
			result := tuple.Tuple5[string, float64, float64, float64, int64]{
				V1: t.V2,
				V2: t.V6,
				V3: t.V7,
				V4: t.V8,
				V5: t.V9,
			}
			results = append(results, result)
			resultCh <- struct{}{}
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, cpuReadingStats)
	dataflow.Add1To1Stream(df, cpuReadingStats, sink)

	return df
}
