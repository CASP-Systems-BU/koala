package test

import (
	"log"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/query/borg/models"
	borgquery "github.com/CASP-Systems-BU/koala/query/borg/query"
)

// Tests per-(jobID, eventType) statistics: median, mean, max for cpu, ram, disk.
// Key is (jobID + "_" + eventType)
// Output is TaskRecord (event + stats): timestamp, jobID, taskIndex, machineID, eventType,
// cpuRequest, ramRequest, localDiskRequest, medianCpu, medianRam, medianDisk, count,
// meanCpu, meanRam, meanDisk, maxCpu, maxRam, maxDisk

var SampleInput = []models.TaskEvent{
	// Key "1001_1": cpu=0.5, ram=200, disk=5
	{V1: 100, V2: "", V3: 1001, V4: 0, V5: 1, V6: "1", V7: "alice",
		V8: 0, V9: 0, V10: 0.5, V11: 200, V12: 5, V13: true},
	// Key "1001_0": cpu=0.4, ram=200, disk=8
	{V1: 200, V2: "", V3: 1001, V4: 0, V5: 1, V6: "0", V7: "alice",
		V8: 0, V9: 0, V10: 0.4, V11: 200, V12: 8, V13: true},
	// Key "1001_1": cpu=0.3, ram=180, disk=10
	{V1: 300, V2: "", V3: 1001, V4: 1, V5: 1, V6: "1", V7: "alice",
		V8: 0, V9: 0, V10: 0.3, V11: 180, V12: 10, V13: true},
	// Key "1001_1": cpu=0.7, ram=230, disk=12
	{V1: 400, V2: "", V3: 1001, V4: 2, V5: 1, V6: "1", V7: "alice",
		V8: 0, V9: 0, V10: 0.7, V11: 230, V12: 12, V13: true},
	// Key "2001_1": cpu=0.6, ram=200, disk=15
	{V1: 500, V2: "", V3: 2001, V4: 0, V5: 2, V6: "1", V7: "bob",
		V8: 0, V9: 0, V10: 0.6, V11: 200, V12: 15, V13: true},
	// Key "1001_0": cpu=0.8, ram=250, disk=20
	{V1: 600, V2: "", V3: 1001, V4: 0, V5: 1, V6: "0", V7: "alice",
		V8: 0, V9: 0, V10: 0.8, V11: 250, V12: 20, V13: true},
}

// Expected results:
//
// Event 0: Key "1001_1", list=[{0.5,200,5,t=100}]
//   filtered=[{0.5,200,5}] n=1
//   median: cpu=0.5, ram=200, disk=5
//   mean:   cpu=0.5, ram=200, disk=5
//   max:    cpu=0.5, ram=200, disk=5
//
// Event 1: Key "1001_0", list=[{0.4,200,8,t=200}]
//   filtered=[{0.4,200,8}] n=1
//   median: cpu=0.4, ram=200, disk=8
//   mean:   cpu=0.4, ram=200, disk=8
//   max:    cpu=0.4, ram=200, disk=8
//
// Event 2: Key "1001_1", list=[{0.3,180,10,t=300},{0.5,200,5,t=100}]
//   sorted desc by t: [{0.3,180,10,t=300},{0.5,200,5,t=100}]
//   filtered=[both] n=2
//   median: cpu=sorted[1]=0.5, ram=sorted[1]=200, disk=sorted[1]=10
//   mean:   cpu=0.4, ram=190, disk=7.5
//   max:    cpu=0.5, ram=200, disk=10
//
// Event 3: Key "1001_1", list=[{0.7,230,12,t=400},{0.3,180,10,t=300},{0.5,200,5,t=100}]
//   sorted desc by t: [{0.7,230,12,t=400},{0.3,180,10,t=300},{0.5,200,5,t=100}]
//   filtered=[all 3] n=3
//   median: cpu=sorted[1]=0.5, ram=sorted[1]=200, disk=sorted[1]=10
//   mean:   cpu=0.5, ram=203.33..., disk=9
//   max:    cpu=0.7, ram=230, disk=12
//
// Event 4: Key "2001_1", list=[{0.6,200,15,t=500}]
//   filtered=[{0.6,200,15}] n=1
//   median: cpu=0.6, ram=200, disk=15
//   mean:   cpu=0.6, ram=200, disk=15
//   max:    cpu=0.6, ram=200, disk=15
//
// Event 5: Key "1001_0", 2 records [0.4,0.8]->median=vals[1]=0.8; [200,250]->250; [8,20]->20
//   sorted desc by t: [{0.8,250,20,t=600},{0.4,200,8,t=200}]
//   filtered=[both] n=2
//   median: cpu=sorted[1]=0.8, ram=sorted[1]=250, disk=sorted[1]=20
//   mean:   cpu=0.6, ram=225, disk=14
//   max:    cpu=0.8, ram=250, disk=20

var ExpectedResults []borgquery.TaskRecord

func init() {
	meanCpu := (float64(0.4) + float64(0.8)) / float64(2)

	expected := []*borgquery.TaskRecord{
		// Event 0: Key "1001_1", 1 record
		{V1: 100, V2: 1001, V3: 0, V4: 1, V5: "1", V6: 0.5, V7: 200, V8: 5,
			V9: 0.5, V10: 200, V11: 5, V12: 1, V13: 0.5, V14: 200, V15: 5, V16: 0.5, V17: 200, V18: 5},
		// Event 1: Key "1001_0", 1 record
		{V1: 200, V2: 1001, V3: 0, V4: 1, V5: "0", V6: 0.4, V7: 200, V8: 8,
			V9: 0.4, V10: 200, V11: 8, V12: 1, V13: 0.4, V14: 200, V15: 8, V16: 0.4, V17: 200, V18: 8},
		// Event 2: Key "1001_1", 2 records
		{V1: 300, V2: 1001, V3: 1, V4: 1, V5: "1", V6: 0.3, V7: 180, V8: 10,
			V9: 0.5, V10: 200, V11: 10, V12: 2, V13: 0.4, V14: 190, V15: 7.5, V16: 0.5, V17: 200, V18: 10},
		// Event 3: Key "1001_1", 3 records
		{V1: 400, V2: 1001, V3: 2, V4: 1, V5: "1", V6: 0.7, V7: 230, V8: 12,
			V9: 0.5, V10: 200, V11: 10, V12: 3, V13: 0.5, V14: 203.33333333333334, V15: 9, V16: 0.7, V17: 230, V18: 12},
		// Event 4: Key "2001_1", 1 record
		{V1: 500, V2: 2001, V3: 0, V4: 2, V5: "1", V6: 0.6, V7: 200, V8: 15,
			V9: 0.6, V10: 200, V11: 15, V12: 1, V13: 0.6, V14: 200, V15: 15, V16: 0.6, V17: 200, V18: 15},
		// Event 5: Key "1001_0", 2 records
		{V1: 600, V2: 1001, V3: 0, V4: 1, V5: "0", V6: 0.8, V7: 250, V8: 20,
			V9: 0.8, V10: 250, V11: 20, V12: 2, V13: meanCpu, V14: 225, V15: 14, V16: 0.8, V17: 250, V18: 20},
	}
	ExpectedResults = make([]borgquery.TaskRecord, len(expected))
	for i, p := range expected {
		p.SetTimestamp(-1)
		ExpectedResults[i] = *p
	}
}

var results []borgquery.TaskRecord
var resultCh chan struct{}

const testListStateSize = 4

func TestJobEventStatsCorrectness(t *testing.T) {
	results = make([]borgquery.TaskRecord, 0)
	resultCh = make(chan struct{}, len(SampleInput))

	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 3
	_, _, _ = testutils.DeployJob(numWorkers, JobEventStats, config)

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
		for i := range results {
			if !reflect.DeepEqual(results[i], ExpectedResults[i]) {
				t.Errorf(
					"Mismatch at index %d:\n  expected: %+v\n  actual:   %+v",
					i, ExpectedResults[i], results[i],
				)
			}
		}
		t.Fatalf("Results mismatch")
	}

	testutils.CleanUpDataFolder()
}

func JobEventStats() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	src := dataflow.NewSource[*models.TaskEvent](
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

	// Key by (jobID, eventType)
	keyAssigner := ka.NewKeyAssigner(
		func(t *models.TaskEvent) string {
			return strconv.FormatInt(t.V3, 10) + "_" + t.V6
		},
	)

	maxListSize := testListStateSize

	getCpu := func(r *borgquery.TaskRecord) float64 { return r.V6 }
	getRam := func(r *borgquery.TaskRecord) float64 { return r.V7 }
	getDisk := func(r *borgquery.TaskRecord) float64 { return r.V8 }

	jobEventStats := dataflow.NewStatefulMapper(
		"jobEventStats",
		keyAssigner,
		func(
			in *models.TaskEvent,
			state *stateType.ListState[*borgquery.TaskRecord],
		) *borgquery.TaskRecord {
			list := state.Get()

			rec := &borgquery.TaskRecord{
				V1: in.V1, V2: in.V3, V3: in.V4, V4: in.V5, V5: in.V6,
				V6: in.V10, V7: in.V11, V8: in.V12,
			}
			list = append(list, rec)

			sort.Slice(list, func(i, j int) bool {
				return list[i].V1 > list[j].V1
			})
			if len(list) > maxListSize {
				list = list[:maxListSize]
			}

			var filtered []*borgquery.TaskRecord
			for _, r := range list {
				if !borgquery.IsWarmupRecord(r) {
					filtered = append(filtered, r)
				}
			}
			n := len(filtered)

			rec.V9, rec.V10, rec.V11 = borgquery.MedianFromFiltered(
				filtered,
				getCpu,
			), borgquery.MedianFromFiltered(
				filtered,
				getRam,
			), borgquery.MedianFromFiltered(
				filtered,
				getDisk,
			)
			rec.V12 = int64(n)
			rec.V13, rec.V14, rec.V15 = borgquery.MeanFromFiltered(
				filtered,
				getCpu,
			), borgquery.MeanFromFiltered(
				filtered,
				getRam,
			), borgquery.MeanFromFiltered(
				filtered,
				getDisk,
			)
			rec.V16, rec.V17, rec.V18 = borgquery.MaxFromFiltered(
				filtered,
				getCpu,
			), borgquery.MaxFromFiltered(
				filtered,
				getRam,
			), borgquery.MaxFromFiltered(
				filtered,
				getDisk,
			)

			state.Update(list)
			return rec
		},
	)
	jobEventStats.SetParallelism(1)
	dataflow.AddOperator(df, jobEventStats)

	sink := dataflow.NewSink(
		"sink",
		func(t *borgquery.TaskRecord) {
			results = append(results, *t)
			resultCh <- struct{}{}
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, jobEventStats)
	dataflow.Add1To1Stream(df, jobEventStats, sink)

	return df
}
