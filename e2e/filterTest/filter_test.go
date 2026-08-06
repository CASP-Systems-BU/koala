package filterTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// In this test, we test the filter operator
// by filtering out records with first character 'a' or 'b'.

var REPEAT = 1000
var faultySink = false

func TestWordCountKey(t *testing.T) {
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, filterQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	// This flatmap query doesn't need watermark and timestamp assigner
	// We add them here only to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(10_000_000)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be compeleted
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check sink
	if faultySink {
		t.Errorf("[E2E] Sink received records with first character 'a' or 'b'")
	}

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func filterQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewSource[*tuple.Tuple2[string, int64]](
		"source",
		func(co collector.Collector) {
			for range REPEAT {
				co.Emit(&tuple.Tuple2[string, int64]{V1: "aaaaa", V2: 1})
				co.Emit(&tuple.Tuple2[string, int64]{V1: "bbbbb", V2: 1})
				co.Emit(&tuple.Tuple2[string, int64]{V1: "ccccc", V2: 1})
				co.Emit(&tuple.Tuple2[string, int64]{V1: "ddddd", V2: 1})
				co.Emit(&tuple.Tuple2[string, int64]{V1: "eeeee", V2: 1})
			}
			co.Emit(&tuple.Tuple2[string, int64]{V1: "END", V2: 10_000_000})
		},
	)

	// We add watarmark and timestamp assigner here only to detect the end of
	// the test
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[string, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Define filter
	filter := dataflow.NewFilter(
		"filter",
		func(in *tuple.Tuple2[string, int64]) bool {
			// Filter out records with first character 'a' or 'b'
			return in.V1[0] != 'a' && in.V1[0] != 'b'
		},
	)
	filter.SetParallelism(2)
	dataflow.AddOperator(query, filter)

	// Define sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[string, int64]) {
			if t.V1[0] == 'a' || t.V1[0] == 'b' {
				faultySink = true
			}
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, filter)
	dataflow.Add1To1Stream(query, filter, sink)

	return query
}
