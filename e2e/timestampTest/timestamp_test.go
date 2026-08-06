package timestampTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// In this test case, we test if the timestamp is correctly propagated
// from source to sink.

// store the sink result's tuple in a array
var sinkResults []*tuple.Tuple2[int64, int]

func TestTimestamp(t *testing.T) {

	// Sync channel to signal the end of the test
	var done = make(chan struct{})

	// initialize the sinkResults
	sinkResults = make([]*tuple.Tuple2[int64, int], 0)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, timestampQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(199890)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// wait for test to complete
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************
	println("Sink results: ", len(sinkResults))
	if len(sinkResults) != 2000 {
		t.Error("Expect 2000 results, but got ", len(sinkResults))
	}
	for _, result := range sinkResults {
		if result.V1*100 != result.GetTimestamp() {
			t.Error(
				"Expect the timestamp to be 100 times the data, but got ",
				result.GetTimestamp(),
			)
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func timestampQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int64, int64]](
		"source",
		func(co collector.Collector) {
			for i := range 2000 {
				// Send the timestamp along with the data
				co.Emit(&tuple.Tuple2[int64, int64]{
					V1: int64(i),       // data
					V2: int64(i * 100), // the timestamp
				})
			}
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[int64, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 1*time.Second, 10)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// KeyAssigner assigns keys to the stateful mapper
	keyAssigner := ka.NewKeyAssigner(func(t *tuple.Tuple2[int64, int64]) int64 {
		return t.V1
	})

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int64, int64],
			state *stateType.ValueState[*tuple.Tuple1[int]],
		) *tuple.Tuple2[int64, int] {

			// Read the state
			curCount, exist := state.Get()
			if !exist {
				curCount = tuple.NewTuple1(0)
			}

			// Increment the counter
			curCount.V1++

			// Write the state
			state.Set(curCount)

			return &tuple.Tuple2[int64, int]{
				V1: in.V1,
				V2: curCount.V1,
			}
		},
	)
	counter.SetParallelism(2)
	dataflow.AddOperator(query, counter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int64, int]) {
		// Throw away the result
		sinkResults = append(sinkResults, in)
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect Mapper -> Counter -> Sink
	dataflow.Add1To1Stream(query, source, counter)
	dataflow.Add1To1Stream(query, counter, sink)

	return query
}
