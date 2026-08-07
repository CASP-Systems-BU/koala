package examples

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

// In this query, we assign source record with current system time as timestamp,
// and aggregate key counters every 500ms tumbling window.

func Example_TumblingWindow_Query() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[string, int64]](
		"source",
		func(co collector.Collector) {
			for {
				co.Emit(&tuple.Tuple2[string, int64]{
					V1: HelperRandomString(5),
					V2: time.Now().UnixNano(),
				})
			}
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[string, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(df, source)

	// Define Tumbling Window
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[string, int64]) string {
			return t.V1
		},
	)
	windowAggregator := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[int]] {
			return stateType.NewValueState(tuple.NewTuple1(0))
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int]], t *tuple.Tuple2[string, int64]) *stateType.ValueState[*tuple.Tuple1[int]] {
			val, _ := acc.Get()
			val.V1 += 1
			acc.Set(val)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int]]) *tuple.Tuple1[int] {
			val, _ := acc.Get()
			return val
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple1[int]], acc2 *stateType.ValueState[*tuple.Tuple1[int]]) *stateType.ValueState[*tuple.Tuple1[int]] {
			val1, _ := acc1.Get()
			val2, _ := acc2.Get()
			val1.V1 += val2.V1
			acc1.Set(val1)
			return acc1
		},
	)

	tumblingWindow := dataflow.NewTumblingWindow(
		"tumblingWindow",
		keyAssigner,
		windowAggregator,
		500*time.Millisecond,
	)
	tumblingWindow.SetParallelism(2)
	dataflow.AddOperator(df, tumblingWindow)

	// Define Sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple1[int]) {
			// throw away the result
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Define edges
	dataflow.Add1To1Stream(df, source, tumblingWindow)
	dataflow.Add1To1Stream(df, tumblingWindow, sink)

	return df
}
