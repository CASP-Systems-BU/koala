package examples

import (
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"golang.org/x/exp/rand"
)

func Example_Counter_Watermark_Query() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[string, int64]](
		"source",
		func(co collector.Collector) {
			// Randomly generate monotonically increasing timestamps
			timeBase := int64(10)
			var time int64
			for {
				time = timeBase + int64(rand.Intn(10))

				co.Emit(&tuple.Tuple2[string, int64]{
					V1: HelperRandomString(5),
					V2: time,
				})

				timeBase += 10
			}
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[string, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 2*time.Second, 10)
	source.SetParallelism(2)
	dataflow.AddOperator(df, source)

	// Define key assigner
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[string, int64]) string {
			return t.V1
		},
	)

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[string, int64],
			state *stateType.ValueState[*tuple.Tuple1[int]],
		) *tuple.Tuple2[string, int] {

			// Read the state
			curCount, exist := state.Get()
			if !exist {
				curCount = tuple.NewTuple1(0)
			}

			// Increment the counter
			curCount.V1++

			// Write the state
			state.Set(curCount)

			return &tuple.Tuple2[string, int]{
				V1: in.V1,
				V2: curCount.V1,
			}
		},
	)
	counter.SetParallelism(2)
	dataflow.AddOperator(df, counter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[string, int]) {
		// Throw away the result
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Counter
	dataflow.Add1To1Stream(df, source, counter)

	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, counter, sink)

	return df
}
