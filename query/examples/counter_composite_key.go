package examples

import (
	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

func Counter_Composite_Key() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[string, string]](
		"source",
		func(co collector.Collector) {
			for {
				co.Emit(&tuple.Tuple2[string, string]{
					V1: HelperRandomString(5),
					V2: "second_part_of_key",
				})
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(df, source)

	// Define key assigner
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[string, string]) string {
			return t.V1 + t.V2
		},
	)

	// Define Counter
	counter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[string, string],
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
