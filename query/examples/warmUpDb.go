package examples

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

func WarmUpDbExample() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int, string]](
		"source",
		func(co collector.Collector) {
			for i := 0; i < 1000; i++ {
				co.Emit(&tuple.Tuple2[int, string]{
					V1: i,
					V2: HelperRandomString(5),
				})
			}
			log.Println("WarmUpDbExample: Source finished emitting data")
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(df, source)

	// Define key assigner
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[int, string]) int {
			return t.V1
		},
	)

	// Define Counter
	dbSetter := dataflow.NewStatefulMapper(
		"statefulMapper",
		keyAssigner,
		func(
			in *tuple.Tuple2[int, string],
			state *stateType.ValueState[*tuple.Tuple2[int, string]],
		) *tuple.Tuple1[int] {

			// Write the state
			state.Set(in)

			return &tuple.Tuple1[int]{
				V1: in.V1,
			}
		},
	)
	dbSetter.SetParallelism(2)
	dataflow.AddOperator(df, dbSetter)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple1[int]) {})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> mapper
	dataflow.Add1To1Stream(df, source, dbSetter)

	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, dbSetter, sink)

	return df
}
