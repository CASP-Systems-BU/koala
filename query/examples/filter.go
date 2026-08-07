package examples

import (
	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

func Example_Filter_Query() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[string, float64]](
		"source",
		func(co collector.Collector) {
			for {
				co.Emit(&tuple.Tuple2[string, float64]{
					V1: HelperRandomString(5),
					V2: 12.345,
				})
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(df, source)

	// Define Filter
	// Filter out records with first character 'a' or 'b'
	filter := dataflow.NewFilter(
		"filter",
		func(in *tuple.Tuple2[string, float64]) bool {
			return in.V1[0] != 'a' && in.V1[0] != 'b'
		},
	)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[string, float64]) {
		// Throw away the result
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Counter
	dataflow.Add1To1Stream(df, source, filter)

	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, filter, sink)

	return df
}
