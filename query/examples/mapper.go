package examples

import (
	"math"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

func Example_Simple_Query() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple1[float64]](
		"source",
		func(co collector.Collector) {
			for {
				co.Emit(&tuple.Tuple1[float64]{
					V1: 12.345,
				})
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(df, source)

	// Define Mapper
	mapper := dataflow.NewMapper(
		"mapper",
		func(in *tuple.Tuple1[float64]) *tuple.Tuple1[float64] {

			// A dummy compute-intensive task - calculate x^x for 100 times
			res := 0.0
			for range 100 {
				res = math.Pow(in.V1, in.V1)
			}
			return &tuple.Tuple1[float64]{
				V1: res,
			}
		},
	)
	mapper.SetParallelism(2)
	dataflow.AddOperator(df, mapper)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple1[float64]) {
		// Throw away the result
	})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Mapper
	dataflow.Add1To1Stream(df, source, mapper)

	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
