package examples

import (
	"math"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"golang.org/x/exp/rand"
)

func Example_Simple_Watermark_Query() *dataflow.Dataflow {

	df := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[float64, int64]](
		"source",
		func(co collector.Collector) {

			// Randomly generate monotonically increasing timestamps
			timeBase := int64(10)
			var time int64
			for {
				time = timeBase + int64(rand.Intn(10))

				co.Emit(&tuple.Tuple2[float64, int64]{
					V1: 12.345,
					V2: time,
				})

				timeBase += 10
			}
		},
	)
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[float64, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 2*time.Second, 10)
	source.SetParallelism(2)
	dataflow.AddOperator(df, source)

	// Define Mapper
	mapper := dataflow.NewMapper(
		"mapper",
		func(in *tuple.Tuple2[float64, int64]) *tuple.Tuple1[float64] {
			// Expansive computation
			res := 0.0
			for i := 0; i < 100; i++ {
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
