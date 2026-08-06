package query1

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/source"
)

// Query 1: Currency Conversion
// Convert each bid value from dollars to euros. Illustrates a simple
// transformation.

// Stream SQL:
// SELECT itemid, DOLTOEUR(price),
//    bidderId, bidTime
// FROM bid;

func Query1() *dataflow.Dataflow {
	df := dataflow.NewDataflow()

	cfg := config.DefaultNexmarkSourceConfig()

	// Define Source
	src := source.NewNexmarkBidSource(
		"source",
		cfg,
	)
	src.SetParallelism(1)
	dataflow.AddOperator(df, src)

	// Define Map
	mapper := Query1Mapper()
	mapper.SetParallelism(1)
	dataflow.AddOperator(df, mapper)

	// Define Sink (no-op)
	sink := dataflow.NewSink("sink", func(in *models.BidEvent) {})
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	// Connect Source -> Mapper
	dataflow.Add1To1Stream(df, src, mapper)
	// Connect Mapper -> Sink
	dataflow.Add1To1Stream(df, mapper, sink)

	return df
}
