package query1

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/models"
)

func Query1Mapper() *dataflow.Mapper[*models.BidEvent, *models.BidEvent] {
	mapper := dataflow.NewMapper(
		"mapper",
		func(in *models.BidEvent) *models.BidEvent {
			in.V3 = int64(float64(in.V3) * 0.85)
			out := in.Copy().(*models.BidEvent)
			return out
		},
	)

	return mapper
}
