package query2

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

func Query2Filter() *dataflow.Filter[*models.BidEvent] {
	filter := dataflow.NewFilter(
		"filter",
		func(in *models.BidEvent) bool {
			return in.V1 == 1007 ||
				in.V1 == 1020 ||
				in.V1 == 2001 ||
				in.V1 == 2019 ||
				in.V1 == 1087
		},
	)

	return filter
}
