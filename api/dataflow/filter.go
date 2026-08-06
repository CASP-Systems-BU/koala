package dataflow

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
)

type Filter[IN tuple.Tuple] struct {
	*Flatmap[IN, IN]
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*Filter[tuple.Tuple])(nil)
var _ OperatorWith1OutputStream[tuple.Tuple] = (*Filter[tuple.Tuple])(nil)

// NewFilter creates a new Filter struct
func NewFilter[IN tuple.Tuple](
	id string,
	f func(IN) bool,
) *Filter[IN] {
	filter := &Filter[IN]{
		Flatmap: NewFlatmap[IN, IN](
			id,
			func(in IN, collector collector.Collector) {
				if f(in) {
					// Input record uses reused memory space managed by
					// Supplier. It's not safe to direcly pass it to Collector.
					collector.Emit(in.Copy())
				}
			},
		),
	}

	return filter
}
