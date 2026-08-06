package dataflow

import (
	"log"
)

func (df *Dataflow) ConnectUpstreamToDownstream(
	upstream Operator,
	downstream Operator,
) {

	// TODO: correctness check - no circle etc.

	// Add upstream -> downstream dependency
	if dep, ok := df.Streams[upstream.GetName()]; ok {
		dep.DownstreamOperators = append(
			dep.DownstreamOperators,
			downstream.GetName(),
		)
		if len(dep.DownstreamOperators) > 1 {
			log.Fatalf(
				"%s: We do not support operator with more than one downstream operators\n",
				upstream.GetName(),
			)
		}
	} else {
		df.Streams[upstream.GetName()] = &Dependency{
			UpstreamOperators:   []string{},
			DownstreamOperators: []string{downstream.GetName()},
		}
	}

	// Add downstream -> upstream dependency
	if dep, ok := df.Streams[downstream.GetName()]; ok {
		dep.UpstreamOperators = append(
			dep.UpstreamOperators,
			upstream.GetName(),
		)
	} else {
		df.Streams[downstream.GetName()] = &Dependency{
			UpstreamOperators:   []string{upstream.GetName()},
			DownstreamOperators: []string{},
		}
	}
}
