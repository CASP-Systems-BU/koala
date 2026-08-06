package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
)

type Dataflow struct {

	// Operator map
	// Operator name -> Operator
	Operators map[string]Operator

	// Global operator ID counter
	// Increment for each unique abstract operator added
	operatorIDCounter uint16

	// Data depandencies
	// Operator name (upstream) -> list of Operator names (downstreams)
	Streams map[string]*Dependency
}

type Dependency struct {
	UpstreamOperators   []string
	DownstreamOperators []string
}

func NewDataflow() *Dataflow {
	return &Dataflow{
		Operators:         make(map[string]Operator),
		operatorIDCounter: 0,
		Streams:           make(map[string]*Dependency),
	}
}

func AddOperator(df *Dataflow, operator Operator) {

	// Give operator an unique id
	operator.SetId(df.operatorIDCounter)
	df.operatorIDCounter++

	// Check if the Operator Name is already present
	if _, ok := df.Operators[operator.GetName()]; ok {
		log.Fatalf("Operator Name already exists: %s\n", operator.GetName())
	}

	df.Operators[operator.GetName()] = operator
}

func (df *Dataflow) GetTotalNumTasks() int {
	numTasks := 0
	for _, op := range df.Operators {
		numTasks += op.GetParallelism()
	}
	return numTasks
}

// User API - add a 1-to-1 stream to the dataflow
// Add1To1Stream connects an upstream operator to a downstream operator in a
// dataflow. This API is for upstream operator with 1 output stream and
// downstream operator with 1 input stream. The upstream output type must match
// the downstream input type
func Add1To1Stream[MATCH_TYPE tuple.Tuple](
	df *Dataflow,
	upstream OperatorWith1OutputStream[MATCH_TYPE],
	downstream OperatorWith1InputStream[MATCH_TYPE],
) {
	// If downstream is stateful, configure its upstream operator to use
	// keyByCollector.
	if downstream.IsStatefulOperator() {
		downstream.SetKeyedOutputStreamForUpstream([]Operator{upstream})
	}

	// Add SubSupplier to downstream through the upstream's collector type
	upstream.AddSubSupplierToDownstream(downstream)

	df.ConnectUpstreamToDownstream(upstream, downstream)
}

// User API - add a 2-to-1 stream to the dataflow
// Add2To1Stream connects two upstream operators to a downstream operator in a
// dataflow. This API is for upstream operators with 1 output stream and
// downstream operator with 2 input streams. The upstream output types must
// match the downstream input types
func Add2To1Stream[MATCH_TYPE1, MATCH_TYPE2 tuple.Tuple](
	df *Dataflow,
	upstream1 OperatorWith1OutputStream[MATCH_TYPE1],
	upstream2 OperatorWith1OutputStream[MATCH_TYPE2],
	downstream OperatorWith2InputStream[MATCH_TYPE1, MATCH_TYPE2],
) {

	// If downstream is stateful, configure its upstream operators to use
	// keyByCollector.
	if downstream.IsStatefulOperator() {
		downstream.SetKeyedOutputStreamForUpstream(
			[]Operator{upstream1, upstream2},
		)
	}

	// Add SubSupplier to downstream through the upstreams' collector types
	upstream1.AddSubSupplierToDownstream(downstream)
	upstream2.AddSubSupplierToDownstream(downstream)

	df.ConnectUpstreamToDownstream(upstream1, downstream)
	df.ConnectUpstreamToDownstream(upstream2, downstream)
}
