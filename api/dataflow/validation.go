package dataflow

import (
	"time"

	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
)

// To enforce tuple type match with respect to dataflow topology at compile
// time (e.g. output type of upstream operator should match input type of
// downstream operator), we define a set of typing interfaces implemented by
// all operators. They are used in AddStream() APIs to validate types when
// connecting operators.

// All operators with 1 upstream operator should implement this
type OperatorWith1InputStream[IN tuple.Tuple] interface {
	Operator

	InputTupleType1() IN
}

// All operators with 2 upstream operators should implement this
type OperatorWith2InputStream[IN1, IN2 tuple.Tuple] interface {
	Operator

	InputTupleType2() (IN1, IN2)
}

// All operators with 1 downstream operator should implement this
type OperatorWith1OutputStream[OUT tuple.Tuple] interface {
	Operator

	OutputTupleType1() OUT
}

// All source operator e.g. Source, KafkaSource, NexmarkSource .. should
// implement the AssignTimestampAndWatermark() API. This is used to check
// if an operator is a source operator for tsAssigner copy purposes
type SourceOperator[OUT tuple.Tuple] interface {
	Operator

	AssignTimestampAndWatermark(
		*ta.TimestampAssigner[OUT],
		time.Duration,
		time.Duration,
	)
}
