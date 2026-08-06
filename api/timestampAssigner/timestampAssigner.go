package timestampAssigner

import "github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"

type TimestampAssignerAPI interface {
	isTimestampAssigner()
}

// TimestampAssigner extracts the timestamp field from the output tuple of the
// Source operator
type TimestampAssigner[OUT tuple.Tuple] struct {

	// User define extractor function
	timestampExtractor func(OUT) int64
}

func NewTimestampAssigner[OUT tuple.Tuple](
	timestampExtractor func(OUT) int64,
) *TimestampAssigner[OUT] {
	return &TimestampAssigner[OUT]{timestampExtractor: timestampExtractor}
}

// Dummy implementation to make TimestampAssigner implement TimestampAssignerAPI
func (ta *TimestampAssigner[OUT]) isTimestampAssigner() {}

// Extract timestamp from the output tuple
func (ta *TimestampAssigner[OUT]) GetTimestamp(t OUT) int64 {
	return ta.timestampExtractor(t)
}
