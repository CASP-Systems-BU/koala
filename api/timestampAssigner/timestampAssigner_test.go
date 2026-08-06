package timestampAssigner_test

import (
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
)

func TestTimestampAssignerWithSingleTimestampField(t *testing.T) {

	ta := timestampAssigner.NewTimestampAssigner(
		func(t *tuple.Tuple1[int64]) int64 {
			return t.V1
		},
	)

	for i := range 100 {
		timestamp := ta.GetTimestamp(&tuple.Tuple1[int64]{V1: int64(i)})
		if timestamp != int64(i) {
			t.Fatalf("Expected timestamp %d, got %d", i, timestamp)
		}
	}
}
