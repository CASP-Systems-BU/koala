package dataflow_test

import (
	"testing"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

// Test the Aggregator struct with integer types
func TestAggregatorInt(t *testing.T) {
	// types
	type INTuple = *tuple.Tuple2[int, int]
	type OUTTuple = *tuple.Tuple1[int]

	// newFunc creates a new accumulator
	agg := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[int]] {
			return stateType.NewValueState(tuple.NewTuple1(0))
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int]], in INTuple) *stateType.ValueState[*tuple.Tuple1[int]] {

			tupleVal, _ := acc.Get()
			tupleVal.V1 += in.V1 + in.V2
			acc.Set(tupleVal)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[int]]) OUTTuple {
			tupleVal, _ := acc.Get()
			return tupleVal
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple1[int]], acc2 *stateType.ValueState[*tuple.Tuple1[int]]) *stateType.ValueState[*tuple.Tuple1[int]] {
			tupleVal1, _ := acc1.Get()
			tupleVal2, _ := acc2.Get()
			tupleVal1.V1 += tupleVal2.V1
			acc1.Set(tupleVal1)
			return acc1
		},
	)

	// test newFunc
	acc := agg.NewFunc()
	tupleVal, _ := acc.Get()
	if tupleVal.V1 != 0 {
		t.Errorf("expected 0, got %v", acc)
	}

	// test addFunc
	acc = agg.AddFunc(acc, &tuple.Tuple2[int, int]{V1: 1, V2: 2})
	tupleVal, _ = acc.Get()
	if tupleVal.V1 != 3 {
		t.Errorf("expected 3, got %v", acc)
	}

	// test outFunc
	out := agg.OutFunc(acc)
	if *(*int)(out.GetField(0)) != 3 {
		t.Errorf("expected 3, got %v", out.GetField(0))
	}

	// test mergeFunc
	acc1 := agg.NewFunc()
	tupleVal, _ = acc1.Get()
	tupleVal.V1 = 1
	acc2 := agg.NewFunc()
	tupleVal, _ = acc2.Get()
	tupleVal.V1 = 2
	acc3 := agg.MergeFunc(acc1, acc2)
	tupleVal, _ = acc3.Get()
	if tupleVal.V1 != 3 {
		t.Errorf("expected 3, got %v", tupleVal)
	}
}

// Test the Aggregator struct with string types
func TestAggregatorString(t *testing.T) {
	// types
	type STRTuple = *tuple.Tuple2[string, string]
	type OUTTuple = *tuple.Tuple1[string]

	// newFunc creates a new accumulator
	agg := dataflow.NewAggregator(
		func() *stateType.ValueState[*tuple.Tuple1[string]] {
			return stateType.NewValueState(tuple.NewTuple1(""))
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[string]], in STRTuple) *stateType.ValueState[*tuple.Tuple1[string]] {
			tupleVal, _ := acc.Get()
			tupleVal.V1 += in.V1 + in.V2
			acc.Set(tupleVal)
			return acc
		},
		func(acc *stateType.ValueState[*tuple.Tuple1[string]]) OUTTuple {
			tupleVal, _ := acc.Get()
			return tupleVal
		},
		func(acc1 *stateType.ValueState[*tuple.Tuple1[string]], acc2 *stateType.ValueState[*tuple.Tuple1[string]]) *stateType.ValueState[*tuple.Tuple1[string]] {
			tupleVal1, _ := acc1.Get()
			tupleVal2, _ := acc2.Get()
			tupleVal1.V1 += tupleVal2.V1
			acc1.Set(tupleVal1)
			return acc1
		},
	)

	// test newFunc
	acc := agg.NewFunc()
	tupleVal, _ := acc.Get()
	if tupleVal.V1 != "" {
		t.Errorf("expected empty string, got %v", acc)
	}

	// test addFunc
	acc = agg.AddFunc(acc, &tuple.Tuple2[string, string]{V1: "a", V2: "b"})
	tupleVal, _ = acc.Get()
	if tupleVal.V1 != "ab" {
		t.Errorf("expected ab, got %v", acc)
	}

	// test outFunc
	out := agg.OutFunc(acc)
	if *(*string)(out.GetField(0)) != "ab" {
		t.Errorf("expected ab, got %v", out.GetField(0))
	}

	// test mergeFunc
	acc1 := agg.NewFunc()
	tupleVal, _ = acc1.Get()
	tupleVal.V1 = "a"
	acc2 := agg.NewFunc()
	tupleVal, _ = acc2.Get()
	tupleVal.V1 = "b"
	acc3 := agg.MergeFunc(acc1, acc2)
	tupleVal, _ = acc3.Get()
	if tupleVal.V1 != "ab" {
		t.Errorf("expected ab, got %v", tupleVal)
	}
}
