package keyAssigner_test

import (
	"strconv"
	"testing"

	"github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

func TestKeyAssignerWithSingleKeyField(t *testing.T) {

	ka := keyAssigner.NewKeyAssigner(func(t *tuple.Tuple1[int64]) int64 {
		return t.V1
	})

	for i := range 100 {
		key := ka.GetKey(&tuple.Tuple1[int64]{V1: int64(i)})
		if key != int64(i) {
			t.Fatalf("Expected key %d, got %d", i, key)
		}
	}
}

func TestKeyAssignerWithCompositeKeyFields(t *testing.T) {

	ka := keyAssigner.NewKeyAssigner(
		func(t *tuple.Tuple2[string, string]) string {
			return t.V1 + t.V2
		},
	)

	for i := range 100 {
		v1 := strconv.Itoa(i)
		v2 := strconv.Itoa(i + 1)
		key := ka.GetKey(&tuple.Tuple2[string, string]{V1: v1, V2: v2})

		if key != v1+v2 {
			t.Fatalf("Expected key %s, got %s", v1+v2, key)
		}
	}
}
