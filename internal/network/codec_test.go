package network_test

import (
	"reflect"
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

// Test if a tuple's timestamp can be set and ser/de correctly
func TestTupleTimestamp(t *testing.T) {

	// Define a tuple type
	type TupleType = *tuple.Tuple2[int64, string]

	// Serialization functions
	var empty TupleType
	encoder := network.CreateEncoder(reflect.TypeOf(empty).Elem())
	decoder := network.CreateDecoder(reflect.TypeOf(empty).Elem())
	sizer := network.CreateSizer(reflect.TypeOf(empty).Elem())

	t.Logf("Created encoder, decoder and sizer for Tuple2[int64, string]")

	tuple1 := &tuple.Tuple2[int64, string]{
		V1: 123,
		V2: "hello",
	}
	tuple1.SetTimestamp(456)

	t.Logf("Created a Tuple2[int64, string] with timestamp 456")

	// Serialize
	buffer := make([]byte, sizer(tuple1))
	encoder(tuple1, buffer)

	t.Logf("Serialized the Tuple2[int64, string]")

	// Deserialize
	tuple2 := &tuple.Tuple2[int64, string]{} // Empty tuple
	decoder(tuple2, buffer)

	t.Logf("Deserialized the Tuple2[int64, string]")

	// Check if the timestamp is correctly set
	if tuple2.GetTimestamp() != 456 {
		t.Errorf(
			"Timestamp not correctly set, got %d, want 456",
			tuple2.GetTimestamp(),
		)
	}
}
