package keyAssigner

import (
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
)

// KeyAssigner extracts the key field from the input tuple
type KeyAssigner[IN tuple.Tuple, K comparable] struct {

	// User define extractor function
	keyExtractor func(IN) K
}

func NewKeyAssigner[IN tuple.Tuple, K comparable](
	keyExtractor func(IN) K,
) *KeyAssigner[IN, K] {
	return &KeyAssigner[IN, K]{keyExtractor: keyExtractor}
}

// Extract key from the input tuple
func (ka *KeyAssigner[IN, K]) GetKey(t IN) K {
	return ka.keyExtractor(t)
}
