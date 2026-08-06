package stateBackend

// Iterate all key value pairs in the state store

type StateIterator interface {

	// Set the iterator idx to the first key-value pair
	First() bool

	// Test if current iterator idx is valid
	Valid() bool

	// Set the iterator idx to the next key-value pair
	Next() bool

	// Get the key of the current key-value pair
	Key() []byte

	// Get the value of the current key-value pair
	Value() []byte

	// Close the iterator
	Close() error
}
