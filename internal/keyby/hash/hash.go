package hash

import (
	"log"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// Hash function interface for the key partition
type HashFunc interface {

	// Given a key, return the hash value
	Hash([]byte) uint64

	// Get the hash value range
	// [lower bound, upper bound] all inclusive
	HashValueRange() (uint64, uint64)
}

// Helper function to generate the key hash function
func GetKeyHashFunc(config *configuration.Configuration) HashFunc {

	var hashFunc HashFunc
	switch config.HashFuncType {
	case "murmurhash":
		hashFunc = NewMurmurHash(config.KeyHashSeed)
	default:
		log.Fatalf("Unsupported hash function type: %s\n", config.HashFuncType)
	}

	return hashFunc
}
