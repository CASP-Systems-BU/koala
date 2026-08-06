package hash

import (
	"github.com/twmb/murmur3"
)

// MurmurHash for consistent hashing across processes
type MurmurHash struct {
	seed uint64
}

// Create the hash function
func NewMurmurHash(seed uint64) *MurmurHash {

	return &MurmurHash{
		seed: seed,
	}
}

/******************************************************************************
							Interface implementation
******************************************************************************/

// Implement the HashFunc interface
func (h *MurmurHash) Hash(key []byte) uint64 {
	return murmur3.SeedSum64(h.seed, key)
}

// Implement the HashFunc interface
func (g *MurmurHash) HashValueRange() (uint64, uint64) {
	return 0, ^uint64(0)
}

/******************************************************************************
							Murmurhash specific methpds
******************************************************************************/

func (h *MurmurHash) GetSeed() uint64 {
	return h.seed
}
