package randGenerator

import (
	"math"
	"math/rand"
	"time"

	"github.com/CASP-Systems-BU/koala/internal/utils"
)

const MIN_STRING_LENGTH int = 3

// Return a random string of up to maxLength
func nextString(random *rand.Rand, maxLength int) string {
	length := random.Intn(maxLength-MIN_STRING_LENGTH) + MIN_STRING_LENGTH
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(65 + random.Intn(25))
	}
	return string(b)
}

// return a random long from [0, n)
func nextLong(random *rand.Rand, n int64) int64 {
	return random.Int63n(n)
}

// return a random price
func nextPrice(random *rand.Rand) int64 {
	// +1 to avoid generating 0 price
	return int64(1 + math.Round(math.Pow(10, random.Float64()*6))*100)
}

// Given the global event ID and inter-event delay, get the current event time.
// We assume base event time starts from 0
func getEventTime(globalEventID int64, interEventDelay utils.Duration) int64 {
	return globalEventID * time.Duration(interEventDelay).Nanoseconds()
}
