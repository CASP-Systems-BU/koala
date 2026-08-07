package watermark

import (
	"time"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
)

type PeriodicWatermarkGenerator struct {

	// Last generated watermark timestamp
	LastGeneratedTime int64

	// The max record timestamp seen so far
	CurrentMaxTime int64

	// Periodic watermark generation interval
	PeriodicInterval time.Duration

	// Max allowed delay for out-of-order records
	MaxAllowedDelay time.Duration
}

func NewPeriodicWatermarkGenerator(
	periodicInterval time.Duration,
	maxAllowedDelay time.Duration,
) *PeriodicWatermarkGenerator {

	return &PeriodicWatermarkGenerator{
		LastGeneratedTime: -1,
		CurrentMaxTime:    -1,
		PeriodicInterval:  periodicInterval,
		MaxAllowedDelay:   maxAllowedDelay,
	}
}

// Gererate current watermark
// Return false if the previous generated watermark is same as the new one
// This indicates the current record MaxTimestamp haven't changed - do nothing
func (g *PeriodicWatermarkGenerator) GenerateWatermark() (*buffer.Watermark, bool) {

	// No record has been emitted yet - no reference to max time - skip
	if g.CurrentMaxTime == -1 {
		return nil, false
	}

	// CurrentMaxTime - MaxAllowedDelay
	newWMTime := g.CurrentMaxTime - g.MaxAllowedDelay.Nanoseconds()

	if newWMTime == g.LastGeneratedTime {
		return nil, false
	} else {
		g.LastGeneratedTime = newWMTime
		return &buffer.Watermark{
			Timestamp: newWMTime,
		}, true
	}
}

// Update the max record timestamp if needed
func (g *PeriodicWatermarkGenerator) UpdateMaxRecordTimestamp(
	newTimestamp int64,
) {

	if newTimestamp > g.CurrentMaxTime {
		g.CurrentMaxTime = newTimestamp
	}
}
