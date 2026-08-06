package metric

import (
	"sync/atomic"
	"time"
)

// Percentage is used to capture the amount of time spent on certain operations
// over a given interval e.g. task Idle time, task Backpressure time

type Percentage struct {

	// Accumulated time spent on the interested operation. We use int64 instead
	// of time.Duration to support atomic operations
	Accumulated int64

	// Measurement interval - align with Config.MetricsInterval
	Interval time.Duration

	// Flag to indicate whether there is any Inc calls
	hasIncCalls int32
}

func NewPercentage(interval time.Duration) *Percentage {

	return &Percentage{
		Accumulated: 0,
		Interval:    interval,
	}
}

func (p *Percentage) Inc(dur time.Duration) {

	atomic.AddInt64(&p.Accumulated, int64(dur))
	atomic.StoreInt32(&p.hasIncCalls, 1)
}

func (p *Percentage) Get() float64 {

	// Get and reset the Accumulated time
	curAcc := atomic.SwapInt64(&p.Accumulated, 0)
	percentage := float64(curAcc) / float64(p.Interval)

	// Return invalid value if no Inc calls
	hasIncCallFlag := atomic.SwapInt32(&p.hasIncCalls, 0)
	if hasIncCallFlag == 0 {
		return -1.0
	}

	if percentage > 1.0 {
		return 1.0
	}
	return percentage
}
