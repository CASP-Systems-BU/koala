package metric

import (
	"sync/atomic"
	"time"
)

// Rate returns per second values. The calculate of rate only happens when Get()
// is called.
type Rate struct {

	// Counter value
	Counter uint64

	// Time duration. This is a fixed value after initialization
	Interval time.Duration

	// Flag to indicate whether there is any Inc calls
	hasIncCalls int32
}

func NewRate(interval time.Duration) *Rate {

	return &Rate{
		Counter:  0,
		Interval: interval,
	}
}

func (r *Rate) Inc(val uint64) {

	atomic.AddUint64(&r.Counter, val)
	atomic.StoreInt32(&r.hasIncCalls, 1)
}

func (r *Rate) Get() float64 {

	// Get and reset the counter
	curCnt := atomic.SwapUint64(&r.Counter, 0)

	// Return invalid value if no Inc calls
	hasIncCallFlag := atomic.SwapInt32(&r.hasIncCalls, 0)
	if hasIncCallFlag == 0 {
		return -1.0
	}

	return float64(curCnt) / r.Interval.Seconds()
}
