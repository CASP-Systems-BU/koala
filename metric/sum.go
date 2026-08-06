package metric

import (
	"sync/atomic"
)

// Sum accumulates values over a period. Get() returns the sum for the
// last period and resets the counter. Used for metrics like numBytesTransferred
// where we report sum(counter) for the last period.
type Sum struct {
	Counter uint64
}

func NewSum() *Sum {
	return &Sum{
		Counter: 0,
	}
}

func (p *Sum) Inc(val uint64) {
	atomic.AddUint64(&p.Counter, val)
}

// Get returns the sum for the last period and resets the counter.
func (p *Sum) Get() float64 {
	curCnt := atomic.SwapUint64(&p.Counter, 0)
	return float64(curCnt)
}
