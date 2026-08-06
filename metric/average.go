package metric

import (
	"sync/atomic"
)

type Average struct {

	// current sum
	sum int64

	// current count
	count uint64
}

func NewAverage() *Average {

	return &Average{
		sum:   0,
		count: 0,
	}
}

func (avg *Average) Inc(val int64) {

	atomic.AddUint64(&avg.count, 1)
	atomic.AddInt64(&avg.sum, val)
}

func (avg *Average) Get() float64 {
	// Note: `sum` and `count` are not updated atomically together,
	// so the returned average may have minor inaccuracies.
	// However, since these values are used to compute intermediate averages,
	// which are further averaged before producing the final result,
	// the small discrepancies will have no significant impact.
	currCnt := atomic.SwapUint64(&avg.count, 0)
	currSum := atomic.SwapInt64(&avg.sum, 0)

	// Return invalid value if no Inc calls
	if currCnt == 0 {
		return -1.0
	}

	return float64(currSum) / float64(currCnt)
}
