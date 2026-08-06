package rateLimiter

type RateLimiter struct {
	// Output rates
	RateLimit []int

	// Define the rate limit change pattern. Each entry in the list indicates
	// after how many GeneratorIntervals to change to the next rate limit
	RateChangeInterval []int

	RateIndex     int
	IntervalIndex int
	Interval      int
}

func NewRateLimiter(ratelimit []int, changeInterval []int) *RateLimiter {

	return &RateLimiter{
		RateLimit:          ratelimit,
		RateChangeInterval: changeInterval,
		RateIndex:          0,
		IntervalIndex:      0,
		Interval:           0,
	}
}

// Get the current rate limit
func (rateLimiter *RateLimiter) GetRateLimit() int {

	if rateLimiter.Interval >= rateLimiter.RateChangeInterval[rateLimiter.IntervalIndex] {
		rateLimiter.Interval = 0
		rateLimiter.RateIndex = (rateLimiter.RateIndex + 1)
		if rateLimiter.RateIndex >= len(rateLimiter.RateLimit) {
			rateLimiter.RateIndex = len(rateLimiter.RateLimit) - 1
		}
		rateLimiter.IntervalIndex = (rateLimiter.IntervalIndex + 1)
		if rateLimiter.IntervalIndex >= len(rateLimiter.RateChangeInterval) {
			rateLimiter.IntervalIndex = len(rateLimiter.RateChangeInterval) - 1
		}
	}
	return rateLimiter.RateLimit[rateLimiter.RateIndex]
}
