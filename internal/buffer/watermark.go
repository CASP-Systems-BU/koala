package buffer

import "log"

type Watermark struct {

	// Timestamp in nanoseconds
	// Use type int64 to match the internal data type of time.Time
	// It's set to -1 if the Watermark is unset
	Timestamp int64
}

var _ WorkUnit = (*Watermark)(nil)

func NewWatermark(time int64) *Watermark {
	return &Watermark{
		Timestamp: time,
	}
}

// Implement WorkUnit interface
func (w *Watermark) GetType() WorkUnitType {
	return WatermarkWorkUnit
}

/******************************************************************************
					 		Watermark specific methods
******************************************************************************/

func (w *Watermark) Unset() bool {
	return w.Timestamp == -1
}

// Update the Watermark timestamp: only allow increasing timestamp
func (w *Watermark) Update(newWM *Watermark) {

	if newWM.Timestamp <= w.Timestamp {
		log.Fatalf(
			"Watermark timestamp should only increase: %d <= %d\n",
			newWM.Timestamp,
			w.Timestamp,
		)
	}

	w.Timestamp = newWM.Timestamp
}

// Update if the new timestamp is smaller
func (w *Watermark) SetMin(newWM *Watermark) {

	if newWM.Timestamp < w.Timestamp {
		w.Timestamp = newWM.Timestamp
	}
}

// Compare the timestamp of two Watermarks, return true if the first watermark
// is larger than the second watermark
func (w *Watermark) LargerThan(other *Watermark) bool {
	return w.Timestamp > other.Timestamp
}

// Copy the Watermark
func CopyWM(wm *Watermark) *Watermark {
	return &Watermark{
		Timestamp: wm.Timestamp,
	}
}
