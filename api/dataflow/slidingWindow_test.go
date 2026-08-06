package dataflow_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
)

// ====== Test sliding window index ======

func TestSlidingWindowIndex_AddPane(t *testing.T) {

	duration := time.Duration(600)
	slide := time.Duration(200)
	index := dataflow.NewSlidingWindowIndex[int](duration, slide)

	// Add a pane at timestamp 1000
	// This means this pane will be in [600, 800, 1000]
	index.AddPane(1, 1000)

	fmt.Println(index.Index)

	// Check if the window exists
	if len(index.Index) != 3 {
		t.Fatalf("Invalid number of windows in the index")
	}
	if !index.ExistsWindow(1, 600) {
		t.Fatalf("Window should exist at timestamp 600")
	}
	if !index.ExistsWindow(1, 800) {
		t.Fatalf("Window should exist at timestamp 800")
	}
	if !index.ExistsWindow(1, 1000) {
		t.Fatalf("Window should exist at timestamp 1000")
	}

	// The pane should exist in the window index
	if len(index.Index[600][1]) != 1 {
		t.Fatalf("Invalid number of panes in the window")
	}
	if len(index.Index[800][1]) != 1 {
		t.Fatalf("Invalid number of panes in the window")
	}
	if len(index.Index[1000][1]) != 1 {
		t.Fatalf("Invalid number of panes in the window")
	}

	// Add another pane at timestamp 1200
	// This pane belongs to 3 windows: 800, 1000, 1200
	index.AddPane(1, 1200)

	fmt.Println(index.Index)

	// Check if the window exists
	if len(index.Index) != 4 {
		t.Fatalf("Invalid number of windows in the index")
	}
	if !index.ExistsWindow(1, 1200) {
		t.Fatalf("Window should exist at timestamp 1200")
	}
}

func TestSlidingWindowIndex_GetWindowsToBeFired(t *testing.T) {

	duration := time.Duration(600)
	slide := time.Duration(200)
	index := dataflow.NewSlidingWindowIndex[int](duration, slide)

	// Populate the index with panes
	var i int64
	base := dataflow.GetPaneStartTime(6000, slide)
	for i = 0; i < 10; i += 1 {
		index.AddPane(1, base+i*200)
	}

	for ts, keyMap := range index.Index {

		// Check the window timestamp
		if ts <= base-duration.Nanoseconds() || ts >= base+600*10 {
			t.Fatalf(
				"Invalid window timestamp in the index %d, should between [%d, %d)",
				ts,
				base,
				base+600*10,
			)
		}

		for key, paneSet := range keyMap {

			// Check the key
			if key != 1 {
				t.Fatalf("Invalid key in the index")
			}

			for _, pane := range paneSet {

				if pane.StartTime < ts || pane.StartTime > ts+600 {
					t.Fatalf(
						"Invalid pane timestamp in the index, should between (%d, %d]",
						ts,
						ts+600,
					)
				}

			}
		}
	}
}

func TestSlidingWindowIndex_Remove(t *testing.T) {
	duration := time.Duration(600)
	slide := time.Duration(200)
	index := dataflow.NewSlidingWindowIndex[int](duration, slide)

	// Populate the index with panes
	var i int64
	base := dataflow.GetPaneStartTime(6000, slide)
	for i = 0; i < 10; i += 1 {
		index.AddPane(1, base+i*200)
		index.AddPane(2, base+i*200)
	}

	fmt.Println(index.Index)

	// Remove all the window index until 6500 for key 1
	for i = 0; i < 5; i += 1 {
		index.Remove(1, 5600+i*200)
	}

	fmt.Println()
	fmt.Println(index.Index)

	// Check if all the index are successfully removed
	for startTime, windows := range index.Index {
		if startTime < 6600 {
			_, ok := windows[1]
			if ok {
				t.Errorf(
					"Should be deleted, get index 1 at timestamp=%d\n",
					startTime,
				)
			}
		}
	}
}

// ========= Test helper functions =========
func TestGetSlidingWindowStartTimes(t *testing.T) {
	paneTimestamps := []int64{}
	for i := range 10 {
		paneTimestamps = append(paneTimestamps, 20000+int64(i)*100)
	}

	var windowDuration time.Duration = 600
	var slide time.Duration = 200
	for _, ts := range paneTimestamps {
		startTimes := dataflow.GetSlidingWindowStartTimes(
			ts,
			windowDuration,
			slide,
		)
		fmt.Println(startTimes)
		for _, st := range startTimes {
			if ts < st || ts >= st+windowDuration.Nanoseconds() {
				t.Fatalf("Invalid start time %d for pane timestamp %d", st, ts)
			}
		}
	}
}

func TestGetPaneStartTime(t *testing.T) {
	var slide time.Duration = 200
	var ts int64 = 19999
	paneStartTime := dataflow.GetPaneStartTime(ts, slide)
	if paneStartTime != 19800 {
		t.Fatalf(
			"Invalid pane start time %d for timestamp %d",
			paneStartTime,
			ts,
		)
	}
}
