package dataflow_test

import (
	"testing"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
)

// Test the window index
func TestWindowIndex(t *testing.T) {
	w := dataflow.NewTumblingWindowIndex[int]()

	// Add some keys
	w.Add(1, 100)
	w.Add(1, 200)
	w.Add(2, 100)
	w.Add(2, 200)

	// Check if the keys exist
	if !w.Exists(1, 100) {
		t.Errorf("Expected key %v to exist", 1)
	}
	if !w.Exists(1, 200) {
		t.Errorf("Expected key %v to exist", 1)
	}
	if !w.Exists(2, 100) {
		t.Errorf("Expected key %v to exist", 2)
	}
	if !w.Exists(2, 200) {
		t.Errorf("Expected key %v to exist", 2)
	}

	// Remove a key
	w.Remove(1, 100)

	// Check if the key is removed
	if w.Exists(1, 100) {
		t.Errorf("Expected key %v to be removed", 1)
	}
}

func TestWindowIndex_GetUpdateWindows(t *testing.T) {
	w := dataflow.NewTumblingWindowIndex[int]()

	// Add some keys
	w.Add(1, 100)
	w.Add(1, 200)
	w.Add(2, 100)
	w.Add(2, 200)

	// Get update windows
	keys, timestamps := w.GetWindowsToBeFired(buffer.NewWatermark(200), 50)

	// Check if the keys and timestamps are correct
	if len(keys) != 2 || len(timestamps) != 2 {
		t.Errorf(
			"Expected 2 keys and timestamps, got %v keys and %v timestamps",
			len(keys),
			len(timestamps),
		)
	}

	windowMap := make(map[int]int64)
	for i := 0; i < len(keys); i++ {
		windowMap[keys[i]] = timestamps[i]
	}

	// Check if the expected windows are present in the map
	startTime, ok := windowMap[1]
	if !ok || startTime != 100 {
		t.Errorf("Expected window [1, 100], got %v", windowMap)
	}

	startTime, ok = windowMap[2]
	if !ok || startTime != 100 {
		t.Errorf("Expected window [2, 100], got %v", windowMap)
	}
}
