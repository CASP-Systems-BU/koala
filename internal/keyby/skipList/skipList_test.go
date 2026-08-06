package skipList

import (
	"bytes"
	"testing"
)

const (
	// Common prefix length to be ignored
	commonPrefixLength = 4
)

// TestKeyOperator is a test key operator for testing skip list
type testKeyOperator struct{}

// Compare two keys
func (tko testKeyOperator) Compare(key1, key2 []byte) int8 {
	return int8(bytes.Compare(key1, key2))
}

// Implement a heuristic to estimate the distance between two keys
func (tko testKeyOperator) KeyDistance(key1, key2 []byte) uint64 {
	return uint64(bytes.Compare(key1, key2))
}

func (tko testKeyOperator) AbsoluteKeyDistance(key1, key2 []byte) float64 {
	return TokenizedLevenshteinDistance(
		key1[commonPrefixLength:],
		key2[commonPrefixLength:],
	)
}

func TestSkipListGetAndInsertWithTheSameOwner(t *testing.T) {
	testKeyOperator := testKeyOperator{}

	skipList := New(testKeyOperator)

	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")
	key4 := []byte("key4")
	key5 := []byte("key5")

	prevElem, foundElem, nextElem, ok := skipList.Get(key1)
	if ok || foundElem != nil || prevElem != nil || nextElem != nil {
		t.Errorf("Failed to get non-existing key1")
	}

	// (TODO) emulate the range query backend which expands the requested key

	// Now let's assume we do not expand anything and insert the key1
	newElem := skipList.Insert(key1, key1, 1, false, prevElem, nextElem)
	// ---------------[key1, key1]----------------
	// prevElem = nil, nextElem = nil
	if newElem == nil {
		t.Errorf("Failed to insert the key range")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key1)
	if !ok || foundElem.RangeOwner != 1 {
		t.Errorf("Failed to get key1")
	}

	if !bytes.Equal(foundElem.RangeLowerBound, key1) ||
		!bytes.Equal(foundElem.RangeUpperBound, key1) {
		t.Errorf("The range lower bound and upper bounds are not set correctly")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key5)
	if ok || foundElem != nil {
		t.Errorf("Failed to get non-existing key5")
	}
	if prevElem == nil || nextElem != nil {
		t.Errorf("The prevElem shall not be nil but the nextElem shall be nil")
	}

	// Assuming we do not expand anything and insert the key5
	newElem = skipList.Insert(key5, key5, 1, false, prevElem, nextElem)
	// ---------------[key1, key1]---------[key5, key5]----------------
	// prevElem = [key1, key1], nextElem = nil
	if newElem == nil {
		t.Errorf("Failed to insert the key range")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key3)
	if ok || foundElem != nil {
		t.Errorf("Failed to get non-existing key3")
	}
	if prevElem == nil || nextElem == nil {
		t.Errorf("The prevElem and nextElem shall not be nil")
	}

	// (TODO) we may need to wrap this to another API on top of skipList
	distance1 := testKeyOperator.AbsoluteKeyDistance(
		prevElem.RangeUpperBound,
		key3,
	)
	distance2 := testKeyOperator.AbsoluteKeyDistance(
		key3,
		nextElem.RangeLowerBound,
	)
	if distance1 > distance2 {
		// Expand to the next element
		newElem = skipList.Insert(key5, key3, 1, false, prevElem, nextElem)
		// ---------------[key1, key1]---------[key3, key5]----------------
		// prevElem = [key1, key1], nextElem = nil
		// Check the boundary
		if !bytes.Equal(newElem.RangeLowerBound, key3) ||
			!bytes.Equal(newElem.RangeUpperBound, key5) {
			t.Errorf(
				"The range lower bound and upper bounds are not set correctly",
			)
		}

		prevElem, foundElem, nextElem, ok = skipList.Get(key4)
		if !ok || foundElem.RangeOwner != 1 {
			// We are supposed to get the key4 as key4 is within the range of
			// [key3, key5]
			t.Errorf("Failed to get key4")
		}

		if prevElem == nil || nextElem != nil {
			t.Errorf(
				"The prevElem shall not be nil but the nextElem shall be nil",
			)
		}

	} else {
		// Expand to the previous element
		newElem = skipList.Insert(key1, key3, 1, true, prevElem, nextElem)
		// ---------------[key1, key3]---------[key5, key5]----------------
		// prevElem = nil, nextElem = [key5, key5]
		// Check the boundary
		if !bytes.Equal(newElem.RangeLowerBound, key1) || !bytes.Equal(newElem.RangeUpperBound, key3) {
			t.Errorf("The range lower bound and upper bounds are not set correctly")
		}

		prevElem, foundElem, nextElem, ok = skipList.Get(key2)
		if !ok || foundElem.RangeOwner != 1 {
			// We are supposed to get the key4 as key4 is within the range of
			// [key1, key3]
			t.Errorf("Failed to get key2")
		}

		if prevElem != nil || nextElem == nil {
			t.Errorf("The prevElem shall be nil but the nextElem shall not be nil")
		}
	}
}

func TestSkipListGetAndInsertWithDifferentOwner(t *testing.T) {
	testKeyOperator := testKeyOperator{}

	skipList := New(testKeyOperator)

	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")
	key4 := []byte("key4")

	prevElem, foundElem, nextElem, ok := skipList.Get(key3)
	if ok || foundElem != nil || prevElem != nil || nextElem != nil {
		t.Errorf("Failed to get non-existing key3")
	}

	// (TODO) emulate the range query backend which expands the requested key

	// Now let's assume we inserted expanded key range [key1, key3]
	// ---------------[key1, key3]----------------------
	// prevElem = nil, nextElem = nil
	newElem := skipList.Insert(key1, key3, 1, true, prevElem, nextElem)
	if newElem == nil {
		t.Errorf("Failed to insert the key range")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key3)
	if !ok || foundElem.RangeOwner != 1 {
		t.Errorf("Failed to get key3")
	}

	if !bytes.Equal(foundElem.RangeLowerBound, key1) ||
		!bytes.Equal(foundElem.RangeUpperBound, key3) {
		t.Errorf("The range lower bound and upper bounds are not set correctly")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key2)
	if !ok || foundElem.RangeOwner != 1 {
		t.Errorf("Failed to get key2")
	}

	// Assuming we insert key4 and expand to key2
	newElem = skipList.Insert(key2, key4, 2, true, foundElem, nextElem)
	// ---------------[key1, key2)--------[key2, key4]--------------
	// prevElem = [key1, key2), nextElem = nil
	if newElem == nil {
		t.Errorf("Failed to insert the key range")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key2)
	if !ok || foundElem.RangeOwner != 2 {
		// We are supposed to get the key4 as key4 is within the range of [key1,
		// key3]
		t.Errorf("Failed to get key2")
	}

	if prevElem == nil || nextElem != nil {
		t.Errorf("The prevElem shall not be nil but the nextElem shall be nil")
	}

	if prevElem.RangeUpperBound == nil ||
		!bytes.Equal(prevElem.RangeUpperBound, key2) ||
		prevElem.UpperBoundInclusive {
		t.Errorf("The range upper bound is not set correctly")
	}

	prevElem, foundElem, nextElem, ok = skipList.Get(key3)
	if !ok || foundElem.RangeOwner != 2 {
		// Now the owner of key3 is 2
		t.Errorf("Failed to get key3")
	}

	if prevElem == nil || nextElem != nil {
		t.Errorf("The prevElem shall not be nil but the nextElem shall be nil")
	}
}
