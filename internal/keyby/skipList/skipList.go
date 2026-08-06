package skipList

import (
	"fmt"
	"log"
	"math/bits"
	"math/rand"
	"time"
)

// TODO: the fine-grained key lookup table owned by each bucket for per-key
// based lookup

const (
	// maxLevel denotes the maximum height of the skiplist.
	maxLevel = 25
	//
)

// The operator interface for key (in case of customized key operator) to
// compare two keys and move to adjacent key.
type KeyOperator interface {
	// Compare two keys and return the result
	Compare([]byte, []byte) int8

	// Return the estimated heurstic distance between two given keys
	AbsoluteKeyDistance([]byte, []byte) float64
}

// The actual element in skip list
type SkipListElement struct {
	next  [maxLevel]*SkipListElement // pointers to the next nodes
	level int                        // current level of the node
	prev  *SkipListElement           // pointer to the previous node

	// The range bounds (inclusive) and the associated owner
	RangeLowerBound     []byte
	RangeUpperBound     []byte
	RangeOwner          uint16
	LowerBoundInclusive bool
	UpperBoundInclusive bool
}

// Check if the key is less than the range lower bound
func (sle *SkipListElement) LessThanRange(
	KeyToCompare []byte,
	keyOperator KeyOperator,
) bool {
	comparedResult := keyOperator.Compare(KeyToCompare, sle.RangeLowerBound)
	return (sle.LowerBoundInclusive && comparedResult < 0) ||
		(!sle.LowerBoundInclusive && comparedResult <= 0)
}

// Check if the key is strictly less than (not including equal to the range
// lower bound)
func (sle *SkipListElement) StrictlyLessThanRange(
	KeyToCompare []byte,
	keyOperator KeyOperator,
) bool {
	return keyOperator.Compare(KeyToCompare, sle.RangeLowerBound) < 0
}

// Check if the key is greater than the range upper bound
func (sle *SkipListElement) GreaterThanRange(
	KeyToCompare []byte,
	keyOperator KeyOperator,
) bool {
	comparedResult := keyOperator.Compare(KeyToCompare, sle.RangeUpperBound)
	return (sle.UpperBoundInclusive && comparedResult > 0) ||
		(!sle.UpperBoundInclusive && comparedResult >= 0)
}

// Check if the key is strictly greater than (not including equal to the range
// upper bound)
func (sle *SkipListElement) StrictlyGreaterThanRange(
	KeyToCompare []byte,
	keyOperator KeyOperator,
) bool {
	return keyOperator.Compare(KeyToCompare, sle.RangeUpperBound) > 0
}

// The skip list data structure
type SkipList struct {
	startLevels   [maxLevel]*SkipListElement // pointers to the start nodes
	keyOperator   KeyOperator
	maxLevel      int
	localMaxLevel int
}

// Create a new skip list with a given seed and a specified key operator.
// Every insert operation will need to specify a random level controlled by
// the seed.
func NewSeed(seed int64, keyOperator KeyOperator) SkipList {
	// Initialize random number generator.
	rand.NewSource(seed)
	//fmt.Printf("SkipList seed: %v\n", seed)

	list := SkipList{
		startLevels:   [maxLevel]*SkipListElement{},
		keyOperator:   keyOperator,
		maxLevel:      maxLevel,
		localMaxLevel: 0,
	}

	return list
}

// Create a new empty and initialized Skiplist with .
func New(keyOperator KeyOperator) SkipList {
	return NewSeed(time.Now().UTC().UnixNano(), keyOperator)
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := maxLevel - 1
	// First we apply some mask which makes sure that we don't get a level
	// above our desired level. Then we find the first set bit.
	var x uint64 = rand.Uint64() & ((1 << uint(maxLevel-1)) - 1)
	// Using rand.Uint64() indicates that we use probability 1/2 when deciding
	// if a newly inserted node should be moved to the adjacent upper level.
	zeroes := bits.TrailingZeros64(x)
	if zeroes <= maxLevel {
		level = zeroes
	}
	return level
}

// Find the level to start searching for the given key.
func (sl *SkipList) findStartingLevelForGet(key []byte, level int) int {
	// Find good entry point so we don't accidentally skip half the list.
	for i := sl.localMaxLevel; i >= 0; i-- {
		// we need to check if the start level is nil or the key is less than
		// the range lower bound
		// of the start level
		if sl.startLevels[i] != nil &&
			!sl.startLevels[i].LessThanRange(key, sl.keyOperator) ||
			i <= level {
			return i
		}
	}
	return 0
}

// Get returns the element with the given key if it exists, and the previous and
// next elements. If the key is not found, the previous and next elements will
// be the closest elements to the key.
func (sl *SkipList) Get(
	key []byte,
) (prevElement *SkipListElement, foundElem *SkipListElement, nextElement *SkipListElement, ok bool) {
	foundElem = nil
	prevElement = nil
	nextElement = nil
	ok = false

	// Empty list
	if sl.IsEmpty() {
		return
	}

	// Find the level to start searching for the given key.
	index := sl.findStartingLevelForGet(key, 0)
	var currentNode *SkipListElement

	currentNode = sl.startLevels[index]
	nextNode := currentNode

	for {
		// Check if the key is in the range of the current node
		if currentNode != nil &&
			!currentNode.LessThanRange(key, sl.keyOperator) &&
			!currentNode.GreaterThanRange(key, sl.keyOperator) {
			ok = true
			foundElem = currentNode
			prevElement = currentNode.prev
			nextElement = currentNode.next[0]
			return
		}

		nextNode = currentNode.next[index]

		if nextNode != nil && !nextNode.LessThanRange(key, sl.keyOperator) {
			// Move to the next node as the key is greater than its lower bound
			// key
			currentNode = nextNode
		} else {
			if index > 0 {
				// Move to the next level if possible
				index--
			} else {
				// Element is not found and we reached the bottom.
				prevElement = currentNode
				nextElement = currentNode.next[0]
				return
			}
		}
	}
}

// IsEmpty checks if the skip list is empty.
func (sl *SkipList) IsEmpty() bool {
	return sl.startLevels[0] == nil
}

// DeleteElement removes the given element in the skip list.
func (sl *SkipList) DeleteElement(elem *SkipListElement) {
	if elem == nil {
		return
	}

	prevElement := elem.prev
	for i := 0; i <= elem.level; i++ {
		// Update the pointers
		prevElement.next[i] = elem.next[i]
		if elem.next[i] != nil {
			elem.next[i].prev = prevElement
		}
		// Also update the start levels
		if sl.startLevels[i] == elem {
			sl.startLevels[i] = elem.next[i]
		}
	}
}

// InsertElementAfter inserts a new element after the existing element. If the
// existing element is nil, the new element will be inserted at the beginning of
// the list.
// (Must ensure existing elem is in the level)
func (sl *SkipList) InsertElementAfter(
	existingElem *SkipListElement,
	newElem *SkipListElement,
) *SkipListElement {

	// Insert the new element
	if existingElem != nil {
		// Insert after the existing element
		newElem.prev = existingElem
		for i := 0; i <= newElem.level; i++ {
			newElem.next[i] = existingElem.next[i]
			existingElem.next[i] = newElem
		}
	} else {
		// Insert at the beginning if existing element is nil
		for i := 0; i <= newElem.level; i++ {
			newElem.next[i] = sl.startLevels[i]
			if sl.startLevels[i] != nil {
				sl.startLevels[i].prev = newElem
			}
			sl.startLevels[i] = newElem
		}
	}

	return newElem
}

// Insert a new element with two adjacent elements and the owner of the range.
// Note: The prevElement and nextElement must be adjacent and the requestedKey
// must be not found in the skip list. The function will return the element that
// contains the requested key and the expanded key
func (sl *SkipList) Insert(
	expandedKey []byte,
	requestedKey []byte,
	owner uint16,
	expandingTowardsPrev bool,
	prevElement, nextElement *SkipListElement,
) *SkipListElement {
	// Two given elements must be adjacent
	if prevElement != nil {
		if prevElement.next[0] != nextElement {
			log.Fatalf("The two given elements are not adjacent")
			return nil
		}
	}

	if nextElement != nil {
		if nextElement.prev != prevElement {
			log.Fatalf("The two given elements are not adjacent")
			return nil
		}
	}

	// Assign a random level for the incoming new element
	level := sl.randomLevel()

	// Only grow the height of the skiplist by one at a time!
	if level > sl.localMaxLevel {
		level = sl.localMaxLevel + 1
		sl.localMaxLevel = level
	}

	var temp_prev *SkipListElement = nil
	var temp_next *SkipListElement = nil
	var newElem *SkipListElement = nil
	var returnElem *SkipListElement = nil
	if expandingTowardsPrev {

		if sl.keyOperator.Compare(expandedKey, requestedKey) > 0 {
			log.Fatalf("The expanded key must be less than the requested key")
		}

		// Create the new element
		newElem = &SkipListElement{
			next:  [maxLevel]*SkipListElement{},
			level: level,

			RangeLowerBound:     expandedKey,
			RangeUpperBound:     requestedKey,
			RangeOwner:          owner,
			LowerBoundInclusive: true,
			UpperBoundInclusive: true,
		}
		returnElem = newElem

		// Expand towards the previous element
		temp_prev = prevElement
		for temp_prev != nil {
			compareResultWithUpperBound := sl.keyOperator.Compare(
				expandedKey,
				temp_prev.RangeUpperBound,
			)
			compareResultWithLowerBound := sl.keyOperator.Compare(
				expandedKey,
				temp_prev.RangeLowerBound,
			)
			if compareResultWithUpperBound > 0 {
				// input state:
				// ---temp_prev.LowerBound--------temp_prev.UpperBound------expandedKey------requestedKey---
				// result state:
				// ---[temp_prev.LowerBound,
				// temp_prev.UpperBound]------[expandedKey, requestedKey]---
				sl.InsertElementAfter(temp_prev, newElem)
				break
			} else if compareResultWithLowerBound > 0 {
				// input state:
				// ---temp_prev.LowerBound------expandedKey------temp_prev.UpperBound------requestedKey---
				if temp_prev.RangeOwner == owner {
					// result state:
					// ---[temp_prev.LowerBound, requestedKey]---
					// We don't need to insert the new element if the owner is
					// the same,
					// we juet need to expand the range
					temp_prev.RangeUpperBound = requestedKey
					temp_prev.UpperBoundInclusive = true
					returnElem = temp_prev
				} else {
					// result state:
					// ---[temp_prev.LowerBound, expandedKey)------[expandedKey,
					// requestedKey]---
					// insert and update the inclusive bound
					sl.InsertElementAfter(temp_prev, newElem)
					temp_prev.RangeUpperBound = expandedKey
					temp_prev.UpperBoundInclusive = false
				}
				break
			} else if compareResultWithLowerBound == 0 {
				// input state:
				// ---temp_prev.LowerBound(expandedKey)------temp_prev.UpperBound------requestedKey---
				// result state:
				// ---[temp_prev.LowerBound, requestedKey]------
				// We do not need to insert the new element if the lower bound
				// is the same
				// we just need to expand the range
				temp_prev.RangeUpperBound = requestedKey
				temp_prev.UpperBoundInclusive = true
				temp_prev.LowerBoundInclusive = true
				temp_prev.RangeOwner = owner
				returnElem = temp_prev
				break
			}

			// ---expandedKey-------temp_prev.LowerBound------temp_prev.UpperBound------requestedKey---
			// Move to the previous element
			temp_prev = temp_prev.prev
			sl.DeleteElement(temp_prev.next[0])
		}

		if temp_prev == nil {
			// Insert the new element at the beginning
			sl.InsertElementAfter(nil, newElem)
		}
	} else {

		// The expanded key must be greater than the requested key
		if sl.keyOperator.Compare(requestedKey, expandedKey) > 0 {
			log.Fatalf("The requested key must be less than the expanded key")
		}

		// Create the new element
		newElem = &SkipListElement{
			next:  [maxLevel]*SkipListElement{},
			level: level,

			RangeLowerBound:     requestedKey,
			RangeUpperBound:     expandedKey,
			RangeOwner:          owner,
			LowerBoundInclusive: true,
			UpperBoundInclusive: true,
		}
		returnElem = newElem

		// Expand towards the next element
		temp_next = nextElement
		for temp_next != nil {
			compareResultWithLowerBound := sl.keyOperator.Compare(requestedKey, temp_next.RangeLowerBound)
			compareResultWithUpperBound := sl.keyOperator.Compare(requestedKey, temp_next.RangeUpperBound)
			if compareResultWithLowerBound < 0 {
				// input state:
				// ---requestedKey------expandedKey-------temp_next.LowerBound------temp_next.UpperBound---
				// result state:
				// ---[requestedKey, expandedKey]------[temp_next.LowerBound,
				// temp_next.UpperBound]---
				sl.InsertElementAfter(prevElement, newElem)
				break
			} else if compareResultWithUpperBound < 0 {
				// input state:
				// ---requestedKey------temp_next.LowerBound------expandedKey------temp_next.UpperBound---
				if temp_next.RangeOwner == owner {
					// result state:
					// ---[requestedKey, temp_next.UpperBound]---
					// We don't need to insert the new element if the owner is
					// the same,
					// we juet need to expand the range
					temp_next.RangeLowerBound = requestedKey
					temp_next.LowerBoundInclusive = true
					returnElem = temp_next
				} else {
					// result state:
					// ---[requestedKey, expandedKey]------(expandedKey,
					// temp_next.UpperBound]---
					// insert and update the inclusive bound
					sl.InsertElementAfter(prevElement, newElem)
					temp_next.RangeLowerBound = expandedKey
					temp_next.LowerBoundInclusive = false
				}
				break
			} else if compareResultWithUpperBound == 0 {
				// input state:
				// ---requestedKey------temp_next.LowerBound------temp_next.UpperBound(expandedKey)---
				// result state:
				// ---[requestedKey, temp_next.UpperBound]---
				// We do not need to insert the new element if the upper bound
				// is the same
				// we just need to expand the range
				temp_next.RangeLowerBound = expandedKey
				temp_next.LowerBoundInclusive = true
				temp_next.UpperBoundInclusive = true
				temp_next.RangeOwner = owner
				returnElem = temp_next
				break
			}

			// ---requestedKey------temp_next.LowerBound------temp_next.UpperBound------expandedKey---
			// Move to the next element
			temp_next = temp_next.next[0]
			sl.DeleteElement(temp_next.prev)
		}

		if temp_next == nil {
			// Insert the new element at the end
			sl.InsertElementAfter(prevElement, newElem)
		}
	}
	return returnElem
}

// Print the skip list
func (sl *SkipList) Print() {
	for i := sl.maxLevel - 1; i >= 0; i-- {
		current := sl.startLevels[i]
		fmt.Printf("Level %d: ", i)
		for current != nil {
			if current.LowerBoundInclusive {
				fmt.Printf("[")
			} else {
				fmt.Printf("(")
			}
			fmt.Printf(
				"%d-%d",
				current.RangeLowerBound,
				current.RangeUpperBound,
			)
			if current.UpperBoundInclusive {
				fmt.Printf("]")
			} else {
				fmt.Printf(")")
			}
			fmt.Printf(":%d", current.RangeOwner)
			current = current.next[i]
			if current != nil {
				fmt.Printf(" -> ")
			}
		}
		fmt.Println()
	}
}
