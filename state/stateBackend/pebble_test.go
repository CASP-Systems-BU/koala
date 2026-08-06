package stateBackend_test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/mus-format/mus-go/raw"
)

func TestPebbleStateBackend_Basic(t *testing.T) {

	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	stateBackend.Set([]byte("lei"), []byte("goodgood"))

	val := stateBackend.Get([]byte("lei"))
	if val == nil {
		t.Errorf("Key not found")
	}

	if string(val) != "goodgood" {
		t.Errorf("Expected 'goodgood', got %v", val)
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_GetUnexistKey(t *testing.T) {
	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	val := stateBackend.Get([]byte("unexist"))
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

// It's ok to set a nil value into pebble
// The return value will be []byte{} with 0 length but not nil
func TestPebbleStateBackend_SetValueNil(t *testing.T) {
	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	// Set value to nil
	stateBackend.Set([]byte("key"), nil)

	// Read the same key
	val := stateBackend.Get([]byte("key"))
	if val == nil {
		t.Errorf("Key not exist. Won't happen!")
	} else if bytes.Equal(val, []byte{}) {
		// Clean up
		testutils.CleanUpDataFolder()
		// Correct!
		return
	} else {
		t.Errorf("Expected []byte{}, got %v", val)
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

// It's ok to set a nil key into pebble
// It will be interpreted as []byte{} with 0 length as key
// So when get the value, use nil or []byte{} as key are equivalent
func TestPebbleStateBackend_SetKeyNil(t *testing.T) {

	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	// Set key to nil
	stateBackend.Set(nil, []byte("value"))

	// Use nil as key to get the value
	val := stateBackend.Get(nil)
	if val == nil {
		t.Errorf("Key not found")
	} else if !bytes.Equal(val, []byte("value")) {
		t.Errorf("Expected 'value', got %v", val)
	}

	// Use []byte{} as key to get the value
	val = stateBackend.Get([]byte{})
	if val == nil {
		t.Errorf("Key not found")
	} else if !bytes.Equal(val, []byte("value")) {
		t.Errorf("Expected 'value', got %v", val)
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_SetMany(t *testing.T) {
	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	stateBackend.SetMany(keys, values)

	for i, key := range keys {
		val := stateBackend.Get(key)
		if val == nil {
			t.Errorf("Key not found")
		} else if !bytes.Equal(val, values[i]) {
			t.Errorf("Expected %v, got %v", values[i], val)
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_Delete(t *testing.T) {
	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	stateBackend.SetMany(keys, values)

	stateBackend.DeleteMany(keys)

	for _, key := range keys {
		val := stateBackend.Get(key)
		if val != nil {
			t.Errorf("Key not deleted")
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_RangeQuery1(t *testing.T) {

	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	// Keys are strings
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	stateBackend.SetMany(keys, values)

	resKeys, resValues := stateBackend.RangeQuery([]byte("key"), []byte("key3"))

	if len(resKeys) != 2 {
		t.Errorf("Expected 2 keys, got %v", len(resKeys))
	}

	for i, key := range resKeys {
		if !bytes.Equal(key, keys[i]) {
			t.Errorf("Expected %v, got %v", keys[i], key)
		}

		if !bytes.Equal(resValues[i], values[i]) {
			t.Errorf("Expected %v, got %v", values[i], resValues[i])
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_RangeQuery2(t *testing.T) {

	// We use uint32 to represent the bucket index

	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	// Keys are uint32
	rawKeys := []uint32{1, 0, 8, 2, 3, 9, 7, 8, 4, 5}
	keys := make([][]byte, len(rawKeys))
	for i, key := range rawKeys {
		keys[i] = make([]byte, 4)
		binary.BigEndian.PutUint32(keys[i], key)
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value0"),
		[]byte("value8"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value9"),
		[]byte("value7"),
		[]byte("value8"),
		[]byte("value4"),
		[]byte("value5"),
	}

	stateBackend.SetMany(keys, values)

	// Range query from 2 to 8
	lower := make([]byte, 4)
	binary.BigEndian.PutUint32(lower, 2)
	upper := make([]byte, 4)
	binary.BigEndian.PutUint32(upper, 8)

	resKeys, resValues := stateBackend.RangeQuery(lower, upper)

	if len(resKeys) != 5 || len(resValues) != 5 {
		t.Errorf("Expected 5 keys, got %v", len(resKeys))
	}

	// The expected order of keys and values with respect to the input values
	expectedIdx := []int{3, 4, 8, 9, 6}

	for i, key := range resKeys {
		if !bytes.Equal(key, keys[expectedIdx[i]]) {
			t.Errorf("Expected %v, got %v", keys[expectedIdx[i]], key)
		}

		if !bytes.Equal(resValues[i], values[expectedIdx[i]]) {
			t.Errorf(
				"Expected %v, got %v",
				values[expectedIdx[i]],
				resValues[i],
			)
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_RangeQueryWithPrefix(t *testing.T) {

	// This test simulates the case where we use the bucketIdx prefix

	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	// Construct keys with the following format:
	// key = bucketIdx + real key
	bucketIdxes := []uint32{1, 2, 3, 1, 2, 3, 1, 2, 3}
	realKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
		[]byte("key9"),
	}

	keys := make([][]byte, len(bucketIdxes))
	for i, idx := range bucketIdxes {
		keySize := raw.SizeUint32(idx) + len(realKeys[i])
		keys[i] = make([]byte, keySize)
		binary.BigEndian.PutUint32(keys[i], idx)
		copy(keys[i][4:], realKeys[i])
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
		[]byte("value6"),
		[]byte("value7"),
		[]byte("value8"),
		[]byte("value9"),
	}

	stateBackend.SetMany(keys, values)

	// Now use range query to get all keys in bucket 2
	lower := make([]byte, 4)
	binary.BigEndian.PutUint32(lower, 2)
	upper := make([]byte, 4)
	binary.BigEndian.PutUint32(upper, 3)

	resKeys, resValues := stateBackend.RangeQuery(lower, upper)

	// We should only pull 3 records
	if len(resKeys) != 3 || len(resValues) != 3 {
		t.Errorf("Expected 3 keys, got %v", len(resKeys))
	}

	// The expected order of keys and values with respect to the input
	// 2-key2, 2-key5, 2-key8
	expectedIdx := []int{1, 4, 7}

	for i, key := range resKeys {

		if !bytes.Equal(key, keys[expectedIdx[i]]) {
			t.Errorf("Expected %v, got %v", keys[expectedIdx[i]], key)
		}

		if !bytes.Equal(resValues[i], values[expectedIdx[i]]) {
			t.Errorf(
				"Expected %v, got %v",
				values[expectedIdx[i]],
				resValues[i],
			)
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_MergeMany(t *testing.T) {
	config := configuration.Default()
	stateBackend := stateBackend.NewPebbleStateBackend(config)

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	stateBackend.SetMany(keys, values)

	// Now merge the values
	mergedValues := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}
	stateBackend.MergeMany(keys, mergedValues)

	// Expected values after merging
	for i := range mergedValues {
		mergedValues[i] = append(values[i], mergedValues[i]...)
	}

	// Check the values after merging
	for i, key := range keys {
		value := stateBackend.Get(key)
		if !bytes.Equal(value, mergedValues[i]) {
			t.Errorf("Expected %v, got %v", mergedValues[i], value)
		}
	}

	// Clean up
	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_ConcurrentGetMany(t *testing.T) {
	config := configuration.Default()
	config.PebbleEnableConcurrentGetMany = true
	config.PebbleGetManyMaxConcurrency = 2
	config.PebbleGetManyBatchSize = 2

	stateBackend := stateBackend.NewPebbleStateBackend(config)
	defer stateBackend.Close()

	// 1. Prepare data
	numKeys := 10
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		// Use a simple suffix.
		suffix := []byte{byte(i)}
		keys[i] = append([]byte("key"), suffix...)
		values[i] = append([]byte("val"), suffix...)
	}
	stateBackend.SetMany(keys, values)

	// 2. Test GetMany with all existing keys
	results := stateBackend.GetMany(keys)
	if len(results) != numKeys {
		t.Errorf("Expected %d results, got %d", numKeys, len(results))
	}
	for i, res := range results {
		if !bytes.Equal(res, values[i]) {
			t.Errorf("Index %d: expected %s, got %s", i, values[i], res)
		}
	}

	// 3. Test GetMany with mixed existing and non-existing keys
	// Key 0, 9 exist. "notfound" doesn't.
	mixedKeys := [][]byte{keys[0], []byte("notfound"), keys[9]}
	mixedResults := stateBackend.GetMany(mixedKeys)

	if len(mixedResults) != 3 {
		t.Errorf("Expected 3 results, got %d", len(mixedResults))
	}
	if !bytes.Equal(mixedResults[0], values[0]) {
		t.Errorf("Expected %s, got %s", values[0], mixedResults[0])
	}
	if mixedResults[1] != nil {
		t.Errorf("Expected nil for missing key, got %s", mixedResults[1])
	}
	if !bytes.Equal(mixedResults[2], values[9]) {
		t.Errorf("Expected %s, got %s", values[9], mixedResults[2])
	}

	// 4. Test GetMany with empty input
	emptyResults := stateBackend.GetMany([][]byte{})
	if len(emptyResults) != 0 {
		t.Errorf("Expected empty result for empty input")
	}

	// 5. Test Batching Logic (Corner case: num keys < batch size)
	// Config has batch size 2. Try with 1 key.
	singleKey := [][]byte{keys[0]}
	singleResult := stateBackend.GetMany(singleKey)
	if len(singleResult) != 1 || !bytes.Equal(singleResult[0], values[0]) {
		t.Errorf("Failed GetMany single key")
	}

	// 6. Test Batching Logic (Corner case: num keys not multiple of batch size)
	// Config has batch size 2. Try with 3 keys.
	oddKeys := keys[:3]
	oddResults := stateBackend.GetMany(oddKeys)
	if len(oddResults) != 3 {
		t.Errorf("Expected 3 results, got %d", len(oddResults))
	}
	for i := 0; i < 3; i++ {
		if !bytes.Equal(oddResults[i], values[i]) {
			t.Errorf("Index %d mismatch", i)
		}
	}

	testutils.CleanUpDataFolder()
}

func TestPebbleStateBackend_ConcurrentGetManyBenchmark(t *testing.T) {
	config := configuration.Default()
	config.PebbleEnableConcurrentGetMany = true
	config.PebbleGetManyBatchSize = 128
	config.PebbleGetManyMaxConcurrency = 20

	stateBackend := stateBackend.NewPebbleStateBackend(config)
	defer stateBackend.Close()

	// 1. Prepare data (2000 keys)
	numKeys := 2000
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := range numKeys {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		keys[i] = append([]byte("bench-key-"), buf...)
		values[i] = append([]byte("bench-val-"), buf...)
	}
	stateBackend.SetMany(keys, values)

	// 2. Measure GetMany
	startTime := time.Now()
	results := stateBackend.GetMany(keys)
	elapsed := time.Since(startTime)

	t.Logf("GetMany for %d keys took %v", numKeys, elapsed)

	// 3. Validation
	if len(results) != numKeys {
		t.Errorf("Expected %d results, got %d", numKeys, len(results))
	}
	for i, res := range results {
		if !bytes.Equal(res, values[i]) {
			t.Errorf("Index %d mismatch", i)
		}
	}

	testutils.CleanUpDataFolder()
}
