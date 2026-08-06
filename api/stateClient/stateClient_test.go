package stateClient_test

import (
	"encoding/binary"
	"log"
	"testing"
	"unsafe"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	sc "github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby/hash"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/varint"
)

/******************************************************************************
					  		 Test SimpleStateClient
******************************************************************************/

func TestSimpleStateClient_FetchAndFlushMemory(t *testing.T) {

	config := configuration.Default()
	config.StateBackendType = "memory"
	memoryBackend := stateBackend.NewMemoryStateBackend()
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: memoryBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// Fetch empty state service
	// Empty states are added to local cache
	keys := []string{"key1", "key2"}
	numFetched := stateClient.FetchSimpleState(keys, []uint16{stateId})
	if numFetched != 2 {
		t.Fatalf("expected to fetch 2 states, got %v\n", numFetched)
	}

	// Set value for "key1"
	state1, ok := stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	_, exist := state1.Get()
	if exist {
		t.Fatalf("expected key1 not exists, got exists")
	}
	state1.Set(tuple.NewTuple1(100))

	// Set value for "key2"
	state2, ok := stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	_, exist = state2.Get()
	if exist {
		t.Fatalf("expected key2 not exists, got exists")
	}
	state2.Set(tuple.NewTuple1(200))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushSimpleState()
	if numOverwrite != 2 || numMerge != 0 || numDelete != 0 {
		t.Errorf(
			"Expect to overwrite 2 states, merge 0 states, delete 0 states, but got overwrite=%v, merge=%v, delete=%v\n",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Now 2 key value pairs should be present in state service
	// Fetch the state service again using a new StateClient (note: use the same
	// StateClient also works)
	stateClient = sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId = sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)
	numFetched = stateClient.FetchSimpleState(keys, []uint16{stateId})
	if numFetched != 2 {
		t.Fatalf("expected to fetch 2 states, got %v\n", numFetched)
	}

	// Get value for "key1"
	state1, ok = stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	val1, exist := state1.Get()
	if !exist {
		t.Fatalf("expected key1 exists, got not exists")
	}
	if val1.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", val1.V1)
	}

	// Get value for "key2"
	state2, ok = stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	val2, exist := state2.Get()
	if !exist {
		t.Fatalf("expected key2 exists, got not exists")
	}
	if val2.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", val2.V1)
	}

	// Now only increment and set the value for "key1"
	val1.V1 += 1
	state1.Set(val1)
	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 0 {
		t.Errorf(
			"Expect to overwrite 1 state, merge 0 states, delete 0 states, but got overwrite=%v, merge=%v, delete=%v\n",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}
}

// Test pebble backend with some corner cases:
//  1. Test deduplication of keys in Fetch - only dedup keys are fetched
//  2. Only a subset of fetched keys are updated - flush should only flush the
//     updated keys
//  3. Test flushing deletion of a key
func TestSimpleStateClient_FetchAndFlushPebble(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	// Set value for "key1" and "key2"
	state1 := stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1.Set(tuple.NewTuple1(100))
	state2 := stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2.Set(tuple.NewTuple1(200))

	// Flush cache to state service
	stateClient.FlushSimpleState()

	// Now 2 key value pairs should be present in state service
	// Fetch the state service again with more requested keys (with duplicates)
	keys = []string{"key1", "key3", "key1", "key2", "key3", "key2"}
	numFetchedKeys := stateClient.FetchSimpleState(keys, []uint16{stateId})
	if numFetchedKeys != 3 {
		t.Fatalf("expected 3 fetched keys after dedup, got %v", numFetchedKeys)
	}

	// Now check "key1" and "key2" have correct values, "key3" is empty
	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state3 := stateClient.GetSimpleState(stateId, "key3").(*stateType.ValueState[*tuple.Tuple1[int]])

	state1Val, exist := state1.Get()
	if !exist || state1Val.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", state1Val)
	}
	state2Val, exist := state2.Get()
	if !exist || state2Val.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", state2Val)
	}
	_, exist = state3.Get()
	if exist {
		t.Fatalf("expected key3 not exists, got exists")
	}

	// Now only update "key1" and flush. The number of flushed keys should be 1
	// since only "key1" is updated since last fetch
	state1.Set(tuple.NewTuple1(101))
	numOverwrite, numMerge, numDelete := stateClient.FlushSimpleState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 1 overwritten key, got %v", numOverwrite)
	}

	// Now iterate all keys in StateService to check the values
	storedStates := make(map[string]int)
	iterator := stateService.StateBackendImpl.GetIterator()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()

		keyStr, _, _ := ord.UnmarshalString(nil, key[constant.KeyPrefixSize:])
		valueInt, _, _ := varint.UnmarshalInt(value)
		storedStates[keyStr] = valueInt
	}
	if len(storedStates) != 2 {
		t.Fatalf(
			"expected 2 key-value pairs in state service, got %v",
			len(storedStates),
		)
	}
	if storedStates["key1"] != 101 {
		t.Fatalf("expected key1=101, got %v", storedStates["key1"])
	}
	if storedStates["key2"] != 200 {
		t.Fatalf("expected key2=200, got %v", storedStates["key2"])
	}

	// Now fetch "key1" and "key2" and delete "key2"
	stateClient.FetchSimpleState([]string{"key1", "key2"}, []uint16{stateId})

	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2.Clear()
	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 0 || numMerge != 0 || numDelete != 1 {
		t.Fatalf("expected 1 deleted key, got %v", numDelete)
	}

	// Now iterate all keys in StateService to check the values
	storedStates = make(map[string]int)
	iterator = stateService.StateBackendImpl.GetIterator()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()

		keyStr, _, _ := ord.UnmarshalString(nil, key[constant.KeyPrefixSize:])
		valueInt, _, _ := varint.UnmarshalInt(value)
		storedStates[keyStr] = valueInt
	}
	// "key2" is deleted and we only have "key1" left
	if len(storedStates) != 1 || storedStates["key1"] != 101 {
		t.Fatalf("expected 1 key1=101, got %v", storedStates)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test if key prefix is correctly formatted in StateService:
// | op_id | bucket_id | state_id | user_key |
func TestSimpleStateClient_KeyPrefixFormat(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(66)

	// Init StateClient and register 2 states
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId1 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateId2 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[string]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	stateClient.FetchSimpleState(keys, []uint16{stateId1, stateId2})
	// Set "key1" and "key2" value for state1 and state2
	state1key1 := stateClient.GetSimpleState(stateId1, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key1.Set(tuple.NewTuple1(100))
	state1key2 := stateClient.GetSimpleState(stateId1, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2.Set(tuple.NewTuple1(200))
	state2key1 := stateClient.GetSimpleState(stateId2, "key1").(*stateType.ValueState[*tuple.Tuple1[string]])
	state2key1.Set(tuple.NewTuple1("value1"))
	state2key2 := stateClient.GetSimpleState(stateId2, "key2").(*stateType.ValueState[*tuple.Tuple1[string]])
	state2key2.Set(tuple.NewTuple1("value2"))
	// Flush cache to state service
	stateClient.FlushSimpleState()

	// Setup expected encoded keys in StateService
	expectedKeysInStateService := make(map[string]struct{})
	keyHashFunc := hash.GetKeyHashFunc(config)
	numBuckets := config.NumBuckets
	for _, key := range keys {
		for _, stateId := range []uint16{stateId1, stateId2} {

			// Now manually encode the key prefix. Note that we do have encoding
			// utils under stateClientUtils, but we want to test the encoding
			// here explicitly
			ptr := unsafe.Pointer(&key)
			buf := make([]byte, constant.KeyPrefixSize+network.SizeString(ptr))

			// 1. Encode op_id
			binary.BigEndian.PutUint16(buf[sc.OperatorIDOffset:], operatorId)

			// 2. Encode state_id
			binary.BigEndian.PutUint16(buf[sc.StateIDOffset:], stateId)

			// 3. Encode key
			network.EncodeString(ptr, buf[sc.KeyOffset:])

			// 4. Encode bucket_id
			bucketIdx := keyHashFunc.Hash(
				buf[sc.KeyOffset:],
			) % uint64(
				numBuckets,
			)
			binary.BigEndian.PutUint32(
				buf[sc.BucketIDOffset:],
				uint32(bucketIdx),
			)

			expectedKeysInStateService[string(buf)] = struct{}{}
		}
	}

	// Traverse the StateService state and check the key prefix
	iterator := stateService.StateBackendImpl.GetIterator()
	defer iterator.Close()
	cnt := 0
	for iterator.First(); iterator.Valid(); iterator.Next() {
		cnt++
		key := iterator.Key()
		if _, ok := expectedKeysInStateService[string(key)]; !ok {
			t.Fatalf("unexpected key = %v", key)
		}
	}
	if cnt != 4 {
		t.Fatalf("expected 4, got %d", cnt)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test multi-state state client:
// 1. Check only request 1 out of 2 states
// 2. Delete partial keys from multi states
func TestSimpleStateClient_MultiState(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register 2 states
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId1 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateId2 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// Write data for both states with 2 keys
	keys := []string{"key1", "key2"}
	numKeyFetched := stateClient.FetchSimpleState(
		keys,
		[]uint16{stateId1, stateId2},
	)
	if numKeyFetched != 4 {
		t.Fatalf("expected 4 fetched keys, got %v", numKeyFetched)
	}
	state1key1 := stateClient.GetSimpleState(stateId1, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key1.Set(tuple.NewTuple1(100))
	state1key2 := stateClient.GetSimpleState(stateId1, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2.Set(tuple.NewTuple1(200))
	state2key1 := stateClient.GetSimpleState(stateId2, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key1.Set(tuple.NewTuple1(300))
	state2key2 := stateClient.GetSimpleState(stateId2, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key2.Set(tuple.NewTuple1(400))
	// Flush cache to state service
	stateClient.FlushSimpleState()

	// Now fetch only stateId1
	numKeyFetched = stateClient.FetchSimpleState(keys, []uint16{stateId1})
	if numKeyFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeyFetched)
	}

	// Check stateId1 values
	state1key1 = stateClient.GetSimpleState(stateId1, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	val1, exist := state1key1.Get()
	if !exist || val1.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", val1)
	}
	state1key2 = stateClient.GetSimpleState(stateId1, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	val2, exist := state1key2.Get()
	if !exist || val2.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", val2)
	}

	// stateId2 should be unset now in cache
	if !stateClient.StateCacheIsUnset(stateId2) {
		t.Fatalf("expected stateId2 unset, got set")
	}

	// Now fetch all keys from both states and only delete partial keys
	numKeyFetched = stateClient.FetchSimpleState(
		keys,
		[]uint16{stateId1, stateId2},
	)
	if numKeyFetched != 4 {
		t.Fatalf("expected 4 fetched keys, got %v", numKeyFetched)
	}
	state1key1 = stateClient.GetSimpleState(stateId1, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2 = stateClient.GetSimpleState(stateId1, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key1 = stateClient.GetSimpleState(stateId2, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key2 = stateClient.GetSimpleState(stateId2, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])

	// Execute partial update and delete
	val1, _ = state1key1.Get()
	val1.V1 += 10
	state1key1.Set(val1)
	state1key2.Clear()
	state2key1.Clear()
	numOverwrite, numMerge, numDelete := stateClient.FlushSimpleState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 2 {
		t.Fatalf(
			"expected 1 overwritten key and 2 deleted keys, got %v, %v, %v",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Now iterate all keys in StateService to check the values
	numTotalKeys := 0
	storedStates := make(map[uint16]map[string]int)
	iterator := stateService.StateBackendImpl.GetIterator()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		numTotalKeys++
		key := iterator.Key()
		value := iterator.Value()

		stateId := binary.BigEndian.Uint16(
			key[sc.StateIDOffset : sc.StateIDOffset+2],
		)
		keyStr, _, _ := ord.UnmarshalString(nil, key[constant.KeyPrefixSize:])
		valueInt, _, _ := varint.UnmarshalInt(value)

		if _, ok := storedStates[stateId]; !ok {
			storedStates[stateId] = make(map[string]int)
		}
		storedStates[stateId][keyStr] = valueInt
	}

	if numTotalKeys != 2 {
		t.Fatalf(
			"expected 2 key-value pairs in state service, got %v",
			numTotalKeys,
		)
	}
	if len(storedStates[stateId1]) != 1 ||
		storedStates[stateId1]["key1"] != 110 {
		t.Fatalf(
			"expected stateId1 with 1 key1=110, got %v",
			storedStates[stateId1],
		)
	}
	if len(storedStates[stateId2]) != 1 ||
		storedStates[stateId2]["key2"] != 400 {
		t.Fatalf(
			"expected stateId2 with 1 key2=400, got %v",
			storedStates[stateId2],
		)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test list state and merge operations: Add(), AddAll(), Update(), Clear()
func TestSimpleStateClient_ListStateAndMerge(t *testing.T) {

	config := configuration.Default()
	config.StateBackendType = "pebble"
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId := sc.RegisterState[*stateType.ListState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	// Set initial value for "key1" and "key2"
	state1 := stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	state1.Add(tuple.NewTuple1(100))
	state2 := stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	state2.Add(tuple.NewTuple1(200))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushSimpleState()
	if numOverwrite != 0 || numMerge != 2 || numDelete != 0 {
		t.Fatalf("expected 2 merged keys, got %v", numMerge)
	}

	// Now fetch them again and apply append operation
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	list1 := state1.Get()
	if len(list1) != 1 || list1[0].V1 != 100 {
		t.Fatalf("expected key1=[100], got %v", list1)
	}
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	list2 := state2.Get()
	if len(list2) != 1 || list2[0].V1 != 200 {
		t.Fatalf("expected key2=[200], got %v", list2)
	}

	// Append new values
	state1.Add(tuple.NewTuple1(101))
	state1.Add(tuple.NewTuple1(102))
	state2.Add(tuple.NewTuple1(201))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 0 || numMerge != 2 || numDelete != 0 {
		t.Fatalf("expected 2 merged keys, got %v", numMerge)
	}

	// Fetch again to verify the values
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 3 ||
		list1[0].V1 != 100 ||
		list1[1].V1 != 101 ||
		list1[2].V1 != 102 {
		t.Fatalf("expected key1=[100,101,102], got %v", list1)
	}
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 2 ||
		list2[0].V1 != 200 ||
		list2[1].V1 != 201 {
		t.Fatalf("expected key2=[200,201], got %v", list2)
	}

	state1.AddAll([]*tuple.Tuple1[int]{
		tuple.NewTuple1(103),
		tuple.NewTuple1(104),
	})
	state2.AddAll([]*tuple.Tuple1[int]{
		tuple.NewTuple1(202),
		tuple.NewTuple1(203),
	})

	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 0 || numMerge != 2 || numDelete != 0 {
		t.Fatalf("expected 2 merged keys, got %v", numMerge)
	}

	// Fetch again to verify the values
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 5 ||
		list1[0].V1 != 100 ||
		list1[1].V1 != 101 ||
		list1[2].V1 != 102 ||
		list1[3].V1 != 103 ||
		list1[4].V1 != 104 {
		t.Fatalf("expected key1=[100,101,102,103,104], got %v", list1)
	}
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 4 ||
		list2[0].V1 != 200 ||
		list2[1].V1 != 201 ||
		list2[2].V1 != 202 ||
		list2[3].V1 != 203 {
		t.Fatalf("expected key2=[200,201,202,203], got %v", list2)
	}

	// Change the list1 and override list 2 with a new list
	state1.Update(list1[:2])
	state2.Update([]*tuple.Tuple1[int]{
		tuple.NewTuple1(801),
		tuple.NewTuple1(802),
	})
	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 2 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 2 overwritten keys, got %v", numOverwrite)
	}

	// Fetch again to verify the values
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 2 ||
		list1[0].V1 != 100 ||
		list1[1].V1 != 101 {
		t.Fatalf("expected key1=[100,101], got %v", list1)
	}
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 2 ||
		list2[0].V1 != 801 ||
		list2[1].V1 != 802 {
		t.Fatalf("expected key2=[801,802], got %v", list2)
	}

	// Delete key1 and add more values to key2
	state1.Clear()
	state2.AddAll([]*tuple.Tuple1[int]{
		tuple.NewTuple1(803),
		tuple.NewTuple1(804),
	})
	numOverwrite, numMerge, numDelete = stateClient.FlushSimpleState()
	if numOverwrite != 0 || numMerge != 1 || numDelete != 1 {
		t.Fatalf(
			"expected 1 merged key and 1 deleted key, got %v, %v, %v",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Fetch again to verify the values
	stateClient.FetchSimpleState(keys, []uint16{stateId})

	state1 = stateClient.GetSimpleState(stateId, "key1").(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 0 {
		t.Fatalf("expected key1=[], got %v", list1)
	}
	state2 = stateClient.GetSimpleState(stateId, "key2").(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 4 ||
		list2[0].V1 != 801 ||
		list2[1].V1 != 802 ||
		list2[2].V1 != 803 ||
		list2[3].V1 != 804 {
		t.Fatalf("expected key2=[801,802,803,804], got %v", list2)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

/******************************************************************************
					  		 Test WindowStateClient
******************************************************************************/

func TestWindowStateClient_FetchAndFlushPebble(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	timestamps := []int64{10, 20}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	// Set value for "key1" and "key2"
	state1 := stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1.Set(tuple.NewTuple1(100))
	state2 := stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2.Set(tuple.NewTuple1(200))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushWindowState()
	if numOverwrite != 2 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 2 overwritten keys, got %v", numOverwrite)
	}

	// Now 2 key value pairs should be present in state service
	// Fetch the state service again using a new StateClient (note: use the same
	// StateClient also works)
	stateClient = sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId = sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	// Get value for "key1"
	state1 = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	val1, exist := state1.Get()
	if !exist || val1.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", val1)
	}

	// Get value for "key2"
	state2 = stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	val2, exist := state2.Get()
	if !exist || val2.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", val2)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test corner cases:
//  1. Deduplication: duplicate keys with same start time, duplicate keys with
//     different start time
//  2. Partial update: only a subset of fetched windows are updated and deleted
//     - flush should only flush the updated windows
func TestWindowStateClient_FetchAndFlushPebble2(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service. There are 3 unique windows:
	// ("key1", 10), ("key2", 20), ("key2", 30)
	keys := []string{"key1", "key1", "key2", "key2"}
	timestamps := []int64{10, 10, 20, 30}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 3 {
		t.Fatalf("expected 3 fetched keys after dedup, got %v", numKeysFetched)
	}

	// Set value for "key1" and "key2"
	state1 := stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1.Set(tuple.NewTuple1(100))
	state2 := stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2.Set(tuple.NewTuple1(200))
	state3 := stateClient.GetWindowState(stateId, "key2", 30).(*stateType.ValueState[*tuple.Tuple1[int]])
	state3.Set(tuple.NewTuple1(300))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushWindowState()
	if numOverwrite != 3 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 3 overwritten keys, got %v", numOverwrite)
	}

	// Now 3 key value pairs should be present in state service
	// Fetch again with partial update
	keys = []string{"key1", "key2", "key2", "key2"}
	timestamps = []int64{10, 20, 30, 40}
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 4 {
		t.Fatalf(
			"expected 4 fetched keys after partial update, got %v",
			numKeysFetched,
		)
	}

	// Update ("key1", 10)
	state1 = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1.Set(tuple.NewTuple1(101))

	// Delete ("key2", 20)
	state2 = stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2.Clear()

	// Flush cache to state service - only 1 window should be flushed
	numOverwrite, numMerge, numDelete = stateClient.FlushWindowState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 1 {
		t.Fatalf("expected 1 overwritten key and 1 deleted key, got %v, %v, %v",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Now iterate all keys in StateService to check the values
	storedStates := make(map[string]map[int64]int)
	iterator := stateService.StateBackendImpl.GetIterator()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()

		keyStr, numBytesRead, _ := ord.UnmarshalString(
			nil,
			key[constant.KeyPrefixSize:],
		)
		windowStartTime, _, _ := varint.UnmarshalInt64(
			key[constant.KeyPrefixSize+numBytesRead:],
		)
		valueInt, _, _ := varint.UnmarshalInt(value)

		if _, ok := storedStates[keyStr]; !ok {
			storedStates[keyStr] = make(map[int64]int)
		}
		storedStates[keyStr][windowStartTime] = valueInt
	}
	if len(storedStates) != 2 || len(storedStates["key1"]) != 1 ||
		len(storedStates["key2"]) != 1 {
		t.Fatalf("expected 2 keys with total 2 windows, got %v", storedStates)
	}
	if storedStates["key1"][10] != 101 {
		t.Fatalf("expected (key1,10)=101, got %v", storedStates["key1"][10])
	}
	if storedStates["key2"][30] != 300 {
		t.Fatalf("expected (key2,30)=300, got %v", storedStates["key2"][30])
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test if key prefix is correctly formatted in StateService:
// | op_id | bucket_id | state_id | user_key | window_start_time |
func TestWindowStateClient_KeyPrefixFormat(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(66)

	// Init StateClient and register 2 states
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId1 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateId2 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	timestamps := []int64{10, 20}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId1, stateId2},
	)
	if numKeysFetched != 4 {
		t.Fatalf("expected 4 fetched keys, got %v", numKeysFetched)
	}
	// Set "key1" and "key2" value for state1 and state2
	state1key1 := stateClient.GetWindowState(stateId1, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key1.Set(tuple.NewTuple1(100))
	state1key2 := stateClient.GetWindowState(stateId1, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2.Set(tuple.NewTuple1(200))
	state2key1 := stateClient.GetWindowState(stateId2, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key1.Set(tuple.NewTuple1(300))
	state2key2 := stateClient.GetWindowState(stateId2, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key2.Set(tuple.NewTuple1(400))
	// Flush cache to state service
	stateClient.FlushWindowState()

	// Setup expected encoded keys in StateService
	expectedKeysInStateService := make(map[string]struct{})
	keyHashFunc := hash.GetKeyHashFunc(config)
	numBuckets := config.NumBuckets
	for i, key := range keys {
		timestamp := timestamps[i]
		for _, stateId := range []uint16{stateId1, stateId2} {

			// Now manually encode the key prefix. Note that we do have encoding
			// utils under stateClientUtils, but we want to test the encoding
			// here explicitly
			keyPtr := unsafe.Pointer(&key)
			timestampPtr := unsafe.Pointer(&timestamp)
			buf := make(
				[]byte,
				constant.KeyPrefixSize+network.SizeString(
					keyPtr,
				)+network.SizeInt64(
					timestampPtr,
				),
			)

			// 1. Encode op_id
			binary.BigEndian.PutUint16(buf[sc.OperatorIDOffset:], operatorId)

			// 2. Encode state_id
			binary.BigEndian.PutUint16(buf[sc.StateIDOffset:], stateId)

			// 3. Encode key
			numBytesWritten := network.EncodeString(keyPtr, buf[sc.KeyOffset:])

			// 4. Encode bucket_id
			bucketIdx := keyHashFunc.Hash(
				buf[sc.KeyOffset:sc.KeyOffset+numBytesWritten],
			) % uint64(
				numBuckets,
			)
			binary.BigEndian.PutUint32(
				buf[sc.BucketIDOffset:],
				uint32(bucketIdx),
			)

			// 5. Encode window_start_time
			network.EncodeInt64(
				timestampPtr,
				buf[sc.KeyOffset+numBytesWritten:],
			)

			expectedKeysInStateService[string(buf)] = struct{}{}
		}
	}

	// Traverse the StateService state and check the key prefix
	iterator := stateService.StateBackendImpl.GetIterator()
	defer iterator.Close()
	cnt := 0
	for iterator.First(); iterator.Valid(); iterator.Next() {
		cnt++
		key := iterator.Key()
		if _, ok := expectedKeysInStateService[string(key)]; !ok {
			t.Fatalf("unexpected key = %v", key)
		}
	}
	if cnt != 4 {
		t.Fatalf("expected 4, got %d", cnt)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test multi-state state client:
// 1. Check only request 1 out of 2 states
// 2. Delete partial keys from multi states
func TestWindowStateClient_MultiState(t *testing.T) {

	config := configuration.Default()
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register 2 states
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId1 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateId2 := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// Write data for both states with 2 windows
	keys := []string{"key1", "key2"}
	timestamps := []int64{10, 20}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId1, stateId2},
	)
	if numKeysFetched != 4 {
		t.Fatalf("expected 4 fetched keys, got %v", numKeysFetched)
	}
	state1key1 := stateClient.GetWindowState(stateId1, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key1.Set(tuple.NewTuple1(100))
	state1key2 := stateClient.GetWindowState(stateId1, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2.Set(tuple.NewTuple1(200))
	state2key1 := stateClient.GetWindowState(stateId2, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key1.Set(tuple.NewTuple1(300))
	state2key2 := stateClient.GetWindowState(stateId2, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key2.Set(tuple.NewTuple1(400))
	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushWindowState()
	if numOverwrite != 4 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 4 overwritten keys, got %v", numOverwrite)
	}

	// Now fetch only stateId1
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId1},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	// Check stateId1 values
	state1key1 = stateClient.GetWindowState(stateId1, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	val1, exist := state1key1.Get()
	if !exist || val1.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", val1)
	}
	state1key2 = stateClient.GetWindowState(stateId1, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	val2, exist := state1key2.Get()
	if !exist || val2.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", val2)
	}

	// stateId2 should be unset now in cache
	if !stateClient.StateCacheIsUnset(stateId2) {
		t.Fatalf("expected stateId2 unset, got set")
	}

	// Now fetch all keys from both states and delete partial keys
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId1, stateId2},
	)
	if numKeysFetched != 4 {
		t.Fatalf("expected 4 fetched keys, got %v", numKeysFetched)
	}
	state1key1 = stateClient.GetWindowState(stateId1, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state1key2 = stateClient.GetWindowState(stateId1, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key1 = stateClient.GetWindowState(stateId2, "key1", 10).(*stateType.ValueState[*tuple.Tuple1[int]])
	state2key2 = stateClient.GetWindowState(stateId2, "key2", 20).(*stateType.ValueState[*tuple.Tuple1[int]])

	// Execute partial delete
	state1key1.Clear()
	state1key2.Clear()
	state2key1.Clear()
	numOverwrite, numMerge, numDelete = stateClient.FlushWindowState()
	if numOverwrite != 0 || numMerge != 0 || numDelete != 3 {
		t.Fatalf(
			"expected 3 deleted keys, got %v, %v, %v",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Now iterate all keys in StateService to check the values
	numTotalKeys := 0
	storedStates := make(map[uint16]map[string]map[int64]int)
	iterator := stateService.StateBackendImpl.GetIterator()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		numTotalKeys++
		key := iterator.Key()
		value := iterator.Value()

		stateId := binary.BigEndian.Uint16(
			key[sc.StateIDOffset : sc.StateIDOffset+2],
		)
		keyStr, numBytesRead, _ := ord.UnmarshalString(
			nil,
			key[constant.KeyPrefixSize:],
		)
		windowStartTime, _, _ := varint.UnmarshalInt64(
			key[constant.KeyPrefixSize+numBytesRead:],
		)
		valueInt, _, _ := varint.UnmarshalInt(value)

		if _, ok := storedStates[stateId]; !ok {
			storedStates[stateId] = make(map[string]map[int64]int)
		}
		if _, ok := storedStates[stateId][keyStr]; !ok {
			storedStates[stateId][keyStr] = make(map[int64]int)
		}
		storedStates[stateId][keyStr][windowStartTime] = valueInt
	}

	if numTotalKeys != 1 {
		t.Fatalf(
			"expected 1 key-value pairs in state service, got %v",
			numTotalKeys,
		)
	}
	if len(storedStates[stateId2]) != 1 ||
		len(storedStates[stateId2]["key2"]) != 1 ||
		storedStates[stateId2]["key2"][20] != 400 {
		t.Fatalf(
			"expected stateId2 with 1 key2=400, got %v",
			storedStates[stateId2],
		)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test list state and merge operations: Add(), AddAll(), Update(), Clear()
func TestWindowStateClient_ListStateAndMerge(t *testing.T) {

	config := configuration.Default()
	config.StateBackendType = "pebble"
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId := sc.RegisterState[*stateType.ListState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// First write some data into state service
	keys := []string{"key1", "key2"}
	timestamps := []int64{10, 20}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	// Set initial value for "key1" and "key2"
	state1 := stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	state1.Add(tuple.NewTuple1(100))
	state2 := stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ListState[*tuple.Tuple1[int]])
	state2.Add(tuple.NewTuple1(200))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushWindowState()
	if numOverwrite != 0 || numMerge != 2 || numDelete != 0 {
		t.Fatalf("expected 2 merged keys, got %v", numMerge)
	}

	// Now fetch them again and apply AddAll() operation
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	state1 = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	list1 := state1.Get()
	if len(list1) != 1 || list1[0].V1 != 100 {
		t.Fatalf("expected key1=[100], got %v", list1)
	}
	state2 = stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ListState[*tuple.Tuple1[int]])
	list2 := state2.Get()
	if len(list2) != 1 || list2[0].V1 != 200 {
		t.Fatalf("expected key2=[200], got %v", list2)
	}

	// Append new values
	state1.AddAll([]*tuple.Tuple1[int]{
		tuple.NewTuple1(101),
		tuple.NewTuple1(102),
	})
	state2.AddAll([]*tuple.Tuple1[int]{
		tuple.NewTuple1(201),
		tuple.NewTuple1(202),
	})

	// Flush cache to state service
	numOverwrite, numMerge, numDelete = stateClient.FlushWindowState()
	if numOverwrite != 0 || numMerge != 2 || numDelete != 0 {
		t.Fatalf("expected 2 merged keys, got %v", numMerge)
	}

	// Fetch again to verify the values, and then apply Update() and Clear()
	// operations
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	state1 = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 3 ||
		list1[0].V1 != 100 ||
		list1[1].V1 != 101 ||
		list1[2].V1 != 102 {
		t.Fatalf("expected key1=[100,101,102], got %v", list1)
	}
	state2 = stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 3 ||
		list2[0].V1 != 200 ||
		list2[1].V1 != 201 ||
		list2[2].V1 != 202 {
		t.Fatalf("expected key2=[200,201,202], got %v", list2)
	}

	state1.Update(list1[:2])
	state2.Clear()

	numOverwrite, numMerge, numDelete = stateClient.FlushWindowState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 1 {
		t.Fatalf("expected 1 overwritten key and 1 deleted key, got %v, %v, %v",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Fetch again to verify the values
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 2 {
		t.Fatalf("expected 2 fetched keys, got %v", numKeysFetched)
	}

	state1 = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	list1 = state1.Get()
	if len(list1) != 2 ||
		list1[0].V1 != 100 ||
		list1[1].V1 != 101 {
		t.Fatalf("expected key1=[100,101], got %v", list1)
	}
	state2 = stateClient.GetWindowState(stateId, "key2", 20).(*stateType.ListState[*tuple.Tuple1[int]])
	list2 = state2.Get()
	if len(list2) != 0 {
		t.Fatalf("expected key2=[], got %v", list2)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

// Test list state with aggregator to simulate window operators:
// NewFunc(), AddFunc(), OutFunc()
func TestWindowStateClient_ListStateWithAggregator(t *testing.T) {

	config := configuration.Default()
	config.StateBackendType = "pebble"
	pebbleBackend := stateBackend.NewPebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: pebbleBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.WindowStateClient,
	)
	stateId := sc.RegisterState[*stateType.ListState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// Define the aggregator functions
	aggregator := dataflow.NewAggregator(
		// NewFunc()
		func() *stateType.ListState[*tuple.Tuple1[int]] {
			tupleList := []*tuple.Tuple1[int]{
				tuple.NewTuple1(100),
				tuple.NewTuple1(200),
			}
			return stateType.NewListState(tupleList)
		},
		// AddFunc() - only define append operation
		func(state *stateType.ListState[*tuple.Tuple1[int]], in *tuple.Tuple1[int]) *stateType.ListState[*tuple.Tuple1[int]] {

			// Output cannot be the direct reference of input value
			newVal := in.Copy().(*tuple.Tuple1[int])
			state.Add(newVal)
			return state
		},
		// OutFunc() - return the length of the list
		func(state *stateType.ListState[*tuple.Tuple1[int]]) *tuple.Tuple1[int] {
			list := state.Get()
			if len(list) == 0 {
				log.Fatalln("list state is empty")
			}
			list[0].V1 = len(list)
			return list[0]
		},
		// MergeFunc() - not used
		func(state1 *stateType.ListState[*tuple.Tuple1[int]], state2 *stateType.ListState[*tuple.Tuple1[int]]) *stateType.ListState[*tuple.Tuple1[int]] {
			return state1
		},
	)

	// First flush the state by aggregator.NewFunc()
	keys := []string{"key1"}
	timestamps := []int64{10}
	numKeysFetched := stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 1 {
		t.Fatalf("expected 1 fetched key, got %v", numKeysFetched)
	}

	state := stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	if state.HasValue() {
		t.Errorf("expected state to be empty")
	}
	state.CopyFrom(aggregator.NewFunc())

	numOverwrite, numMerge, numDelete := stateClient.FlushWindowState()
	if numOverwrite != 1 || numMerge != 0 || numDelete != 0 {
		t.Fatalf("expected 1 overwritten key, got %v", numOverwrite)
	}

	// Fetch again to verify the values
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 1 {
		t.Fatalf("expected 1 fetched key, got %v", numKeysFetched)
	}

	state = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	list := state.Get()
	if len(list) != 2 ||
		list[0].V1 != 100 ||
		list[1].V1 != 200 {
		t.Fatalf("expected key1=[100,200], got %v", list)
	}

	// Now apply aggregator.AddFunc()
	state.CopyFrom(aggregator.AddFunc(state, tuple.NewTuple1(300)))
	state.CopyFrom(aggregator.AddFunc(state, tuple.NewTuple1(400)))

	numOverwrite, numMerge, numDelete = stateClient.FlushWindowState()
	if numOverwrite != 0 || numMerge != 1 || numDelete != 0 {
		t.Fatalf("expected 1 merged key, got %v", numMerge)
	}

	// Fetch again to verify the values
	numKeysFetched = stateClient.FetchWindowState(
		keys,
		timestamps,
		[]uint16{stateId},
	)
	if numKeysFetched != 1 {
		t.Fatalf("expected 1 fetched key, got %v", numKeysFetched)
	}

	state = stateClient.GetWindowState(stateId, "key1", 10).(*stateType.ListState[*tuple.Tuple1[int]])
	list = state.Get()
	if len(list) != 4 ||
		list[0].V1 != 100 ||
		list[1].V1 != 200 ||
		list[2].V1 != 300 ||
		list[3].V1 != 400 {
		t.Fatalf("expected key1=[100,200,300,400], got %v", list)
	}

	// Now apply aggregator.OutFunc()
	out := aggregator.OutFunc(state)
	if out.V1 != 4 {
		t.Fatalf("expected output=4, got %v", out.V1)
	}

	// Cleanup
	testutils.CleanUpDataFolder()
}

/******************************************************************************
					  		 Test Remote Pebble
******************************************************************************/

func TestSimpleStateClient_FetchAndFlushRemotePebble(t *testing.T) {

	addrs, _, stopRemotePebbleServers := testutils.StartRemotePebbleTestServers(
		3,
	)
	defer stopRemotePebbleServers()

	config := configuration.Default()
	config.StateBackendType = "remote-pebble"
	config.RemotePebbleAddrs = addrs
	config.NumBuckets = 64
	config.DataPlanePort = "8790"
	remoteBackend := stateBackend.NewRemotePebbleStateBackend(config)
	stateService := &state.StateService{
		Config:           config,
		StateBackendImpl: remoteBackend,
	}
	operatorId := uint16(1)

	// Init StateClient and register a state
	stateClient := sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId := sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)

	// Fetch empty state service
	keys := []string{"key1", "key2"}
	numFetched := stateClient.FetchSimpleState(keys, []uint16{stateId})
	if numFetched != 2 {
		t.Fatalf("expected to fetch 2 states, got %v\n", numFetched)
	}

	// Set value for "key1"
	state1, ok := stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	state1.Set(tuple.NewTuple1(100))

	// Set value for "key2"
	state2, ok := stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	state2.Set(tuple.NewTuple1(200))

	// Flush cache to state service
	numOverwrite, numMerge, numDelete := stateClient.FlushSimpleState()
	if numOverwrite != 2 || numMerge != 0 || numDelete != 0 {
		t.Errorf(
			"Expect to overwrite 2 states, merge 0 states, delete 0 states, but got overwrite=%v, merge=%v, delete=%v\n",
			numOverwrite,
			numMerge,
			numDelete,
		)
	}

	// Fetch again using a new StateClient
	stateClient = sc.NewStateClient[string](
		sc.SimpleStateClient,
	)
	stateId = sc.RegisterState[*stateType.ValueState[*tuple.Tuple1[int]]](
		stateClient,
	)
	stateClient.Setup(operatorId, stateService, config)
	numFetched = stateClient.FetchSimpleState(keys, []uint16{stateId})
	if numFetched != 2 {
		t.Fatalf("expected to fetch 2 states, got %v\n", numFetched)
	}

	// Get value for "key1"
	state1, ok = stateClient.GetSimpleState(stateId, "key1").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	val1, exist := state1.Get()
	if !exist {
		t.Fatalf("expected key1 exists, got not exists")
	}
	if val1.V1 != 100 {
		t.Fatalf("expected key1=100, got %v", val1.V1)
	}

	// Get value for "key2"
	state2, ok = stateClient.GetSimpleState(stateId, "key2").(*stateType.ValueState[*tuple.Tuple1[int]])
	if !ok {
		t.Fatalf("type assertion failed\n")
	}
	val2, exist := state2.Get()
	if !exist {
		t.Fatalf("expected key2 exists, got not exists")
	}
	if val2.V1 != 200 {
		t.Fatalf("expected key2=200, got %v", val2.V1)
	}

	testutils.CleanUpDataFolder()
}
