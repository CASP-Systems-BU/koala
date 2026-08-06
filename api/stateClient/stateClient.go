package stateClient

import (
	"log"
	"reflect"
	"unsafe"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateCache"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby/hash"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state"
)

/*
StateClient bridges the stateful operator and the StateService. It provides APIs
for state access with 2 major functionalities:
1. (de)serialization of keys/values. StateService exposes byte-oriented APIs.
2. Memory caching in operator space - avoid frequent access to StateService. If
   the operator embeds multiple states, StateClient maintains a separate cache
   for each state.

StateClient provides 2 set of APIs:
1. Simple state: regular key-value state
2. Window state: key and its window start time as the compound key
*/

type StateClientType int

const (
	SimpleStateClient StateClientType = iota
	WindowStateClient
)

type StateClient[K comparable] struct {

	// Copy of current operator ID
	operatorID uint16

	// Pointer to the State Service
	stateService *state.StateService

	// Current state ID counter for register state
	curStateIdCounter uint16

	// Simple state client or Window state client
	stateClientType StateClientType

	// Key hash function used to calculate the bucket idx given the key.
	keyHashFunc hash.HashFunc
	// Total number of buckets
	numBuckets int64

	// Codec for key
	keyEncoder network.EncodeFunc
	keyDecoder network.DecodeFunc
	keySizer   network.SizeFunc

	/**************************************************************************
					Batch-oriented cache (reset for every batch)
	**************************************************************************/

	// Map from StateID to its cache. We support multiple states in StateClient.
	// each state has an ID and its dedicated cache.
	cache map[uint16]stateCache.StateCache[K]

	// List of deduplicated user keys for the batch - avoid duplicate state
	// access if same key appears multiple times in the batch
	dedupedKeys []K

	// List of bucket IDs corresponding to dedupedKeys
	bucketIDs []int64

	// Serialized keys for all states. It's used to avoid duplicate
	// serialization in both Fetch() and Flush()
	serializedKeys map[uint16][][]byte

	// [Window State Cache Only] Along with dedupedKeys, window state also needs
	// to track the start time of each window
	dedupedWindowStartTimes []int64
}

/******************************************************************************
					  Init at Job Graph construction time
******************************************************************************/

func NewStateClient[K comparable](
	stateClientType StateClientType,
) *StateClient[K] {
	return &StateClient[K]{
		curStateIdCounter: 0,
		stateClientType:   stateClientType,
		cache:             make(map[uint16]stateCache.StateCache[K]),
	}
}

// Stateful operator should explicitly register its state(s)
func RegisterState[T stateType.StateType, K comparable](
	sc *StateClient[K],
) uint16 {

	curStateID := sc.curStateIdCounter
	sc.curStateIdCounter++

	switch sc.stateClientType {
	case SimpleStateClient:
		sc.cache[curStateID] = stateCache.NewSimpleStateCache[K, T]()
	case WindowStateClient:
		sc.cache[curStateID] = stateCache.NewWindowStateCache[K, T]()
	default:
		log.Fatalf("unknown state client type: %v", sc.stateClientType)
	}

	return curStateID
}

/******************************************************************************
						  Setup at task placement time
******************************************************************************/

func (sc *StateClient[K]) Setup(
	operatorID uint16,
	stateService *state.StateService,
	config *configuration.Configuration,
) {
	sc.operatorID = operatorID
	sc.stateService = stateService
	sc.keyHashFunc = hash.GetKeyHashFunc(config)
	sc.numBuckets = config.NumBuckets

	// Get codec for key
	var key K
	sc.keyEncoder = network.EncodeFuncFromKind(reflect.TypeOf(key).Kind())
	sc.keyDecoder = network.DecodeFuncFromKind(reflect.TypeOf(key).Kind())
	sc.keySizer = network.TupleSizeFuncFromKind(reflect.TypeOf(key).Kind())

	// Setup state cache
	if len(sc.cache) <= 0 {
		log.Fatalln(
			"Check operator implementation: no state registered in state client",
		)
	}

	// Disable incremental serialization at flush for TiKV since it does not
	// support merge operation. Currently missing merge support for memory state
	// backend - disable incremental serialization for memory too.
	allowIncrementalSerialization := true
	if config.StateBackendType == "tikv" ||
		config.StateBackendType == "memory" ||
		config.StateBackendType == "remote-pebble" {
		allowIncrementalSerialization = false
	}
	for _, cache := range sc.cache {
		cache.Setup(allowIncrementalSerialization)
	}
}

/******************************************************************************
						StateClient APIs for Simple State
******************************************************************************/

// Fetch simple state from StateService to memory cache for specified state IDs
// 1. Serialize requested keys
// 2. Request StateSerive
// 3. Deserialize obtained values
// 4. Update local StateClient cache
// Return the number of fetched keys - same user key with multiple states are
// counted multiple times. If key not found in state service, it is counted as
// fetched as well since it is set to empty state in cache.
func (sc *StateClient[K]) FetchSimpleState(keys []K, stateIDs []uint16) int {
	//t1 := time.Now()
	if sc.stateClientType == WindowStateClient {
		log.Fatalln("For window state client, use FetchWindowState()")
	}
	if len(keys) == 0 {
		log.Fatalf("Empty keys to fetch")
	}
	sc.validateStateIDs(stateIDs)

	sc.stateService.MetricCollector.UpdateRecordNumberPerBatch(int64(len(keys)))
	//t2 := time.Now()
	// De-duplicate keys to avoid duplicate fetch from StateService
	keysSeen := make(map[K]struct{})
	sc.dedupedKeys = make([]K, 0, len(keys))
	for _, key := range keys {
		if _, ok := keysSeen[key]; !ok {
			keysSeen[key] = struct{}{}
			sc.dedupedKeys = append(sc.dedupedKeys, key)
		}
	}
	sc.stateService.MetricCollector.UpdateKeyNumberPerBatch(int64(len(sc.dedupedKeys)))
	// Allocate space for serialized keys
	sc.serializedKeys = make(map[uint16][][]byte)
	for _, stateID := range stateIDs {
		sc.serializedKeys[stateID] = make([][]byte, 0, len(sc.dedupedKeys))
	}

	//t3 := time.Now()

	// Serialize keys
	sc.bucketIDs = make([]int64, len(sc.dedupedKeys))
	for i, key := range sc.dedupedKeys {

		// Serialize the key for the 1st state (the 1st state id won't be empty)
		stateID := stateIDs[0]
		serializedKey, bucketId := sc.encodeSimpleKey(key, stateID)
		sc.bucketIDs[i] = bucketId
		sc.serializedKeys[stateID] = append(
			sc.serializedKeys[stateID],
			serializedKey,
		)

		// Serialize the key for multi-state operators
		sc.serializeMultiStateKeys(serializedKey, stateIDs[1:])
	}
	//t4 := time.Now()

	// Fetch state values from StateService
	serializedValues := sc.stateService.GetManyMultiState(
		sc.operatorID,
		sc.serializedKeys,
		sc.bucketIDs,
	)

	//t5 := time.Now()
	// For fetched state, reset their cache accordingly. For state that is not
	// requested, clean up the cache for safety.
	for stateID, stateCache := range sc.cache {
		if values, ok := serializedValues[stateID]; ok {
			stateCache.SetSimpleStateCache(sc.dedupedKeys, values)
		} else {
			stateCache.CleanUp()
		}
	}

	// Return the number of fetched keys (counting same user key with multiple
	// states multiple times). If key not found in state service, it is counted
	// as fetched as well since it is set to empty state in cache.
	numFetchedKeys := 0
	for _, values := range serializedValues {
		numFetchedKeys += len(values)
	}
	/*
		t6 := time.Now()
		if t6.Sub(t1) > time.Millisecond {
			fmt.Println("Fetch simple state total time:", t6.Sub(t1), " validateTime:", t2.Sub(t1), " DedupTime:", t3.Sub(t2), " SerializeTime:", t4.Sub(t3), " GetManyTime:", t5.Sub(t4), "SetCacheTime", t6.Sub(t5))
		}
	*/
	return numFetchedKeys
}

// Flush local cache to the state service. Flush must be called after Fetch
// since serializedKeys and dedupedKeys are dependent on Fetch. Return the
// number of flushed keys: (i) overwrite, (ii) merge, and (iii) delete.
func (sc *StateClient[K]) FlushSimpleState() (int, int, int) {

	if sc.stateClientType == WindowStateClient {
		log.Fatalln("For window state client, use FlushWindowState()")
	}

	// Prepare serialized data for data service APIs. We group all requests
	// into 3 types: overwrite, merge, and delete
	allSerializedKeysToWrite := make(map[uint16][][]byte)
	allBucketIDsToWrite := make(map[uint16][]int64)
	allSerializedValuesToWrite := make(map[uint16][][]byte)
	allSerializedKeysToMerge := make(map[uint16][][]byte)
	allBucketIDsToMerge := make(map[uint16][]int64)
	allSerializedValuesToMerge := make(map[uint16][][]byte)
	allSerializedKeysToDelete := make(map[uint16][][]byte)
	allBucketIDsToDelete := make(map[uint16][]int64)

	// Go over current states in cache to get data to flush
	for stateID, serializedKeysPerState := range sc.serializedKeys {

		serializedKeysToWrite,
			bucketIDsToWrite,
			serializedValuesToWrite,
			serializedKeysToMerge,
			bucketIDsToMerge,
			serializedValuesToMerge,
			serializedKeysToDelete,
			bucketIDsToDelete := sc.cache[stateID].GetSimpleStateFlushData(
			sc.dedupedKeys,
			serializedKeysPerState,
			sc.bucketIDs,
		)

		if len(serializedKeysToWrite) > 0 {
			allSerializedKeysToWrite[stateID] = serializedKeysToWrite
			allBucketIDsToWrite[stateID] = bucketIDsToWrite
			allSerializedValuesToWrite[stateID] = serializedValuesToWrite
		}
		if len(serializedKeysToMerge) > 0 {
			allSerializedKeysToMerge[stateID] = serializedKeysToMerge
			allBucketIDsToMerge[stateID] = bucketIDsToMerge
			allSerializedValuesToMerge[stateID] = serializedValuesToMerge
		}
		if len(serializedKeysToDelete) > 0 {
			allSerializedKeysToDelete[stateID] = serializedKeysToDelete
			allBucketIDsToDelete[stateID] = bucketIDsToDelete
		}
	}

	// Flush: Overwrite state to StateService
	if len(allSerializedKeysToWrite) > 0 {
		sc.stateService.SetManyMultiState(
			allSerializedKeysToWrite,
			allSerializedValuesToWrite,
			allBucketIDsToWrite,
		)
	}

	// Flush: Merge write state to StateService
	if len(allSerializedKeysToMerge) > 0 {
		sc.stateService.MergeManyMultiState(
			allSerializedKeysToMerge,
			allSerializedValuesToMerge,
			allBucketIDsToMerge,
		)
	}

	// Flush: Delete state from StateService
	if len(allSerializedKeysToDelete) > 0 {
		sc.stateService.DeleteMany(
			allSerializedKeysToDelete,
			allBucketIDsToDelete,
		)
	}

	// Return the number of flushed keys
	numOverwrite := 0
	numMerge := 0
	numDelete := 0
	for _, keys := range allSerializedKeysToWrite {
		numOverwrite += len(keys)
	}
	for _, keys := range allSerializedKeysToMerge {
		numMerge += len(keys)
	}
	for _, keys := range allSerializedKeysToDelete {
		numDelete += len(keys)
	}
	return numOverwrite, numMerge, numDelete
}

// Get the state from the local cache
func (sc *StateClient[K]) GetSimpleState(stateID uint16, key K) any {

	stateCache, ok := sc.cache[stateID]
	if !ok {
		log.Fatalf("state ID %d not registered in state client", stateID)
	}
	if stateCache.IsUnset() {
		log.Fatalf(
			"state ID cache %d is unset - Fetch is not called properly",
			stateID,
		)
	}

	return stateCache.GetSimpleState(key)
}

/******************************************************************************
						StateClient APIs for Window State
******************************************************************************/

// Fetch window state from StateService to memory cache for specified state IDs
// 1. Serialize requested keys
// 2. Request StateSerive
// 3. Deserialize obtained values
// 4. Update local StateClient cache
// Return the number of fetched keys - same user key with multiple states are
// counted multiple times, also include multiple windows per key. If key not
// found in state service, it is counted as fetched as well since it is set to
// empty state in cache.
func (sc *StateClient[K]) FetchWindowState(
	keys []K,
	timestamps []int64,
	stateIDs []uint16,
) int {

	if sc.stateClientType == SimpleStateClient {
		log.Fatalln("For Simple state client, use FetchSimpleState()")
	}
	if len(keys) == 0 {
		log.Fatalf("Empty keys to fetch")
	}
	if len(keys) != len(timestamps) {
		log.Fatalf("Keys and timestamps must have the same length")
	}
	sc.validateStateIDs(stateIDs)

	// Deduplicate [key + windowStartTime] pairs to avoid duplicate fetch from
	// StateService
	keysSeen := make(map[K]map[int64]struct{})
	sc.dedupedKeys = make([]K, 0, len(keys))
	sc.dedupedWindowStartTimes = make([]int64, 0, len(timestamps))
	for i := range keys {

		windowsPerKey, ok := keysSeen[keys[i]]
		if ok {
			if _, ok := windowsPerKey[timestamps[i]]; ok {
				continue
			} else {
				windowsPerKey[timestamps[i]] = struct{}{}
			}
		} else {
			keysSeen[keys[i]] = make(map[int64]struct{})
			keysSeen[keys[i]][timestamps[i]] = struct{}{}
		}
		sc.dedupedKeys = append(sc.dedupedKeys, keys[i])
		sc.dedupedWindowStartTimes = append(
			sc.dedupedWindowStartTimes,
			timestamps[i],
		)
	}

	// Allocate space for serialized keys
	sc.serializedKeys = make(map[uint16][][]byte)
	for _, stateID := range stateIDs {
		sc.serializedKeys[stateID] = make([][]byte, 0, len(sc.dedupedKeys))
	}

	// Serialize [key + windowStartTime] pairs
	sc.bucketIDs = make([]int64, len(sc.dedupedKeys))
	for i, key := range sc.dedupedKeys {

		// Serialize the key for the 1st state (the 1st state id won't be empty)
		stateID := stateIDs[0]
		serializedKey, bucketId := sc.encodeWindowKey(
			key,
			sc.dedupedWindowStartTimes[i],
			stateID,
		)
		sc.bucketIDs[i] = bucketId
		sc.serializedKeys[stateID] = append(
			sc.serializedKeys[stateID],
			serializedKey,
		)

		// Serialize the key for multi-state operators
		sc.serializeMultiStateKeys(serializedKey, stateIDs[1:])
	}

	// Request StateService: fetch state values
	serializedValues := sc.stateService.GetManyMultiState(
		sc.operatorID,
		sc.serializedKeys,
		sc.bucketIDs,
	)

	// For fetched state, reset their cache accordingly. For state that is not
	// requested, clean up the cache for safety.
	for stateID, stateCache := range sc.cache {
		if values, ok := serializedValues[stateID]; ok {
			stateCache.SetWindowStateCache(
				sc.dedupedKeys,
				sc.dedupedWindowStartTimes,
				values,
			)
		} else {
			stateCache.CleanUp()
		}
	}

	// Return the number of fetched keys (counting same user key with multiple
	// states multiple times, also include multiple windows per key)
	numFetchedKeys := 0
	for _, values := range serializedValues {
		numFetchedKeys += len(values)
	}
	return numFetchedKeys
}

// Flush local cache to the state service. Flush must be called after Fetch
// since serializedKeys, dedupedKeys, and dedupedWindowStartTimes are
// dependent on Fetch. Return the number of flushed keys: (i) overwrite, (ii)
// merge, and (iii) delete.
func (sc *StateClient[K]) FlushWindowState() (int, int, int) {

	if sc.stateClientType == SimpleStateClient {
		log.Fatalln("For Simple state client, use FlushSimpleState()")
	}

	// Prepare serialized data for data service APIs. We group all requests
	// into 3 types: overwrite, merge, and delete
	allSerializedKeysToWrite := make(map[uint16][][]byte)
	allBucketIDsToWrite := make(map[uint16][]int64)
	allSerializedValuesToWrite := make(map[uint16][][]byte)
	allSerializedKeysToMerge := make(map[uint16][][]byte)
	allBucketIDsToMerge := make(map[uint16][]int64)
	allSerializedValuesToMerge := make(map[uint16][][]byte)
	allSerializedKeysToDelete := make(map[uint16][][]byte)
	allBucketIDsToDelete := make(map[uint16][]int64)

	// Go over current states in cache to get data to flush
	for stateID, serializedKeysPerState := range sc.serializedKeys {

		serializedKeysToWrite,
			bucketIDsToWrite,
			serializedValuesToWrite,
			serializedKeysToMerge,
			bucketIDsToMerge,
			serializedValuesToMerge,
			serializedKeysToDelete,
			bucketIDsToDelete := sc.cache[stateID].GetWindowStateFlushData(
			sc.dedupedKeys,
			sc.dedupedWindowStartTimes,
			serializedKeysPerState,
			sc.bucketIDs,
		)

		if len(serializedKeysToWrite) > 0 {
			allSerializedKeysToWrite[stateID] = serializedKeysToWrite
			allBucketIDsToWrite[stateID] = bucketIDsToWrite
			allSerializedValuesToWrite[stateID] = serializedValuesToWrite
		}
		if len(serializedKeysToMerge) > 0 {
			allSerializedKeysToMerge[stateID] = serializedKeysToMerge
			allBucketIDsToMerge[stateID] = bucketIDsToMerge
			allSerializedValuesToMerge[stateID] = serializedValuesToMerge
		}
		if len(serializedKeysToDelete) > 0 {
			allSerializedKeysToDelete[stateID] = serializedKeysToDelete
			allBucketIDsToDelete[stateID] = bucketIDsToDelete
		}
	}

	// Flush: Write state to StateService
	if len(allSerializedKeysToWrite) > 0 {
		sc.stateService.SetManyMultiState(
			allSerializedKeysToWrite,
			allSerializedValuesToWrite,
			allBucketIDsToWrite,
		)
	}

	// Flush: Merge write state to StateService
	if len(allSerializedKeysToMerge) > 0 {
		sc.stateService.MergeManyMultiState(
			allSerializedKeysToMerge,
			allSerializedValuesToMerge,
			allBucketIDsToMerge,
		)
	}

	// Flush: Delete state from StateService
	if len(allSerializedKeysToDelete) > 0 {
		sc.stateService.DeleteMany(
			allSerializedKeysToDelete,
			allBucketIDsToDelete,
		)
	}

	// Return the number of flushed keys
	numOverwrite := 0
	numMerge := 0
	numDelete := 0
	for _, keys := range allSerializedKeysToWrite {
		numOverwrite += len(keys)
	}
	for _, keys := range allSerializedKeysToMerge {
		numMerge += len(keys)
	}
	for _, keys := range allSerializedKeysToDelete {
		numDelete += len(keys)
	}
	return numOverwrite, numMerge, numDelete
}

// Get the state from the local cache
func (sc *StateClient[K]) GetWindowState(
	stateID uint16,
	key K,
	timestamp int64,
) any {

	stateCache, ok := sc.cache[stateID]
	if !ok {
		log.Fatalf("state ID %d not registered in state client", stateID)
	}
	if stateCache.IsUnset() {
		log.Fatalf(
			"state ID cache %d is unset - Fetch is not called properly",
			stateID,
		)
	}

	return stateCache.GetWindowState(key, timestamp)
}

// Delete all the fetched windows and flush the deletions to state service
// DeleteAllAndFlush() must be called after Fetch. We kept the serialized
// formatted keys from Fetch to avoid duplicate serialization
func (sc *StateClient[K]) DeleteAllAndFlush() {

	allBucketIDsToDelete := make(map[uint16][]int64)
	for stateID := range sc.serializedKeys {
		allBucketIDsToDelete[stateID] = sc.bucketIDs
	}

	// Request StateService: delete state values
	sc.stateService.DeleteMany(sc.serializedKeys, allBucketIDsToDelete)
}

// Delete the specific window panes for sliding windows. The input keys and
// timestamps pairs should exists in the previous Fetch() call.
func (sc *StateClient[K]) DeleteManyAndFlush(
	pairs map[KeyAndStartTime[K]]struct{},
) {

	// Identify the affected serialized keys and their corresponding bucket IDs
	allSerializedKeysToDelete := make(map[uint16][][]byte)
	allBucketIDsToDelete := make(map[uint16][]int64)

	for i, key := range sc.dedupedKeys {
		startTime := sc.dedupedWindowStartTimes[i]
		pair := KeyAndStartTime[K]{Key: key, StartTime: startTime}
		_, ok := pairs[pair]

		// This key/startTime pair should be deleted. Delete all corresponding
		// states if multi-state exists
		if ok {
			for stateID, serializedKeysPerState := range sc.serializedKeys {
				allSerializedKeysToDelete[stateID] = append(
					allSerializedKeysToDelete[stateID],
					serializedKeysPerState[i],
				)
				allBucketIDsToDelete[stateID] = append(
					allBucketIDsToDelete[stateID],
					sc.bucketIDs[i],
				)
			}
			delete(pairs, pair)
		}
	}

	// If not all the pairs were found, throw an error
	if len(pairs) != 0 {
		log.Fatalf(
			"DeleteManyAndFlush: Not all pairs to be deleted exists from the previous Fetch().",
		)
	}

	// Request StateService: delete state values
	sc.stateService.DeleteMany(allSerializedKeysToDelete, allBucketIDsToDelete)
}

/******************************************************************************
								   Helpers APIs
******************************************************************************/

// Calculate the bucket ID given the key
func (sc *StateClient[K]) GetBucketIdx(key K) uint64 {

	ptrToKey := unsafe.Pointer(&key)
	serializedKey := make([]byte, sc.keySizer(ptrToKey))
	sc.keyEncoder(ptrToKey, serializedKey)
	return sc.keyHashFunc.Hash(serializedKey) % uint64(sc.numBuckets)
}

/******************************************************************************
							    APIs for testing
******************************************************************************/

func (sc *StateClient[K]) StateCacheIsUnset(stateID uint16) bool {
	stateCache, ok := sc.cache[stateID]
	if !ok {
		log.Fatalf("state ID %d not registered in state client", stateID)
	}
	return stateCache.IsUnset()
}
