package dataflow

import (
	"bytes"
	"encoding/json"
	"log"
	"slices"
	"sync"

	"github.com/CASP-Systems-BU/koala/api/collector"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
)

// CustomWindowJoin operator supports joining 2 input streams with user-defined
// custom windowing logic through custom timer registration and triggering.

type CustomWindowJoin[OUT, IN1, IN2 tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType] struct {
	*StatefulOperatorBase2Upstream[IN1, IN2, K]

	// Join has 2 states
	StateID1 uint16
	StateID2 uint16

	// Timer structure for custom window triggering
	Timer *Timer[K]

	// [stop-and-restart] Protect concurrent access by stop-and-restart
	// MigrateTaskMetadata() when the task is both sender and receiver
	metadataMigrateLock sync.Mutex

	// UDF that takes 1 input record from the 1st upstream and output 0 or more
	// output records
	F1 func(IN1, V1, V2, TimerService, collector.Collector)

	// UDF that takes 1 input record from the 2nd upstream and output 0 or more
	// output records
	F2 func(IN2, V1, V2, TimerService, collector.Collector)

	// UDF that takes the registered timer timestamp and output 0 or more
	// output records
	OnTimer func(int64, V1, V2, collector.Collector)
}

// Type validation at compile time
var _ OperatorWith2InputStream[tuple.Tuple, tuple.Tuple] = (*CustomWindowJoin[tuple.Tuple, tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*CustomWindowJoin[tuple.Tuple, tuple.Tuple, tuple.Tuple, any, stateType.StateType, stateType.StateType])(
	nil,
)

// API exposed to users to define the operator
func NewCustomWindowJoin[OUT, IN1, IN2 tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType](
	name string,
	/*************************** 1st input stream ****************************/
	upstream1 OperatorWith1OutputStream[IN1],
	keyAssigner1 *ka.KeyAssigner[IN1, K],
	f1 func(IN1, V1, V2, TimerService, collector.Collector),
	/*************************** 2nd input stream ****************************/
	upstream2 OperatorWith1OutputStream[IN2],
	keyAssigner2 *ka.KeyAssigner[IN2, K],
	f2 func(IN2, V1, V2, TimerService, collector.Collector),
	/*************************** Timer & OnTimer ***************************/
	onTimer func(int64, V1, V2, collector.Collector),
) *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2] {

	join := &CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]{
		StatefulOperatorBase2Upstream: NewStatefulOperatorBase2Upstream(
			name,
			upstream1,
			keyAssigner1,
			upstream2,
			keyAssigner2,
			stateClient.SimpleStateClient,
		),
		Timer:               NewTimer[K](),
		metadataMigrateLock: sync.Mutex{},
		F1:                  f1,
		F2:                  f2,
		OnTimer:             onTimer,
	}

	// Register state types to StateClient
	join.StateID1 = stateClient.RegisterState[V1](
		join.StateClient,
	)
	join.StateID2 = stateClient.RegisterState[V2](
		join.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 2 upstream operators expected
	join.Supplier = supplier.NewRoundRobinSupplier(name, 2)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	join.Collector = collector.NewRoundRobinCollector[OUT](name)

	return join
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	switch subSupplierName {
	case w.UpstreamName1:
		processBatchCustomWindowJoin(
			workUnit,
			w.KeyAssigner1,
			w.StateClient,
			w.StateID1,
			w.StateID2,
			w.F1,
			w.Timer,
			w.Collector,
		)
	case w.UpstreamName2:
		processBatchCustomWindowJoin(
			workUnit,
			w.KeyAssigner2,
			w.StateClient,
			w.StateID1,
			w.StateID2,
			w.F2,
			w.Timer,
			w.Collector,
		)
	default:
		log.Fatalf(
			"CustomWindowJoin: workUnit comes from unexpected SubSupplier %s",
			subSupplierName,
		)
	}
}

// Processing time watermark
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) ProcessProgressedWatermark(
	wm *buffer.Watermark,
) {

	// Get all timers that are expired. Keys and timestamps are in order of
	// timestamp from low to high
	keys, timestamps := w.Timer.getExpiredTimers(wm)
	if len(keys) == 0 {
		return
	}

	// Fetch all related states from the state service
	w.StateClient.FetchSimpleState(keys, []uint16{w.StateID1, w.StateID2})

	// Execute OnTimer() for each expired timer in order of timestamp
	var state1 V1
	var state2 V2
	var ok bool
	for i, key := range keys {

		state1, ok = w.StateClient.GetSimpleState(w.StateID1, key).(V1)
		if !ok {
			log.Fatalln("Fetched state1 failed for type assertion")
		}

		state2, ok = w.StateClient.GetSimpleState(w.StateID2, key).(V2)
		if !ok {
			log.Fatalln("Fetched state2 failed for type assertion")
		}

		// Set timestamp for the output record as the timer timestamp
		w.Collector.SetCurrentTimestamp(timestamps[i])

		// Call user-defined OnTimer() function
		w.OnTimer(timestamps[i], state1, state2, w.Collector)

		// Remove the timer from active timers after execution
		w.Timer.remove(key, timestamps[i])
	}

	// Flush the related states if there are any updates (update or delete)
	w.StateClient.FlushSimpleState()
}

// [stop-and-restart] Extract in-memory timers for migration
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) FetchMetadataForMigration(
	affectedBuckets map[uint64]struct{},
) []byte {

	keysExamined := make(map[K]uint64)
	timersToBeTransferred := make(map[int64]map[K]bool)

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	w.metadataMigrateLock.Lock()
	for timestamp, keys := range w.Timer.ActiveTimers {

		for key := range keys {
			bucketId, ok := keysExamined[key]
			if !ok {
				bucketId = w.StateClient.GetBucketIdx(key)
				keysExamined[key] = bucketId
			}

			// Find all active timers that fall into affected buckets
			if _, ok := affectedBuckets[bucketId]; ok {

				timersPerTimestamp, ok := timersToBeTransferred[timestamp]
				if !ok {
					timersPerTimestamp = make(map[K]bool)
					timersToBeTransferred[timestamp] = timersPerTimestamp
				}
				timersPerTimestamp[key] = true

				// Delete this active timer from current worker
				w.Timer.remove(key, timestamp)
			}
		}
	}
	w.metadataMigrateLock.Unlock()

	// Serialize timersToBeTransferred to bytes
	res, err := json.Marshal(timersToBeTransferred)
	if err != nil {
		log.Fatalln("Failed to serialize timers for migration:", err)
	}
	return res
}

// [stop-and-restart] Insert migrated window indices to local index
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) InsertMigratedMetadata(
	metadata []byte,
) {

	var receivedTimers map[int64]map[K]bool
	if err := json.Unmarshal(metadata, &receivedTimers); err != nil {
		log.Fatalln("Failed to deserialize migrated timers:", err)
	}

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	w.metadataMigrateLock.Lock()
	for timestamp, keys := range receivedTimers {

		for key := range keys {
			// Add the timer to local Timer
			w.Timer.add(key, timestamp)
		}
	}
	w.metadataMigrateLock.Unlock()
}

// [lazy protocol] Implement ConstructFastForwardMetadata() for fast forward
// Extract and encode active timers to be transferred to other workers upon
// reconfiguration. Return map[destWorkerId]FastForwardMetadata
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata {

	// Map for bookkeeping if a key (i) has been serialized for RoutingTable
	// lookup, and (ii) which worker it's assigned to.
	// This is to avoid duplicate serialization of the key
	keysExamined := make(map[K]uint16)

	// Active timers to be transferred. There can be multiple dest workers
	// map[destWorkerId]map[timer timestamp]map[key] = true
	timersToBeTransferred := make(map[uint16]map[int64]map[K]bool)

	// Traverse all active timers to identify the timers to be transferred
	for timestamp, keys := range w.Timer.ActiveTimers {

		for key := range keys {

			destWorker, ok := keysExamined[key]
			if !ok {
				// This is the first time this key is examined - lookup the
				// RoutingTable for its owner worker
				destWorker = w.GetOwnerWorkerUponReconfig(key)
				keysExamined[key] = destWorker
			}

			if destWorker != w.WorkerId {
				// This is an active timer to be transferred to the new owner
				timersToBeTransferredPerWorker, ok := timersToBeTransferred[destWorker]
				if !ok {
					timersToBeTransferredPerWorker = make(map[int64]map[K]bool)
					timersToBeTransferred[destWorker] = timersToBeTransferredPerWorker
				}

				// Add the timer to timersToBeTransferredPerWorker
				timersWithSameTimestamp, ok := timersToBeTransferredPerWorker[timestamp]
				if !ok {
					timersWithSameTimestamp = make(map[K]bool)
					timersToBeTransferredPerWorker[timestamp] = timersWithSameTimestamp
				}
				timersWithSameTimestamp[key] = true

				// Delete the active timer from local Timer
				w.Timer.remove(key, timestamp)
			}
		}
	}

	// Serialize the timersToBeTransferred as FastForwardMetadata messages
	res := make(map[uint16]*buffer.FastForwardMetadata)
	for destWorker, timersToBeTransferredPerWorker := range timersToBeTransferred {
		res[destWorker] = w.SerializeToFastForwardMetadata(
			timersToBeTransferredPerWorker,
		)
	}
	return res
}

// [lazy protocol] Process the received FastForwardMetadata workunit
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) ProcessFastForwardMetadata(
	metadata *buffer.FastForwardMetadata,
) {

	var receivedTimers map[int64]map[K]bool
	dec := json.NewDecoder(bytes.NewReader(metadata.SerializedMetadata))
	if err := dec.Decode(&receivedTimers); err != nil {
		log.Fatalln("Failed to decode FastForwardMetadata:", err)
	}

	// Traverse the received timers and add to local Timer
	for timestamp, keys := range receivedTimers {
		for key := range keys {
			w.Timer.add(key, timestamp)
		}
	}
}

// Validate input type at compile time
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) InputTupleType2() (IN1, IN2) {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (w *CustomWindowJoin[OUT, IN1, IN2, K, V1, V2]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}

/******************************************************************************
								 	   Timer
******************************************************************************/

// API exposed to users
type TimerService interface {
	// User API: register a timer with the given timestamp
	RegisterTimer(timestamp int64)
}

var _ TimerService = (*Timer[any])(nil)

type Timer[K comparable] struct {

	// Current key being processed - used internally to prepare RegisterTimer()
	// API such that users don't need to pass in the key
	currentKey K

	// Active timers organized by registered timestamp
	ActiveTimers map[int64]map[K]struct{}
}

func NewTimer[K comparable]() *Timer[K] {
	return &Timer[K]{
		ActiveTimers: make(map[int64]map[K]struct{}),
	}
}

// User API: register a timer with the given timestamp. Note: setCurrentKey()
// must be called before this to set the current working key
func (t *Timer[K]) RegisterTimer(timestamp int64) {
	t.add(t.currentKey, timestamp)
}

// Store the current working key before user calls RegisterTimer()
func (t *Timer[K]) setCurrentKey(key K) {
	t.currentKey = key
}

// Helper function to add a timer
func (t *Timer[K]) add(key K, timestamp int64) {
	if _, exists := t.ActiveTimers[timestamp]; !exists {
		t.ActiveTimers[timestamp] = make(map[K]struct{})
	}
	t.ActiveTimers[timestamp][key] = struct{}{}
}

// Remove an active timer
func (t *Timer[K]) remove(key K, timestamp int64) {

	if timersWithSameTimestamp, ok := t.ActiveTimers[timestamp]; ok {
		delete(timersWithSameTimestamp, key)
		if len(timersWithSameTimestamp) == 0 {
			delete(t.ActiveTimers, timestamp)
		}
	}
}

// Get all timers that are expired given the current watermark. Returned keys
// and timestamps are in order of timestamp from low to high
func (t *Timer[K]) getExpiredTimers(curTime *buffer.Watermark) ([]K, []int64) {

	sortedTimestamps := make([]int64, 0)
	totalNumExpiredTimers := 0
	for timestamp, keys := range t.ActiveTimers {
		if timestamp <= curTime.Timestamp {
			sortedTimestamps = append(sortedTimestamps, timestamp)
			totalNumExpiredTimers += len(keys)
		}
	}

	// Sort the timestamps from low to high in place
	slices.Sort(sortedTimestamps)

	expiredKeys := make([]K, 0, totalNumExpiredTimers)
	expiredTimestamps := make([]int64, 0, totalNumExpiredTimers)

	// Fill up the results in order of timestamp such that timer is processed
	// with respect to timestamp order
	for _, timestamp := range sortedTimestamps {
		for key := range t.ActiveTimers[timestamp] {
			expiredKeys = append(expiredKeys, key)
			expiredTimestamps = append(expiredTimestamps, timestamp)
		}
	}
	return expiredKeys, expiredTimestamps
}

/******************************************************************************
							Utils for CustomWindowJoin
******************************************************************************/

func processBatchCustomWindowJoin[IN tuple.Tuple, K comparable, V1 stateType.StateType, V2 stateType.StateType](
	workUnit buffer.WorkUnit,
	keyAssigner *ka.KeyAssigner[IN, K],
	stateClient *stateClient.StateClient[K],
	stateID1 uint16,
	stateID2 uint16,
	f func(IN, V1, V2, TimerService, collector.Collector),
	timer *Timer[K],
	collector collector.Collector,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Failed to convert workUnit to Batch[IN]")
	}

	// Extract all key fields from the batch
	keys := make([]K, batch.TotalNumRecords)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {
		keys[i] = keyAssigner.GetKey(record)
	}

	// Prefetch the batch state to memory before processing
	stateClient.FetchSimpleState(keys, []uint16{stateID1, stateID2})

	var state1 V1
	var state2 V2
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get state for current record
		state1, ok = stateClient.GetSimpleState(stateID1, keys[i]).(V1)
		if !ok {
			log.Fatalln("Fetched state1 failed for type assertion")
		}
		state2, ok = stateClient.GetSimpleState(stateID2, keys[i]).(V2)
		if !ok {
			log.Fatalln("Fetched state2 failed for type assertion")
		}

		// Set timestamp for the output record as the input record timestamp
		collector.SetCurrentTimestamp(record.GetTimestamp())

		// Set current working key in Timer
		timer.setCurrentKey(keys[i])

		// Call user-defined function
		f(record, state1, state2, timer, collector)
	}

	// Flush the related states if there are any updates (update or delete)
	stateClient.FlushSimpleState()
}
