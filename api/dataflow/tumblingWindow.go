package dataflow

import (
	"bytes"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
)

type TumblingWindow[IN, OUT tuple.Tuple, K comparable, V stateType.StateType] struct {
	*StatefulOperatorBase1Upstream[IN, K]

	// TumblingWindow only has 1 state
	StateID uint16

	// We only support imcremental window now. User define UDFs in aggregator
	Aggregator *Aggregator[IN, OUT, V]

	// Duration of the window
	Duration time.Duration

	// Index to track active windows
	Index *TumblingWindowIndex[K]

	// [stop-and-restart] Protect concurrent access by stop-and-restart
	// MigrateTaskMetadata() when the task is both sender and receiver
	metadataMigrateLock sync.Mutex
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*TumblingWindow[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*TumblingWindow[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

// NewTumblingWindow creates a new TumblingWindow struct
func NewTumblingWindow[IN tuple.Tuple, OUT tuple.Tuple, K comparable, V stateType.StateType](
	name string,
	keyAssigner *ka.KeyAssigner[IN, K],
	aggregator *Aggregator[IN, OUT, V],
	duration time.Duration,
) *TumblingWindow[IN, OUT, K, V] {

	// Duration should be positive
	if duration <= 0 {
		log.Fatalln("Duration should be positive")
	}

	tumblingWindow := &TumblingWindow[IN, OUT, K, V]{
		StatefulOperatorBase1Upstream: NewStatefulOperatorBase1Upstream(
			name,
			keyAssigner,
			stateClient.WindowStateClient,
		),
		Aggregator:          aggregator,
		Duration:            duration,
		Index:               NewTumblingWindowIndex[K](),
		metadataMigrateLock: sync.Mutex{},
	}

	// Register state type to StateClient
	tumblingWindow.StateID = stateClient.RegisterState[V](
		tumblingWindow.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	tumblingWindow.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	tumblingWindow.Collector = collector.NewRoundRobinCollector[OUT](name)

	return tumblingWindow
}

// ProcessBatch processes a batch of records
func (tw *TumblingWindow[IN, OUT, K, V]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	// Extract key fields & timestamps from the batch
	keys := make([]K, batch.TotalNumRecords)
	window_starts := make([]int64, batch.TotalNumRecords)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {
		keys[i] = tw.KeyAssigner.GetKey(record)
		window_starts[i] = tw.getWindowStartTime(record.GetTimestamp())
	}

	// Fetch state to local cache
	tw.StateClient.FetchWindowState(keys, window_starts, []uint16{tw.StateID})

	var window_start int64
	var curState V
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get window state for current record
		window_start = tw.getWindowStartTime(record.GetTimestamp())
		curState, ok = tw.StateClient.GetWindowState(tw.StateID, keys[i], window_start).(V)
		if !ok {
			log.Fatalln("Fetched state failed for type assertion")
		}

		// If this is a newly created window with empty state, initialize it
		// and add it to the active window index
		if !curState.HasValue() {
			curState.CopyFrom(tw.Aggregator.NewFunc())
			tw.Index.Add(keys[i], window_start)
		}

		// Execute aggregate function
		curState.CopyFrom(tw.Aggregator.AddFunc(curState, record))
	}

	// Flush the state to the state service
	tw.StateClient.FlushWindowState()
}

// Processing time watermark
func (tw *TumblingWindow[IN, OUT, K, V]) ProcessProgressedWatermark(
	wm *buffer.Watermark,
) {

	keys, timestamps := tw.Index.GetWindowsToBeFired(wm, tw.Duration)

	// Do nothing if there is no window to be fired
	if len(keys) == 0 {
		return
	}

	// Fetch all the closed windows from the state service to local cache first
	tw.StateClient.FetchWindowState(keys, timestamps, []uint16{tw.StateID})

	// Fire all closed window and emit the result
	for i, key := range keys {

		closedWindow, ok := tw.StateClient.GetWindowState(tw.StateID, key, timestamps[i]).(V)
		if !ok {
			log.Fatalln("Fetched state failed for type assertion")
		}

		// Set timestamp for the output record as the end of the window
		tw.Collector.SetCurrentTimestamp(
			timestamps[i] + tw.Duration.Nanoseconds(),
		)

		tw.Collector.Emit(
			tw.Aggregator.OutFunc(closedWindow),
		)

		// Remove the window from the index after firing it
		tw.Index.Remove(key, timestamps[i])
	}

	// Remove fired windows from state service
	tw.StateClient.DeleteAllAndFlush()
}

// [stop-and-restart] Extract window indices for migration
func (tw *TumblingWindow[IN, OUT, K, V]) FetchMetadataForMigration(
	affectedBuckets map[uint64]struct{},
) []byte {

	keysExamined := make(map[K]uint64)
	indexToBeTransferred := make(map[int64]map[K]bool)

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	tw.metadataMigrateLock.Lock()
	for startTime, windows := range tw.Index.Index {

		for key := range windows {
			bucketId, ok := keysExamined[key]
			if !ok {
				bucketId = tw.StateClient.GetBucketIdx(key)
				keysExamined[key] = bucketId
			}

			// Find all active window indices that fall into affected buckets
			if _, ok := affectedBuckets[bucketId]; ok {

				keysPerStartTime, ok := indexToBeTransferred[startTime]
				if !ok {
					keysPerStartTime = make(map[K]bool)
					indexToBeTransferred[startTime] = keysPerStartTime
				}
				keysPerStartTime[key] = true

				// Delete this active window from current worker
				tw.Index.Remove(key, startTime)
			}
		}
	}
	tw.metadataMigrateLock.Unlock()

	// Serialize indexToBeTransferred to bytes
	res, err := json.Marshal(indexToBeTransferred)
	if err != nil {
		log.Fatalln("Failed to serialize window index for migration:", err)
	}
	return res
}

// [stop-and-restart] Insert migrated window indices to local index
func (tw *TumblingWindow[IN, OUT, K, V]) InsertMigratedMetadata(
	metadata []byte,
) {

	var receivedIndex map[int64]map[K]bool
	if err := json.Unmarshal(metadata, &receivedIndex); err != nil {
		log.Fatalln("Failed to deserialize migrated window index:", err)
	}

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	tw.metadataMigrateLock.Lock()
	for startTime, keys := range receivedIndex {
		for key := range keys {

			// Add the window to the local index
			tw.Index.Add(key, startTime)
		}
	}
	tw.metadataMigrateLock.Unlock()
}

// [lazy protocol] Implement ConstructFastForwardMetadata() for fast forward
// Extract and encode active window indices to be transferred to other workers
// Return: map[destWorkerId]FastForwardMetadata
func (tw *TumblingWindow[IN, OUT, K, V]) ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata {

	// Map for bookkeeping if a key (i) has been serialized for RoutingTable
	// lookup, and (ii) which worker it's assigned to.
	// This is to avoid duplicate serialization of the key
	keysExamined := make(map[K]uint16)

	// Widnow indices to be transferred. There can be multiple dest workers
	// map[destWorkerId][window startTime][key] = true
	indexToBeTransferred := make(map[uint16]map[int64]map[K]bool)

	// Traverse all active windows to identify the affected keys
	for startTime, windows := range tw.Index.Index {

		for key := range windows {

			destWorker, ok := keysExamined[key]
			if !ok {
				// This is the first time this key is examined - lookup the
				// RoutingTable for its owner worker
				destWorker = tw.GetOwnerWorkerUponReconfig(key)
				keysExamined[key] = destWorker
			}

			if destWorker != tw.WorkerId {
				// This is an active window to be transferred to the new owner
				indexToBeTransferredPerWorker, ok := indexToBeTransferred[destWorker]
				if !ok {
					indexToBeTransferredPerWorker = make(map[int64]map[K]bool)
					indexToBeTransferred[destWorker] = indexToBeTransferredPerWorker
				}

				// Add the window to indexToBeTransferredPerWorker
				keysPerStartTime, ok := indexToBeTransferredPerWorker[startTime]
				if !ok {
					keysPerStartTime = make(map[K]bool)
					indexToBeTransferredPerWorker[startTime] = keysPerStartTime
				}
				keysPerStartTime[key] = true

				// Delete this active window from current worker
				tw.Index.Remove(key, startTime)
			}
		}
	}

	// Serialize indexToBeTransferred as FastForwardMetadata messages
	res := make(map[uint16]*buffer.FastForwardMetadata)
	for destWorkerId, indices := range indexToBeTransferred {
		res[destWorkerId] = tw.SerializeToFastForwardMetadata(indices)
	}
	return res
}

// [lazy protocol] Implement ProcessFastForwardMetadata() for fast forward
// FastForwardMetadata is the first message received for fast forward channel
func (tw *TumblingWindow[IN, OUT, K, V]) ProcessFastForwardMetadata(
	metadata *buffer.FastForwardMetadata,
) {

	var receivedIndex map[int64]map[K]bool
	dec := json.NewDecoder(bytes.NewReader(metadata.SerializedMetadata))
	if err := dec.Decode(&receivedIndex); err != nil {
		log.Fatalln("Failed to decode FastForwardMetadata:", err)
	}

	// Traverse the received index and add it to the local index
	for startTime, keys := range receivedIndex {
		for key := range keys {

			// Add the window to the local index
			tw.Index.Add(key, startTime)
		}
	}
}

// Validate input type at compile time
func (tw *TumblingWindow[IN, OUT, K, V]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (tw *TumblingWindow[IN, OUT, K, V]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}

/******************************************************************************
						Util methods for tumbing window
******************************************************************************/

// Calculate the start time of the window
func (tw *TumblingWindow[IN, OUT, K, V]) getWindowStartTime(
	timestamp int64,
) int64 {
	return timestamp - (timestamp % tw.Duration.Nanoseconds())
}

/******************************************************************************
								  Window Index
******************************************************************************/

type TumblingWindowIndex[K comparable] struct {

	// Active windows are organized by start time
	Index map[int64]map[K]struct{}
}

func NewTumblingWindowIndex[K comparable]() *TumblingWindowIndex[K] {

	return &TumblingWindowIndex[K]{
		Index: make(map[int64]map[K]struct{}),
	}
}

// Check if the window exists in the index
func (w *TumblingWindowIndex[K]) Exists(key K, timestamp int64) bool {

	windowsWithSameStartTime, ok := w.Index[timestamp]
	if !ok {
		return false
	}

	_, ok = windowsWithSameStartTime[key]
	return ok
}

// Add a new window to the index
func (w *TumblingWindowIndex[K]) Add(key K, timestamp int64) {

	if _, ok := w.Index[timestamp]; !ok {
		w.Index[timestamp] = make(map[K]struct{})
	}
	w.Index[timestamp][key] = struct{}{}
}

// Remove the window from the index
func (w *TumblingWindowIndex[K]) Remove(key K, timestamp int64) {

	if windowsWithSameStartTime, ok := w.Index[timestamp]; ok {
		delete(windowsWithSameStartTime, key)
		if len(windowsWithSameStartTime) == 0 {
			delete(w.Index, timestamp)
		}
	}
}

// Scan all active windows and fetch the ones to be fired
func (w *TumblingWindowIndex[K]) GetWindowsToBeFired(
	curTime *buffer.Watermark,
	winDuration time.Duration,
) ([]K, []int64) {

	// Windows to be fired are the ones whose end time is <= current time
	threshold := curTime.Timestamp - winDuration.Nanoseconds()

	keys := make([]K, 0)
	timestamps := make([]int64, 0)

	for winStartTime, windows := range w.Index {
		if winStartTime <= threshold {
			for key := range windows {
				keys = append(keys, key)
				timestamps = append(timestamps, winStartTime)
			}
		}
	}

	return keys, timestamps
}
