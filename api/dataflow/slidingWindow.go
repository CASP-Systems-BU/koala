package dataflow

import (
	"bytes"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
)

// SlidingWindow adopts an efficient implementation to avoid duplicating state
// storage for overlapping windows.
// We define the struct Pane to indicate a partial window with duration of a
// slide. A sliding window consists of multiple Panes and a Pane can be
// referenced by overlapping sliding windows. Only one copy of the state is
// stored for the Pane.

type SlidingWindow[IN, OUT tuple.Tuple, K comparable, V stateType.StateType] struct {
	*StatefulOperatorBase1Upstream[IN, K]

	// SlidingWindow only has 1 state
	StateID uint16

	// We only support imcremental window now. User define UDFs in aggregator
	Aggregator *Aggregator[IN, OUT, V]

	// Duration of the window
	Duration time.Duration

	// Slide of the window
	Slide time.Duration

	// Index to track active windows
	Index *SlidingWindowIndex[K]

	// [stop-and-restart] Protect concurrent access by stop-and-restart
	// MigrateTaskMetadata() when the task is both sender and receiver
	metadataMigrateLock sync.Mutex
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*SlidingWindow[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*SlidingWindow[tuple.Tuple, tuple.Tuple, any, stateType.StateType])(
	nil,
)

func NewSlidingWindow[IN tuple.Tuple, OUT tuple.Tuple, K comparable, V stateType.StateType](
	name string,
	keyAssigner *ka.KeyAssigner[IN, K],
	aggregator *Aggregator[IN, OUT, V],
	duration time.Duration,
	slide time.Duration,
) *SlidingWindow[IN, OUT, K, V] {

	// Duration should be positive
	if duration <= 0 {
		log.Fatalln("Duration should be positive")
	}

	// Slide should be positive
	if slide <= 0 {
		log.Fatalln("Slide should be positive")
	}

	// Duration must be a multiple of slide. We require that a sliding window
	// must contain 1 or multiple panes
	if duration%slide != 0 {
		log.Fatalln("Duration must be a multiple of slide")
	}

	slidingWindow := &SlidingWindow[IN, OUT, K, V]{
		StatefulOperatorBase1Upstream: NewStatefulOperatorBase1Upstream(
			name,
			keyAssigner,
			stateClient.WindowStateClient,
		),
		Aggregator:          aggregator,
		Duration:            duration,
		Slide:               slide,
		Index:               NewSlidingWindowIndex[K](duration, slide),
		metadataMigrateLock: sync.Mutex{},
	}

	// Register state type to StateClient
	slidingWindow.StateID = stateClient.RegisterState[V](
		slidingWindow.StateClient,
	)

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	slidingWindow.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	slidingWindow.Collector = collector.NewRoundRobinCollector[OUT](name)

	return slidingWindow
}

func (sw *SlidingWindow[IN, OUT, K, V]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {
	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	// extract key fields & panes' timestamps from the batch
	keys := make([]K, batch.TotalNumRecords)
	paneStarts := make([]int64, batch.TotalNumRecords)
	for i, record := range batch.Records[0:batch.TotalNumRecords] {
		keys[i] = sw.KeyAssigner.GetKey(record)
		paneStarts[i] = GetPaneStartTime(record.GetTimestamp(), sw.Slide)
	}

	// Fetch state to local cache
	sw.StateClient.FetchWindowState(keys, paneStarts, []uint16{sw.StateID})

	var paneStart int64
	var curState V
	for i, record := range batch.Records[0:batch.TotalNumRecords] {

		// Get pane state for current record
		paneStart = GetPaneStartTime(record.GetTimestamp(), sw.Slide)
		curState, ok = sw.StateClient.GetWindowState(sw.StateID, keys[i], paneStart).(V)
		if !ok {
			log.Fatalln("Fetched state failed for type assertion")
		}

		// If the pane does not exist, initialize it and add it to the index
		if !curState.HasValue() {
			curState.CopyFrom(sw.Aggregator.NewFunc())
			sw.Index.AddPane(keys[i], paneStart)
		}

		// Execute aggregate function
		curState.CopyFrom(sw.Aggregator.AddFunc(curState, record))
	}

	// Flush the state to the state service
	sw.StateClient.FlushWindowState()
}

func (sw *SlidingWindow[IN, OUT, K, V]) ProcessProgressedWatermark(
	wm *buffer.Watermark,
) {

	// Given progressed watermark, get the windows to be fired
	// - windowsToFire: list of windows to be fired
	// - panes: map of panes needed by the windows to be fired (window state is
	// 	 stored in the unit of panes, so we need pane info to fetch state)
	windowsToFire, panes := sw.Index.GetWindowsToBeFired(wm)
	if len(windowsToFire) == 0 {
		return
	}
	if len(panes) == 0 {
		log.Fatalf(
			"Panes to be fired should exist, for windowToFire=%v\n",
			windowsToFire,
		)
	}

	// Fetch state of all requried panes to local cache
	keys := make([]K, len(panes))
	paneStarts := make([]int64, len(panes))
	i := 0
	for pane := range panes {
		keys[i] = pane.Key
		paneStarts[i] = pane.StartTime
		i++
	}
	sw.StateClient.FetchWindowState(keys, paneStarts, []uint16{sw.StateID})

	// List of panes awaiting to be removed
	panesToRemove := make(map[stateClient.KeyAndStartTime[K]]struct{}, 0)

	// Aggregate the result for each window and emit the result
	for _, window := range windowsToFire {
		result := sw.Aggregator.NewFunc()
		for _, pane := range window.Panes {

			paneState, ok := sw.StateClient.GetWindowState(sw.StateID, pane.Key, pane.StartTime).(V)
			if !ok {
				log.Fatalln("Fetched state failed for type assertion")
			}
			result = sw.Aggregator.MergeFunc(result, paneState)

			// Delete the pane from the StateService if it's never needed again
			if pane.StartTime <= wm.Timestamp-int64(sw.Duration) {
				panesToRemove[stateClient.KeyAndStartTime[K]{
					Key:       pane.Key,
					StartTime: pane.StartTime,
				}] = struct{}{}
			}
		}
		// Set timestamp for the output record as the end of the window
		sw.Collector.SetCurrentTimestamp(
			window.StartTime + sw.Duration.Nanoseconds(),
		)
		outTuple := sw.Aggregator.OutFunc(result)
		sw.Collector.Emit(outTuple)

		// Remove window from index
		sw.Index.Remove(window.Key, window.StartTime)
	}

	// Early return if all panes need to be kept for future windows
	if len(panesToRemove) == 0 {
		return
	}

	// Remove fired windows from state service
	sw.StateClient.DeleteManyAndFlush(panesToRemove)
}

// [stop-and-restart] Extract window indices for migration
func (sw *SlidingWindow[IN, OUT, K, V]) FetchMetadataForMigration(
	affectedBuckets map[uint64]struct{},
) []byte {

	keysExamined := make(map[K]uint64)
	indexToBeTransferred := make(map[int64]map[K][]int64)

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	sw.metadataMigrateLock.Lock()
	for startTime, windows := range sw.Index.Index {

		for key, panes := range windows {
			bucketId, ok := keysExamined[key]
			if !ok {
				bucketId = sw.StateClient.GetBucketIdx(key)
				keysExamined[key] = bucketId
			}

			// Find all active window indices that fall into affected buckets
			if _, ok := affectedBuckets[bucketId]; ok {

				keysPerStartTime, ok := indexToBeTransferred[startTime]
				if !ok {
					keysPerStartTime = make(map[K][]int64)
					indexToBeTransferred[startTime] = keysPerStartTime
				}

				for _, pane := range panes {
					keysPerStartTime[key] = append(
						keysPerStartTime[key],
						pane.StartTime,
					)
				}

				// Delete this active window from current worker
				sw.Index.Remove(key, startTime)
			}
		}
	}
	sw.metadataMigrateLock.Unlock()

	// Serialize indexToBeTransferred to bytes
	res, err := json.Marshal(indexToBeTransferred)
	if err != nil {
		log.Fatalln("Failed to serialize sliding window metadata:", err)
	}
	return res
}

// [stop-and-restart] Insert migrated window indices to local index
func (sw *SlidingWindow[IN, OUT, K, V]) InsertMigratedMetadata(
	metadata []byte,
) {

	var receivedIndex map[int64]map[K][]int64
	if err := json.Unmarshal(metadata, &receivedIndex); err != nil {
		log.Fatalln("Failed to deserialize sliding window metadata:", err)
	}

	// Synchronize access to metadata during migration - this task can be
	// sender and receiver at the same time
	sw.metadataMigrateLock.Lock()
	for windowStartTime, windowsByStartTime := range receivedIndex {
		for key, paneStartTimes := range windowsByStartTime {

			// Add windows to the local index
			sw.Index.Add(
				key,
				windowStartTime,
				paneStartTimes,
			)
		}
	}
	sw.metadataMigrateLock.Unlock()
}

// [lazy protocol] Implement ConstructFastForwardMetadata() for fast forward
// Extract and encode active window indices to be transferred to other workers
// Return: map[destWorkerId]FastForwardMetadata
func (sw *SlidingWindow[IN, OUT, K, V]) ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata {

	// Map for bookkeeping if a key (i) has been serialized for RoutingTable
	// lookup, and (ii) which worker it's assigned to.
	// This is to avoid duplicate serialization of the key
	keysExamined := make(map[K]uint16)

	// Affected windows to be transferred. There can be multiple dest workers.
	// Note: we should pass the affected windows instead of just panes.
	// Otherwise, the pane will create fired windows in new worker.
	// map[destWorkerId]map[windowStartTime]map[key][]PaneStartTime
	indexToBeTransferred := make(map[uint16]map[int64]map[K][]int64)

	for startTime, windows := range sw.Index.Index {

		for key, panes := range windows {

			destWorker, ok := keysExamined[key]

			if !ok {
				// This is the first time this key is examined - lookup the
				// RoutingTable for its owner worker
				destWorker = sw.GetOwnerWorkerUponReconfig(key)
				keysExamined[key] = destWorker
			}

			if destWorker != sw.WorkerId {
				// This is an active window that needs to be transferred to
				// destWorker
				indexToBeTransferredPerWorker, ok := indexToBeTransferred[destWorker]
				if !ok {
					indexToBeTransferredPerWorker = make(
						map[int64]map[K][]int64,
					)
					indexToBeTransferred[destWorker] = indexToBeTransferredPerWorker
				}

				// Add the window to indexToBeTransferredPerWorker
				windowsPerStartTime, ok := indexToBeTransferredPerWorker[startTime]
				if !ok {
					windowsPerStartTime = make(map[K][]int64)
					indexToBeTransferredPerWorker[startTime] = windowsPerStartTime
				}

				for _, pane := range panes {
					windowsPerStartTime[key] = append(
						windowsPerStartTime[key],
						pane.StartTime,
					)
				}

				// Delete this active window from current worker
				sw.Index.Remove(key, startTime)
			}
		}
	}

	// Serialize indexToBeTransferred as FastForwardMetadata messages
	res := make(map[uint16]*buffer.FastForwardMetadata)
	for destWorkerId, indices := range indexToBeTransferred {
		res[destWorkerId] = sw.SerializeToFastForwardMetadata(indices)
	}
	return res
}

// [lazy protocol] Implement ProcessFastForwardMetadata() for fast forward
// FastForwardMetadata is the first message received for fast forward channel
func (sw *SlidingWindow[IN, OUT, K, V]) ProcessFastForwardMetadata(
	metadata *buffer.FastForwardMetadata,
) {

	var receivedIndex map[int64]map[K][]int64
	dec := json.NewDecoder(bytes.NewReader(metadata.SerializedMetadata))
	if err := dec.Decode(&receivedIndex); err != nil {
		log.Fatalln("Failed to decode FastForwardMetadata:", err)
	}

	// Traverse the received index and add it to the local index
	for windowStartTime, windowsByStartTime := range receivedIndex {
		for key, paneStartTimes := range windowsByStartTime {

			// Add windows to the local index
			sw.Index.Add(
				key,
				windowStartTime,
				paneStartTimes,
			)
		}
	}
}

// Validate input type at compile time
func (sw *SlidingWindow[IN, OUT, K, V]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (sw *SlidingWindow[IN, OUT, K, V]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}

/******************************************************************************
	 						 Sliding window index
******************************************************************************/

// SlidingWindowIndex is the index to track active windows and panes in the
// windows
type SlidingWindowIndex[K comparable] struct {

	// Active windows organized by start time
	// map[startTime]map[key] -> list of window panes
	// - Each pane is a partial window with duration of a slide
	// - Each sliding window consists of multiple panes
	// - Each pane is uniquely identified by the key and its start time
	Index map[int64]map[K][]Pane[K]

	// Copy of sliding window duration
	Duration time.Duration

	// Copy of sliding window slide
	Slide time.Duration
}

func NewSlidingWindowIndex[K comparable](
	duration time.Duration,
	slide time.Duration,
) *SlidingWindowIndex[K] {
	return &SlidingWindowIndex[K]{
		Index:    make(map[int64]map[K][]Pane[K]),
		Duration: duration,
		Slide:    slide,
	}
}

// Check if a sliding window exists in the index - only used by test
func (w *SlidingWindowIndex[K]) ExistsWindow(key K, timestamp int64) bool {
	windows, ok := w.Index[timestamp]
	if !ok {
		return false
	}
	_, ok = windows[key]
	return ok
}

// Add a new pane to the index: add the pane to all windows it belongs to and
// create new windows if not exist in index
// AddPane() is called only when it's a new Pane (otherwise, this Pane exists
// in the state store), so we can safely append it all existing windows without
// worrying about duplicates in the Pane slice.
func (w *SlidingWindowIndex[K]) AddPane(key K, paneStartTime int64) {

	// Add the pane to the Pane map
	pane := NewPane(key, paneStartTime)

	// Get start times of all windows this pane belongs to
	windowStartTimes := GetSlidingWindowStartTimes(
		paneStartTime,
		w.Duration,
		w.Slide,
	)

	for _, windowStartTime := range windowStartTimes {

		// Create the window set at a given start time if not exist
		windows, ok := w.Index[windowStartTime]
		if !ok {
			windows = make(map[K][]Pane[K])
			w.Index[windowStartTime] = windows
		}

		// Append the key to the specific window - init a slice if windows[key]
		// does not exist
		windows[key] = append(windows[key], pane)
	}
}

// Remove the window with corresponding key and timestamp from the index
func (w *SlidingWindowIndex[K]) Remove(key K, timestamp int64) {

	if windowsWithSameStartTime, ok := w.Index[timestamp]; ok {
		delete(windowsWithSameStartTime, key)
		if len(windowsWithSameStartTime) == 0 {
			delete(w.Index, timestamp)
		}
	}
}

// Return all windows to be fired and their panes
func (w *SlidingWindowIndex[K]) GetWindowsToBeFired(
	wm *buffer.Watermark,
) ([]WindowsToBeFired[K], map[Pane[K]]bool) {

	windowsToBeFired := make([]WindowsToBeFired[K], 0)
	panes := make(map[Pane[K]]bool, 0)

	threshold := wm.Timestamp - w.Duration.Nanoseconds()
	for windowStartTime, windows := range w.Index {

		// Windows to be fired are the ones whose end time is <= current time
		if windowStartTime <= threshold {

			for key, paneSet := range windows {
				for _, pane := range paneSet {
					panes[pane] = true
				}
				windowsToBeFired = append(
					windowsToBeFired,
					WindowsToBeFired[K]{
						Key:       key,
						StartTime: windowStartTime,
						Panes:     paneSet,
					},
				)
			}
		}
	}

	return windowsToBeFired, panes
}

// [Lazy protocol] Add a new window from FastForwardMetadata to the index
func (w *SlidingWindowIndex[K]) Add(
	key K,
	startTime int64,
	paneStartTimes []int64,
) {
	// Create the window set at a given start time if not exist
	windows, ok := w.Index[startTime]
	if !ok {
		windows = make(map[K][]Pane[K])
		w.Index[startTime] = windows
	}

	// Append the key to the specific window - init a slice if windows[key]
	// does not exist
	for _, paneStartTime := range paneStartTimes {
		pane := NewPane(key, paneStartTime)
		windows[key] = append(windows[key], pane)
	}
}

/******************************************************************************
	 							  Helper utils
******************************************************************************/

// WindowsToBeFired is the struct to hold the windows that should be fired
type WindowsToBeFired[K comparable] struct {
	Key       K
	StartTime int64

	Panes []Pane[K]
}

// A Pane identifies a partial window with duration of a slide
// A sliding window: [ pane1 | pane2 | pane3 | ... ]
type Pane[K comparable] struct {
	Key       K
	StartTime int64
}

func NewPane[K comparable](key K, startTime int64) Pane[K] {
	return Pane[K]{
		Key:       key,
		StartTime: startTime,
	}
}

// Given a pane's starting time, return all its sliding window's starting time
func GetSlidingWindowStartTimes(
	paneStartTime int64,
	windowDuration time.Duration,
	windowSlide time.Duration,
) []int64 {

	if windowDuration%windowSlide != 0 {
		log.Fatalln("Duration must be a multiple of slide")
	}

	startTimes := make([]int64, 0)
	duration := windowDuration.Nanoseconds()
	slide := windowSlide.Nanoseconds()

	lower := paneStartTime - duration + slide
	upper := paneStartTime
	for i := lower; i <= upper; i += slide {
		startTimes = append(startTimes, i)
	}
	return startTimes
}

// Given a event timestamp, return the pane's starting time
func GetPaneStartTime(
	timestamp int64,
	slide time.Duration,
) int64 {
	// each pane has length of slide
	return timestamp - (timestamp % slide.Nanoseconds())
}
