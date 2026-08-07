package collector

import (
	"log"
	"reflect"
	"unsafe"

	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/network"
	"github.com/CASP-Systems-BU/koala/internal/utils"
)

// T: output typle type
type KeybyCollector[T tuple.Tuple, K comparable] struct {
	*CollectorBase[T]

	// Map of pending batches for each downstream: workerId -> batch
	CurBatches map[uint16]*buffer.Batch[T]

	// Routing table for downstream
	RoutingTable *keyby.PartitionTable

	// key assigner
	KeyAssigner *ka.KeyAssigner[T, K]

	// Key encoder and size function
	keyEncoder network.EncodeFunc
	keySizer   network.SizeFunc
}

var _ Collector = (*KeybyCollector[tuple.Tuple, any])(nil)

// Constructor with keyby indexing called at compile time
func NewKeybyCollector[T tuple.Tuple, K comparable](
	keyAssigner *ka.KeyAssigner[T, K],
	operatorID string,
) *KeybyCollector[T, K] {

	collectorBase := NewCollectorBase[T](operatorID)

	var key K
	encoder := network.EncodeFuncFromKind(reflect.TypeOf(key).Kind())
	sizer := network.TupleSizeFuncFromKind(reflect.TypeOf(key).Kind())

	return &KeybyCollector[T, K]{
		CollectorBase: collectorBase,
		KeyAssigner:   keyAssigner,
		keyEncoder:    encoder,
		keySizer:      sizer,
	}
}

// Implement Collector interface - setup the collector
// During the runtime, establish the downstream network connections and
// allocate buffers based on coordinator task assignment request
func (c *KeybyCollector[T, K]) Setup(para *utils.OperatorSetupParas) {

	if para.KeybyCollectorRoutingTable == nil {
		log.Fatalln("KeybyCollector: keyby routing table is needed")
	}

	c.SetupCollectorBase(
		para.OperatorName,
		para.WorkerID,
		para.DownstreamInfoList,
		para.Config,
		para.MetricCollector,
	)

	// Allocate an empty CurBatches map
	curBatches := make(
		map[uint16]*buffer.Batch[T],
		len(para.DownstreamInfoList),
	)
	for _, downstreamInfo := range para.DownstreamInfoList {
		curBatches[uint16(downstreamInfo.WorkerId)] = buffer.AllocateOutputBatch[T]()
	}
	c.CurBatches = curBatches

	// Set the routing table for downstream
	c.RoutingTable = para.KeybyCollectorRoutingTable

	// Start the timer timeout routine if it's enabled
	if c.Config.EnablePendingBatchTimeout {
		go c.monitorTimerTimeout(c.flushAllPendingBatches)
	}

	// Start the watermark generator routine if it's enabled
	// This can only happen to source
	if c.WatermarkGenerator != nil {
		go c.startWatermarkGenerator(c.BroadcastWatermark)
	}
}

// Implement Collector interface - consume the resulting record
func (c *KeybyCollector[T, K]) Emit(t tuple.Tuple) {

	// Update the record timestamp
	c.updateRecordTimestamp(t.(T))

	// Serialize the key field of the output tuple for hash function
	key := c.KeyAssigner.GetKey(t.(T))
	ptrToKey := unsafe.Pointer(&key)
	serializedKey := make([]byte, c.keySizer(ptrToKey))
	c.keyEncoder(ptrToKey, serializedKey)

	// Emiting record is synchronized
	c.Lock()

	// Determine which downstream task to go based on key partition
	downstreamWorkerId := c.RoutingTable.KeyToWorkerID(serializedKey)
	downstream := c.getDownstreamByWorkerID(downstreamWorkerId)
	targetBatch := c.getPendingBatchByWorkerID(downstreamWorkerId)
	recordSize := c.GetSize(t)
	c.MetricCollector.UpdateRecordSizePerBatch(int64(recordSize))

	// Check if current batch has sufficient space to hold the record
	if recordSize+int(targetBatch.TotalNumBytes) > int(c.Config.BatchSize) {

		// Current batch is full and cannot hold the record
		// Flush the current batch first - push the batch into the output
		// buffer
		downstream.PushToBuffer(targetBatch)

		// Init a new batch to put the current record
		c.CurBatches[downstreamWorkerId] = buffer.AllocateOutputBatch[T]()

		// Check if the record size itself exceeds the batch size limit
		if recordSize > int(c.Config.BatchSize) {
			log.Fatalln("Record size exceeds batch size limit - no support!")
		}
	}

	// Add the record to the current batch
	c.CurBatches[downstreamWorkerId].AddRecord(t.(T), recordSize)

	// If this is source with watermark generator added, update the max
	// record timestamp
	if c.WatermarkGenerator != nil {
		c.updateMaxRecordTimestamp(t.(T))
	}

	c.Unlock()
}

// Implement Collector interface - pause the emit()
func (c *KeybyCollector[T, K]) PauseEmit() {

	// Acquire the lock to block other accesses to Collector
	c.Lock()

	// Set the flag
	c.Paused = true

	// Flush all pending batches
	c.flushAllPendingBatches()

	// Broadcast the drainingBarrier to all downstreams
	c.broadcastDrainBarrier()
}

// 1. Add/Remove downstream connections
// 2. Update routing for keyby collector
func (c *KeybyCollector[T, K]) UpdateRouting(
	downstreamsToAdd []*pb.DownstreamInfo,
	downstreamsToRemove []*pb.DownstreamInfo,
	newDownstreamKR *pb.SerializedPartitionTable,
) {

	if newDownstreamKR == nil {
		log.Fatalln("KeybyCollector: new KeyPartition is needed")
	}

	// Collector re-config is synchronized
	// In stop-and-restart protocol, pause is called before re-config, so lock
	// is already acquired by pause and we can safely apply the change. We only
	// need to acquire lock if it's not paused already - this is needed by
	// zero-downtime re-config protocol where pause is not needed.
	if !c.Paused {
		c.Lock()
	}

	// Flush the pending batches first to be safe
	c.flushAllPendingBatches()

	// Connect to new downstreams
	c.connectToNewDownstreamsHelper(downstreamsToAdd)

	// [Lazy protocol] broadcast inflight barriers before removing downstreams
	if c.Config.ReconfigProtocol == "lazy" {
		c.broadcastInflightBarrier()
	}

	// Delete removed downstreams
	c.removeDownstreamsHelper(downstreamsToRemove)

	// Update routing table for updated key partition
	c.RoutingTable = keyby.DeserializePartitionTable(
		newDownstreamKR.BucketRanges,
		c.Config,
	)

	// Rebuild the CurBatches map based on updated downstreams
	c.CurBatches = make(map[uint16]*buffer.Batch[T], len(c.Downstreams))
	for workerId := range c.Downstreams {
		c.CurBatches[workerId] = buffer.AllocateOutputBatch[T]()
	}

	if !c.Paused {
		c.Unlock()
	}
}

// Terminate downstream connections and clean up
func (c *KeybyCollector[T, K]) Terminate() {
	c.Lock()
	defer c.Unlock()

	// Flush all pending batches
	c.flushAllPendingBatches()

	// Push termination signal to all downstream buffers
	c.terminate()
}

// Implement Collector interface - broadcast watermark
// Two cases to broadcast watermark:
// 1. Watermark generator at the source - this is already synchronized
// 2. Task main loop to broadcast a received watermark from upstream - need to
// handle sync here
func (c *KeybyCollector[T, K]) BroadcastWatermark(
	wm *buffer.Watermark,
	needSync bool,
) {

	if needSync {
		c.Lock()
	}

	// Flush all pending batches
	c.flushAllPendingBatches()

	// Broadcast the watermark to all downstreams
	c.broadcastWatermarkHelper(wm)

	if needSync {
		c.Unlock()
	}
}

/******************************************************************************
					   Keyby collector specific utils
******************************************************************************/

// Flush all pending batches if any of them is not empty
// This is always executed in a synchronized manner
func (c *KeybyCollector[T, K]) flushAllPendingBatches() {

	for workerId, batch := range c.CurBatches {

		// Flush the batch if it's not empty
		if batch.TotalNumRecords > 0 {

			// Flush the batch
			c.Downstreams[workerId].PushToBuffer(batch)

			// Init a new batch
			c.CurBatches[workerId] = buffer.AllocateOutputBatch[T]()
		}
	}
}

// Given worker id, get the target pending batch from CurBatches
func (c *KeybyCollector[T, K]) getPendingBatchByWorkerID(
	workerId uint16,
) *buffer.Batch[T] {

	targetBatch, ok := c.CurBatches[workerId]
	if !ok {
		log.Fatalf("CurBatches: worker id %d not found\n", workerId)
	}

	return targetBatch
}

// [lazy protocol] Broadcast inflight barrier to all downstreams
func (c *KeybyCollector[T, K]) broadcastInflightBarrier() {

	// Broadcast the drainingBarrier to all downstreams
	for _, downstream := range c.Downstreams {
		downstream.PushToBuffer(buffer.NewInflightBarrier())
	}

	log.Println("[Lazy INFO] InflightBarrier sent to all downstreams...")
}
