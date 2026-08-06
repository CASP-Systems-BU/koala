package collector

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
)

type RoundRobinCollector[T tuple.Tuple] struct {
	*CollectorBase[T]

	// Current output batch
	// There will be only 1 active output batch for Round-robin collector
	CurBatch *buffer.Batch[T]

	// Index list for sequential traversal of downstreams: list of worker ids
	// This is used to implement round-robin distribution
	DownstreamList []uint16

	// Idx of the next downstream to send batch (round-robin)
	NextDownstreamIdx int
}

var _ Collector = (*RoundRobinCollector[tuple.Tuple])(nil)

// Constructor called at compile time
func NewRoundRobinCollector[T tuple.Tuple](
	operatorID string,
) *RoundRobinCollector[T] {

	collectorBase := NewCollectorBase[T](operatorID)

	return &RoundRobinCollector[T]{
		CollectorBase: collectorBase,
	}
}

// Implement Collector interface - setup the collector
// During the runtime, establish the downstream network connections and
// allocate buffers based on coordinator task assignment request
func (c *RoundRobinCollector[T]) Setup(para *utils.OperatorSetupParas) {

	c.SetupCollectorBase(
		para.OperatorName,
		para.DownstreamInfoList,
		para.Config,
		para.MetricCollector,
	)

	// Init the pending batch
	c.CurBatch = buffer.AllocateOutputBatch[T]()

	// Init the downstream list
	downstreamList := make([]uint16, 0, len(para.DownstreamInfoList))
	for _, downstreamInfo := range para.DownstreamInfoList {
		downstreamList = append(downstreamList, uint16(downstreamInfo.WorkerId))
	}
	c.DownstreamList = downstreamList

	// Init the round-robin pointer
	c.NextDownstreamIdx = -1

	// Start the timer timeout routine if it's enabled
	if c.Config.EnablePendingBatchTimeout {
		go c.monitorTimerTimeout(c.flushPendingBatch)
	}

	// Start the watermark generator routine if it's enabled
	// This can only happen to source
	if c.WatermarkGenerator != nil {
		go c.startWatermarkGenerator(c.BroadcastWatermark)
	}
}

// Implement Collector interface - consume the resulting record
func (c *RoundRobinCollector[T]) Emit(t tuple.Tuple) {

	// Update the record timestamp
	c.updateRecordTimestamp(t.(T))
	recordSize := c.GetSize(t)

	// Emiting record is synchronized
	c.Lock()

	// Check if current batch has sufficient space to hold the new record
	if recordSize+int(c.CurBatch.TotalNumBytes) > int(c.Config.BatchSize) {

		// Current batch is full and cannot hold the record -> flush it
		c.flushPendingBatch()

		// Check if the new record itself exceeds the batch size limit
		if recordSize > int(c.Config.BatchSize) {
			log.Fatalln("Record size exceeds batch size limit - no support!")
		}
	}

	// Add the record to the current batch
	c.CurBatch.AddRecord(t.(T), recordSize)

	// If this is source with watermark generator added, update the max
	// record timestamp
	if c.WatermarkGenerator != nil {
		c.updateMaxRecordTimestamp(t.(T))
	}

	c.Unlock()
}

// Implement Collector interface - pause the emit()
func (c *RoundRobinCollector[T]) PauseEmit() {

	// Acquire the lock to block other accesses to Collector
	c.Lock()

	// Set the flag
	c.Paused = true

	// Flush the pending batch
	c.flushPendingBatch()

	// Broadcast the drainingBarrier to all downstreams
	c.broadcastDrainBarrier()
}

// Add/Remove downstream connections
func (c *RoundRobinCollector[T]) UpdateRouting(
	downstreamsToAdd []*pb.DownstreamInfo,
	downstreamsToRemove []*pb.DownstreamInfo,
	newDownstreamKR *pb.SerializedKeyLookupTable,
) {

	if newDownstreamKR != nil {
		log.Fatalln("RoundRobinCollector: new KeyPartition is not needed")
	}

	// Collector re-config is synchronized
	// In stop-and-restart protocol, pause is called before re-config, so lock
	// is already acquired by pause and we can safely apply the change. We only
	// need to acquire lock if it's not paused already - this is needed by
	// zero-downtime re-config protocol where pause is not needed.
	if !c.Paused {
		c.Lock()
	}

	// Flush the pending batch first to be safe
	c.flushPendingBatch()

	// Connect to new downstreams
	c.connectToNewDownstreamsHelper(downstreamsToAdd)

	// Delete removed downstreams
	c.removeDownstreamsHelper(downstreamsToRemove)

	// Rebuild the round-robin downstream list using the updated downstream map
	// and reset the pointer
	c.DownstreamList = make([]uint16, 0, len(c.Downstreams))
	for downstreamWorkerID := range c.Downstreams {
		c.DownstreamList = append(c.DownstreamList, downstreamWorkerID)
	}
	c.NextDownstreamIdx = -1

	log.Printf(
		"[%s Routing Update INFO] Added %d downstreams, Removed %d downstreams, Current total downstreams: %d\n",
		c.OperatorName,
		len(downstreamsToAdd),
		len(downstreamsToRemove),
		len(c.DownstreamList),
	)

	if !c.Paused {
		c.Unlock()
	}
}

// Terminate downstream connections and clean up
func (c *RoundRobinCollector[T]) Terminate() {
	c.Lock()
	defer c.Unlock()

	// Flush the pending batch
	c.flushPendingBatch()

	// Push termination signal to all downstream buffers
	c.terminate()
}

// Implement Collector interface - broadcast watermark
// Two cases to broadcast watermark:
// 1. Watermark generator at the source - this is already synchronized
// 2. Task main loop to broadcast a received watermark from upstream - need to
// handle sync here
func (c *RoundRobinCollector[T]) BroadcastWatermark(
	wm *buffer.Watermark,
	needSync bool,
) {

	if needSync {
		c.Lock()
	}

	// Flush the pending batch
	c.flushPendingBatch()

	// Broadcast the watermark to all downstreams
	c.broadcastWatermarkHelper(wm)

	if needSync {
		c.Unlock()
	}
}

/******************************************************************************
				   RoundRobinCollector collector specific utils
******************************************************************************/

// Helper function to increment the downstream pointer in round-robin fashion
func (c *RoundRobinCollector[T]) incrementNextDownstreamIdx() *network.Downstream[T] {

	c.NextDownstreamIdx += 1
	if c.NextDownstreamIdx >= len(c.DownstreamList) {
		c.NextDownstreamIdx = 0
	}

	// Get the downstream by worker id
	nextDownstream := c.getDownstreamByWorkerID(
		c.DownstreamList[c.NextDownstreamIdx],
	)

	return nextDownstream
}

// Flush the pending batch if it's not empty
// This is always executed in a synchronized manner
func (c *RoundRobinCollector[T]) flushPendingBatch() {

	if c.CurBatch.TotalNumRecords > 0 {

		// Move the downstream pointer - round robin
		nextDownstream := c.incrementNextDownstreamIdx()

		// Flush the batch - push the batch into the output buffer
		// This is a blocking call - keep pushing until success
		nextDownstream.PushToBuffer(c.CurBatch)

		// Reset the current pending batch with a new one
		c.CurBatch = buffer.AllocateOutputBatch[T]()
	}
}
