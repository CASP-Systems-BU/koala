package lazy

import (
	"context"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
)

// PeerCollector is used to forward records to peer tasks of the same operator
// during lazy protocol. PeerCollector is different from Collector - the data
// structure used for collecting output records to downstream operators (even
// though they share some functionalities and utiles). PeerCollector is only
// included in in stateful operators.
// Each PeerCollector corresponds to a specific upstream operator during fast
// forwarding. For example, if a stateful operator has 2 upstream operators,
// it will have 2 PeerCollectors, one for each upstream operator.
// T: tuple type of the input stream from the upstream operator.

type PeerCollector[T tuple.Tuple] struct {
	sync.Mutex

	// Global configuration
	Config *configuration.Configuration

	// Copy of operator name this PeerCollector belongs to
	OperatorName string

	// Copy of the local worker ID
	WorkerId uint16

	// Upstream operator name of the current task
	UpstreamName string

	// Downstreams: mapping from workerId to Downstream structure
	Downstreams map[uint16]*network.Downstream[T]

	// Map of pending batches for each downstream: workerId -> batch
	CurBatches map[uint16]*buffer.Batch[T]

	// Encoding function
	Encoder network.TupleEncoder

	// Size function
	GetSize network.TupleSizer

	// If the timer timeout is enabled, the following fields are used
	// Cancel ctx cancel function for timer timeout
	TimerCancel context.CancelFunc
	// Routine wait group for timer timeout routine
	TimerWaitGroup sync.WaitGroup

	// Pointer to the MetricCollector. This is used to update output related
	// metrics during runtime
	MetricCollector *metric.MetricCollector
}

// Compile time constructor
func NewPeerCollector[T tuple.Tuple](
	operatorName string,
	workerId uint16,
	upstreamName string,
	config *configuration.Configuration,
) *PeerCollector[T] {

	var zero T
	encoder := network.CreateEncoder(reflect.TypeOf(zero).Elem())
	sizer := network.CreateSizer(reflect.TypeOf(zero).Elem())

	return &PeerCollector[T]{
		Config:       config,
		OperatorName: operatorName,
		WorkerId:     workerId,
		UpstreamName: upstreamName,
		Encoder:      encoder,
		GetSize:      sizer,
	}
}

/******************************************************************************
						PeerCollector exposed APIs
******************************************************************************/

// Activate PeerCollector when re-config is triggered. Output channel type:
// 1 - single peer channel (single-upstream op)
// 2 - multi-peer channel (multi-upstream op) - primary (carry metadata)
// 3 - multi-peer channel (multi-upstream op) - secondary
// TODO: we need to distinguish the type of peer channel due to the current
// implementation of multi-upstream operators (2 peer connections per task)
// Only lazy-by-key needs this. Refactor this implementation in the future
func (c *PeerCollector[T]) Activate(
	peerList []*pb.DownstreamInfo,
	metricCollector *metric.MetricCollector,
	outputChannelType uint8,
) {

	// Initialize empty CurBatches
	curBatches := make(
		map[uint16]*buffer.Batch[T],
		len(peerList),
	)
	for _, peer := range peerList {
		curBatches[uint16(peer.WorkerId)] = buffer.AllocateOutputBatch[T]()
	}
	c.CurBatches = curBatches

	// Initialize the downstream structures
	c.Downstreams = make(
		map[uint16]*network.Downstream[T],
		len(peerList),
	)

	for _, peer := range peerList {

		log.Printf(
			"[INFO] Op %s creating peer connection struct %s from upstream %s",
			c.OperatorName,
			peer.String(),
			c.UpstreamName,
		)

		// Init the downstream connection using the upstream's name
		downstream := network.NewDownStream[T](
			peer.DataPlaneAddr,
			c.Config,
			metricCollector,
			c.UpstreamName,
		)
		c.Downstreams[uint16(peer.WorkerId)] = downstream
	}

	// Start network routines to consume the buffer
	for _, downstream := range c.Downstreams {
		go downstream.ConsumeOutputBuffer(
			c.Encoder,
			outputChannelType,
			c.WorkerId,
		)
	}

	// Start the timer timeout routine if it's enabled
	if c.Config.EnablePendingBatchTimeout {
		ctx, cancel := context.WithCancel(context.Background())
		c.TimerCancel = cancel
		c.TimerWaitGroup.Add(1)
		go c.monitorTimerTimeout(ctx)
	}
}

// De-activate PeerCollector after re-config
func (c *PeerCollector[T]) Deactivate() {

	// End the timer timeout routine
	if c.Config.EnablePendingBatchTimeout {
		c.TimerCancel()

		// Wait until the timer routine is successfully terminated
		c.TimerWaitGroup.Wait()
		log.Println("[info] PeerCollector timer timeout routine is terminated")

		// Reset the timer wait group for next re-config
		c.TimerWaitGroup = sync.WaitGroup{}
	}

	// Flush the remaining pending batches
	// No need for lock now since Timer routine is killed and emit() will not
	// be called anymore
	c.flushAllPendingBatches()

	// Push termination signals to all output buffers & network routines
	// Network routines and output buffer will be eventually closed after this
	for _, downstream := range c.Downstreams {
		downstream.PushToBuffer(buffer.NewTerminationSignal())
	}

	// Release resources in peer collector
	c.Downstreams = nil
	c.CurBatches = nil
}

// Emit record to peers
func (c *PeerCollector[T]) Emit(
	record tuple.Tuple,
	workerId uint16,
) {

	// Emit records in critical section - avoid interference with timer routine
	// timer routine will also flush pending batches
	c.Lock()
	defer c.Unlock()

	downstream := c.getDownstreamByWorkerID(workerId)
	pendingBatch := c.getPendingBatchByWorkerID(workerId)
	recordSize := c.GetSize(record)

	// If the record size exceeds the batch size, flush the current batch
	if recordSize+int(pendingBatch.TotalNumBytes) > int(c.Config.BatchSize) {

		// Flush the current batch
		downstream.PushToBuffer(pendingBatch)

		// Init a new batch to put the current record
		c.CurBatches[workerId] = buffer.AllocateOutputBatch[T]()

		// Check if the record size itself exceeds the batch size limit
		if recordSize > int(c.Config.BatchSize) {
			log.Fatalln("Record size exceeds batch size limit - no support!")
		}
	}

	// Add the record to the current batch
	c.CurBatches[workerId].AddRecord(record.(T), recordSize)
}

// Send fast forward metadata
func (c *PeerCollector[T]) SendFastForwardMetadata(
	metadata map[uint16]*buffer.FastForwardMetadata,
) {
	// Timer routine can concurently flush batches to Downstreams
	c.Lock()
	defer c.Unlock()

	// Send each ForwardMetadata to the corresponding downstream
	for workerId, fastForwardMetadata := range metadata {
		downstream := c.getDownstreamByWorkerID(workerId)

		// Push the fast forward metadata to the downstream buffer
		downstream.PushToBuffer(fastForwardMetadata)
	}
}

// Broadcast the watermark to all connected peers
func (c *PeerCollector[T]) BroadcastWatermark(
	watermark *buffer.Watermark,
) {
	// Timer routine can concurently flush batches to Downstreams
	c.Lock()
	defer c.Unlock()

	c.flushAllPendingBatches()

	// Broadcast the watermark to all connected peers
	for _, downstream := range c.Downstreams {
		// Push the watermark to the downstream buffer
		downstream.PushToBuffer(watermark)
	}
}

// [lazy-by-key] Send the key lookup table - KeyMaps for affected buckets
func (c *PeerCollector[T]) SendKeyMaps(
	serializedKeyMaps map[uint16]*buffer.MigratingKeyMap,
) {
	// Timer routine can concurently flush batches to Downstreams
	c.Lock()
	defer c.Unlock()

	// Send each KeyMap to the corresponding downstream
	for workerId, keyMap := range serializedKeyMaps {
		downstream := c.getDownstreamByWorkerID(workerId)

		// Push the KeyMap to the downstream buffer
		downstream.PushToBuffer(keyMap)
	}
}

/******************************************************************************
					     PeerCollector internal utils
******************************************************************************/

// Timer timeout routine to early flush pending batches
func (c *PeerCollector[T]) monitorTimerTimeout(ctx context.Context) {
	defer c.TimerWaitGroup.Done()

	timeoutInterval := c.Config.PendingBatchTimeoutInterval

	// Periodically triggers the timeout
	for {
		time.Sleep(timeoutInterval)

		select {
		// First check the termination signal at each iteration
		case <-ctx.Done():
			// Terminate the routine
			return
		default:
			// Flush all pending batches in critical section - block emit()
			// while flushing pending batches
			c.Lock()
			c.flushAllPendingBatches()
			c.Unlock()
		}
	}
}

// Flush all pending batches
func (c *PeerCollector[T]) flushAllPendingBatches() {

	for workerId, batch := range c.CurBatches {

		// If the batch is not empty, flush it
		if batch.TotalNumRecords > 0 {
			// Flush the batch
			c.Downstreams[workerId].PushToBuffer(batch)
			// Init a new batch
			c.CurBatches[workerId] = buffer.AllocateOutputBatch[T]()
		}
	}
}

// Get downstream by workerId
func (c *PeerCollector[T]) getDownstreamByWorkerID(
	workerId uint16,
) *network.Downstream[T] {

	downstream, ok := c.Downstreams[workerId]
	if !ok {
		log.Fatalf("Downstream not found for workerId %d\n", workerId)
	}

	return downstream
}

// Get pending batch by workerId
func (c *PeerCollector[T]) getPendingBatchByWorkerID(
	workerId uint16,
) *buffer.Batch[T] {
	targetBatch, ok := c.CurBatches[workerId]
	if !ok {
		log.Fatalf("CurBatches: workerId %d not found\n", workerId)
	}
	return targetBatch
}
