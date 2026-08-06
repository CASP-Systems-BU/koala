package collector

import (
	"log"
	"reflect"
	"sync"
	"time"

	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/watermark"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
)

// Collector defines and manages all downstream-related structure of the
// operator, including output buffers, output network routines, data
// distribution pattern, and encoder etc.

type Collector interface {

	// Setup collector during runtime
	Setup(*utils.OperatorSetupParas)

	// Handle the generated record of the operator
	// Push it into the output buffer
	Emit(tuple.Tuple)

	// Pause the emit()
	PauseEmit()

	// Resume the emit()
	ResumeEmit()

	// Update downstream routing. Input:
	// 1. new downstream tasks to connect
	// 2. old downstream tasks to remove
	// 3. updated routing table
	UpdateRouting(
		[]*pb.DownstreamInfo,
		[]*pb.DownstreamInfo,
		*pb.SerializedKeyLookupTable,
	)

	// Terminate outbound connections and clean up
	Terminate()

	// Broadcast the watermark to all downstreams
	BroadcastWatermark(*buffer.Watermark, bool)

	// Set the watermark generator
	SetTsAssignerAndWatermarkGenerator(
		ta.TimestampAssignerAPI,
		*watermark.PeriodicWatermarkGenerator,
	)

	// Get the watermark generator
	GetWatermarkGenerator() *watermark.PeriodicWatermarkGenerator

	// Get the timestamp assigner
	GetTsAssigner() (ta.TimestampAssignerAPI, bool)

	// Set the current timestamp
	SetCurrentTimestamp(int64)

	// Construct a SubSupplier for the downstream operator
	ConstructSubSupplier() supplier.SubSupplier
}

// Common fields owned by all collectors
type CollectorBase[T tuple.Tuple] struct {

	// Lock for Collector structures
	// Please see issue #36 for Collector concurrent access pattern
	sync.Mutex

	// Copy of name of the current operator for logging purposes
	OperatorName string

	// Store the global config
	Config *configuration.Configuration

	// Downstreams: mapping from workerId to Downstream structure
	Downstreams map[uint16]*network.Downstream[T]

	// Encoding function
	Encoder network.TupleEncoder

	// Size function
	GetSize network.TupleSizer

	// If the Collector is paused by stop-and-restart protocol
	Paused bool

	// Watermark generator
	WatermarkGenerator *watermark.PeriodicWatermarkGenerator

	// Timestamp assigner
	TimestampAssigner *ta.TimestampAssigner[T]

	// The timestamp that will be assigned to the emitted record
	// when calling Emit()
	CurrentTimestamp int64

	// Pointer to the MetricCollector. This is used to update output related
	// metrics during runtime
	MetricCollector *metric.MetricCollector
}

// Constructor called at compile time
func NewCollectorBase[T tuple.Tuple](
	operatorName string,
) *CollectorBase[T] {

	// Create the Encoding function based on type T
	var zero T
	encoder := network.CreateEncoder(reflect.TypeOf(zero).Elem())
	// Create the Size function based on type T
	sizer := network.CreateSizer(reflect.TypeOf(zero).Elem())

	return &CollectorBase[T]{
		Encoder:          encoder,
		GetSize:          sizer,
		CurrentTimestamp: -1,
		OperatorName:     operatorName,
	}
}

// Runtime setup
func (c *CollectorBase[T]) SetupCollectorBase(
	operatorName string,
	downstreamInfoList []*pb.DownstreamInfo,
	config *configuration.Configuration,
	metricCollector *metric.MetricCollector,
) {

	if metricCollector == nil {
		log.Println("[info] No metric collector is configured")
	}
	c.OperatorName = operatorName

	// Initialize the downstream structures
	downstreams := make(
		map[uint16]*network.Downstream[T],
		len(downstreamInfoList),
	)

	for _, downstreamInfo := range downstreamInfoList {

		log.Printf(
			"[Task %s INFO] Connecting to downstream %s",
			c.OperatorName,
			downstreamInfo.String(),
		)

		downstream := network.NewDownStream[T](
			downstreamInfo.DataPlaneAddr,
			config,
			metricCollector,
			c.OperatorName,
		)
		downstreams[uint16(downstreamInfo.WorkerId)] = downstream
	}

	c.Config = config
	c.Downstreams = downstreams
	c.MetricCollector = metricCollector

	// Start output network routines
	c.startOutputNetworkRoutines()
}

/******************************************************************************
					Base implementation of interface methods
******************************************************************************/

// Implement the ResumeEmit() method
// PauseEmit() is implemented in Collector implementations
func (c *CollectorBase[T]) ResumeEmit() {

	// Reset the flag
	c.Paused = false

	// Release the lock to allow other accesses to Collector
	c.Unlock()

	log.Println("Collector resumed")
}

// Implement the SetTsAssignerAndWatermarkGenerator() method
// Set tsAssigner and watermark generator to source if configured
func (c *CollectorBase[T]) SetTsAssignerAndWatermarkGenerator(
	tsAssignerAPI ta.TimestampAssignerAPI,
	wmGenerator *watermark.PeriodicWatermarkGenerator,
) {

	// Convert the interface to the actual type
	tsAssigner, ok := tsAssignerAPI.(*ta.TimestampAssigner[T])
	if !ok {
		log.Fatalf(
			"[error] Invalid timestamp assigner type: %T\n",
			tsAssignerAPI,
		)
	}

	c.WatermarkGenerator = wmGenerator
	c.TimestampAssigner = tsAssigner
}

// Implement the GetWatermarkGenerator() method
func (c *CollectorBase[T]) GetWatermarkGenerator() *watermark.PeriodicWatermarkGenerator {
	return c.WatermarkGenerator
}

// Implement the GetTsAssigner() method
func (c *CollectorBase[T]) GetTsAssigner() (ta.TimestampAssignerAPI, bool) {

	if c.TimestampAssigner == nil {
		return nil, false
	}

	return c.TimestampAssigner, true
}

// Implement the SetCurrentTimestamp() method
// Update the current timestamp of the collector
func (c *CollectorBase[T]) SetCurrentTimestamp(timestamp int64) {
	c.CurrentTimestamp = timestamp
}

func (c *CollectorBase[T]) ConstructSubSupplier() supplier.SubSupplier {
	return supplier.NewRoundRobinSubSupplier[T](c.OperatorName)
}

/******************************************************************************
						   Common utils for all collectors
******************************************************************************/

// Start all network routines to consume output buffers
func (c *CollectorBase[T]) startOutputNetworkRoutines() {

	// TODO 1: Add configurable network routines - now 1 per downstream
	// TODO 2: Track all started go routines and add graceful termination

	// Start all output network routines
	for _, downstream := range c.Downstreams {
		go downstream.ConsumeOutputBuffer(c.Encoder, false)
	}
}

// Connect to new downstreams during re-config
func (c *CollectorBase[T]) connectToNewDownstreamsHelper(
	newDownstreamList []*pb.DownstreamInfo,
) {

	for _, ds := range newDownstreamList {

		// Create a new downstream
		newDownstream := network.NewDownStream[T](
			ds.DataPlaneAddr,
			c.Config,
			c.MetricCollector,
			c.OperatorName,
		)
		go newDownstream.ConsumeOutputBuffer(c.Encoder, false)

		// Add the new downstream to the map
		_, ok := c.Downstreams[uint16(ds.WorkerId)]
		if ok {
			log.Fatalf(
				"[error] Worker %d already exists in downstreams\n",
				ds.WorkerId,
			)
		}
		c.Downstreams[uint16(ds.WorkerId)] = newDownstream
	}
}

// Remove existing downstreams
func (c *CollectorBase[T]) removeDownstreamsHelper(
	downstreamsToRemove []*pb.DownstreamInfo,
) {

	for _, ds := range downstreamsToRemove {

		workerId := uint16(ds.WorkerId)

		// Get the downstream to be removed
		downstream, ok := c.Downstreams[workerId]
		if !ok {
			log.Fatalf(
				"[error] Worker %d not found in downstreams\n",
				ds.WorkerId,
			)
		}

		// Terminate the downstream connection
		downstream.PushToBuffer(buffer.NewTerminationSignal())

		// Remove from the map
		delete(c.Downstreams, workerId)
	}
}

// Terminate downstream connections and clean up
func (c *CollectorBase[T]) terminate() {

	// Broadcast termination signal to all ouptut buffers -> the network
	// routines will eventually terminate downstrea connections
	for _, downstream := range c.Downstreams {
		downstream.PushToBuffer(buffer.NewTerminationSignal())
	}
}

// Broadcast the drainingBarrier to all downstreams
func (c *CollectorBase[T]) broadcastDrainBarrier() {

	for _, downstream := range c.Downstreams {
		downstream.PushToBuffer(buffer.NewDrainBarrier())
	}
	log.Println("DrainBarrier sent to all downstreams...")
}

// Broadcast the watermark to all downstreams
func (c *CollectorBase[T]) broadcastWatermarkHelper(wm *buffer.Watermark) {

	for _, downstream := range c.Downstreams {
		downstream.PushToBuffer(wm)
	}

	if c.Config.WatermarkDebug {
		log.Printf(
			"[Watermark info] Broadcast %d sent to all downstreams...\n",
			wm.Timestamp,
		)
	}
}

// Timer timeout routine
func (c *CollectorBase[T]) monitorTimerTimeout(
	flushPendingBatchExecutor func(),
) {

	log.Println("[info] Pending batch timeout is enabled")
	timeoutInterval := c.Config.PendingBatchTimeoutInterval

	// Periodically triggers timeout
	for {
		time.Sleep(timeoutInterval)

		// Timer timeout is synchronized
		c.Lock()

		// Flush the pending batch
		flushPendingBatchExecutor()

		c.Unlock()
	}
}

// Watermark generator routine for source
func (c *CollectorBase[T]) startWatermarkGenerator(
	broadcastWatermarkExecutor func(*buffer.Watermark, bool),
) {

	interval := c.WatermarkGenerator.PeriodicInterval

	// Periodically triggers watermark generation
	for {

		time.Sleep(interval)

		// Calculate current watermark in a synchronized manner
		// CurrentMaxTime of the watermark generator can be concurrently
		// updated by Emit() at the source
		c.Lock()
		wm, progressed := c.WatermarkGenerator.GenerateWatermark()
		if !progressed {
			c.Unlock()
			continue
		}

		// Broadcast the watermark to downstreams - already synchronized
		broadcastWatermarkExecutor(wm, false)

		c.Unlock()
	}
}

// Update max record timestamp for source with watermark generator added
func (c *CollectorBase[T]) updateMaxRecordTimestamp(record T) {

	// Get timestamp field of source output record
	timestamp := c.TimestampAssigner.GetTimestamp(record)

	// Update the max record timestamp if needed
	c.WatermarkGenerator.UpdateMaxRecordTimestamp(timestamp)
}

// Update the timestamp of a record
func (c *CollectorBase[T]) updateRecordTimestamp(record T) {

	// Check if the collector is source with watermark generator
	if c.WatermarkGenerator != nil {

		// if so, set the current timestamp to the record's timestamp field
		timestamp := c.TimestampAssigner.GetTimestamp(record)
		record.SetTimestamp(timestamp)
	} else {

		// Include two cases, both set the timestamp to c.CurrentTimestamp
		// 1. The collector is a source without watermark generator, set
		// timestamp to -1 (c.CurrentTimestamp is initialized to -1)
		// 2. The collector is a task, set timestamp to the current timestamp
		record.SetTimestamp(c.CurrentTimestamp)
	}
}

// Get downstream by worker id
func (c *CollectorBase[T]) getDownstreamByWorkerID(
	workerID uint16,
) *network.Downstream[T] {

	downstream, ok := c.Downstreams[workerID]

	// The requested downstream worker must be present in the collector
	if !ok {
		log.Fatalf("Downstream not found by worker id: %d\n", workerID)
	}

	return downstream
}
