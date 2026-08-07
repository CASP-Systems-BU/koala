package dataflow

import (
	"log"
	"net"
	"strconv"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"github.com/CASP-Systems-BU/koala/metric"
)

type Operator interface {

	/**************************************************************************
					Methods to be implemented by all Operators
	**************************************************************************/

	// Initialize the operator after receiving the task placement request.
	Setup(*utils.OperatorSetupParas)

	/**************************************************************************
					Methods to be implemented by Source Operators
	**************************************************************************/

	// Run source operator. Non-source operators don't need to implement this.
	RunSource()

	/**************************************************************************
				Methods to be implemented by Non-source Operators
	**************************************************************************/

	// Process a batch of records from upstream. Source operators don't need to
	// implement this.
	//
	// Parameters:
	//  1. A batch of records
	//  2. Name of the SubSupplier (upstream operator) that this batch is from
	ProcessBatch(buffer.WorkUnit, string)

	/**************************************************************************
						Exposed APIs for all Operators
	**************************************************************************/

	// Get operator ID - unique identifier of the operator
	GetId() uint16

	// Set operator ID
	SetId(uint16)

	// Get operator name - unique for all operators
	GetName() string

	// Check if this is the source operator
	IsSource() bool

	// Check if this is the sink operator
	IsSink() bool

	// Check if the operator is stateful
	IsStatefulOperator() bool

	// Init a new Upstream structure when a new connection is received
	InitNewUpstream(net.Conn)

	// Get next WorkUnit from upstream - only apply for non-source operators
	// Return:
	// 1. The next WorkUnit from upstreams
	// 2. If the workunit comes from peer input channel
	// 3. The name of the upstream operator where the WorkUnit comes from
	// 4. [DRRS] If this read is successful - it could return nothing if no
	//    input is available in peer-only mode to consume wait buffer
	GetWorkUnit() (buffer.WorkUnit, bool, string, bool)

	// Get the parallelism of the operator
	GetParallelism() int

	// Pause the processing of the task (pause Collector emit())
	Pause()

	// Resume the processing of the task (resume Collector emit())
	Resume()

	// Set the output stream of the upstream operator to be a keyed stream. Note
	// that we set keyby collector of the upstream operator through method of
	// downstream stateful operator. Input parameter is the upstream operator(s)
	// whose collector will be set to keyby collector.
	SetKeyedOutputStreamForUpstream(upstreams []Operator)

	// Reset the collector of the operator. This is used to reset KeyByCollector
	// for keyed stream
	SetCollector(collector.Collector)

	// Get the current collector of the operator. This is used to reset
	// KeyByCollector
	// for keyed stream
	GetCollector() collector.Collector

	// Add a SubSupplier to the Supplier. This is called at JobGraph
	// construction time when the operator is created.
	AddSubSupplier(supplier.SubSupplier)

	// Construct a SubSupplier for the downstream operator. The upstream
	// operator calls this method.
	AddSubSupplierToDownstream(Operator)

	// Update downstream routing
	UpdateDownstreamRouting(
		[]*pb.DownstreamInfo,
		[]*pb.DownstreamInfo,
		*pb.SerializedKeyLookupTable,
	)

	// Terminate the task
	Terminate()

	// Process progressed watermark
	ProcessProgressedWatermark(*buffer.Watermark)

	// Broadcast watermark to downstreams
	BroadcastWatermark(*buffer.Watermark)

	// Get the task watermark - this is an API for testing
	GetTaskWatermarkForTesting() *buffer.Watermark

	// Update the input-related metrics in MetricCollector
	UpdateInputMetrics(buffer.WorkUnit)

	// Update the processing latency in MetricCollector
	UpdateProcessingLatency()

	// [Stop-and-restart] Fetch local in-memory task metadata for affected
	// key space (buckets)
	FetchMetadataForMigration(map[uint64]struct{}) []byte

	// [Stop-and-restart] Insert the fetched in-memory task metadata to local
	// task metadata structure
	InsertMigratedMetadata([]byte)

	// [Lazy protocol] Initialize the lazy reconfiguration with metadata
	// 1. ExpectedDrainBarriers: expected number of in-flight barriers
	// 2. ExpectedInboundPeers: expected number of inbound peer connections
	// 3. IsShuttingDown: whether the task is going to be shutdown after
	//    reconfiguration
	InitLazyReconfig(int32, int32, bool)

	// [Lazy protocol] If during reconfiguration under lazy protocol, we first
	// traverse the record batch to fast forward records whose ownership has
	// been transferred to another worker. This method only applies for
	// stateful operators as stateless operators don't have ownership transfer
	FastForward(buffer.WorkUnit, string) (buffer.WorkUnit, bool)

	// [Lazy protocol] Wait all expected inbound peer connections to connect
	WaitAllInboundPeersToConnect()

	// [Lazy protocol] Wait all tasks to finish reconfiguration:
	// 1. All in-flight barriers from upstreams are received
	// 2. All inbound peer connections are closed
	WaitTasksReconfigDone()

	// [Lazy protocol] Set Routing table and activate PeerCollector for fast
	// forward
	PrepareLazyProtocol([]*pb.DownstreamInfo, *keyby.KeyLookupTable)

	// [Lazy protocol] Construct FastForward Metadata to connected peers
	// This is the method overridden by stateful operators that needs to pass
	// metadata upon fast forwarding - e.g. window operators
	ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata

	// [Lazy protocol] Send FastForwardMetadata to connected peers
	SendFastForwardMetadata(map[uint16]*buffer.FastForwardMetadata)

	// [Lazy protocol] Process FastForwardMetadata received from peers
	ProcessFastForwardMetadata(metadata *buffer.FastForwardMetadata)

	// [Lazy protocol] Broadcast the current task watermark to connected peers
	// If the watermark is nil, it will query the current task watermark
	// from the Supplier
	BroadcastWatermarkToPeers(*buffer.Watermark)

	// [Lazy protocol] Notify control plane the main routine has entered
	// reconfiguration phase
	NotifyReconfigStart()

	// [Lazy protocol] Wait for reconfiguration phase to start
	WaitReconfigStart()

	// [Lazy protocol] Exit the transition phase when InFlightBarrier is
	// received
	ExitTransitionPhase()

	// [DRRS] Split input batch and push blocked records to WaitBuffer
	SplitBatchForDRRS(buffer.WorkUnit, bool, string) (buffer.WorkUnit, bool)

	// [DRRS] Consume WaitBuffer
	ConsumeDRRSWaitBuffer(Operator, bool)

	// [DRRS] Check if a progressed watermark can be processed. Return true if
	// it is inserted into wait buffer and cannot be processed
	BlockWatermarkDRRS(*buffer.Watermark) bool

	// [DRRS] Enter termination waiting phase, set the flag to let the main
	// routine check termination condition
	EnterTerminationPhase()

	// [DRRS] Exit the DRRS reconfiguration phase, reset the flag
	ExitDRRSReconfigPhase()

	// [DRRS] Wait on WaitBuffer clear and state migration done
	WaitOnWaitBufferAndStateMigration()

	// [DRRS] Enter DRRS reconfiguration phase
	EnterDRRSReconfigPhase()

	IfDRRSInReconfig() bool

	IsUpstreamWaitBufferEmpty() bool
}

// Base struct for all operators
type OperatorBase struct {
	// Operator name and id
	Name string
	Id   uint16

	// Parallelism
	Parallelism int

	// Collector - downstream
	Collector collector.Collector

	// Supplier - upstream
	Supplier supplier.Supplier

	// Is the operator stateful
	IsStateful bool

	// Metric collector
	MetricCollector *metric.MetricCollector

	// [Logging] Store local worker id after task assignment for logging
	WorkerIdForLogging uint16
}

func NewOperatorBase(operatorName string) *OperatorBase {
	return &OperatorBase{
		Name:       operatorName,
		IsStateful: false,
		// Default parallelism is 1
		Parallelism: 1,
	}
}

/******************************************************************************
 					Base implementation of interface methods
******************************************************************************/

func (op *OperatorBase) GetId() uint16 {
	return op.Id
}

func (op *OperatorBase) SetId(id uint16) {
	op.Id = id
}

func (op *OperatorBase) GetName() string {
	return op.Name
}

func (op *OperatorBase) IsSource() bool {
	return op.Supplier == nil
}

func (op *OperatorBase) IsSink() bool {
	return op.Collector == nil
}

func (op *OperatorBase) IsStatefulOperator() bool {
	return op.IsStateful
}

func (op *OperatorBase) InitNewUpstream(connection net.Conn) {
	if op.Supplier == nil {
		return
	}
	op.Supplier.InitNewUpstream(connection)
}

func (op *OperatorBase) GetWorkUnit() (buffer.WorkUnit, bool, string, bool) {
	return op.Supplier.GetWorkUnit()
}

func (op *OperatorBase) GetParallelism() int {
	return op.Parallelism
}

func (op *OperatorBase) Pause() {
	op.Collector.PauseEmit()
}

func (op *OperatorBase) Resume() {
	op.Collector.ResumeEmit()
}

// This is a dummy implementation to be override by actual non-source operators
func (op *OperatorBase) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {
	log.Fatalln("No implementation for ProcessRecord()")
}

// This is a dummy implementation to be override by source operator
func (op *OperatorBase) RunSource() {
	log.Fatalln("No implementation for RunSource()")
}

// This is a dummy implementation to be override by stateful operators
func (op *OperatorBase) SetKeyedOutputStreamForUpstream(upstreams []Operator) {
	log.Fatalln(
		"SetKeyedOutputStreamForUpstream() not implemented for stateless operator",
	)
}

func (op *OperatorBase) SetCollector(collector collector.Collector) {
	op.Collector = collector
}

func (op *OperatorBase) GetCollector() collector.Collector {
	return op.Collector
}

func (op *OperatorBase) AddSubSupplier(subSupplier supplier.SubSupplier) {

	if op.IsSource() {
		log.Fatalln("Source operator cannot add SubSupplier")
	}
	op.Supplier.AddSubSupplier(subSupplier)
}

func (op *OperatorBase) AddSubSupplierToDownstream(downstream Operator) {

	if op.IsSink() {
		log.Fatalln("Sink operator cannot construct SubSupplier to downstream")
	}

	downstream.AddSubSupplier(op.Collector.ConstructSubSupplier())
}

func (op *OperatorBase) UpdateDownstreamRouting(
	downstreamsToAdd []*pb.DownstreamInfo,
	downstreamsToRemove []*pb.DownstreamInfo,
	newDownstreamKR *pb.SerializedKeyLookupTable,
) {

	if op.IsSink() {
		log.Fatalln("Sink cannot UpdateDownstreamRouting()")
	}

	op.Collector.UpdateRouting(
		downstreamsToAdd,
		downstreamsToRemove,
		newDownstreamKR,
	)
}

func (op *OperatorBase) InitLazyReconfig(
	expectedDrainBarriers int32,
	expectedInboundPeers int32,
	isShuttingDown bool,
) {
	op.Supplier.InitLazyReconfig(
		expectedDrainBarriers,
		expectedInboundPeers,
	)

	// Notify supplier to expect close of all upstream connections
	if isShuttingDown {
		op.Supplier.NotifyShuttingDown()
	}
}

func (op *OperatorBase) WaitAllInboundPeersToConnect() {
	op.Supplier.WaitAllInboundPeersToConnect()
}

func (op *OperatorBase) WaitTasksReconfigDone() {
	op.Supplier.WaitTasksReconfigDone()
}

func (op *OperatorBase) Terminate() {

	// Cannot kill a source task
	if op.IsSource() {
		log.Fatalln("Source task cannot be terminated")
	}

	// All upstream connections must have been drained clearly. Notify Supplier
	// to expect close of all upstream connections
	op.Supplier.NotifyShuttingDown()

	// Flush the output buffer and terminate downstream connections
	op.Collector.Terminate()

	// Gracefully shutdown the metric collector to stop reporting metrics
	// This is a non-blocking call that terminates metric reporting in the
	// background - need to wait if we will exit the process
	op.MetricCollector.Terminate()

	// TODO: gracefully shutdown other structures and resources as needed
}

// Task Watermark has progressed - trigger any time-related processing
// This is the default implementation for all operators. It can be overriden by
// operators that have specific time-related processing.
func (op *OperatorBase) ProcessProgressedWatermark(wm *buffer.Watermark) {

	// This is the default implementation. Do nothing
}

// Broadcast the progressed watermark to downstreams
func (op *OperatorBase) BroadcastWatermark(wm *buffer.Watermark) {

	if !op.IsSink() {
		op.Collector.BroadcastWatermark(wm, true)
	}
}

// Get the task watermark - this is an API for testing
func (op *OperatorBase) GetTaskWatermarkForTesting() *buffer.Watermark {

	if op.IsSource() {
		log.Fatalln("Source operator has no TaskWatermark")
	}

	return op.Supplier.GetTaskWatermark()
}

// Record the input metrics for the operator
func (operator *OperatorBase) UpdateInputMetrics(workUnit buffer.WorkUnit) {

	currTime := time.Now()

	// Record the time when the batch is pulled from the input buffer for
	// processing
	operator.MetricCollector.UpdateBatchPullTime(currTime)

	// Record the time spent in the input buffer
	operator.MetricCollector.UpdateInputBufferQueueTime(workUnit)

	// Record the input rate for the operator
	operator.MetricCollector.UpdateInputRate(workUnit)
}

// Record the processing latency for the operator
func (operator *OperatorBase) UpdateProcessingLatency() {

	// Record the time between two consecutive batch pulls
	operator.MetricCollector.UpdateProcessingLatency()
}

// Fetch local in-memory task metadata for affected key space (buckets)
func (operator *OperatorBase) FetchMetadataForMigration(
	affectedBuckets map[uint64]struct{},
) []byte {
	log.Fatalln("FetchMetadataForMigration() not implemented")
	return nil
}

// Insert the fetched in-memory task metadata to local task metadata structure
func (operator *OperatorBase) InsertMigratedMetadata([]byte) {
	log.Fatalln("InsertMigratedMetadata() not implemented")
}

// Extract records that need immediate forwarding without local processing
// The default implementation is only invoked by stateless operators. Stateful
// operators override this method for fast forwarding
func (op *OperatorBase) FastForward(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) (buffer.WorkUnit, bool) {

	log.Fatalln("FastForward() not supported for stateless operators")
	return workUnit, false
}

// This default implementation for stateless operators - fast forward not
// supported. Stateful operators override this method.
func (op *OperatorBase) PrepareLazyProtocol(
	peerList []*pb.DownstreamInfo,
	routingTable *keyby.KeyLookupTable,
) {
	log.Fatalln("PrepareLazyProtocol() not supported for stateless operators")
}

// This is the default implementation for all operators - stateless operators
// shouldn't construct fast forward metadata
func (op *OperatorBase) ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata {
	log.Fatalln(
		"ConstructFastForwardMetadata() not supported for stateless operators",
	)
	return nil
}

// This is the default implementation for all operators - stateless operators
// shouldn't send fast forward metadata
func (op *OperatorBase) SendFastForwardMetadata(
	metadata map[uint16]*buffer.FastForwardMetadata,
) {
	log.Fatalln(
		"SendFastForwardMetadata() not supported for stateless operators",
	)
}

// This is the default implementation for stateless operators - fast
// forward metadata processing not supported
func (op *OperatorBase) ProcessFastForwardMetadata(
	metadata *buffer.FastForwardMetadata,
) {
	log.Fatalln(
		"ProcessFastForwardMetadata() not supported for stateless operators",
	)
}

// This is the default implementation for stateless operators - broadcast
// watermark to peers not supported.
func (op *OperatorBase) BroadcastWatermarkToPeers(wm *buffer.Watermark) {
	log.Fatalln(
		"BroadcastWatermarkToPeers() not supported for stateless operators",
	)
}

// This default implementation for stateless operators - not supported. Stateful
// operators override this method.
func (op *OperatorBase) NotifyReconfigStart() {
	log.Fatalln("NotifyReconfigStart() not supported for stateless operators")
}

// This default implementation for stateless operators - not supported. Stateful
// operators override this method.
func (op *OperatorBase) WaitReconfigStart() {
	log.Fatalln("WaitReconfigStart() not supported for stateless operators")
}

// This default implementation for stateless operators - not supported. Stateful
// operators override this method.
func (op *OperatorBase) ExitTransitionPhase() {
	log.Fatalln("ExitTransitionPhase() not supported for stateless operators")
}

// This is the default Setup() method for all operators - invoked after task
// assignment request during runtime. Common setup steps should be included
// here. It can be overriden by outer operators if they have operator-specific
// setup steps. In that case, this Setup() method should be called within the
// outer override method.
// Possible hiarachy:
// CustomOperator.Setup()->OperatorBase.Setup()
// CustomOperator.Setup()->StatefulOperatorBase.Setup()->OperatorBase.Setup()
func (op *OperatorBase) Setup(para *utils.OperatorSetupParas) {

	// Init the metric collector for the operator
	op.MetricCollector = metric.NewMetricCollector(
		para.Config.MetricsInterval,
		para.Config.CoordinatorMetricCollectorAddr,
		op.IsSource(),
		op.IsSink(),
		op.GetName()+":"+strconv.Itoa(int(para.WorkerID)),
	)

	// Store a pointer to MetricCollector in setup parameters - needed by
	// Collector Setup()
	para.MetricCollector = op.MetricCollector

	// Setup the Supplier - upstream structures
	if op.Supplier != nil {
		op.Supplier.Setup(para)

		// [lazy protocol] Start a new task during lazy protocol reconfig
		if para.ExpectedDrainBarriers != nil &&
			para.ExpectedInboundPeers != nil {
			op.Supplier.InitLazyReconfig(
				*para.ExpectedDrainBarriers,
				*para.ExpectedInboundPeers,
			)
		}
	}

	// Setup the Collector - downstream structures
	if op.Collector != nil {
		op.Collector.Setup(para)
	}

	// [Logging] Store local worker id after task assignment for logging
	op.WorkerIdForLogging = para.WorkerID

	// Start metrics collector routine
	go op.MetricCollector.Run()
}

// [DRRS] Dummy implementation for stateless operators - do nothing and return
// the original work unit
func (op *OperatorBase) SplitBatchForDRRS(
	workUnit buffer.WorkUnit,
	isPeer bool,
	subSupplierName string,
) (buffer.WorkUnit, bool) {
	return workUnit, true
}

// [DRRS] Dummy implementation for stateless operators - do nothing
func (op *OperatorBase) ConsumeDRRSWaitBuffer(
	curTask Operator,
	inTransition bool,
) {
}

// [DRRS]
func (op *OperatorBase) EnterTerminationPhase() {
	log.Fatalln(
		"EnterTerminationPhase() not supported for stateless operators",
	)
}

// [DRRS]
func (op *OperatorBase) ExitDRRSReconfigPhase() {
	log.Fatalln(
		"ExitDRRSReconfigPhase() not supported for stateless operators",
	)
}

// [DRRS]
func (op *OperatorBase) WaitOnWaitBufferAndStateMigration() {
	log.Fatalln(
		"WaitOnWaitBufferAndStateMigration() not supported for stateless operators",
	)
}

// [DRRS] Return true if the watermark is inserted into wait buffer and cannot
// be processed
func (op *OperatorBase) BlockWatermarkDRRS(wm *buffer.Watermark) bool {
	return false
}

// [DRRS]
func (op *OperatorBase) EnterDRRSReconfigPhase() {
	log.Fatalln(
		"EnterDRRSReconfigPhase() not supported for stateless operators",
	)
}

func (op *OperatorBase) IfDRRSInReconfig() bool {
	return false
}

func (op *OperatorBase) IsUpstreamWaitBufferEmpty() bool {
	log.Fatalln(
		"IsUpstreamWaitBufferEmpty() not supported for stateless operators",
	)
	return true
}

///////////////////////////////////////////////////////////////////////////////
// 				    		Util functions/structs							 //
///////////////////////////////////////////////////////////////////////////////

func (op *OperatorBase) SetParallelism(para int) {
	op.Parallelism = para
}

func (op *OperatorBase) SetStatefulOperator() {
	op.IsStateful = true
}
