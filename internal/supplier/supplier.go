package supplier

import (
	"log"
	"math"
	"net"
	"sync"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/syncflag"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/raw"
)

// Supplier defines and manages all upstream-related structures of the operator.

type Supplier interface {

	// Setup Suplier at task placement time
	Setup(*utils.OperatorSetupParas)

	// Add a new SubSupplier. This is called at JobGraph construction time
	AddSubSupplier(subSupplier SubSupplier)

	// Return one buffer element and which upstream operator (operator name) it
	// comes from.
	GetWorkUnit() (buffer.WorkUnit, bool, string, bool)

	// Init a new upstream when a new upstream connection is established
	InitNewUpstream(net.Conn)

	// Query current task watermark
	GetTaskWatermark() *buffer.Watermark

	// Get names of all SubSuppliers (upstream operators)
	GetSubSupplierNames() []string

	// Notify Supplier that the task is shutting down
	NotifyShuttingDown()

	// [lazy protocol] Init lazy reconfiguration metadata
	// 1. Expected number of in-flight barriers to reach alignment
	// 2. Expected number of inbound peer connections
	InitLazyReconfig(int32, int32)

	// [lazy protocol] Wait all expected inbound peer connections to connect
	WaitAllInboundPeersToConnect()

	// [lazy protocol] Wait all tasks to finish reconfiguration:
	// 1. All in-flight barriers from upstreams are received
	// 2. All inbound peer connections are closed
	WaitTasksReconfigDone()

	// [DRRS] Check if all inbound peer connections are closed
	IfAllInboundPeersClosed() bool

	// [DRRS] Set the flag to only consume peer input channels
	// There are 3 places we may switch between peer-only and all-channel mode:
	// 1. SplitBatchForDRRS(): push a non-processable batch into wait buffer
	//      may turn to peer-only mode
	// 2. BlockWatermarkDRRS(): push an un-progressed watermark into wait buffer
	//      may turn to peer-only mode
	// 3. ConsumeDRRSWaitBuffer(): consume from upstream wait buffer,
	//      may turn to all-channel mode
	// These 3 cases guarantee that upsrteam wait buffer will only accept more
	// input when it has extra space. It blocks consuming upstream if upstream
	// wait buffer is full. Note that a watermark from peer channel can still
	// be pushed into upstream wait buffer even in peer-only mode.
	SetPeerOnlySupplier(bool)

	IsPeerOnlySupplier() bool
}

type SupplierBase struct {
	sync.Mutex

	// Copy of name of the current operator for logging purposes
	OperatorName string

	// Pointer to the global config
	Config *configuration.Configuration

	// Mapping from upstream operator name to SubSupplier
	SubSuppliers map[string]SubSupplier

	// Expected number of SubSuppliers. This will be used to validate job graph
	ExpectedNumSubSuppliers int

	// Current watermark among all upstreams: min(WM1, WM2, ..., WMn)
	// We keep the TaskWatermark within Supplier since it's determined by
	// individual watermarks from all upstreams.
	// Watermark is unset (-1) if any upstream WM is unset
	// Watermark of each individual upstream is stored in Upstream.WM
	TaskWatermark *buffer.Watermark

	// Number of expected upstreams at initial task deployment phase: used to
	// block the progress of TaskWatermark until all upstreams are connected.
	// This considers upstream connections from all SubSuppliers.
	ExpectNumUpstream int

	// Flag that only flips once after all expected upstreams are connected at
	// task deployment phase
	TaskDeploymentPhasePassed bool

	// Pointer to the MetricCollector. This is used to update output related
	// metrics during runtime
	MetricCollector *metric.MetricCollector

	// [stop-and-restart] Track the data draining progress
	DrainBarrierCounter int

	// [Logging] Local worker id after task assignment
	WorkerIdForLogging uint16

	/**************************************************************************
						    Lazy protocol related fields
	**************************************************************************/

	// Track the number of received in-flight barriers
	InflightBarrierCounter int

	// Expected number of in-flight barriers to reach alignment. It is only set
	// when a reconfiguration is triggered. Possible values:
	// -1 (default): currently not waiting for in-flight barrier alignment
	// >0: waiting for alignment, expected number of in-flight barriers
	ExpectedInflightBarriers int

	// Track the number of active inbound peer connections
	InboundPeerCounter int

	// Expected number of inbound peer connections during lazy reconfiguration:
	// -1 (default): the task is not involved with inbound peer connections
	// >0: the expected number of inbound peer connections
	// -2: the task is waiting for all inbound peer connections to terminate
	ExpectedInboundPeers int

	// Sync channel to notify that all expected inbound peers are connected
	AllInboundPeersConnected *syncflag.SyncFlag

	// Sync channel to notify termination of reconfig phase. 2 conditions:
	// 1. All in-flight barriers from upstreams are received
	// 2. All inbound peer connections are closed
	ReconfigTerminated *syncflag.SyncFlag

	// If the supplier is shutting down
	IsShuttingDown bool

	// [DRRS] Flag to indicate if the supplier should only consume peers
	OnlyConsumePeers bool
}

// SupplierBase constructor at compile time
func NewSupplierBase(
	operatorName string,
	expectedNumSubSuppliers int,
) *SupplierBase {

	subSuppliers := make(map[string]SubSupplier)
	return &SupplierBase{
		OperatorName:             operatorName,
		ExpectedNumSubSuppliers:  expectedNumSubSuppliers,
		SubSuppliers:             subSuppliers,
		InflightBarrierCounter:   0,
		ExpectedInflightBarriers: -1,
		InboundPeerCounter:       0,
		ExpectedInboundPeers:     -1,
		AllInboundPeersConnected: syncflag.NewSyncFlag(),
		ReconfigTerminated:       syncflag.NewSyncFlag(),
	}
}

/******************************************************************************
 					Base implementation of interface methods
******************************************************************************/

// [DRRS] Set the flag to only consume peer input channels
func (s *SupplierBase) SetPeerOnlySupplier(onlyPeer bool) {
	s.Lock()
	defer s.Unlock()

	s.OnlyConsumePeers = onlyPeer
	for _, subSupplier := range s.SubSuppliers {
		subSupplier.SetPeerOnlySupplier(onlyPeer)
	}
}

// [DRRS] Check if the supplier is only consuming peer channels
func (s *SupplierBase) IsPeerOnlySupplier() bool {
	s.Lock()
	defer s.Unlock()
	return s.OnlyConsumePeers
}

// Setup at task placement time
func (s *SupplierBase) Setup(para *utils.OperatorSetupParas) {

	s.Config = para.Config
	s.TaskWatermark = buffer.NewWatermark(-1)
	s.ExpectNumUpstream = para.ExpectNumUpstream
	s.MetricCollector = para.MetricCollector

	if len(s.SubSuppliers) == 0 {
		log.Fatalln("SubSuppliers cannot be empty.")
	}
	if len(s.SubSuppliers) != s.ExpectedNumSubSuppliers {
		log.Fatalf(
			"Supplier %s: expected %d SubSuppliers, but got %d",
			s.OperatorName,
			s.ExpectedNumSubSuppliers,
			len(s.SubSuppliers),
		)
	}

	// Logging
	s.WorkerIdForLogging = para.WorkerID

	// Setup all SubSuppliers
	for _, subSupplier := range s.SubSuppliers {
		subSupplier.Setup()
	}
}

// Add a new SubSupplier at JobGraph construction phase
func (s *SupplierBase) AddSubSupplier(subSupplier SubSupplier) {

	if len(s.SubSuppliers) >= s.ExpectedNumSubSuppliers {
		log.Fatalf(
			"Supplier %s: cannot add more SubSuppliers than expected (%d)",
			s.OperatorName,
			s.ExpectedNumSubSuppliers,
		)
	}

	if _, ok := s.SubSuppliers[subSupplier.GetOperatorName()]; ok {
		log.Fatalf(
			"Supplier %s: SubSupplier %s already exists",
			s.OperatorName,
			subSupplier.GetOperatorName(),
		)
	}

	s.SubSuppliers[subSupplier.GetOperatorName()] = subSupplier
}

// Triggered by upstream when connection is established -> add a new upstream
// into the supplier
func (s *SupplierBase) InitNewUpstream(conn net.Conn) {

	// Identify the upstream operator name and connection type
	upstreamName, isPeer := getConnectionNameAndType(conn)
	subSupplier, ok := s.SubSuppliers[upstreamName]
	if !ok {
		log.Fatalf(
			"New upstream connection from %s is not expected at %s",
			upstreamName,
			s.OperatorName,
		)
	}

	s.Lock()
	subSupplier.AddUpstream(conn, s.Config, s.MetricCollector, isPeer)
	totalNumUpstreams := s.getNumUpstreams()
	if totalNumUpstreams == s.ExpectNumUpstream {
		s.TaskDeploymentPhasePassed = true
	}

	// If this is a peer connection, increment the inbound peer counter and
	// check if all inbound peers are connected. This could only be true for
	// lazy protocol
	if isPeer {

		if s.ExpectedInboundPeers <= 0 {
			log.Fatalf(
				"InitNewUpstream: unexpected inbound peer connection. Current expected inbound peers: %d\n",
				s.ExpectedInboundPeers,
			)
		}
		s.InboundPeerCounter += 1
		if s.InboundPeerCounter == s.ExpectedInboundPeers {

			// Mark the supplier status to waiting for terminations of all peers
			s.ExpectedInboundPeers = -2

			// All expected inbound peer connections are established, flip the
			// sync flag to unblock waiting routines (lazy protocol step 3,
			// coordinator will wait for all peers to be connected before next)
			s.AllInboundPeersConnected.Signal()
		}
	}
	s.Unlock()

	log.Printf(
		"[%s Supplier] New upstream added from operator %s. Current number of upstreams: %d\n",
		s.OperatorName,
		upstreamName,
		totalNumUpstreams,
	)
}

// Get current TaskWatermark
func (s *SupplierBase) GetTaskWatermark() *buffer.Watermark {
	s.Lock()
	defer s.Unlock()

	// Get current TaskWatermark
	return buffer.CopyWM(s.TaskWatermark)
}

func (s *SupplierBase) GetSubSupplierNames() []string {

	subSupplierNames := make([]string, 0, len(s.SubSuppliers))
	for name := range s.SubSuppliers {
		subSupplierNames = append(subSupplierNames, name)
	}
	return subSupplierNames
}

func (s *SupplierBase) NotifyShuttingDown() {
	s.Lock()
	defer s.Unlock()

	// Set shutting down flag in Subpplier
	s.IsShuttingDown = true

	// Set shutting down flag in SubSuppliers
	for _, subSupplier := range s.SubSuppliers {
		subSupplier.NotifyShuttingDown()
	}
}

func (s *SupplierBase) InitLazyReconfig(
	expectedDrainBarriers int32,
	expectedInboundPeers int32,
) {
	s.Lock()
	defer s.Unlock()

	// Validate the initial status of lazy reconfiguration metadata
	if s.ExpectedInflightBarriers != -1 || s.ExpectedInboundPeers != -1 {
		log.Fatalln(
			"InitLazyReconfig: lazy reconfiguration metadata not clean",
		)
	}

	if expectedDrainBarriers <= 0 {
		log.Fatalln(
			"InitLazyReconfig: expectedDrainBarriers should be > 0",
		)
	}
	s.ExpectedInflightBarriers = int(expectedDrainBarriers)
	s.ExpectedInboundPeers = int(expectedInboundPeers)
}

func (s *SupplierBase) WaitAllInboundPeersToConnect() {

	// Control plane wait here until all expected inbound peer connections are
	// established
	s.AllInboundPeersConnected.Wait()

	// Reset the sync flag for future reconfigurations
	s.AllInboundPeersConnected.Reset()
}

func (s *SupplierBase) WaitTasksReconfigDone() {

	// 1. Wait until all in-flight barriers from upstreams are received
	// 2. Wait until all inbound peer connections are closed
	s.ReconfigTerminated.Wait()

	// Reset the sync flag for future reconfigurations
	s.ReconfigTerminated.Reset()
}

// [DRRS] Check if all peers are closed
func (s *SupplierBase) IfAllInboundPeersClosed() bool {
	s.Lock()
	defer s.Unlock()
	return s.ExpectedInboundPeers == -1
}

/******************************************************************************
 							 Supplier common utils
******************************************************************************/

// Get the upstream operator name and connection type of the newly established
// upstream connection. Return:
// 1. upstream operator name
// 2. if the connection is a peer connection
func getConnectionNameAndType(conn net.Conn) (string, bool) {

	buf := make([]byte, 8)
	err := network.ReadAll(conn, buf, 8)
	if err != nil {
		log.Fatalf("Error tcp reading: %v", err)
	}
	length, _, err := raw.UnmarshalUint64(buf)
	if err != nil {
		log.Fatalf("Error decoding operatorID length: %v", err)
	}

	buf = make([]byte, length)
	err = network.ReadAll(conn, buf, length)
	if err != nil {
		log.Fatalf("Error tcp reading: %v", err)
	}
	upstreamName, _, err := ord.UnmarshalString(nil, buf)
	if err != nil {
		log.Fatalf("Error decoding operatorID: %v", err)
	}

	// Read connection type
	buf = make([]byte, 1)
	err = network.ReadAll(conn, buf, 1)
	if err != nil {
		log.Fatalf("Error tcp reading connection type: %v", err)
	}
	isPeerChannel, _, err := raw.UnmarshalUint8(buf)
	if err != nil {
		log.Fatalf("Error decoding connection type: %v", err)
	}

	if isPeerChannel == 1 {
		return upstreamName, true
	}
	return upstreamName, false
}

// Get total number of upstreams across all SubSuppliers
func (s *SupplierBase) getNumUpstreams() int {

	total := 0
	for _, subSupplier := range s.SubSuppliers {
		total += subSupplier.GetNumUpstreams()
	}
	return total
}

// Helper to process the next workunit. Return true if this workunit needs to be
// returned back to the task for processing. Return false if the processing ends
// within the supplier e.g. un-progressed watermark, unaligned drainBarrier,
// unaligned inflightBarrier, etc.
func (s *SupplierBase) preprocessWorkUnit(
	workUnit buffer.WorkUnit,
	subSupplierame string,
) (buffer.WorkUnit, bool) {

	switch workUnit.GetType() {

	case buffer.BatchWorkUnit:
		return workUnit, true

	case buffer.DrainBarrierWorkUnit:
		// [stop-and-restart] Process the DrainBarrier. Return true if all
		// DrainBarriers from upstreams are received
		if s.handleDrainBarrier() {
			return workUnit, true
		} else {
			return nil, false
		}

	case buffer.WatermarkWorkUnit:
		// Process a received Watermark. Return true if the TaskWatermark
		// pogresses
		if progressedWM, ok := s.handleWatermark(workUnit, subSupplierame); ok {
			return progressedWM, true
		} else {
			return nil, false
		}

	case buffer.InflightBarrierWorkUnit:
		// [lazy protocol] All inflight records after reconfiguration at this
		// upstream have all arrived. Check if all barriers from all upstreams
		// are received
		if s.handleInflightBarrier(subSupplierame) {
			return workUnit, true
		} else {
			return nil, false
		}

	case buffer.TerminationSignalWorkUnit:
		// The corresponding upstream is terminated
		if !s.TaskDeploymentPhasePassed {
			// We don't allow closing an existing upstream connection while the
			// supplier is still at TaskDeploymentPhase (waiting for all
			// expected upstreams to be connected) - e.g. peer upstream
			// connection terminated before actual upstream connection is
			// established.
			// TODO: completely avoid this case though it should be very rare
			log.Fatalln(
				"RemoveUpstream: shouldn't remove an upstream while TaskDeploymentPhase is not passed",
			)
		}

		// Release resources and remove it from the Upstreams list
		subSupplier := s.SubSuppliers[subSupplierame]
		subSupplier.RemoveUpstream()

		// Check the type of the closing connection. If it's a peer connection,
		// check if all inbound peer connections are closed
		terminationWorkunit := workUnit.(*buffer.TerminationSignal)
		if terminationWorkunit.IsPeer() {

			if s.ExpectedInboundPeers != -2 {
				log.Fatalf(
					"RemoveUpstream: unexpected inbound peer termination. Current expected inbound peers: %d\n",
					s.ExpectedInboundPeers,
				)
			}

			s.InboundPeerCounter -= 1
			if s.InboundPeerCounter == 0 {

				log.Printf(
					"[Worker %d Inbound Peer INFO] All inbound peers are closed",
					s.WorkerIdForLogging,
				)

				// All inbound peer connections are closed, reset
				s.ExpectedInboundPeers = -1

				// Mark termination of reconfiguration if in-flight barriers
				// are also aligned
				if s.ExpectedInflightBarriers == -1 {
					s.ReconfigTerminated.Signal()
				}
			}
			// Whenever an inbound peer is removed, check the watermark of
			// remaining upstreams, there could be a case remaining upstreams
			// can already trigger to fire the next watermark. This step is not
			// necessary for correctness, just to not delaying the watermark
			// progress in case a delayed/slow inbound peer channel
			return s.checkTaskWatermarkProgress()
		}
		return nil, false

	case buffer.CheckpointBarrierWorkUnit:
		log.Fatalln(
			"RoundRobinSupplier: CheckpointBarrierWorkUnit not implemented",
		)

	case buffer.FastForwardMetadataWorkUnit:
		return workUnit, true

	default:
		log.Fatalf("RoundRobinSupplier: Unknown WorkUnit type")
	}

	// Won't reach here. We explicitly handled each case above
	return nil, false
}

// [stop-and-restart] Receive a DrainBarrier and check if all upstreams have
// received it
func (s *SupplierBase) handleDrainBarrier() bool {

	s.DrainBarrierCounter += 1
	totalNumUpstreams := s.getNumUpstreams()
	if s.DrainBarrierCounter == totalNumUpstreams {
		// Reset it to 0
		s.DrainBarrierCounter = 0
		return true
	} else if s.DrainBarrierCounter > totalNumUpstreams {
		log.Fatalf("DrainBarrierCounter > Total number of upstreams\n")
	}

	return false
}

// [lazy protocol] Process InflightBarrier within Supplier
func (s *SupplierBase) handleInflightBarrier(subSupplierame string) bool {

	// Validate Supplier status
	if s.ExpectedInflightBarriers <= 0 {
		log.Fatalln(
			"[lazy protocol]: unexpected InflightBarrier received",
		)
	}

	// [DRRS] Set the in-flight barrier status for this upstream input channel
	// for in-flight barrier alignment, we block the received channel until
	// all in-flight barriers are received - this is to avoid fullfill wait
	// buffer (block new data from upstream) before all in-flight barriers are
	// received (deadlock may happen if upstream is blocked on sending)
	subSupplier := s.SubSuppliers[subSupplierame]
	subSupplier.SetInflightBarrierReceived()

	s.InflightBarrierCounter += 1
	if s.InflightBarrierCounter == s.ExpectedInflightBarriers {

		// In-flight barriers have reached alignment, reset related fields
		s.InflightBarrierCounter = 0
		s.ExpectedInflightBarriers = -1

		// Reset in-flight barrier status for all upstream input channels
		for _, subSupplier := range s.SubSuppliers {
			subSupplier.ResetInflightBarrier()
		}

		// Mark termination of reconfiguration on this task if inbound peers
		// tracking are also inactive
		if s.ExpectedInboundPeers == -1 {
			s.ReconfigTerminated.Signal()
		}

		// Termination of peer collector will be handled outside in main loop
		return true
	}
	return false
}

// Process Watermark within Supplier. Return true if TaskWatermark progresses
// To determine if the TaskWatermark progresses, we need to traverse watermarks
// of all upstreams to find the min(WM1, WM2, ..., WMn). There 2 special cases:
//  1. If any upstream WM is unset (upstream connected but no wm received yet),
//     the TaskWatermark should not progress.
//  2. We pass expectedNumUpstreams during task deployment. The TaskWatermark
//     should not progress until all upstreams are connected. Processing can
//     start with at least one upstream connected.
func (s *SupplierBase) handleWatermark(
	wmWorkUnit buffer.WorkUnit,
	subSupplierame string,
) (*buffer.Watermark, bool) {

	wm, ok := wmWorkUnit.(*buffer.Watermark)
	if !ok {
		log.Fatalf("Supplier handleWatermark(): intput should be wm type\n")
	}

	if s.Config.WatermarkDebug {
		log.Printf(
			"[Watermark info][SubSupplier %s] Received watermark %d\n",
			subSupplierame,
			wm.Timestamp,
		)
	}

	// Update the watermark for the input channel this wm comes from
	subSupplier := s.SubSuppliers[subSupplierame]
	subSupplier.UpdateWatermark(wm)

	// Check if all expected upstreams are connected during task deployment
	// Do not allow TaskWatermark progress until all upstreams are connected
	if !s.TaskDeploymentPhasePassed {
		return nil, false
	}

	return s.checkTaskWatermarkProgress()
}

// Traverse watermarks of all upstreams of all SubSuppliers to check if the
// TaskWatermark progresses. If it does, update the TaskWatermark and return it
// to the task, otherwise return false. If any upstream WM is unset, it means we
// haven't received any watermark from that upstream yet, also return false.
func (s *SupplierBase) checkTaskWatermarkProgress() (*buffer.Watermark, bool) {

	curMinWM := buffer.NewWatermark(math.MaxInt64)

	for _, subSupplier := range s.SubSuppliers {
		subSupplierWM, ok := subSupplier.GetSubSupplierWM()
		if !ok {
			// This SubSupplier's watmermark is unset
			return nil, false
		}

		curMinWM.SetMin(subSupplierWM)
	}

	// Progress TaskWatermark if curMinWM is larger
	if curMinWM.LargerThan(s.TaskWatermark) {
		s.TaskWatermark.Update(curMinWM)

		if s.Config.WatermarkDebug {
			log.Printf(
				"[Watermark info] Watermark progressed to %d\n",
				s.TaskWatermark.Timestamp,
			)
		}

		return curMinWM, true
	} else {
		return nil, false
	}
}
