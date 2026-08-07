package dataflow

import (
	"log"
	"sync/atomic"
	"time"

	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/lazy"
	"github.com/CASP-Systems-BU/koala/internal/utils"
)

// Base struct for all stateful operators with 1 upstream operator
// IN: input tuple type
// K: key type

type StatefulOperatorBase1Upstream[IN tuple.Tuple, K comparable] struct {
	*StatefulOperatorBase[K]

	// Key assigner to extract the key from the input keyed stream.
	KeyAssigner *ka.KeyAssigner[IN, K]

	// [lazy protocol] Collector for fast forwarding records to peer workers.
	// It's only used if lazy protocol is enabled
	PeerCollector *lazy.PeerCollector[IN]
}

/******************************************************************************
					  Init at Job Graph construction time
******************************************************************************/

func NewStatefulOperatorBase1Upstream[IN tuple.Tuple, K comparable](
	operatorName string,
	keyAssigner *ka.KeyAssigner[IN, K],
	stateClientType stateClient.StateClientType,
) *StatefulOperatorBase1Upstream[IN, K] {

	return &StatefulOperatorBase1Upstream[IN, K]{
		StatefulOperatorBase: NewStatefulOperatorBase[K](
			operatorName,
			stateClientType,
		),
		KeyAssigner: keyAssigner,
	}
}

/******************************************************************************
						  Setup at task placement time
******************************************************************************/

// Override StatefulOperatorBase Setup() method
func (op *StatefulOperatorBase1Upstream[IN, K]) Setup(
	para *utils.OperatorSetupParas,
) {

	if op.KeyAssigner == nil {
		log.Fatalf(
			"Input stream KeyAssigner unset for stateful operator %s",
			op.Name,
		)
	}
	subSupplierNames := op.Supplier.GetSubSupplierNames()
	if len(subSupplierNames) != 1 {
		log.Fatalln("Expected exactly one SubSupplier for stateful operator")
	}
	op.StatefulOperatorBase.Setup(para)

	// [lazy protocol] Setup PeerCollector if lazy protocol is used
	if para.Config.ReconfigProtocol == "lazy" {
		op.PeerCollector = lazy.NewPeerCollector[IN](
			op.Name,
			subSupplierNames[0],
			para.Config,
		)
	}
}

/******************************************************************************
		    	  Implement Operator APIs for stateful operators
******************************************************************************/

// Set upstream operator's collector to keyby collector
func (op *StatefulOperatorBase1Upstream[IN, K]) SetKeyedOutputStreamForUpstream(
	upstreams []Operator,
) {

	// Only expect 1 upstream operator
	if len(upstreams) != 1 {
		log.Fatalln(
			"SetKeyedOutputStreamForUpstream(): Expected exactly one upstream operator",
		)
	}
	setKeybyCollectorForUpstream(op.KeyAssigner, upstreams[0])
}

// [lazy protocol] During reconfiguration phase, stateful operators need to
// check the input records if their ownership has been transferred to another
// worker. If so, the operator should forward them immediately based on the
// updated RoutingTable.
// Note: input subSupplierName is not used with single upstream since we only
// have 1 PeerCollector - no need to identify which PeerCollector to use
func (op *StatefulOperatorBase1Upstream[IN, K]) FastForward(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) (buffer.WorkUnit, bool) {

	return fastForwardToPeerCollector(
		workUnit,
		op.PeerCollector,
		op.KeyAssigner,
		op.LazyProtocolBase,
	)
}

// [lazy protocol] Upon receiving StartFastForward control message at Worker,
// prepare and initialize lazy-protocol related metadata
func (op *StatefulOperatorBase1Upstream[IN, K]) PrepareLazyProtocol(
	peerList []*pb.DownstreamInfo,
	routingTable *keyby.KeyLookupTable,
) {

	op.prepareLazyProtocol(routingTable)

	// Activate PeerCollector
	op.PeerCollector.Activate(peerList, op.MetricCollector)
}

// [lazy protocol] Send operator-specific metadata through PeerCollector as the
// first message during fast forward phase
func (op *StatefulOperatorBase1Upstream[IN, K]) SendFastForwardMetadata(
	metadata map[uint16]*buffer.FastForwardMetadata,
) {

	// Metadata is not needed for fast forward, return immediately
	if metadata == nil {
		return
	}
	op.PeerCollector.SendFastForwardMetadata(metadata)
}

// [lazy protocol] Broadcast current task watermark to all connected peers
func (op *StatefulOperatorBase1Upstream[IN, K]) BroadcastWatermarkToPeers(
	wm *buffer.Watermark,
) {

	if wm == nil {
		// Query current task watermark (maintained in Supplier)
		wm = op.Supplier.GetTaskWatermark()
		if wm.Unset() {
			// Do nothing if watermark is unset (this query may not need wm)
			return
		}
	}
	op.PeerCollector.BroadcastWatermark(wm)
}

// [lazy protocol] Fast forwarding phase is done, release related resources
func (op *StatefulOperatorBase1Upstream[IN, K]) ExitTransitionPhase() {

	op.exitTransitionPhase()

	// Destroy the PeerCollector and terminate related routines
	op.PeerCollector.Deactivate()
}

// [DRRS] Split records of an incoming batch into (i) processable batch, and
// (ii) records that are inserted into WaitBuffer. Returns:
//   - Processable batch
//   - If there are processable records in the batch (e.g. all records could
//     have been inserted into WaitBuffer)
func (op *StatefulOperatorBase1Upstream[IN, K]) SplitBatchForDRRS(
	workUnit buffer.WorkUnit,
	isPeer bool,
	subSupplierName string,
) (buffer.WorkUnit, bool) {

	return getProcessableBatch(
		workUnit,
		isPeer,
		subSupplierName,
		op.KeyAssigner,
		op.StateClient,
		op.Supplier,
		op.WaitBuffer,
	)
}

// [DRRS] Consume WaitBuffer
// TODO: code duplication with statefulOperatorBase2Upstream
func (op *StatefulOperatorBase1Upstream[IN, K]) ConsumeDRRSWaitBuffer(
	curTask Operator,
	inTransition bool,
) {

	if op.WaitBuffer.IsEmpty() {

		// Check if we are at termination phase, report termination status.
		// This is to gurantee all wait buffer is consumed and state migration
		// finish before termination
		if atomic.LoadInt32(&op.InTerminationPhase) == 1 {

			// Check if state migration has finished
			migrationTerminated := op.StateClient.CheckMigrationTerminationStatus()

			// Confirm if all peers are closed
			allPeersClosed := op.Supplier.IfAllInboundPeersClosed()

			// If termination condition met, reset necessary metadata and notify
			// the control plane to proceed with termination
			if migrationTerminated && allPeersClosed {

				atomic.StoreInt32(&op.InTerminationPhase, 0)

				// Reset state service bucket map
				op.StateClient.ResetDRRSMetadata()

				// If the supplier is still in peer-only mode, report error
				if op.Supplier.IsPeerOnlySupplier() {
					log.Fatalln("Supplier still in peer-only mode at termination")
				}

				// Reset DRRS reconfig flag
				op.ExitDRRSReconfigPhase()

				op.DRRSBlockOnWaitBufferAndStateMigration.Signal()
			}
		}
		return
	}

	if !op.IfDRRSInReconfig() {
		log.Fatalln(
			"ConsumeDRRSWaitBuffer() called when not in DRRS reconfiguration",
		)
	}

	// Do nothing if state migration has no progress
	migrationProgressed, migrationDone := op.StateClient.GetOverallMigrationStatus()
	if !migrationProgressed && !migrationDone {
		return
	}

	/**************************************************************************
					      First consume peer wait buffers
	**************************************************************************/
	start := time.Now()
	numProcessedPeerBatch := 0
	numProcessedUpstreamBatch := 0
	numProcessedWatermark := 0
	// Traverse peer buffer to identify processable work units
	for e := op.WaitBuffer.PeerBuffer.Front(); e != nil; {
		next := e.Next()
		drrsBatch, ok := e.Value.(*buffer.DRRSBatch[IN])
		if !ok {
			log.Fatalln("Failed to cast WorkUnit to DRRSBatch")
		}

		// Separate processable records from blocked records
		processableBatch, blockedBatch := processDRRSBatch(
			drrsBatch,
			op.StateClient,
		)

		// If still has blocked records, replace the existing batch in
		// WaitBuffer; else remove the batch from WaitBuffer
		if blockedBatch.NumRecords > 0 {
			e.Value = blockedBatch
		} else {
			op.WaitBuffer.PeerBuffer.Remove(e)
		}

		// Process the processable batch
		if processableBatch.GetNumRecords() > 0 {
			numProcessedPeerBatch++
			curTask.ProcessBatch(processableBatch, drrsBatch.SubSupplierName)
		}
		e = next
	}

	numLeftInPeerBuffer := op.WaitBuffer.PeerBuffer.Len()
	if migrationDone {
		// Peer buffer must be empty
		if numLeftInPeerBuffer != 0 {
			log.Fatalln("Peer buffer not empty after migration done")
		}
	}

	// If peer buffer is still not empty, we cannot proceed to upstream buffer
	numLeftInUpstreamBuffer := op.WaitBuffer.UpstreamBuffer.Len()
	if numLeftInPeerBuffer > 0 {
		if DEBUG {
			log.Printf(
				"\n  [DRRS WaitBuffer INFO]\n  Processed %d peer batches, %d upstream batches, %d watermark, time taken: %v\n  Status: PeerBuffer still not empty with %d left. %d elements blocked in upstream buffer.State migration done: %v\n",
				numProcessedPeerBatch,
				numProcessedUpstreamBatch,
				numProcessedWatermark,
				time.Since(start),
				numLeftInPeerBuffer,
				numLeftInUpstreamBuffer,
				migrationDone,
			)
		}
		return
	}

	// If peer buffer is empty, but not all peer channels are closed
	if !op.Supplier.IfAllInboundPeersClosed() {
		if DEBUG {
			log.Printf(
				"\n  [DRRS WaitBuffer INFO]\n  Processed %d peer batches, %d upstream batches, %d watermark, time taken: %v\n  Status: PeerBuffer empty but peers not closed. %d elements blocked in upstream buffer. State migration done: %v\n",
				numProcessedPeerBatch,
				numProcessedUpstreamBatch,
				numProcessedWatermark,
				time.Since(start),
				numLeftInUpstreamBuffer,
				migrationDone,
			)
		}
		return
	}

	/**************************************************************************
		     Proceed if peer wait buffer is empty and peers are closed
	**************************************************************************/

	// Now input from inbound peers are all consumed (confirm barrier aligned),
	// we can safely process work units in the upstream wait buffer
	hasBlockedRecordsBeforeWatermark := false
outer:
	for e := op.WaitBuffer.UpstreamBuffer.Front(); e != nil; {
		next := e.Next()

		// WorkUnit in upstream buffer could be either DRRSBatch or Watermark
		switch v := e.Value.(type) {
		case *buffer.DRRSBatch[IN]:

			// Separate processable records from blocked records
			processableBatch, blockedBatch := processDRRSBatch(v, op.StateClient)

			if blockedBatch.NumRecords > 0 {
				e.Value = blockedBatch
				hasBlockedRecordsBeforeWatermark = true
			} else {
				op.WaitBuffer.UpstreamBuffer.Remove(e)
				op.WaitBuffer.NumBatchesInUpstreamBuffer--
			}

			if processableBatch.GetNumRecords() > 0 {
				numProcessedUpstreamBatch++
				curTask.ProcessBatch(processableBatch, v.SubSupplierName)
			}
		case *buffer.Watermark:
			if hasBlockedRecordsBeforeWatermark {

				// Cannot process watermark if there are blocked records before
				// waterark, break the loop and return
				break outer
			} else {

				// This is the 1st work unit in the upstream buffer, process it
				op.WaitBuffer.UpstreamBuffer.Remove(e)
				op.WaitBuffer.NumWatermarkInUpstreamBuffer--
				numProcessedWatermark++

				if inTransition {
					curTask.BroadcastWatermarkToPeers(v)
				}
				// Process the watermark locally
				curTask.ProcessProgressedWatermark(v)
				curTask.BroadcastWatermark(v)
			}
		default:
			log.Fatalln("Unknown WorkUnit type in upstream WaitBuffer")
		}
		e = next
	}

	if migrationDone {
		// Upstream buffer must be empty if migration is done and all peer
		// channels are closed
		if op.WaitBuffer.UpstreamBuffer.Len() != 0 {
			log.Fatalln("Upstream buffer not empty after migration done")
		}
	}

	// Allow consuming upstream if upstream wait buffer is < threshold
	if !op.WaitBuffer.ShouldBlockUpstream() {
		op.Supplier.SetPeerOnlySupplier(false)
	}

	if DEBUG {
		log.Printf(
			"\n  [DRRS WaitBuffer INFO]\n  Processed %d peer batches, %d upstream batches, %d watermark, time taken: %v\n  Status: Peers closed and PeerBuffer empty with %d left in UpstreamBuffer. State migration done: %v\n",
			numProcessedPeerBatch,
			numProcessedUpstreamBatch,
			numProcessedWatermark,
			time.Since(start),
			op.WaitBuffer.UpstreamBuffer.Len(),
			migrationDone,
		)
	}
}
