package dataflow

import (
	"log"

	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/lazy"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
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
			para.WorkerID,
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
	routingTable *keyby.PartitionTable,
) {

	op.prepareLazyProtocol(routingTable)

	// Activate PeerCollector
	op.PeerCollector.Activate(peerList, op.MetricCollector, 1)
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

// [Lazy protocol][Lazy-by-key] Construct and send KeyMaps over peer channels
func (op *StatefulOperatorBase1Upstream[IN, K]) ConstructAndSendKeyMaps(
	keyLookupTableV2 *keyby.KeyLookupTableV2,
) {

	// Send the constructed MigratingKeyMaps over PeerCollector
	serializedKeyMaps := keyLookupTableV2.SerializeMigratingKeyMaps(
		op.WorkerId,
		op.RoutingTable,
	)
	op.PeerCollector.SendKeyMaps(serializedKeyMaps)
}

// [lazy protocol] Fast forwarding phase is done, release related resources
func (op *StatefulOperatorBase1Upstream[IN, K]) ExitTransitionPhase() {

	op.exitTransitionPhase()

	// Destroy the PeerCollector and terminate related routines
	op.PeerCollector.Deactivate()
}
