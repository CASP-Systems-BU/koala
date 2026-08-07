package dataflow

import (
	"log"

	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/lazy"
	"github.com/CASP-Systems-BU/koala/internal/utils"
)

// Base struct for all stateful operators with 2 upstream operators
// IN1: input tuple type from upstream operator 1
// IN2: input tuple type from upstream operator 2
// K: key type

type StatefulOperatorBase2Upstream[IN1, IN2 tuple.Tuple, K comparable] struct {
	*StatefulOperatorBase[K]

	/****************************** Upstream 1 *******************************/
	// Name of the upstream operator 1
	UpstreamName1 string

	// Key assigner to extract the key from the input keyed stream.
	KeyAssigner1 *ka.KeyAssigner[IN1, K]

	// [lazy protocol] Collector to fast forward records from upstream 1 to peer
	// workers. It's only used if lazy protocol is enabled
	PeerCollector1 *lazy.PeerCollector[IN1]

	/****************************** Upstream 2 *******************************/
	// Name of the upstream operator 2
	UpstreamName2 string

	// Key assigner to extract the key from the input keyed stream.
	KeyAssigner2 *ka.KeyAssigner[IN2, K]

	// [lazy protocol] Collector to fast forward records from upstream 2 to peer
	// workers. It's only used if lazy protocol is enabled
	PeerCollector2 *lazy.PeerCollector[IN2]
}

/******************************************************************************
					  Init at Job Graph construction time
******************************************************************************/

// Enforce the type match for upstream 1 and upstream 2: their output type must
// match the respective key assigner input type
func NewStatefulOperatorBase2Upstream[IN1, IN2 tuple.Tuple, K comparable](
	operatorName string,
	upstream1 OperatorWith1OutputStream[IN1],
	keyAssigner1 *ka.KeyAssigner[IN1, K],
	upstream2 OperatorWith1OutputStream[IN2],
	keyAssigner2 *ka.KeyAssigner[IN2, K],
	stateClientType stateClient.StateClientType,
) *StatefulOperatorBase2Upstream[IN1, IN2, K] {

	return &StatefulOperatorBase2Upstream[IN1, IN2, K]{
		StatefulOperatorBase: NewStatefulOperatorBase[K](
			operatorName,
			stateClientType,
		),
		UpstreamName1: upstream1.GetName(),
		KeyAssigner1:  keyAssigner1,
		UpstreamName2: upstream2.GetName(),
		KeyAssigner2:  keyAssigner2,
	}
}

/******************************************************************************
						  Setup at task placement time
******************************************************************************/

// Override StatefulOperatorBase Setup() method
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) Setup(
	para *utils.OperatorSetupParas,
) {

	if op.KeyAssigner1 == nil || op.KeyAssigner2 == nil {
		log.Fatalf(
			"Input stream KeyAssigner(s) unset for stateful operator %s",
			op.Name,
		)
	}
	subSupplierNames := op.Supplier.GetSubSupplierNames()
	if len(subSupplierNames) != 2 {
		log.Fatalln("Expected exactly two SubSuppliers for stateful operator")
	}
	op.StatefulOperatorBase.Setup(para)

	// [lazy protocol] Setup PeerCollectors if lazy protocol is used
	if para.Config.ReconfigProtocol == "lazy" {
		op.PeerCollector1 = lazy.NewPeerCollector[IN1](
			op.Name,
			para.WorkerID,
			op.UpstreamName1,
			para.Config,
		)
		op.PeerCollector2 = lazy.NewPeerCollector[IN2](
			op.Name,
			para.WorkerID,
			op.UpstreamName2,
			para.Config,
		)
	}
}

/******************************************************************************
		    	  Implement Operator APIs for stateful operators
******************************************************************************/

// Set upstream operator's collector to keyby collector
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) SetKeyedOutputStreamForUpstream(
	upstreams []Operator,
) {

	if len(upstreams) != 2 {
		log.Fatalln(
			"SetKeyedOutputStreamForUpstream(): Expected exactly two upstream operators",
		)
	}
	upstream1 := upstreams[0]
	upstream2 := upstreams[1]

	/*
		Enforced by type inference of the following API definition, we must
		have upstream1 matches 1st group of struct fields (UpstreamName1,
		KeyAssigner1, PeerCollector1), and upstream2 matches 2nd group of struct
		fields (UpstreamName2, KeyAssigner2, PeerCollector2).

		func Add2To1Stream[MATCH_TYPE1, MATCH_TYPE2 tuple.Tuple](
			df *Dataflow,
			upstream1 OperatorWith1OutputStream[MATCH_TYPE1],
			upstream2 OperatorWith1OutputStream[MATCH_TYPE2],
			downstream OperatorWith2InputStream[MATCH_TYPE1, MATCH_TYPE2],
		)

		Therefore, by default, upstream1 should match KeyAssigner1, and
		upstream2 should match KeyAssigner2. However, if upstream1 and upstream2
		have the same output type, type validation is bypassed in Add2To1Stream
		API. upstream1 and upstream2 become interchangeable when user calls
		Add2To1Stream - this is problemetic if we always assume upstream1
		matches KeyAssigner1 and upstream2 matches KeyAssigner2. To guarantee
		correctness, we have to check the names of upstream1 and upstream2 for
		correct matching.
	*/

	if upstream1.GetName() == op.UpstreamName1 {
		setKeybyCollectorForUpstream(op.KeyAssigner1, upstream1)
	} else if upstream1.GetName() == op.UpstreamName2 {
		setKeybyCollectorForUpstream(op.KeyAssigner2, upstream1)
	} else {
		log.Fatalf(
			"Upstream operator %s is not expected by operator %s",
			upstream1.GetName(),
			op.Name,
		)
	}

	if upstream2.GetName() == op.UpstreamName2 {
		setKeybyCollectorForUpstream(op.KeyAssigner2, upstream2)
	} else if upstream2.GetName() == op.UpstreamName1 {
		setKeybyCollectorForUpstream(op.KeyAssigner1, upstream2)
	} else {
		log.Fatalf(
			"Upstream operator %s is not expected by operator %s",
			upstream2.GetName(),
			op.Name,
		)
	}
}

// [lazy protocol] During reconfiguration phase, stateful operators need to
// check the input records if their ownership has been transferred to another
// worker. If so, the operator should forward them immediately based on the
// updated RoutingTable.
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) FastForward(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) (buffer.WorkUnit, bool) {

	switch subSupplierName {
	case op.UpstreamName1:
		return fastForwardToPeerCollector(
			workUnit,
			op.PeerCollector1,
			op.KeyAssigner1,
			op.LazyProtocolBase,
		)
	case op.UpstreamName2:
		return fastForwardToPeerCollector(
			workUnit,
			op.PeerCollector2,
			op.KeyAssigner2,
			op.LazyProtocolBase,
		)
	default:
		log.Fatalf(
			"SubSupplierName %s is not expected at operator %s for fast forward\n",
			subSupplierName,
			op.Name,
		)
	}
	return nil, false
}

// [lazy protocol] Upon receiving StartFastForward control message at Worker,
// prepare and initialize lazy-protocol related metadata
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) PrepareLazyProtocol(
	peerList []*pb.DownstreamInfo,
	routingTable *keyby.PartitionTable,
) {

	op.prepareLazyProtocol(routingTable)

	// Activate PeerCollectors
	op.PeerCollector1.Activate(peerList, op.MetricCollector, 2)
	op.PeerCollector2.Activate(peerList, op.MetricCollector, 3)
}

// [lazy protocol] Send operator-specific metadata through PeerCollector as the
// first message during fast forward phase
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) SendFastForwardMetadata(
	metadata map[uint16]*buffer.FastForwardMetadata,
) {

	// Metadata is not needed for fast forward, return immediately
	if metadata == nil {
		return
	}

	// When we have multiple PeerCollectors, only 1 of them needs to send the
	// metadata. Explaination:
	// - Metadata is owned by the operator not a specific Upstream which is
	//   associated with a specific PeerCollector. We only need any one of the
	//   PeerCollectors to send the metadata and don't need to send multiple
	//   copies.
	// - Correctness is preserved: Metadata is still processed at the target
	//   task before watermark progress because watermark broadcast still
	//   happens after metadata at the selected PeerCollector. The other
	//   PeerCollector(s) that do not send metadata will not interfere with the
	//   watermark broadcast of the selected PeerCollector. Please see details
	//   in related issues: how watermark progress in lazy protocol.
	op.PeerCollector1.SendFastForwardMetadata(metadata)
}

// [lazy protocol] Broadcast current task watermark to all connected peers
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) BroadcastWatermarkToPeers(
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
	// It's safe to pass wm to multiple PeerCollectors because wm is immutable
	// after this point
	op.PeerCollector1.BroadcastWatermark(wm)
	op.PeerCollector2.BroadcastWatermark(wm)
}

// [Lazy protocol][Lazy-by-key] Construct and send KeyMaps over peer channels
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) ConstructAndSendKeyMaps(
	keyLookupTableV2 *keyby.KeyLookupTableV2,
) {

	// Send the constructed MigratingKeyMaps over PeerCollector
	serializedKeyMaps := keyLookupTableV2.SerializeMigratingKeyMaps(
		op.WorkerId,
		op.RoutingTable,
	)
	op.PeerCollector1.SendKeyMaps(serializedKeyMaps)
}

// [lazy protocol] Fast forwarding phase is done, release related resources
func (op *StatefulOperatorBase2Upstream[IN1, IN2, K]) ExitTransitionPhase() {

	op.exitTransitionPhase()

	// Destroy the PeerCollector and terminate related routines
	op.PeerCollector1.Deactivate()
	op.PeerCollector2.Deactivate()
}
