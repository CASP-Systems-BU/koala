package dataflow

import (
	"container/list"
	"log"
	"sync/atomic"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/lazy"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/syncflag"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
)

// This is the base struct for all stateful operators. On top of it, we have
// another layer of base struct for different number of upstreams.
// Example stateful operator embedding hierarchy:
// 1. StatefulMapper ->
// 2. StatefulOperatorBase1Upstream (stateful operator with 1 upstream) ->
// 3. StatefulOperatorBase ->
// 4. OperatorBase

var DEBUG = false

type StatefulOperatorBase[K comparable] struct {
	*OperatorBase

	// [lazy protocol] Optional: Metadata for lazy protocol fast forwarding
	*lazy.LazyProtocolBase[K]

	// Client to communicate with StateService
	StateClient *stateClient.StateClient[K]

	// [DRRS] WaitBuffer for records that cannot be processed yet
	WaitBuffer *WaitBuffer

	// [DRRS] If at termination phase
	InTerminationPhase int32

	// [DRRS] Termination signal channel
	DRRSBlockOnWaitBufferAndStateMigration *syncflag.SyncFlag

	// [DRRS] Flag to indicate if DRRS reconfiguration is in progress.
	// 0: Not in reconfiguration
	// 1: In reconfiguration
	InDRRSReconfig int32
}

/******************************************************************************
					  Init at Job Graph construction time
******************************************************************************/

func NewStatefulOperatorBase[K comparable](
	operatorName string,
	stateClientType stateClient.StateClientType,
) *StatefulOperatorBase[K] {

	opBase := NewOperatorBase(operatorName)
	opBase.SetStatefulOperator()

	return &StatefulOperatorBase[K]{
		OperatorBase: opBase,
		StateClient:  stateClient.NewStateClient[K](stateClientType),
	}
}

/******************************************************************************
						  Setup at task placement time
******************************************************************************/

// Override OperatorBase Setup() method. It calls the base Setup() method from
// OperatorBase
func (op *StatefulOperatorBase[K]) Setup(
	para *utils.OperatorSetupParas,
) {

	op.OperatorBase.Setup(para)
	op.StateClient.Setup(
		op.Id,
		para.StateService,
		para.Config,
	)

	// [lazy protocol] Init LazyProtocolBase if lazy protocol is used
	if para.Config.ReconfigProtocol == "lazy" {
		op.LazyProtocolBase = lazy.NewLazyProtocolBase[K](para.WorkerID)

		if para.Config.LazyProtocolVersion == "drrs" {
			// Init WaitBuffer for DRRS
			op.WaitBuffer = NewWaitBuffer(para.Config.WaitBufferSize)

			// Init termination signal
			op.DRRSBlockOnWaitBufferAndStateMigration = syncflag.NewSyncFlag()
		}
	}
}

/******************************************************************************
		    	  Implement Operator APIs for stateful operators
Note: If the API implementation involves input type or PeerCollector, the API
will be implemented in corresponding StatefulOperatorBaseXUpstream struct
******************************************************************************/

func (op *StatefulOperatorBase[K]) EnterDRRSReconfigPhase() {
	atomic.StoreInt32(&op.InDRRSReconfig, 1)
}

func (op *StatefulOperatorBase[K]) ExitDRRSReconfigPhase() {
	atomic.StoreInt32(&op.InDRRSReconfig, 0)
}

func (op *StatefulOperatorBase[K]) IfDRRSInReconfig() bool {
	return atomic.LoadInt32(&op.InDRRSReconfig) == 1
}

// [DRRS] Notify the operator to start checking termination condition
func (op *StatefulOperatorBase[K]) EnterTerminationPhase() {
	atomic.StoreInt32(&op.InTerminationPhase, 1)
}

// [DRRS] Wait on WaitBuffer clear and state migration done
func (op *StatefulOperatorBase[K]) WaitOnWaitBufferAndStateMigration() {

	op.DRRSBlockOnWaitBufferAndStateMigration.Wait()

	// State migration has done and WaitBuffer is cleared, reset metadata
	op.DRRSBlockOnWaitBufferAndStateMigration.Reset()
}

// [DRRS] Check if a watermark is inserted into wait buffer and cannot be
// processed immediately. Return true if it is blocked
func (op *StatefulOperatorBase[K]) BlockWatermarkDRRS(
	wm *buffer.Watermark,
) bool {

	// If wait buffer is empty, it can be processed
	if op.WaitBuffer.IsEmpty() {
		return false
	}

	// Insert the watermark into wait buffer. Note that watermark is always
	// inserted into upstream wait buffer and will not trigger a supplier
	// switch to peer-only mode
	op.WaitBuffer.EnqueueWatermark(wm)
	return true
}

// Fetch local in-memory task metadata for affected key space (buckets)
func (op *StatefulOperatorBase[K]) FetchMetadataForMigration(
	affectedBuckets map[uint64]struct{},
) []byte {
	return nil
}

// Implement the interface method ConstructFastForwardMetadata() - override the
// base implementation. Stateful operators that needs to pass metadata over
// fast forward should override this method, otherwise it returns nil by default
func (op *StatefulOperatorBase[K]) ConstructFastForwardMetadata() map[uint16]*buffer.FastForwardMetadata {
	return nil
}

// Override the interface method ProcessFastForwardMetadata() - this method has
// to be implemented by specific stateful operators that need fast forward
// metadata
func (op *StatefulOperatorBase[K]) ProcessFastForwardMetadata(
	metadata *buffer.FastForwardMetadata,
) {
	log.Fatalln("ProcessFastForwardMetadata() not implemented")
}

// Implement the interface method NotifyReconfigStart() - override the base
// implementation. Only stateful operator supports this method
func (op *StatefulOperatorBase[K]) NotifyReconfigStart() {
	op.ReconfigStartChan <- struct{}{}
}

// Implement the interface method WaitReconfigStart() - override the base
// implementation. Only stateful operator supports this method
func (op *StatefulOperatorBase[K]) WaitReconfigStart() {
	<-op.ReconfigStartChan
}

func (op *StatefulOperatorBase[K]) IsUpstreamWaitBufferEmpty() bool {
	return op.WaitBuffer.UpstreamWaitBufferNoBatch()
}

/******************************************************************************
		   			   Utils for all StatefulOperatorBase
******************************************************************************/

// Set upstream operator's collector to keyby collector
func setKeybyCollectorForUpstream[IN tuple.Tuple, K comparable](
	downstreamKeyAssigner *ka.KeyAssigner[IN, K],
	upstream Operator,
) {

	// Use generic type of downstream stateful operator to init the keyby
	// collector of the upstream operator
	keyByCollector := collector.NewKeybyCollector(
		downstreamKeyAssigner,
		upstream.GetName(),
	)

	// If the upstream operator is source, we could have added watermark
	// generator to its collector. If we want to reset its collector to keyby
	// collector, we need to copy its possible watermark generator as well.
	if _, ok := upstream.(SourceOperator[IN]); ok {
		collector := upstream.GetCollector()
		// Timesamp assigner and watermark generator are set together
		// They are either all set or all not set. Check if timestamp assigner
		// is set is enough
		tsAssigner, ok := collector.GetTsAssigner()
		if ok {
			log.Println(
				"[TimeAssigner INFO] Source inits KeyByCollector with TimestampAssigner and WatermarkGenerator",
			)
			keyByCollector.SetTsAssignerAndWatermarkGenerator(
				tsAssigner,
				collector.GetWatermarkGenerator(),
			)
		}
	}
	upstream.SetCollector(keyByCollector)
}

// [lazy protocol] Fast forward records in input batch to PeerCollector based on
// RoutingTable. Return a batch that contains remaining records whose ownership
// is still local. If all records are forwarded, return nil.
func fastForwardToPeerCollector[IN tuple.Tuple, K comparable](
	workUnit buffer.WorkUnit,
	peerCollector *lazy.PeerCollector[IN],
	keyAssigner *ka.KeyAssigner[IN, K],
	// Note: we cannot define fastForwardToPeerCollector() as a method of
	// StatefulOperatorBase because Go doesn't support generic types in struct
	// methods.
	metadata *lazy.LazyProtocolBase[K],
) (buffer.WorkUnit, bool) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to FastForward() cannot be converted to Batch[IN]")
	}

	// Allocate a new batch to store the remaining records for local processing
	remainingBatch := buffer.AllocateOutputBatch[IN]()

	// Traverse the batch and check if need to fast forward
	for _, record := range batch.Records[0:batch.TotalNumRecords] {

		// Query routing table for the updated owner worker of the key
		key := keyAssigner.GetKey(record)
		ownerWorkerId := metadata.GetOwnerWorkerUponReconfig(key)

		if ownerWorkerId == metadata.WorkerId {

			// If the key is owned by the local worker, add the record to the
			// remaining batch for local processing. Size of the remainingBatch
			// is no longer needed: set a invalid dummy value -1
			remainingBatch.AddRecord(record, -1)

		} else {

			// If the key is owned by another worker, fast forward the record
			// Copy the input record for output since the input memory cannot be
			// safely accessed by collectors

			// Copy the record
			newRecord := record.Copy()

			// Call PeerCollector to forward the record
			// TODO: now subSupplierName parameter is not used. If we have
			// multiple SubSuppliers, we will have multiple PeerCollectors
			// and the subSupplierName will be used to identify which
			// PeerCollector to forward the record
			peerCollector.Emit(newRecord, ownerWorkerId)
		}
	}

	// Return the remaining records for local processing. If the remainingBatch
	// is empty, return true to indicate that all records have been forwarded
	if remainingBatch.TotalNumRecords > 0 {
		return remainingBatch, false
	} else {
		return nil, true
	}
}

// [lazy protocol] Initialize lazy-protocol related metadata
func (op *StatefulOperatorBase[K]) prepareLazyProtocol(
	routingTable *keyby.KeyLookupTable,
) {

	// Set the routing table for runtime fast forwarding
	op.RoutingTable = routingTable
}

// [lazy protocol] Release resources after fast forward phase ends
func (op *StatefulOperatorBase[K]) exitTransitionPhase() {
	op.RoutingTable = nil
}

/******************************************************************************
		   			   			  DRRS utils
******************************************************************************/

// [DRRS] Helper to examine an incoming batch and split records into
// (i) processable batch, and (ii) records that are inserted into WaitBuffer
// Returns:
//   - Processable batch
//   - If there are processable records in the batch (e.g. all records could
//     have been inserted into WaitBuffer)
func getProcessableBatch[IN tuple.Tuple, K comparable](
	workUnit buffer.WorkUnit,
	isFromPeer bool,
	subSupplierName string,
	keyAssigner *ka.KeyAssigner[IN, K],
	stateClient *stateClient.StateClient[K],
	supplier supplier.Supplier,
	waitBuffer *WaitBuffer,
) (buffer.WorkUnit, bool) {

	inputBatch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to FastForward() cannot be converted to Batch[IN]")
	}
	processableBatch := buffer.AllocateOutputBatch[IN]()
	waitBufferBatch := buffer.NewDRRSBatch[IN](isFromPeer, subSupplierName)

	// Check the state migration status of all records in the input batch
	// Each key (bucket) has 3 possible statuses:
	//   -1: not affected by reconfig
	//   0: affected but not yet migrated
	//   1: affected and migrated
	keys := make([]K, 0, inputBatch.TotalNumRecords)
	for _, record := range inputBatch.Records[0:inputBatch.TotalNumRecords] {
		keys = append(keys, keyAssigner.GetKey(record))
	}
	buckets, keyStatus := stateClient.GetKeysMigrationStatus(keys)

	if isFromPeer {

		// This batch is from inbound peer channels, it must be affected by the
		// reconfig. We only check its state migration status to decide
		// processability
		for i, record := range inputBatch.Records[0:inputBatch.TotalNumRecords] {
			switch keyStatus[i] {
			case -1:
				log.Fatalln(
					"A record from peer channel must be affected by reconfig",
				)
			case 0:
				// This key is affected but not yet migrated -> WaitBuffer
				// Deep copy the record to push into wait buffer. Input record
				// memory is not safe to use after its original batch is
				// processed in main loop
				newRec := record.Copy().(IN)
				waitBufferBatch.AddRecord(newRec, buckets[i])
			case 1:
				// This key is affected and has been successfully migrated
				processableBatch.AddRecord(record, -1)
			default:
				log.Fatalln("Unknown key migration status:", keyStatus[i])
			}
		}
	} else {

		// This batch is from upstream operator, we should check (i) state
		// migration status (if the key is even affected by reconfig), (ii) if
		// peer channels have been closed
		allInboundPeersClosed := supplier.IfAllInboundPeersClosed()
		for i, record := range inputBatch.Records[0:inputBatch.TotalNumRecords] {
			switch keyStatus[i] {
			case -1:
				// This key is not affected by reconfig, it's processable. Note
				// that in non-reconfig phase, all keys fall into this category
				processableBatch.AddRecord(record, -1)
			case 0:
				// This key is affected but not yet migrated -> WaitBuffer
				newRec := record.Copy().(IN)
				waitBufferBatch.AddRecord(newRec, buckets[i])
			case 1:
				// This key is affected and has been successfully migrated, we
				// should also check if all peer channels are closed (confirm
				// barriers are aligned)
				if allInboundPeersClosed {
					processableBatch.AddRecord(record, -1)
				} else {
					// Peer channels are not closed yet -> WaitBuffer
					newRec := record.Copy().(IN)
					waitBufferBatch.AddRecord(newRec, buckets[i])
				}
			default:
				log.Fatalln("Unknown key migration status:", keyStatus[i])
			}
		}
	}

	// Push the wait buffer batch into WaitBuffer
	if waitBufferBatch.NumRecords > 0 {
		waitBuffer.EnqueueDRRSBatch(waitBufferBatch, isFromPeer)

		// Switch to only consume peer channels if upstream wait buffer exceeds
		// threshold - this is to block input from upstreams
		if waitBuffer.ShouldBlockUpstream() {
			supplier.SetPeerOnlySupplier(true)
		}
	}

	if processableBatch.TotalNumRecords == 0 {
		return nil, false
	} else {
		return processableBatch, true
	}
}

// Given a DRRS batch, separate processable records from blocked records
func processDRRSBatch[IN tuple.Tuple, K comparable](
	drrsBatch *buffer.DRRSBatch[IN],
	stateClient *stateClient.StateClient[K],
) (*buffer.Batch[IN], *buffer.DRRSBatch[IN]) {

	// Extract processable/blocked records from this DRRS batch
	bucketStatus := stateClient.GetBucketsMigrationStatus(drrsBatch.BucketIds)
	processableBatch := buffer.AllocateOutputBatch[IN]()
	blockedBatch := buffer.NewDRRSBatch[IN](
		drrsBatch.IsFromPeer,
		drrsBatch.SubSupplierName,
	)

	for i := 0; i < drrsBatch.NumRecords; i++ {
		switch bucketStatus[i] {
		case 1:
			// Bucket has been migrated, record is processable
			processableBatch.AddRecord(drrsBatch.Records[i], -1)
		case 0:
			// Bucket not yet migrated, keep the record in WaitBuffer
			blockedBatch.AddRecord(drrsBatch.Records[i], drrsBatch.BucketIds[i])
		default:
			log.Fatalln("Invalid drrs bucket migration status")
		}
	}
	return processableBatch, blockedBatch
}

// [DRRS] Wait buffer to hold records that are affected by reconfig and cannot
// be processed yet. Access to WaitBuffer is single-threaded within the worker
// main loop, no lock needed
type WaitBuffer struct {

	// FIFO queue with O(1) insertion and removal to hold inputs from inbound
	// peer channels. Each element in the queue is a WorkUnit - it can only be
	// a DRRSBatch
	PeerBuffer *list.List

	// FIFO queue with O(1) insertion and removal to hold inputs from upstream
	// operators. Each element in the queue is a WorkUnit - it can be DRRSBatch
	// or a Watermark
	UpstreamBuffer *list.List

	// Threshold to block input from upstream operators and only allow input
	// from inbound peer channels. This is max out-of-orderness allowed such
	// that we can process processable records without FIFO input order. This
	// should be configured with respect to total input buffer size
	BlockUpstreamThreshold int64

	// Tracking info inside upstream wait buffer for debugging
	NumWatermarkInUpstreamBuffer int64
	NumBatchesInUpstreamBuffer   int64
}

func NewWaitBuffer(blockUpstreamThreshold int64) *WaitBuffer {
	return &WaitBuffer{
		PeerBuffer:             list.New(),
		UpstreamBuffer:         list.New(),
		BlockUpstreamThreshold: blockUpstreamThreshold,
	}
}

func (b *WaitBuffer) EnqueueDRRSBatch(
	workUnit buffer.WorkUnit,
	isFromPeer bool,
) {
	if isFromPeer {
		b.PeerBuffer.PushBack(workUnit)
	} else {
		b.NumBatchesInUpstreamBuffer++
		if b.NumBatchesInUpstreamBuffer > b.BlockUpstreamThreshold {
			log.Fatalln("DRRS Batch in upstream wait buffer exceeds threshold")
		}
		b.UpstreamBuffer.PushBack(workUnit)
	}
}

// Watermark will be only pushed into upstream buffer
func (b *WaitBuffer) EnqueueWatermark(wm *buffer.Watermark) {
	b.NumWatermarkInUpstreamBuffer++
	b.UpstreamBuffer.PushBack(wm)
}

// Check if WaitBuffer is empty
func (b *WaitBuffer) IsEmpty() bool {
	return b.PeerBuffer.Len() == 0 && b.UpstreamBuffer.Len() == 0
}

func (b *WaitBuffer) UpstreamWaitBufferNoBatch() bool {
	return b.NumBatchesInUpstreamBuffer == 0
}

// Check if to upstream wait buffer has exceeded threshold so that we should
// block upstream input channels. Note that we cannot guarantee to absolutely
// limit the upstream wait buffer size due to that watermark can also be from
// peer channels where we have to push them into upstream wait buffer no matter
// what the current wait buffer size is. So we use only num DRRS batches to
// check if to block instead of buffer size
func (b *WaitBuffer) ShouldBlockUpstream() bool {
	return b.NumBatchesInUpstreamBuffer >= b.BlockUpstreamThreshold
}
