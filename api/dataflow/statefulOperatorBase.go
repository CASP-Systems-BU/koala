package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/collector"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/lazy"
	"github.com/CASP-Systems-BU/koala/internal/utils"
)

// This is the base struct for all stateful operators. On top of it, we have
// another layer of base struct for different number of upstreams.
// Example stateful operator embedding hierarchy:
// 1. StatefulMapper ->
// 2. StatefulOperatorBase1Upstream (stateful operator with 1 upstream) ->
// 3. StatefulOperatorBase ->
// 4. OperatorBase

type StatefulOperatorBase[K comparable] struct {
	*OperatorBase

	// [lazy protocol] Optional: Metadata for lazy protocol fast forwarding
	*lazy.LazyProtocolBase[K]

	// Client to communicate with StateService
	StateClient *stateClient.StateClient[K]
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
	}
}

/******************************************************************************
		    	  Implement Operator APIs for stateful operators
Note: If the API implementation involves input type or PeerCollector, the API
will be implemented in corresponding StatefulOperatorBaseXUpstream struct
******************************************************************************/

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
	routingTable *keyby.PartitionTable,
) {

	// Set the routing table for runtime fast forwarding
	op.RoutingTable = routingTable
}

// [lazy protocol] Release resources after fast forward phase ends
func (op *StatefulOperatorBase[K]) exitTransitionPhase() {
	op.RoutingTable = nil
}
