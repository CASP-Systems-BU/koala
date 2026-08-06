package worker

import (
	"log"
	"sync/atomic"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/syncflag"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state"
	"google.golang.org/grpc"
)

type Worker struct {

	// Global config
	Config *configuration.Configuration

	// Unique worker ID - this is assigned by coordinator after registration
	WorkerId uint16

	// User-defined dataflow
	Dataflow *dataflow.Dataflow

	// Connection to the Coordinator (control plane channel)
	Stream grpc.BidiStreamingClient[pb.WorkerToCoordinator, pb.CoordinatorToWorker]

	// Task assignment - set after task assignment request from coordinator
	AssignedTask dataflow.Operator

	// Sync flag to check if the assigned task setup has finished
	TaskAssigned *syncflag.SyncFlag

	// Sync channel to indicate the task has been assigned
	TaskReady chan struct{}

	// [stop-and-restart] Fetched in-memory task metadata during state migration
	TaskInitMetadata [][]byte

	// State service
	StateService *state.StateService

	// [Lazy protocol] Atomic flag to indicate if the worker is during reconfig
	// 0: Not in reconfiguration
	// 1: In reconfiguration
	LazyProtocolPhase int32
}

func NewWorker(
	config *configuration.Configuration,
	df *dataflow.Dataflow,
) *Worker {

	return &Worker{
		Config: config,
		// Collect user-defined dataflow
		Dataflow:     df,
		TaskAssigned: syncflag.NewSyncFlag(),
		TaskReady:    make(chan struct{}),
		StateService: state.NewStateService(config),
	}
}

func (w *Worker) Run() {

	// Start the state comm service for external state access
	go w.startStateCommService(w.StateService)

	// Start data plane service
	go w.StartDataPlaneService()

	// Establish the control plane channel to the Coordinator
	go w.StartControlPlane()

	// Wait until task is ready - triggered by TaskAssignment message
	<-w.TaskReady

	// Run the task
	if w.AssignedTask.IsSource() {

		// This is a source task - Execute source method
		w.AssignedTask.RunSource()
	} else {

		// This is a non-source task - start the main loop and consume input

		// Get the reconfiguration protocol
		var useLazyProtocol bool
		var useDRRS bool
		if w.Config.ReconfigProtocol == "lazy" {
			useLazyProtocol = true

			if w.Config.LazyProtocolVersion == "drrs" {
				useDRRS = true
			}
		}
		// [Lazy protocol] Flag to indicate if worker is during reconfig under
		// lazy protocol
		var inTransition bool

		for {

			// [DRRS] Before processing new work unit, check if there are any
			// unblocked existing work units in WaitBuffer to process
			if useDRRS {
				w.AssignedTask.ConsumeDRRSWaitBuffer(w.AssignedTask, inTransition)
			}

			// Update the processing latency in MetricCollector.
			w.AssignedTask.UpdateProcessingLatency()

			// [Lazy protocol] Check the inTransition flag before processing
			// the work unit (stateless op will never be in transition)
			if useLazyProtocol {
				inTransition = w.updateInTransitionFlag(inTransition)
			}

			// Get the next WorkUnit from upstream
			var workUnit buffer.WorkUnit
			var isPeer bool
			var subSupplierName string
			var ok bool
			for {
				// [DRRS] Case that GetWorkUnit() return ok = false: peer-only
				// mode with no input available
				workUnit, isPeer, subSupplierName, ok = w.AssignedTask.GetWorkUnit()
				if ok {
					break
				}

				if !useDRRS {
					log.Fatalln("not drrs but getworkunit not ok")
				}

				// Try to consume wait buffer again which will possibly switch
				// supplier back to all-channel mode
				w.AssignedTask.ConsumeDRRSWaitBuffer(w.AssignedTask, inTransition)
			}

			switch workUnit.GetType() {

			case buffer.BatchWorkUnit:
				// WorkUnit is a batch of data records

				// Update input-related metrics in MetricCollector
				w.AssignedTask.UpdateInputMetrics(workUnit)

				// [Lazy protocol] Fast forward in-flight records if the worker
				// is during reconfiguration phase under lazy protocol: trigger
				// the slow path with RoutingTable lookup
				if inTransition {
					var allRecordsForwarded bool
					workUnit, allRecordsForwarded = w.AssignedTask.FastForward(workUnit, subSupplierName)
					if allRecordsForwarded {
						continue
					}
				}

				// [DRRS] Now remaining records are for local processing.
				// If DRRS reconfig is in progress (avoid extra overhead if not
				// in reconfig), identify processable batch and put the rest
				// into WaitBuffer
				if useDRRS && w.AssignedTask.IfDRRSInReconfig() {
					var hasProcessableRecords bool
					workUnit, hasProcessableRecords = w.AssignedTask.SplitBatchForDRRS(workUnit, isPeer, subSupplierName)
					if !hasProcessableRecords {
						continue
					}
				}

				// Process records in the batch locally
				w.AssignedTask.ProcessBatch(workUnit, subSupplierName)

			case buffer.WatermarkWorkUnit:
				// workUnit is a progressed Watermark
				wm, ok := workUnit.(*buffer.Watermark)
				if !ok {
					log.Fatalln("Failed to cast WorkUnit to Watermark")
				}

				// [DRRS] Check if we can process the watermark
				if useDRRS {

					// If wait buffer is not empty, we cannot process the
					// watermark yet - push it to the WaitBuffer
					if w.AssignedTask.BlockWatermarkDRRS(wm) {
						continue
					}
				}

				// [Lazy protocol] Forward progressed watermark to peers if the
				// worker is during reconfiguration phase under lazy protocol
				if inTransition {
					w.AssignedTask.BroadcastWatermarkToPeers(wm)
				}

				// Process the watermark locally
				w.AssignedTask.ProcessProgressedWatermark(wm)
				w.AssignedTask.BroadcastWatermark(wm)

			case buffer.InflightBarrierWorkUnit:
				// [lazy protocol] workUnit is an InflightBarrier

				// First check if the worker is in reconfiguration phase.
				// If not, it means this task's key space is not affected by
				// repartition, and FastForward is never started. We do nothing
				// in this case and throw away the inflight barrier.
				// Otherwise, it indicates all in-flight records after reconfig
				// have arrived, we can exit the reconfiguration phase.
				if inTransition {
					w.exitTransitionPhase()
				}

				// [DRRS] When in-flight barriers are aligned, there should be
				// no data inserted into wait buffer yet
				if useDRRS {
					if !w.AssignedTask.IsUpstreamWaitBufferEmpty() {
						log.Fatalln(
							"Upstream wait buffer not empty when receiving aligned in-flight barrier",
						)
					}
				}

			case buffer.DrainBarrierWorkUnit:
				// [stop-and-restart] workUnit is an algined DrainBarrier: we
				// get drainBarriers from all upstreams -> notify Coordiantor
				// draining success.
				w.NotifyDataDrained()

			case buffer.CheckpointBarrierWorkUnit:
				// workUnit is a CheckpointBarrier
				log.Fatalln("CheckpointBarrierWorkUnit is not implemented yet")

			case buffer.FastForwardMetadataWorkUnit:
				// workUnit is a FastForwardMetadata
				ffMetadata, ok := workUnit.(*buffer.FastForwardMetadata)
				if !ok {
					log.Fatalln("Failed to cast WorkUnit to FastForwardMetadata")
				}

				// [lazy protocol] FastForwardMetadata is received from a peer
				w.AssignedTask.ProcessFastForwardMetadata(ffMetadata)

			default:
				log.Fatalln("Unknown WorkUnit type in Worker main loop", workUnit.GetType())
			}
		}
	}
}

/******************************************************************************
								  Helper utils
******************************************************************************/

// [Lazy protocol] Check if the worker is in reconfiguration phase
func (w *Worker) updateInTransitionFlag(alreadyInTransition bool) bool {

	if atomic.LoadInt32(&w.LazyProtocolPhase) == 1 {

		// If the reconfiguration is just started, notify the control plane
		// that the main routine has entered reconfiguration phase - e.g. all
		// keys whose ownership has been reassigned will be fast forwarded to
		// the peer channels from now on.
		if !alreadyInTransition {
			w.AssignedTask.NotifyReconfigStart()

			// Send FastForward Metadata to connected peers. This step is only
			// effective if the operator implements it
			fastForwardMetadata := w.AssignedTask.ConstructFastForwardMetadata()
			w.AssignedTask.SendFastForwardMetadata(fastForwardMetadata)

			// Broadcast task watermark to all connected peers
			w.AssignedTask.BroadcastWatermarkToPeers(nil)
		}
		return true
	} else {
		return false
	}
}

// [Lazy protocol] Exit the transition phase when InFlightBarrier is received
// Release all related resources - e.g. peer collector, routing table
func (w *Worker) exitTransitionPhase() {

	// Flip the flag back to 0
	atomic.StoreInt32(&w.LazyProtocolPhase, 0)

	w.AssignedTask.ExitTransitionPhase()
}
