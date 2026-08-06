package coordinator

import (
	"log"
	"sync"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
)

// Worker managed by the coordinator

type ManagedWorker struct {

	// Worker ID
	WorkerId uint16

	// Control plane connection for this worker (initiated after registration)
	Stream pb.CooridnatorService_RegisterToCoordinatorServer

	// Data plane addr of the worker: IP:DataPlanePort
	DataPlaneAddr string

	// State comm addr of the worker: IP:StateCommPort
	StateCommAddr string

	// If the worker is assigned a task
	IsAvailable bool
}

// Deploy the task to the worker
// The ACK is received only after the task is successfully deployed and ready to
// run: successfully connects to all downstreams and raedy for upstream.
func (w *ManagedWorker) DeployTask(msg *pb.TaskAssignment, wg *sync.WaitGroup) {
	defer wg.Done()

	// Send the TaskAssignment message to the worker
	taskAssignmentMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_TaskAssignmentMsg{
			TaskAssignmentMsg: msg,
		},
	}
	if err := w.Stream.Send(taskAssignmentMsg); err != nil {
		log.Fatalf("Failed to send task assignment message: %v\n", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Task ready to run"
	w.WaitACK(expectedAckMsg)
}

// Stop the task on the worker - for scale-down
func (w *ManagedWorker) StopTask(msg *pb.Terminate, wg *sync.WaitGroup) {
	defer wg.Done()

	// Send the Terminate message to the worker
	terminateMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_TerminateMsg{
			TerminateMsg: msg,
		},
	}
	if err := w.Stream.Send(terminateMsg); err != nil {
		log.Fatalf("Failed to send terminate message: %v", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Task terminated"
	w.WaitACK(expectedAckMsg)
}

// Send control message to pause the worker
func (w *ManagedWorker) Pause() {

	// Send Pause message to the worker
	pauseMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_PauseMsg{
			PauseMsg: &pb.Pause{},
		},
	}
	if err := w.Stream.Send(pauseMsg); err != nil {
		log.Fatalf("Failed to send pause message: %v", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Task successfully paused"
	w.WaitACK(expectedAckMsg)
}

// Send control message to resume the worker
func (w *ManagedWorker) Resume() {

	// Send Resume message to the worker
	resumeMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_RestartMsg{
			RestartMsg: &pb.Restart{},
		},
	}
	if err := w.Stream.Send(resumeMsg); err != nil {
		log.Fatalf("Failed to send restart message: %v", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Task restarted"
	w.WaitACK(expectedAckMsg)
}

// Wait for the data drained message from the worker
func (w *ManagedWorker) WaitDataDrain() {

	expectedAckMsg := "Data drained"
	w.WaitACK(expectedAckMsg)

	log.Printf("Data drained message received\n")
}

// Send state migration message to the worker
// The received worker uses MigrationInfo to pull state from the source worker
func (w *ManagedWorker) StateMigration(
	migrationInfoList []*pb.MigrationInfo,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Send StateMigration message to the worker
	stateMigrationMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_StateMigrationMsg{
			StateMigrationMsg: &pb.StateMigration{
				MigrationInfoList: migrationInfoList,
			},
		},
	}
	if err := w.Stream.Send(stateMigrationMsg); err != nil {
		log.Fatalf("Failed to send state migration message: %v", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "State migration done"
	w.WaitACK(expectedAckMsg)
}

// Notify the task to add/remove downstream connections and update its
// downstream routing table
func (w *ManagedWorker) UpdateDownstreamRouting(
	msg *pb.UpdateDownstreamRouting,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Send UpdateDownstreamRouting message to the worker
	updateDownstreamRoutingMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_UpdateDownstreamRoutingMsg{
			UpdateDownstreamRoutingMsg: msg,
		},
	}
	if err := w.Stream.Send(updateDownstreamRoutingMsg); err != nil {
		log.Fatalf("Failed to send update downstream routing message: %v", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Downstream routing updated"
	w.WaitACK(expectedAckMsg)
}

// Wait for the ACK message from the worker and check the ack msg
func (w *ManagedWorker) WaitACK(ackMsg string) {

	// Wait for the response from the worker
	in, err := w.Stream.Recv()
	if err != nil {
		log.Fatalf(
			"Failed to receive Worker ack with expected ack msg %s: %v\n",
			ackMsg,
			err,
		)
	}

	// Check the ack msg
	responseMsg, ok := in.Message.(*pb.WorkerToCoordinator_ResponseMsg)
	if !ok || responseMsg.ResponseMsg.Info != ackMsg {
		log.Fatalf(
			"Worker ACK format incorrect with expected ack msg: %s\n",
			ackMsg,
		)
	}
}

// Send ACK to the worker with ack msg
func (w *ManagedWorker) AckWorker(ackMsg string) {

	responseMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_ResponseMsg{
			ResponseMsg: &pb.Response{
				Info: ackMsg,
			},
		},
	}
	if err := w.Stream.Send(responseMsg); err != nil {
		log.Fatalf("Failed to ACK Worker with ack msg %s: %v\n", ackMsg, err)
	}
}

// Send registration ACK to the worker with assigned worker id
func (w *ManagedWorker) AckRegistration(workerId uint16, info string) {

	responseMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_RegistrationResponseMsg{
			RegistrationResponseMsg: &pb.RegistrationResponse{
				Info:     info,
				WorkerId: uint32(workerId),
			},
		},
	}
	if err := w.Stream.Send(responseMsg); err != nil {
		log.Fatalf("Failed to ACK Worker registration: %v\n", err)
	}
}

// [Lazy protocol] Initialize lazy reconfiguration
func (w *ManagedWorker) InitLazyReconfig(
	initLazyReconfig *pb.InitLazyReconfig,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Send InitLazyReconfig message to the worker
	initLazyReconfigMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_InitLazyReconfigMsg{
			InitLazyReconfigMsg: initLazyReconfig,
		},
	}
	if err := w.Stream.Send(initLazyReconfigMsg); err != nil {
		log.Fatalf("Failed to send init lazy reconfig message: %v\n", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Lazy reconfiguration initialized"
	w.WaitACK(expectedAckMsg)
}

// [Lazy protocol] Start fast forward
func (w *ManagedWorker) FastForward(
	startFastForward *pb.StartFastForward,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Send FastForward message to the worker
	fastForwardMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_StartFastForwardMsg{
			StartFastForwardMsg: startFastForward,
		},
	}
	if err := w.Stream.Send(fastForwardMsg); err != nil {
		log.Fatalf("Failed to send fast forward message: %v\n", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Fast forward started"
	w.WaitACK(expectedAckMsg)
}

// [Lazy protocol] Wait all expected inbound peer connections to be established
func (w *ManagedWorker) WaitAllInboundPeersToConnect(wg *sync.WaitGroup) {
	defer wg.Done()

	// Send WaitInboundPeers message to the worker
	waitInboundPeersMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_WaitInboundPeersToConnectMsg{
			WaitInboundPeersToConnectMsg: &pb.WaitInboundPeersToConnect{},
		},
	}
	if err := w.Stream.Send(waitInboundPeersMsg); err != nil {
		log.Fatalf("Failed to send wait inbound peers message: %v\n", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "All inbound peers connected"
	w.WaitACK(expectedAckMsg)
}

// [lazy-by-key] [eventual migration for cancelling task] Notify the worker to
// eventually fetch all state from cancelling tasks. Blocks until the worker
// finishes migrating all affected keys.
func (w *ManagedWorker) EventualStateMigration(
	msg *pb.EventualStateMigration,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	eventualMigrationMsg := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_EventualStateMigrationMsg{
			EventualStateMigrationMsg: msg,
		},
	}
	if err := w.Stream.Send(eventualMigrationMsg); err != nil {
		log.Fatalf("Failed to send eventual state migration message: %v\n", err)
	}

	expectedAckMsg := "Eventual state migration done"
	w.WaitACK(expectedAckMsg)
}

// [Lazy protocol] Wait all tasks to finish reconfiguration:
// 1. All in-flight barriers from upstreams are received
// 2. All inbound peer connections are closed
func (w *ManagedWorker) WaitAllTasksReconfigDone(wg *sync.WaitGroup) {
	defer wg.Done()

	waitReconfigDone := &pb.CoordinatorToWorker{
		Message: &pb.CoordinatorToWorker_WaitReconfigDoneMsg{
			WaitReconfigDoneMsg: &pb.WaitReconfigDone{},
		},
	}
	if err := w.Stream.Send(waitReconfigDone); err != nil {
		log.Fatalf("Failed to send wait reconfig done message: %v\n", err)
	}

	// Wait for the response from the worker
	expectedAckMsg := "Reconfiguration done"
	w.WaitACK(expectedAckMsg)
}
