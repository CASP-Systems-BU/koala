package worker

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (w *Worker) StartControlPlane() {

	// Register to the Coordinator
	w.RegisterToCoordinator()

	// Worker ID is assigned after registration. We pass it to State Service
	w.StateService.WorkerID = w.WorkerId

	// Main loop for receving control messages from the Coordinator
	for {

		// Receive a control msg from Coordinator
		in, err := w.Stream.Recv()
		if err != nil {
			log.Fatalf(
				"[Worker %d Terminated] Worker shut down by terminated Coordinator\n",
				w.WorkerId,
			)
		}

		// Based on different control message type, trigger different handlers
		switch msg := in.Message.(type) {
		case *pb.CoordinatorToWorker_TaskAssignmentMsg:
			w.HandleTaskAssignment(msg)
		case *pb.CoordinatorToWorker_PauseMsg:
			w.HandlePause(msg)
		case *pb.CoordinatorToWorker_RestartMsg:
			w.HandleRestart(msg)
		case *pb.CoordinatorToWorker_StateMigrationMsg:
			w.HandleStateMigration(msg)
		case *pb.CoordinatorToWorker_UpdateDownstreamRoutingMsg:
			w.HandleUpdateDownstreamRouting(msg)
		case *pb.CoordinatorToWorker_StartFastForwardMsg:
			w.HandleStartFastForward(msg)
		case *pb.CoordinatorToWorker_TerminateMsg:
			w.HandleTerminate(msg)
		case *pb.CoordinatorToWorker_InitLazyReconfigMsg:
			w.HandleInitLazyReconfig(msg)
		case *pb.CoordinatorToWorker_WaitInboundPeersToConnectMsg:
			w.HandleWaitInboundPeers(msg)
		case *pb.CoordinatorToWorker_WaitReconfigDoneMsg:
			w.HandleWaitReconfigDone(msg)
		case *pb.CoordinatorToWorker_PushStateDRRSMsg:
			w.HandlePushStateDRRS(msg)
		default:
			log.Fatalf("Unknown message type: %v\n", msg)
		}
	}
}

func (w *Worker) RegisterToCoordinator() {

	// Connect to the coordinator
	conn, err := grpc.NewClient(
		w.Config.CoordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the coordinator: %v", err)
	}
	client := pb.NewCooridnatorServiceClient(conn)
	stream, err := client.RegisterToCoordinator(context.Background())
	if err != nil {
		log.Fatalf("Failed to create stream to the coordinator: %v", err)
	}

	// Store the stream for future use
	w.Stream = stream

	// The first Worker-to-Coordinator message is Registration
	registrationMsg := &pb.WorkerToCoordinator{
		Message: &pb.WorkerToCoordinator_RegistrationMsg{
			RegistrationMsg: &pb.Registration{
				DataPlanePort: w.Config.DataPlanePort,
				StateCommPort: w.Config.StateCommPort,
			},
		},
	}
	if err := stream.Send(registrationMsg); err != nil {
		log.Fatalf("Failed to send registration message: %v", err)
	}

	// Wait for the registration ACK from the Coordinator
	expectedAckMsg := "Registration success"
	w.WaitRegistrationAck(expectedAckMsg)

	log.Println("Registration to Coordinator success!")
}

/******************************************************************************
				   Handlers for Coordinator control messages
******************************************************************************/

// Handler for TaskAssignment message
func (w *Worker) HandleTaskAssignment(
	msg *pb.CoordinatorToWorker_TaskAssignmentMsg,
) {
	taskAssignment := msg.TaskAssignmentMsg

	// Set the assigned task
	w.AssignedTask = w.Dataflow.Operators[taskAssignment.TaskId]

	// Construct the task setup parameters
	para := &utils.OperatorSetupParas{
		Config:             w.Config,
		OperatorName:       taskAssignment.TaskId,
		WorkerID:           w.WorkerId,
		StateService:       w.StateService,
		DownstreamInfoList: taskAssignment.DownstreamInfoList,
		ExpectNumUpstream:  int(taskAssignment.ExpectedNumUpstream),
	}

	// if the task is a source operator, set the source replica id
	if taskAssignment.SourceReplicaId != nil {
		para.SourceReplicaID = *taskAssignment.SourceReplicaId
	}

	// Set the routing table for keyby collector if it's passed in
	if taskAssignment.KeybyCollectorRoutingTable != nil {
		para.KeybyCollectorRoutingTable = keyby.DeserializeKeyLookupTable(
			taskAssignment.KeybyCollectorRoutingTable.BucketRanges,
			w.Config,
		)
	}

	// Set the State Lookup Table for State Service under lazy protocol
	if taskAssignment.StateLookupTable != nil {
		w.StateService.StateLookupTable = keyby.DeserializeKeyLookupTable(
			taskAssignment.StateLookupTable.BucketRanges,
			w.Config,
		)
	}

	// Set the peer state service addresses under lazy protocol
	if taskAssignment.PeerStateService != nil {
		w.StateService.PeerStateServiceMap = make(map[uint16]string)
		w.StateService.PeerStateServiceMapLock = sync.Mutex{}
		for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
			w.StateService.PeerStateServiceMap[uint16(peer.WorkerId)] = peer.StateServiceAddr
		}

		// Prepare long-lived remote state access connections
		switch w.Config.LazyProtocolVersion {
		case "basic":

			w.StateService.BucketMigrationConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk],
			)
		case "optimized":

			w.StateService.AsyncBucketMigrationConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.AsyncBucketMigrationRequest, pb.AsyncBucketMigrationResponse],
			)
		case "no-migration":

			w.StateService.ReadConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.ReadRequest, pb.ReadResponse],
			)
			w.StateService.OverwriteConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response],
			)
			w.StateService.MergeConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.WriteRequest, pb.Response],
			)
			w.StateService.DeleteConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.DeleteRequest, pb.Response],
			)
		case "drrs":
			// Do nothing
		default:
			log.Fatalf(
				"Unknown lazy protocol version: %s\n",
				w.Config.LazyProtocolVersion,
			)
		}
	}

	// Init lazy reconfig metadata - they are nil if not in lazy reconfig phase
	para.ExpectedDrainBarriers = taskAssignment.ExpectedDrainBarriers
	para.ExpectedInboundPeers = taskAssignment.ExpectedInboundPeers

	log.Printf(
		"Worker %d:%s (WorkerId:DataPlanePort) received task [%s]\n",
		w.WorkerId,
		w.Config.DataPlanePort,
		taskAssignment.TaskId,
	)

	// Initialize the assigned task
	w.AssignedTask.Setup(para)

	w.StateService.MetricCollector = para.MetricCollector

	// [stop-and-restart] During stop-and-restart state migration, we could have
	// fetched the in-memory task metadata (stored in worker memory) from the
	// remote worker. During task initialization, we should load the fetched
	// metadata if exists
	if len(w.TaskInitMetadata) > 0 {
		totalReceivedMetadataSize := 0
		for _, metadata := range w.TaskInitMetadata {
			w.AssignedTask.InsertMigratedMetadata(metadata)
			totalReceivedMetadataSize += len(metadata)
		}
	}

	// [DRRS] Initialize bucket owner map in state service
	if w.Config.ReconfigProtocol == "lazy" &&
		w.Config.LazyProtocolVersion == "drrs" {

		// This is a new task for DRRS reconfiguration
		if taskAssignment.DrrsMigrationInfo != nil {

			migrationInfoList := taskAssignment.DrrsMigrationInfo.MigrationInfoList
			numInboundStatePeers := int32(len(migrationInfoList))
			if numInboundStatePeers > 0 {

				// This is a receiver task that receives buckets from peers
				// Initialize the bucket owner map and its metadata
				w.StateService.InitBucketMigrationMap(
					migrationInfoList,
					numInboundStatePeers,
				)
			}

			if w.AssignedTask.IfDRRSInReconfig() {
				log.Fatalln(
					"DRRS already in reconfig phase during task assignment",
				)
			}
			w.AssignedTask.EnterDRRSReconfigPhase()
		}
	}

	// Now task is successfully initialized
	// Signal all waiting incoming connections from upstream tasks to proceed
	w.TaskAssigned.Signal()
	log.Println("Task initialized! Waiting for upstream connections...")

	// Notify worker main routine that worker is completely ready to run
	w.TaskReady <- struct{}{}

	// Set the assign operator ID to the state service
	w.StateService.OperatorID = w.AssignedTask.GetId()

	// Send the response back to the Coordinator
	ackMsg := "Task ready to run"
	w.AckCoordinator(ackMsg)
}

// Handler for Pause message
func (w *Worker) HandlePause(msg *pb.CoordinatorToWorker_PauseMsg) {

	// Pause the task
	w.AssignedTask.Pause()

	// Send the response back to the Coordinator
	ackMsg := "Task successfully paused"
	w.AckCoordinator(ackMsg)
}

// Handler for Restart message
func (w *Worker) HandleRestart(msg *pb.CoordinatorToWorker_RestartMsg) {

	// Resume the task
	w.AssignedTask.Resume()

	// Send the response back to the Coordinator
	ackMsg := "Task restarted"
	w.AckCoordinator(ackMsg)
}

// Handler for StateMigration message
// Coordinator notifies this worker to pull state from another worker
func (w *Worker) HandleStateMigration(
	msg *pb.CoordinatorToWorker_StateMigrationMsg,
) {
	migrationInfoList := msg.StateMigrationMsg.MigrationInfoList

	if len(migrationInfoList) == 0 {
		log.Fatalf("Invalid state migration info list\n")
	}

	// Linearly pull state from each worker
	// TODO: now the control plane handles the data migration directly. Should
	// move it to data service or task api
	for _, migrationInfo := range migrationInfoList {
		w.requestMigration(migrationInfo)
	}

	// Now state migration is done, notify the Coordinator
	ackMsg := "State migration done"
	w.AckCoordinator(ackMsg)
}

// [DRRS] Handle push state request for DRRS
func (w *Worker) HandlePushStateDRRS(
	msg *pb.CoordinatorToWorker_PushStateDRRSMsg,
) {
	migrationInfoList := msg.PushStateDRRSMsg.MigrationInfoList

	if len(migrationInfoList) == 0 {
		log.Fatalf("Invalid DRRS state migration info list\n")
	}

	// Concurrently push state to each target worker
	for _, migrationInfo := range migrationInfoList {

		// Start the push-based state migration routine in the background
		go w.pushStateMigrationDRRS(migrationInfo)
	}

	// Now state migration is initiated, notify the Coordinator
	ackMsg := "DRRS state migration initiated"
	w.AckCoordinator(ackMsg)
}

// Handler for updating downstream routing while the job is running
// 1. Add/Remove downstream connections
// 2. Update the collector routing table for keyby collector
func (w *Worker) HandleUpdateDownstreamRouting(
	msg *pb.CoordinatorToWorker_UpdateDownstreamRoutingMsg,
) {

	req := msg.UpdateDownstreamRoutingMsg
	w.AssignedTask.UpdateDownstreamRouting(
		req.DownstreamsToAdd,
		req.DownstreamsToRemove,
		req.KeybyCollectorRoutingTable,
	)

	// Send the response back to the Coordinator
	ackMsg := "Downstream routing updated"
	w.AckCoordinator(ackMsg)
}

// Handle task termination message
func (w *Worker) HandleTerminate(
	msg *pb.CoordinatorToWorker_TerminateMsg,
) {

	// Terminate the assigned task
	w.AssignedTask.Terminate()

	// Send the response back to the Coordinator
	ackMsg := "Task terminated"
	w.AckCoordinator(ackMsg)

	// TODO: gracefully shutdown the worker process if needed
}

// [Lazy protocol] Handler for InitLazyReconfig message
func (w *Worker) HandleInitLazyReconfig(
	msg *pb.CoordinatorToWorker_InitLazyReconfigMsg,
) {

	req := msg.InitLazyReconfigMsg

	// Add new peer state service addresses if any
	w.StateService.PeerStateServiceMapLock.Lock()
	for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
		w.StateService.PeerStateServiceMap[uint16(peer.WorkerId)] = peer.StateServiceAddr
	}
	w.StateService.PeerStateServiceMapLock.Unlock()

	// Initialize lazy reconfiguration metadata
	w.AssignedTask.InitLazyReconfig(
		req.ExpectedDrainBarriers,
		req.ExpectedInboundPeers,
		req.IsShuttingDown,
	)

	// [DRRS] Initialize bucket owner map in state service
	if w.Config.ReconfigProtocol == "lazy" &&
		w.Config.LazyProtocolVersion == "drrs" {

		migrationInfoList := req.DrrsMigrationInfo.MigrationInfoList
		numInboundStatePeers := int32(len(migrationInfoList))
		if numInboundStatePeers > 0 {

			// This is a receiver task that receives buckets from peers
			// Initialize the bucket owner map and its metadata
			w.StateService.InitBucketMigrationMap(
				migrationInfoList,
				numInboundStatePeers,
			)
		}

		if w.AssignedTask.IfDRRSInReconfig() {
			log.Fatalln(
				"DRRS already in reconfig phase during lazy reconfig init",
			)
		}
		w.AssignedTask.EnterDRRSReconfigPhase()
	}

	// Send the response back to the Coordinator
	ackMsg := "Lazy reconfiguration initialized"
	w.AckCoordinator(ackMsg)
}

// [Lazy protocol] Handler for StartFastForward message: update local routing
// table and establish peer-to-peer channels for fast forwarding
func (w *Worker) HandleStartFastForward(
	msg *pb.CoordinatorToWorker_StartFastForwardMsg,
) {

	// Check if fast forward is allowed
	if w.Config.ReconfigProtocol != "lazy" {
		log.Fatalf("Fast forward is only allowed under lazy protocol\n")
	}
	if atomic.LoadInt32(&w.LazyProtocolPhase) != 0 {
		log.Fatalf("Worker is already in reconfiguration phase\n")
	}
	if !w.AssignedTask.IsStatefulOperator() {
		log.Fatalf("Fast forward is only allowed for stateful operators\n")
	}

	req := msg.StartFastForwardMsg

	// Set the routing table and activate the PeerCollector for fast forward
	w.AssignedTask.PrepareLazyProtocol(
		req.PeerInfoList,
		keyby.DeserializeKeyLookupTable(
			req.UpdatedRoutingTable.BucketRanges,
			w.Config,
		),
	)

	// Now Routing table is updated and PeerCollector is activated, we can flip
	// the flag to start fast forwarding: transition to reconfiguration phase.
	// Now the worker main routine will apply fast forward before processing
	// the local records
	atomic.StoreInt32(&w.LazyProtocolPhase, 1)

	// Wait until the main routine enters reconfiguration phase before ack the
	// Coordinator. This is to ensure that the main routine has stopped
	// processing transferred keys and guarantee single-writer
	w.AssignedTask.WaitReconfigStart()

	// Now we can guarantee coordinator that fast forward successfully started
	ackMsg := "Fast forward started"
	w.AckCoordinator(ackMsg)
}

// [Lazy protocol] Wait all expected inbound peer connections to be established
func (w *Worker) HandleWaitInboundPeers(
	msg *pb.CoordinatorToWorker_WaitInboundPeersToConnectMsg,
) {

	// Wait until all expected inbound peer connections are established
	w.AssignedTask.WaitAllInboundPeersToConnect()

	ackMsg := "All inbound peers connected"
	w.AckCoordinator(ackMsg)
}

// [Lazy protocol] Wait for reconfiguration done signal from the task
func (w *Worker) HandleWaitReconfigDone(
	msg *pb.CoordinatorToWorker_WaitReconfigDoneMsg,
) {

	// 1. Wait until all in-flight barriers from upstreams are received
	// 2. Wait until all inbound peer connections are closed
	w.AssignedTask.WaitTasksReconfigDone()

	// [DRRS] Wait for DRRS migration to be done
	if w.Config.ReconfigProtocol == "lazy" &&
		w.Config.LazyProtocolVersion == "drrs" {

		// Notify main routine to check termination condition
		w.AssignedTask.EnterTerminationPhase()

		// Wait on wait buffer clear and state migration done
		w.AssignedTask.WaitOnWaitBufferAndStateMigration()
	}

	ackMsg := "Reconfiguration done"
	w.AckCoordinator(ackMsg)
}

/******************************************************************************
				   				Helper functions
******************************************************************************/

// Worker notifies the Coordinator that in-flight data has been drained
func (w *Worker) NotifyDataDrained() {

	// Notify the Coordinator in-flight data has been drained
	ackMsg := "Data drained"
	w.AckCoordinator(ackMsg)
}

// Send ACK to the Coordinator with ack msg
func (w *Worker) AckCoordinator(ackMsg string) {

	responseMsg := &pb.WorkerToCoordinator{
		Message: &pb.WorkerToCoordinator_ResponseMsg{
			ResponseMsg: &pb.Response{
				Info: ackMsg,
			},
		},
	}
	if err := w.Stream.Send(responseMsg); err != nil {
		log.Fatalf(
			"Failed to ACK Coordinator with ack msg %s: %v\n",
			ackMsg,
			err,
		)
	}
}

// Wait for the ACK message from Coordinator and check the ack msg
func (w *Worker) WaitACK(ackMsg string) {

	// Wait for the response from the Coordinator
	in, err := w.Stream.Recv()
	if err != nil {
		log.Fatalf(
			"Failed to receive Coordinator ack with expected ack msg %s: %v",
			ackMsg,
			err,
		)
	}

	// Check the ack msg
	responseMsg, ok := in.Message.(*pb.CoordinatorToWorker_ResponseMsg)
	if !ok || responseMsg.ResponseMsg.Info != ackMsg {
		log.Fatalf(
			"Coordinator ACK format incorrect with expected ack msg: %s\n",
			ackMsg,
		)
	}
}

// Wait for registration response from the Coordinator
func (w *Worker) WaitRegistrationAck(ackMsg string) {

	// Wait for the response from the Coordinator
	in, err := w.Stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive Coordinator ack: %v", err)
	}

	// Check the ack msg
	registrationResponse, ok := in.Message.(*pb.CoordinatorToWorker_RegistrationResponseMsg)
	if !ok || registrationResponse.RegistrationResponseMsg.Info != ackMsg {
		log.Fatalf("Coordinator registratoin ACK incorrect\n")
	}

	// Set the worker ID
	w.WorkerId = uint16(registrationResponse.RegistrationResponseMsg.WorkerId)

	// Print out the assigned worker ID
	log.Printf(
		"[info] Worker ID assigned by Coordinator: %d\n",
		registrationResponse.RegistrationResponseMsg.WorkerId,
	)
}
