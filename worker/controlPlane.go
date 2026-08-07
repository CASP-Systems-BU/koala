package worker

import (
	"context"
	"encoding/gob"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"github.com/CASP-Systems-BU/koala/state"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (w *Worker) startControlPlane() {

	// Register to the Coordinator
	w.RegisterToCoordinator()

	// Worker ID is assigned after registration. We pass it to State Service
	w.StateService.WorkerID = w.WorkerId

	// Main loop for receving control messages from the Coordinator
	for {

		// Receive a control msg from Coordinator
		in, err := w.Stream.Recv()
		if err != nil {
			if w.Config.IsWarmup {
				// TODO: Store stateservice data elsewhere
				/* Json version
				if w.StateService.StateLookupTable != nil {
					warmupMessage := w.StateService.StateLookupTable.MockBuckets
					filePath := w.Config.LookupTableWarmUpDataFolder + "/" + strconv.FormatUint(uint64(w.WorkerId), 10) + "_warmup.json"
					log.Printf("Worker %d: Start serializing %d items to %s...\n", w.WorkerId, len(warmupMessage[0].KeyLookUpTable), filePath)
					startTime := time.Now()

					file, err := os.Create(filePath)
					if err != nil {
						log.Fatalf("Failed to create file %s: %v\n", filePath, err)
					}
					defer file.Close()

					encoder := json.NewEncoder(file)
					if err := encoder.Encode(warmupMessage); err != nil {
						log.Fatalf("Failed to encode data: %v\n", err)
					}

					duration := time.Since(startTime)
					log.Printf("Worker %d: Done! Wrote warmup data in %v.\n", w.WorkerId, duration)
					log.Println(w.WorkerId, "Write warmupdata into warmupfile", len(warmupMessage[0].KeyLookUpTable))
				} else {
					log.Println(w.WorkerId)
				}
				*/
				if w.StateService.StateLookupTableV2 != nil && w.Config.IsWarmup && w.Config.LazyProtocolVersion == "by-key" {
					warmupMessage := w.StateService.StateLookupTableV2.Buckets
					filePath := w.Config.LookupTableWarmUpDataFolder + "/" + strconv.FormatUint(uint64(w.WorkerId), 10) + "_warmup.gob"

					log.Printf("Worker %d: Start serializing (Gob) to %s...\n", w.WorkerId, filePath)

					startTime := time.Now()
					file, err := os.Create(filePath)
					if err != nil {
						log.Fatalf("Failed to create file %s: %v\n", filePath, err)
					}
					defer file.Close()
					encoder := gob.NewEncoder(file)
					if err := encoder.Encode(warmupMessage); err != nil {
						log.Fatalf("Failed to encode data: %v\n", err)
					}
					duration := time.Since(startTime)

					log.Printf("Worker %d: Done! Wrote binary warmup data in %v.\n", w.WorkerId, duration)
				} else {
					log.Println(w.WorkerId)
				}
			}
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
		case *pb.CoordinatorToWorker_EventualStateMigrationMsg:
			w.HandleEventualStateMigration(msg)
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
		para.KeybyCollectorRoutingTable = keyby.DeserializePartitionTable(
			taskAssignment.KeybyCollectorRoutingTable.BucketRanges,
			w.Config,
		)
	}

	// Set the State Lookup Table for State Service under lazy protocol
	if taskAssignment.StateLookupTable != nil {

		// Use per-key based state lookup table under "lazy-by-key" protocol
		if w.Config.LazyProtocolVersion == "by-key" {
			w.StateService.StateLookupTableV2 = keyby.DeserializeKeyLookupTableV2(
				taskAssignment.StateLookupTable.BucketRanges,
				w.Config,
				w.WorkerId,
			)
		} else {
			w.StateService.StateLookupTable = keyby.DeserializeKeyLookupTable(
				taskAssignment.StateLookupTable.BucketRanges,
				w.Config,
			)
		}
	}

	// Prepare and pre-establish peer state service connections
	if taskAssignment.PeerStateService != nil {

		switch w.Config.LazyProtocolVersion {
		case "basic":

			w.StateService.BucketMigrationConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.BucketMigrationRequest, pb.StateChunk],
			)
			for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
				// Avoid to connect self
				if uint16(peer.WorkerId) == w.WorkerId {
					continue
				}

				// Pre-establish connection to the peer state service
				w.StateService.EstablishBucketMigrationConn(
					uint16(peer.WorkerId),
					peer.StateServiceAddr,
				)
			}

		case "by-key":

			switch w.Config.LazyByKeyStateCommAPIType {
			case "grpc":

				w.StateService.ByKeyMigrationConn = make(
					map[uint16]grpc.BidiStreamingClient[pb.LazyByKeyStateRequest, pb.KeyResponse],
				)
				for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
					// Avoid to connect self
					if uint16(peer.WorkerId) == w.WorkerId {
						continue
					}

					// Pre-establish connection to the peer state service
					w.StateService.EstablishByKeyMigrationConn(
						uint16(peer.WorkerId),
						peer.StateServiceAddr,
					)
				}
			case "tcp":

				w.StateService.ByKeyMigrationTcpConn = make(
					map[uint16]*state.LazyByKeyTcpConn,
				)
				for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
					// Avoid to connect self
					if uint16(peer.WorkerId) == w.WorkerId {
						continue
					}

					// Pre-establish TCP connection to the peer state service
					w.StateService.EstablishByKeyMigrationTcpConn(
						uint16(peer.WorkerId),
						peer.StateServiceAddr,
					)
				}
			default:
				log.Fatalf(
					"Unknown lazy-by-key state comm API type: %s\n",
					w.Config.LazyByKeyStateCommAPIType,
				)
			}

		case "optimized":

			w.StateService.AsyncBucketMigrationConn = make(
				map[uint16]grpc.BidiStreamingClient[pb.LazyOptStateRequest, pb.LazyOptStateResponse],
			)
			for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
				// Avoid to connect self
				if uint16(peer.WorkerId) == w.WorkerId {
					continue
				}

				// Pre-establish connection to the peer state service
				w.StateService.EstablishAsyncBucketMigrationConn(
					uint16(peer.WorkerId),
					peer.StateServiceAddr,
				)
			}

			// Allocate the async state flush routine pool
			var err error
			w.StateService.LazyOptStateFlushPool, err = ants.NewPool(
				w.Config.StateFlushRoutinePoolSize,
			)
			if err != nil {
				log.Fatalf(
					"Failed to create lazy optimized state flush pool: %v",
					err,
				)
			}

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
			for _, peer := range taskAssignment.PeerStateService.PeerStateServiceInfoList {
				// Avoid to connect self
				if uint16(peer.WorkerId) == w.WorkerId {
					continue
				}

				// Pre-establish connection to the peer state service
				w.StateService.EstablishNoMigrationConns(
					uint16(peer.WorkerId),
					peer.StateServiceAddr,
				)
			}

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

	/* Json Version
	// Reload warmupData to stateService
	if w.Config.LoadWarmupData {
		// TODO: LoadDataFrom warmupFile

		filePath := w.Config.LookupTableWarmUpDataFolder + "/" + strconv.FormatUint(uint64(w.WorkerId), 10) + "_warmup.json"
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Println("WorkerID", w.WorkerId, "has no warmupData data")
		} else {
			val := []partition.MockBucket{}
			startTime := time.Now()
			err = json.Unmarshal(data, &val)
			if err != nil {
				fmt.Printf("Failed to parse warmupData: %v\n", err)
				return
			}
			deserializingTime := time.Since(startTime)
			log.Println(w.WorkerId, "Reload warmupData complete, warmupdata Value:", val, "takes", deserializingTime)
			w.StateService.StateLookupTable.MockBuckets = val
		}
	}
	*/
	if w.Config.LoadWarmupData && w.Config.LazyProtocolVersion == "by-key" && w.Config.ReconfigProtocol == "lazy" {
		// TODO: LoadDataFrom warmupFile
		filePath := w.Config.LookupTableWarmUpDataFolder + "/" + strconv.FormatUint(uint64(w.WorkerId), 10) + "_warmup.gob"
		file, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Println("WorkerID", w.WorkerId, "has no warmupData file, skipping.")
			} else {
				log.Printf("Failed to open warmup file %s: %v\n", filePath, err)
			}
		} else {
			defer file.Close()

			startTime := time.Now()
			val := []partition.BucketV2{}

			decoder := gob.NewDecoder(file)
			if err := decoder.Decode(&val); err != nil {
				log.Printf("Failed to decode (gob) warmupData: %v\n", err)
				return
			}

			deserializingTime := time.Since(startTime)

			log.Printf("Worker %d: Reload complete. Loaded items in %v.\n", w.WorkerId, deserializingTime)

			w.StateService.StateLookupTableV2.Buckets = val
		}
	}
	log.Printf(
		"Worker %d:%s (WorkerId:DataPlanePort) received task [%s]\n",
		w.WorkerId,
		w.Config.DataPlanePort,
		taskAssignment.TaskId,
	)

	// Initialize the assigned task
	w.AssignedTask.Setup(para)

	w.StateService.MetricCollector = para.MetricCollector

	if remotePebble, ok := w.StateService.StateBackendImpl.(*stateBackend.RemotePebbleStateBackend); ok {
		remotePebble.MetricCollector = para.MetricCollector
	}

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

	// Connect to new peer state services if any
	switch w.Config.LazyProtocolVersion {
	case "basic":

		for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
			w.StateService.EstablishBucketMigrationConn(
				uint16(peer.WorkerId),
				peer.StateServiceAddr,
			)
		}
	case "by-key":

		switch w.Config.LazyByKeyStateCommAPIType {
		case "grpc":

			for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
				w.StateService.EstablishByKeyMigrationConn(
					uint16(peer.WorkerId),
					peer.StateServiceAddr,
				)
			}
		case "tcp":

			for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
				w.StateService.EstablishByKeyMigrationTcpConn(
					uint16(peer.WorkerId),
					peer.StateServiceAddr,
				)
			}
		default:
			log.Fatalf(
				"Unknown lazy-by-key state comm API type: %s\n",
				w.Config.LazyByKeyStateCommAPIType,
			)
		}

	case "optimized":

		for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
			w.StateService.EstablishAsyncBucketMigrationConn(
				uint16(peer.WorkerId),
				peer.StateServiceAddr,
			)
		}
	case "no-migration":

		for _, peer := range req.PeerStateService.PeerStateServiceInfoList {
			w.StateService.EstablishNoMigrationConns(
				uint16(peer.WorkerId),
				peer.StateServiceAddr,
			)
		}
	default:
		log.Fatalf(
			"Unknown lazy protocol version: %s\n",
			w.Config.LazyProtocolVersion,
		)
	}

	// Initialize lazy reconfiguration metadata
	w.AssignedTask.InitLazyReconfig(
		req.ExpectedDrainBarriers,
		req.ExpectedInboundPeers,
		req.IsShuttingDown,
	)

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
		keyby.DeserializePartitionTable(
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

	w.AssignedTask.WaitTasksReconfigDone()

	ackMsg := "Reconfiguration done"
	w.AckCoordinator(ackMsg)
}

// [lazy-by-key][eventual migration for cancelling task] Handle eventual state
// migration: store affected bucket metadata in StateService, flip the atomic
// flag to trigger additional fetch in remoteReadWithByKeyMigration, then block
// until all affected keys are migrated before ACKing the coordinator.
func (w *Worker) HandleEventualStateMigration(
	msg *pb.CoordinatorToWorker_EventualStateMigrationMsg,
) {

	req := msg.EventualStateMigrationMsg

	// Start eventual state migration process
	w.StateService.SetEventualMigrationMeta(req.AffectedBuckets)

	// Block until all affected keys are migrated
	w.StateService.WaitEventualMigrationDone()

	ackMsg := "Eventual state migration done"
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
