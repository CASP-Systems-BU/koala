package coordinator

import (
	"log"
	"maps"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby/partition"
)

/******************************************************************************
					  	  Common utils for Rescale()
******************************************************************************/

// Process the rescaleConfig message and get the updated worker list for
// rescale - support both scale-up and scale-down. Returns:
// - updated full worker list
// - new worker list (can be empty)
// - removed worker list (can be empty)
// - target operator
func (s *APIServer) prepareReconfiguration(
	rescaleConfig *pb.RescaleConfig,
) ([]*ManagedWorker, []*ManagedWorker, []*ManagedWorker, dataflow.Operator) {

	// Validate the rescale config
	targetOperatorName := rescaleConfig.TargetRescaleOp
	targetOperator, ok := s.Coordinator.Dataflow.Operators[targetOperatorName]
	if !ok {
		log.Fatalf("Target operator %s does not exist\n", targetOperatorName)
	}
	curWorkerList, ok := s.Coordinator.TaskPlacementPlan[targetOperatorName]
	if !ok {
		log.Fatalf(
			"Target operator %s placement plan does not exist\n",
			targetOperatorName,
		)
	}

	// Get the target parallelism and current parallelism
	targetPara := rescaleConfig.TargetParallelism
	if targetPara <= 0 {
		log.Fatalf(
			"Invalid target parallelism: targetPara=%d\n",
			targetPara,
		)
	}
	curPara := int64(len(curWorkerList))

	// Identify scale-up or scale-down
	if targetPara == curPara {

		// TODO: enable this when we support key space rebalancing and task
		// placement change without changing parallelism
		log.Fatalf(
			"Invalid rescale config: targetPara=%d equals curPara=%d\n",
			targetPara,
			curPara,
		)
	}

	updatedWorkers := make([]*ManagedWorker, 0, int(targetPara))
	var newWorkers []*ManagedWorker
	var removedWorkers []*ManagedWorker
	if targetPara > curPara {

		// Scale-up case: acquire (targetPara - curPara) more workers
		newWorkers = s.Coordinator.WorkerManager.AllocateRandomWorkers(
			int(targetPara - curPara),
		)

		// Build the updated worker list with new workers
		for _, worker := range curWorkerList {
			updatedWorkers = append(updatedWorkers, worker)
		}
		for _, worker := range newWorkers {
			updatedWorkers = append(updatedWorkers, worker)
		}
	} else {

		// Scale-down case: remove (curPara - targetPara) workers
		// Deterministically remove the last few workers in the current list
		for i, worker := range curWorkerList {
			if int64(i) < targetPara {
				updatedWorkers = append(updatedWorkers, worker)
			} else {
				removedWorkers = append(removedWorkers, worker)
			}
		}
	}

	// Log the reconfiguration worker changes
	curWorkerLogging := make([]uint16, 0, len(curWorkerList))
	for _, worker := range curWorkerList {
		curWorkerLogging = append(curWorkerLogging, worker.WorkerId)
	}
	updatedWorkerLogging := make([]uint16, 0, len(updatedWorkers))
	for _, worker := range updatedWorkers {
		updatedWorkerLogging = append(updatedWorkerLogging, worker.WorkerId)
	}
	workersToAddLogging := make([]uint16, 0, len(newWorkers))
	for _, worker := range newWorkers {
		workersToAddLogging = append(workersToAddLogging, worker.WorkerId)
	}
	worksToRemoveLogging := make([]uint16, 0, len(removedWorkers))
	for _, worker := range removedWorkers {
		worksToRemoveLogging = append(worksToRemoveLogging, worker.WorkerId)
	}
	log.Printf(
		"[Reconfig worker INFO] Worker num change: %d -> %d\n",
		len(curWorkerList),
		len(updatedWorkers),
	)
	log.Printf(
		"[Reconfig worker INFO] Worker list change details: %v -> %v\n",
		curWorkerLogging,
		updatedWorkerLogging,
	)
	log.Printf("[Reconfig worker INFO] New workers: %v\n", workersToAddLogging)
	log.Printf(
		"[Reconfig worker INFO] Removed workers: %v\n",
		worksToRemoveLogging,
	)

	return updatedWorkers, newWorkers, removedWorkers, targetOperator
}

// Update key space partition for stateful operator during re-config
func (s *APIServer) updateKeyPartition(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
) map[uint16]map[uint16][]int {

	// Build the updated worker list with new workers
	updatedWorkerIds := make([]uint16, 0, len(updatedWorkers))
	for _, worker := range updatedWorkers {
		updatedWorkerIds = append(updatedWorkerIds, worker.WorkerId)
	}

	// Update the key partition with the updated worker list
	var policy partition.PartitionPolicy
	switch s.Coordinator.Config.PartitionPolicy {
	case "consistent-hashing":
		policy = partition.NewHashPartitionPolicy(s.Coordinator.Config)
	case "uniform":
		policy = partition.NewUniformPartitionPolicy(s.Coordinator.Config)
	default:
		log.Fatalf(
			"Unsupported partition policy: %s\n",
			s.Coordinator.Config.PartitionPolicy,
		)
	}
	curKeyPartition, ok := s.Coordinator.KeyPartitions[targetOperator.GetName()]
	if !ok {
		log.Fatalf(
			"Key partition for operator %s does not exist\n",
			targetOperator.GetName(),
		)
	}

	updatePartitionTable := true
	if s.Coordinator.Config.ReconfigProtocol == "lazy" &&
		s.Coordinator.Config.LazyProtocolVersion == "drrs" {
		updatePartitionTable = false
	}

	// Reconfigure() internally updates the current key partition
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	bucketOwnerChanges := curKeyPartition.Reconfigure(
		updatedWorkerIds,
		policy,
		updatePartitionTable,
	)
	if len(bucketOwnerChanges) == 0 {
		log.Fatalf("Repartition has no effect\n")
	}

	// [Logging] log the bucket owner changes for debugging
	logBucketOwnerChanges(bucketOwnerChanges)
	return bucketOwnerChanges
}

func logBucketOwnerChanges(bucketOwnerChanges map[uint16]map[uint16][]int) {
	log.Println()
	log.Println(
		"Bucket owner transfer INFO (Bucket list in format of [startIdx, endIdx]):",
	)
	for srcWorker, destMap := range bucketOwnerChanges {
		for destWorker, bucketIndices := range destMap {

			var ranges [][2]int
			start := bucketIndices[0]
			prev := bucketIndices[0]
			for i := 1; i < len(bucketIndices); i++ {
				if bucketIndices[i] == prev+1 {
					prev = bucketIndices[i]
				} else {
					ranges = append(ranges, [2]int{start, prev})
					start = bucketIndices[i]
					prev = bucketIndices[i]
				}
			}
			// Add the last range
			ranges = append(ranges, [2]int{start, prev})

			// Log a worker owner transfer entry
			log.Printf(
				"  Worker %d -> Worker %d: %v\n",
				srcWorker,
				destWorker,
				ranges,
			)
		}
	}
	log.Println()
}

// Convert source-based map to dest-based map
// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
// migrationPlan: map[dest worker][]*pb.MigrationInfo
// Make it public to be accessed by tests
func (s *APIServer) GenerateMigrationPlan(
	bucketOwnerChanges map[uint16]map[uint16][]int,
) map[uint16][]*pb.MigrationInfo {

	migrationPlan := make(map[uint16][]*pb.MigrationInfo)
	for sourceWorker, destWorkerMap := range bucketOwnerChanges {
		for destWorker, bucketIndices := range destWorkerMap {

			// Check if destWorker is already in the migration plan
			migrationInfoList, ok := migrationPlan[destWorker]
			if !ok {
				migrationInfoList = make([]*pb.MigrationInfo, 0)
			}

			// bucketIndices is a sorted list of bucket indices. Now we want to
			// convert it to a list of bucket ranges. For example, if
			// bucketIndices = [0, 1, 2, 4, 5], we want to convert it to
			// [0, 2], [4, 5]. We do this by iterating through the list and
			// checking if the next index is equal to the current index + 1.
			// If it is, we continue. If not, we add the current range to the
			// list of ranges and start a new range.
			var bucketRanges []*pb.BucketRange
			start, end := bucketIndices[0], bucketIndices[0]
			for i := 1; i < len(bucketIndices); i++ {
				if bucketIndices[i] == end+1 {
					end = bucketIndices[i]
				} else {
					bucketRanges = append(
						bucketRanges,
						&pb.BucketRange{
							LowerBucketIdx: int64(start),
							UpperBucketIdx: int64(end),
						},
					)
					start, end = bucketIndices[i], bucketIndices[i]
				}
			}
			// Add the last bucket range
			bucketRanges = append(
				bucketRanges,
				&pb.BucketRange{
					LowerBucketIdx: int64(start),
					UpperBucketIdx: int64(end),
				},
			)

			// Append the migration info for a src worker to the list
			migrationInfoList = append(
				migrationInfoList,
				&pb.MigrationInfo{
					TargetWorkerAddr: s.Coordinator.WorkerManager.GetWorkerStateCommAddr(
						sourceWorker,
					),
					BucketRanges: bucketRanges,
				},
			)

			// Reflect the migration info list update to the migration map
			migrationPlan[destWorker] = migrationInfoList
		}
	}

	return migrationPlan
}

// Get downstream info
func (s *APIServer) getDownstreamInfoList(
	targetOperatorId string,
) ([]*pb.DownstreamInfo, *pb.SerializedKeyLookupTable) {

	// Get downstream information
	var downstreamInfoList []*pb.DownstreamInfo
	downstreamOperatorIds := s.Coordinator.Dataflow.Streams[targetOperatorId].DownstreamOperators
	if len(downstreamOperatorIds) != 1 {
		log.Fatalln(
			"We do not support operators with multiple downstream operators",
		)
	}
	downstreamTasks := s.Coordinator.TaskPlacementPlan[downstreamOperatorIds[0]]
	for _, task := range downstreamTasks {
		downstreamInfoList = append(downstreamInfoList, &pb.DownstreamInfo{
			WorkerId:      uint32(task.WorkerId),
			DataPlaneAddr: task.DataPlaneAddr,
		})
	}

	// Check if the downstream operator is stateful
	var keybyCollectorRoutingTable *pb.SerializedKeyLookupTable
	if s.Coordinator.Dataflow.Operators[downstreamOperatorIds[0]].IsStatefulOperator() {
		keybyCollectorRoutingTable = &pb.SerializedKeyLookupTable{
			BucketRanges: s.Coordinator.KeyPartitions[downstreamOperatorIds[0]].Serialize(),
		}
	}

	return downstreamInfoList, keybyCollectorRoutingTable
}

// Get list of upstream tasks for target operator. This includes tasks from all
// upstream operators. Return: (i) list of upstream tasks, (ii) number of
// logical upstream operators
func (s *APIServer) getUpstreamList(
	targetOperatorName string,
) ([]*ManagedWorker, int) {

	// Get the upstream operator(s)
	upstreamOperatorNames := s.Coordinator.Dataflow.Streams[targetOperatorName].UpstreamOperators
	if len(upstreamOperatorNames) == 0 {
		log.Fatalln(
			"Invalid empty upstream operators",
		)
	}

	// Collect tasks from all upstream operators
	upstreamTasks := make([]*ManagedWorker, 0)
	for _, upstreamOperatorName := range upstreamOperatorNames {

		tasksPerUpstream, ok := s.Coordinator.TaskPlacementPlan[upstreamOperatorName]
		if !ok {
			log.Fatalf(
				"Upstream operator %s placement plan does not exist\n",
				upstreamOperatorName,
			)
		}
		upstreamTasks = append(upstreamTasks, tasksPerUpstream...)
	}
	return upstreamTasks, len(upstreamOperatorNames)
}

// Notify all upstream tasks of the target operator to update the routing:
// 1. Add/remove downstream connections as needed
// 2. Update the keyby collector routing table if needed
func (s *APIServer) notifyUpstreamTasks(
	targetOperator dataflow.Operator,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
	upstreamTasks []*ManagedWorker,
) {
	start := time.Now()

	downstreamsToAddList := make([]*pb.DownstreamInfo, 0, len(newWorkers))
	for _, worker := range newWorkers {
		downstreamsToAddList = append(
			downstreamsToAddList,
			&pb.DownstreamInfo{
				WorkerId:      uint32(worker.WorkerId),
				DataPlaneAddr: worker.DataPlaneAddr,
			},
		)
	}

	downstreamsToRemoveList := make(
		[]*pb.DownstreamInfo,
		0,
		len(removedWorkers),
	)
	for _, worker := range removedWorkers {
		downstreamsToRemoveList = append(
			downstreamsToRemoveList,
			&pb.DownstreamInfo{
				WorkerId:      uint32(worker.WorkerId),
				DataPlaneAddr: worker.DataPlaneAddr,
			},
		)
	}

	// If the target operator is stateful, update the routing table (key
	// partition) for its upstream tasks KeyByCollector
	var keybyCollectorRoutingTable *pb.SerializedKeyLookupTable
	if targetOperator.IsStatefulOperator() {
		updatedKeyPartition := s.Coordinator.KeyPartitions[targetOperator.GetName()]
		keybyCollectorRoutingTable = &pb.SerializedKeyLookupTable{
			BucketRanges: updatedKeyPartition.Serialize(),
		}
	}

	// Notify all upstream tasks to update their downstream routing
	var wg sync.WaitGroup
	for _, task := range upstreamTasks {
		wg.Add(1)
		go task.UpdateDownstreamRouting(&pb.UpdateDownstreamRouting{
			DownstreamsToAdd:           downstreamsToAddList,
			DownstreamsToRemove:        downstreamsToRemoveList,
			KeybyCollectorRoutingTable: keybyCollectorRoutingTable,
		}, &wg)
	}
	wg.Wait()

	if s.Coordinator.Config.ReconfigProtocol == "lazy" {
		log.Printf(
			"[Lazy Rescale INFO] Step 4: Upstream routing updated - time taken: %v\n",
			time.Since(start),
		)
	}
}

/******************************************************************************
					  Utils for stop-and-restart Rescale()
******************************************************************************/

// Drain in-flight data for stop and restart api
func (s *APIServer) drainInflightData(
	targetOperator dataflow.Operator,
) []*ManagedWorker {

	targetOperatorName := targetOperator.GetName()

	// Get the upstream tasks
	upstreamTasks, _ := s.getUpstreamList(targetOperatorName)

	// First pause the upstream tasks
	start := time.Now()
	log.Printf("[Rescale info] Start draining in-flight data\n")

	for _, task := range upstreamTasks {
		task.Pause()
	}

	// Wait for the data drained notification from all target operator tasks
	targetTasks := s.Coordinator.TaskPlacementPlan[targetOperatorName]
	for _, task := range targetTasks {
		task.WaitDataDrain()
	}

	end := time.Now()
	log.Printf(
		"[Stop-and-restart INFO] All data drained! Data draining time: %v\n\n",
		end.Sub(start),
	)

	return upstreamTasks
}

// Re-compute the key partition and migrate state for stateful operator.
// targetOperator: the stateful operator to be reconfigured
// updatedWorkers: the updated worker list after reconfiguration
func (s *APIServer) reconfigureStatefulOp(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
) {

	log.Printf("[Rescale info] Recompute new key partition and migrate state\n")
	start := time.Now()

	// Re-partition the key space based on updated worker list
	bucketOwnerChanges := s.updateKeyPartition(targetOperator, updatedWorkers)

	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	// We need to re-organize bucket ownership transfer info as follows:
	// Map[dest worker][]*pb.MigrationInfo
	// Each MigrationInfo contains the source worker and the []*BucketRange
	migrationPlan := s.GenerateMigrationPlan(bucketOwnerChanges)

	// Migrate the state for stop and restart protocol
	s.migrateState(migrationPlan)

	end := time.Now()
	log.Printf("\n[Rescale info] State migration done!\n")
	log.Printf("[Timer info] State migration time: %v\n", end.Sub(start))
}

// Execute state migration
// migrationPlan: map[dest worker][]*pb.MigrationInfo
func (s *APIServer) migrateState(
	migrationPlan map[uint16][]*pb.MigrationInfo,
) {

	var wg sync.WaitGroup
	for destWorkerId, migrationInfoList := range migrationPlan {

		// Send the StateMigration request to the dest worker which will pull
		// the state from source workers
		destWorker := s.Coordinator.WorkerManager.GetWorker(destWorkerId)
		wg.Add(1)
		go destWorker.StateMigration(migrationInfoList, &wg)
	}
	wg.Wait()
}

// Deploy new tasks for scale-up and remove old tasks for scale-down
func (s *APIServer) rescaleTasksStopAndRestart(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
	expectedNumUpstreams int32, // Needed by new tasks
) {

	// Get the downstream information of the target operator for new tasks
	targetOperatorName := targetOperator.GetName()
	downstreamInfoList, keybyCollectorRoutingTable := s.getDownstreamInfoList(
		targetOperatorName,
	)
	var wg sync.WaitGroup

	// Deploy the new tasks
	for _, worker := range newWorkers {
		wg.Add(1)
		go worker.DeployTask(&pb.TaskAssignment{
			TaskId:                     targetOperatorName,
			DownstreamInfoList:         downstreamInfoList,
			ExpectedNumUpstream:        expectedNumUpstreams,
			KeybyCollectorRoutingTable: keybyCollectorRoutingTable,
		}, &wg)
	}

	// Stop the removed tasks
	for _, worker := range removedWorkers {
		wg.Add(1)
		go worker.StopTask(&pb.Terminate{}, &wg)
	}
	wg.Wait()

	// Update the task placement plan after task deployment/removal
	s.Coordinator.TaskPlacementPlan[targetOperatorName] = updatedWorkers
	log.Printf(
		"\n[Rescale info] New tasks deployed and running; old tasks removed\n\n",
	)
}

// Resume the upstreams
func (s *APIServer) resumeUpstreams(upstreamTasks []*ManagedWorker) {

	for _, task := range upstreamTasks {
		task.Resume()
	}

	log.Printf("\n[Rescale info] Task successfully resumed \n\n")
}

/******************************************************************************
					  		Utils for lazy Rescale()
******************************************************************************/

// Initialize all tasks of the target operator for lazy protocol
//  1. Start new tasks if needed
//  2. Send required metadata to all target tasks:
//     (i) expected in-flight drain barriers
//     (ii) expected inbound peers
func (s *APIServer) initializeTargetTasksLazy(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
) ([]*ManagedWorker, map[uint16]map[uint16][]int, []uint16) {
	start := time.Now()
	targetOperatorName := targetOperator.GetName()

	// Update the target operator key partition in the coordinator
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	bucketOwnerChanges := s.updateKeyPartition(
		targetOperator,
		updatedWorkers,
	)

	// Convert bucket transfer plan from sender-based to receiver-based to
	// extract inbound peer info for receiver tasks.
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	// migrationPlan: map[dest worker][]*pb.MigrationInfo
	migrationPlan := s.GenerateMigrationPlan(bucketOwnerChanges)

	// Set state lookup table for new task. This reflects the existing key
	// location before repartitioning the key space
	stateLookupTable := &pb.SerializedKeyLookupTable{
		BucketRanges: s.Coordinator.StateLookupTables[targetOperatorName].Serialize(),
	}

	// Build peer state service info for all receiver tasks - they need
	// these addresses to remotely pull state. We include all target tasks
	// (existing tasks, new tasks, and tasks to be removed) into the peer
	// state service list.
	// First add updated workers (including new workers)
	allPeerStateService := &pb.PeerStateService{}
	for _, worker := range updatedWorkers {
		allPeerStateService.PeerStateServiceInfoList = append(
			allPeerStateService.PeerStateServiceInfoList,
			&pb.PeerStateServiceInfo{
				WorkerId:         uint32(worker.WorkerId),
				StateServiceAddr: worker.StateCommAddr,
			},
		)
	}
	// Then add old workers to be removed
	for _, worker := range removedWorkers {
		allPeerStateService.PeerStateServiceInfoList = append(
			allPeerStateService.PeerStateServiceInfoList,
			&pb.PeerStateServiceInfo{
				WorkerId:         uint32(worker.WorkerId),
				StateServiceAddr: worker.StateCommAddr,
			},
		)
	}

	// Get downstream info of target operator
	downstreamInfoList, keybyCollectorRoutingTable := s.getDownstreamInfoList(
		targetOperatorName,
	)

	// Notify all tasks of the target operator to initialize for lazy protocol
	// including existing tasks, new tasks, and tasks to be removed. There are
	// two types of control messages to send:
	// 1. TaskAssignemnt: deploy a new task along with lazy metadata
	// 2. InitLazyReconfig: only send lazy metadata to existing tasks
	var wg sync.WaitGroup

	// Step 1: deploy new tasks with lazy init metadata
	upstreamTasks, numUpstreamOperators := s.getUpstreamList(targetOperatorName)
	numUpstreams := len(upstreamTasks)
	expectedDrainBarriers := int32(numUpstreams)
	for _, worker := range newWorkers {

		// We assume all new tasks should be assigned some key space
		// after repartition - this is not 100% true in some rare
		// consistent hashing case, but we report error if this happens
		srcTaskList, ok := migrationPlan[worker.WorkerId]
		if !ok {
			log.Fatalln("No key space partitioned to new task")
		}
		expectedNumUpstreams := numUpstreams + len(srcTaskList)

		// Sender task will establish a separate peer connection for each
		// logical upstream operator
		expectedInboundPeers := int32(len(srcTaskList) * numUpstreamOperators)

		wg.Add(1)
		go worker.DeployTask(&pb.TaskAssignment{
			TaskId:                     targetOperatorName,
			DownstreamInfoList:         downstreamInfoList,
			ExpectedNumUpstream:        int32(expectedNumUpstreams),
			KeybyCollectorRoutingTable: keybyCollectorRoutingTable,
			StateLookupTable:           stateLookupTable,    // Not used by DRRS
			PeerStateService:           allPeerStateService, // Not used by DRRS
			// Lazy reconfig metadata
			ExpectedDrainBarriers: &expectedDrainBarriers,
			ExpectedInboundPeers:  &expectedInboundPeers,
			// DRRS
			DrrsMigrationInfo: &pb.StateMigration{
				MigrationInfoList: srcTaskList,
			},
		}, &wg)
		log.Printf(
			"[InitLazyReconfig] New task with lazy metadata sent to worker %d\n",
			worker.WorkerId,
		)
	}

	// Step 2: send lazy init metadata to existing tasks
	// Get existing tasks from old task placement plan - this list includes
	// (updatedWorkers - newWorkers + removedWorkers)
	removedWorkerMap := make(map[uint16]bool)
	for _, worker := range removedWorkers {
		removedWorkerMap[worker.WorkerId] = true
	}

	// Send peer state service info for new tasks for existing tasks
	newPeerStateService := &pb.PeerStateService{}
	for _, worker := range newWorkers {
		newPeerStateService.PeerStateServiceInfoList = append(
			newPeerStateService.PeerStateServiceInfoList,
			&pb.PeerStateServiceInfo{
				WorkerId:         uint32(worker.WorkerId),
				StateServiceAddr: worker.StateCommAddr,
			},
		)
	}

	curWorkers := s.Coordinator.TaskPlacementPlan[targetOperatorName]
	for _, worker := range curWorkers {

		// Check if this task is a receiver task
		expectedInboundPeers := int32(-1)

		// [DRRS] Construct migration info for this existing task
		var migrationInfoList []*pb.MigrationInfo
		srcTaskList, ok := migrationPlan[worker.WorkerId]
		if ok {
			expectedInboundPeers = int32(
				len(srcTaskList) * numUpstreamOperators,
			)
			migrationInfoList = srcTaskList
		}

		taskToBeRemoved := false
		if _, ok := removedWorkerMap[worker.WorkerId]; ok {
			taskToBeRemoved = true
		}

		wg.Add(1)
		go worker.InitLazyReconfig(&pb.InitLazyReconfig{
			ExpectedDrainBarriers: expectedDrainBarriers,
			ExpectedInboundPeers:  expectedInboundPeers,
			IsShuttingDown:        taskToBeRemoved,
			PeerStateService:      newPeerStateService, // Not used by DRRS
			// DRRS
			DrrsMigrationInfo: &pb.StateMigration{
				MigrationInfoList: migrationInfoList,
			},
		}, &wg)
		log.Printf(
			"[InitLazyReconfig] Init metadata sent to worker %d\n",
			worker.WorkerId,
		)
	}
	wg.Wait()

	// Note: we update the coordinator task placement plan at the end of lazy
	// reconfig to include possible task removal

	// Build receiver task list for future use
	var receiverTaskList []uint16
	for destWorkerId := range migrationPlan {
		receiverTaskList = append(receiverTaskList, destWorkerId)
	}

	log.Println(
		"[Lazy Rescale INFO] Step 1: Lazy reconfig initialized - time taken:",
		time.Since(start),
	)
	return upstreamTasks, bucketOwnerChanges, receiverTaskList
}

// Fast forward for stateful operator
func (s *APIServer) fastForward(
	targetOperator dataflow.Operator,
	bucketOwnerChanges map[uint16]map[uint16][]int,
) {
	start := time.Now()
	if len(bucketOwnerChanges) == 0 {
		log.Fatalf("Bucket owner changes not initialized\n")
	}

	targetOperatorId := targetOperator.GetName()

	// Get the updated routing table
	updatedRoutingTable := &pb.SerializedKeyLookupTable{
		BucketRanges: s.Coordinator.KeyPartitions[targetOperatorId].Serialize(),
	}

	// Send FastForward message to all existing workers whose key space has
	// changed by repartition - these tasks need to forward inflight records
	// whose key are assigned to new tasks. Do nothing for existing tasks whose
	// key space does not change (these tasks will not appear in the
	// bucketOwnerChanges map)
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	var wg sync.WaitGroup
	for srcWorkerId, destMap := range bucketOwnerChanges {

		log.Printf(
			"[Fast Forward INFO] Sending fast forward request to worker %d\n",
			srcWorkerId,
		)
		// Build the peer downstream info list
		peerInfo := make([]*pb.DownstreamInfo, 0, len(destMap))
		for destWorkerId := range destMap {
			peerInfo = append(
				peerInfo,
				&pb.DownstreamInfo{
					WorkerId: uint32(destWorkerId),
					DataPlaneAddr: s.Coordinator.WorkerManager.GetWorkerDataPlaneAddr(
						destWorkerId,
					),
				},
			)
		}

		// Send the FastForward message to the worker
		srcWorker := s.Coordinator.WorkerManager.GetWorker(srcWorkerId)
		wg.Add(1)
		go srcWorker.FastForward(&pb.StartFastForward{
			UpdatedRoutingTable: updatedRoutingTable,
			PeerInfoList:        peerInfo,
		}, &wg)
	}
	wg.Wait()

	log.Printf(
		"[Lazy Rescale INFO] Step 2: All fast forward started - time taken: %v\n",
		time.Since(start),
	)
}

// Wait all receiver tasks to successfully receive all expected inbound peer
// connections - for watermark progress safety
func (s *APIServer) waitAllInboundPeersToConnect(
	receiverWorkers []uint16,
) {
	start := time.Now()

	log.Printf(
		"[Wait Inbound Peers INFO] Checking receiver workers: %v for peer channel status ...\n",
		receiverWorkers,
	)
	var wg sync.WaitGroup
	for _, workerId := range receiverWorkers {

		worker := s.Coordinator.WorkerManager.GetWorker(workerId)
		wg.Add(1)
		go worker.WaitAllInboundPeersToConnect(&wg)
	}
	wg.Wait()

	log.Printf(
		"[Lazy Rescale INFO] Step 3: All inbound peers connected - time taken: %v\n",
		time.Since(start),
	)
}

// Wait all target tasks to exit reconfig phase:
func (s *APIServer) waitAllTasksReconfigDone(
	updatedWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
) {
	start := time.Now()

	waitWorkerIds := make([]uint16, 0, len(updatedWorkers)+len(removedWorkers))
	for _, worker := range updatedWorkers {
		waitWorkerIds = append(waitWorkerIds, worker.WorkerId)
	}
	for _, worker := range removedWorkers {
		waitWorkerIds = append(waitWorkerIds, worker.WorkerId)
	}
	log.Printf(
		"[Wait Tasks Reconfig Done INFO] Waiting workers to finish: %v\n",
		waitWorkerIds,
	)

	var wg sync.WaitGroup
	for _, worker := range updatedWorkers {
		wg.Add(1)
		go worker.WaitAllTasksReconfigDone(&wg)
	}
	for _, worker := range removedWorkers {
		wg.Add(1)
		go worker.WaitAllTasksReconfigDone(&wg)
	}
	wg.Wait()

	log.Printf(
		"[Lazy Rescale INFO] Step 5: All target tasks reconfig done - time taken: %v\n",
		time.Since(start),
	)
}

// Terminate removed tasks
func (s *APIServer) terminateTasks(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
) {

	start := time.Now()
	removedTaskIds := make([]uint16, 0, len(removedWorkers))
	for _, worker := range removedWorkers {
		removedTaskIds = append(removedTaskIds, worker.WorkerId)
	}
	log.Printf(
		"[Terminate Tasks INFO] Terminating removed tasks on workers: %v\n",
		removedTaskIds,
	)

	var wg sync.WaitGroup
	for _, worker := range removedWorkers {
		wg.Add(1)
		go worker.StopTask(&pb.Terminate{}, &wg)
	}
	wg.Wait()

	// Update the task placement plan after task deployment/removal
	s.Coordinator.TaskPlacementPlan[targetOperator.GetName()] = updatedWorkers

	log.Printf(
		"[Lazy Rescale INFO] Step 6: Removed tasks terminated - time taken: %v\n",
		time.Since(start),
	)
}

/******************************************************************************
					  			Utils for DRRS
******************************************************************************/

// [DRRS] Execute a subscale reconfig operation
func (s *APIServer) reconfigDRRSSubscale(
	targetOperator dataflow.Operator,
	updatedWorkers []*ManagedWorker,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
	bucketOwnerChanges map[uint16]map[uint16][]int,
	subScaleId int,
	totalNumSubscales int,
) {

	log.Printf("===========================================\n")
	log.Printf(
		"	    Starting DRRS subscale %d/%d ...\n",
		subScaleId,
		totalNumSubscales,
	)
	log.Printf("===========================================\n")

	// Log subscale info
	updatedWorkerIds := make([]uint16, 0, len(updatedWorkers))
	for _, w := range updatedWorkers {
		updatedWorkerIds = append(updatedWorkerIds, w.WorkerId)
	}
	newWorkerIds := make([]uint16, 0, len(newWorkers))
	for _, w := range newWorkers {
		newWorkerIds = append(newWorkerIds, w.WorkerId)
	}
	removedWorkerIds := make([]uint16, 0, len(removedWorkers))
	for _, w := range removedWorkers {
		removedWorkerIds = append(removedWorkerIds, w.WorkerId)
	}

	log.Printf("Updated workers: %v\n", updatedWorkerIds)
	log.Printf("New workers: %v\n", newWorkerIds)
	log.Printf("Removed workers: %v\n", removedWorkerIds)
	logBucketOwnerChanges(bucketOwnerChanges)

	// Step 0: Update the key partition table in coordinator
	s.updateKeyPartitionTableDRRS(targetOperator, bucketOwnerChanges)

	// Step 1: Start new tasks if needed and notify all target tasks required
	// metadata for reconfiguration
	upstreamTasks, receiverWorkers := s.initializeTargetTasksDRRS(
		targetOperator,
		newWorkers,
		removedWorkers,
		bucketOwnerChanges,
		subScaleId,
	)

	// Step 2: Fast forward (peer channel)
	s.fastForward(targetOperator, bucketOwnerChanges)

	// Step 3: Wait all receiver tasks to successfully receive all expected
	// inbound peer connections
	s.waitAllInboundPeersToConnect(receiverWorkers)

	// Step 4: Notify all sender tasks to start state migration
	s.notifySendersToMigrateStateDRRS(bucketOwnerChanges)

	// Step 5: Update upstream routing and broadcast inflight barriers
	s.notifyUpstreamTasks(
		targetOperator,
		newWorkers,
		removedWorkers,
		upstreamTasks,
	)

	// Step 6: Wait for termination signal from all target tasks
	s.waitAllTasksReconfigDone(updatedWorkers, removedWorkers)

	// Step 7: Stop removed tasks and update task placement plan in coordinator
	s.terminateTasks(targetOperator, updatedWorkers, removedWorkers)

	log.Printf("===========================================\n")
	log.Printf(
		"	      DRRS subscale %d/%d done!\n",
		subScaleId,
		totalNumSubscales,
	)
	log.Printf("===========================================\n")
}

// Define all needed info for executing a DRRS subscale
type DRRSSubscaleMetadata struct {
	UpdatedWorkers     []*ManagedWorker
	NewWorkers         []*ManagedWorker
	RemovedWorkers     []*ManagedWorker
	BucketOwnerChanges map[uint16]map[uint16][]int
}

// Split the DRRS reconfiguration into multiple subscales
func (s *APIServer) getDRRSSubscales(
	updatedWorkers []*ManagedWorker,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
	bucketOwnerChanges map[uint16]map[uint16][]int,
	numSubscales int,
) []*DRRSSubscaleMetadata {

	if numSubscales <= 0 {
		log.Fatalf("Invalid numSubscales: %d (must be >= 1)", numSubscales)
	}

	// Build maps for new and removed workers for lookup
	newMap := make(map[uint16]*ManagedWorker)
	for _, w := range newWorkers {
		newMap[w.WorkerId] = w
	}
	removedMap := make(map[uint16]*ManagedWorker)
	for _, w := range removedWorkers {
		removedMap[w.WorkerId] = w
	}

	// originalExisting = finalUpdated - newWorkers + removedWorkers
	existingMap := make(map[uint16]*ManagedWorker)
	for _, w := range updatedWorkers {
		id := w.WorkerId
		if _, isNew := newMap[id]; !isNew {
			existingMap[id] = w
		}
	}
	for _, w := range removedWorkers {
		existingMap[w.WorkerId] = w
	}

	// For balancing, we split each src->dst pair's bucket slice across
	// subscales
	// as evenly as possible (round-robin-ish by contiguous chunks).
	// Prepare an array of per-pair splits: map[pairKey] -> [][]int with length
	// numSubscales
	type pairKey struct{ src, dst uint16 }
	pairSplits := make(map[pairKey][][]int)

	for src, dstMap := range bucketOwnerChanges {
		for dst, buckets := range dstMap {
			if len(buckets) == 0 {
				log.Fatalf("empty bucket list for src %d -> dst %d", src, dst)
			}
			// compute sizes for each subscale: distribute as evenly as possible
			base := len(buckets) / numSubscales
			rem := len(buckets) % numSubscales
			// keep explicit slots per subscale (may be empty)
			splits := make([][]int, numSubscales)
			idx := 0
			for i := range numSubscales {
				take := base
				if i < rem {
					take++
				}
				if take == 0 {
					// leave nil/empty slice in this slot
					continue
				}
				part := make([]int, take)
				copy(part, buckets[idx:idx+take])
				splits[i] = part
				idx += take
			}
			pairSplits[pairKey{src: src, dst: dst}] = splits
		}
	}

	// Post-process pairSplits to ensure that if a pair targets a new worker,
	// there is at least one bucket assigned to subscale 0; and if a pair
	// originates from a removed worker, there is at least one bucket assigned
	// to the last subscale. To avoid empty assignments, we move a bucket from
	// other slots into the required slot when needed.
	for pair, splits := range pairSplits {
		src := pair.src
		dst := pair.dst
		// ensure first slot has a bucket if dst is new
		if _, isNew := newMap[dst]; isNew {
			if len(splits[0]) == 0 {
				// find first later slot with data
				for j := 1; j < numSubscales; j++ {
					if len(splits[j]) > 0 {
						// move the first element from splits[j] to splits[0]
						moved := splits[j][0]
						splits[j] = splits[j][1:]
						splits[0] = append(splits[0], moved)
						break
					}
				}
			}
		}
		// ensure last slot has a bucket if src is removed
		if _, isRemoved := removedMap[src]; isRemoved {
			if len(splits[numSubscales-1]) == 0 {
				// find last earlier slot with data
				for j := numSubscales - 2; j >= 0; j-- {
					if len(splits[j]) > 0 {
						// move the last element from splits[j] to last slot
						lastIdx := len(splits[j]) - 1
						moved := splits[j][lastIdx]
						splits[j] = splits[j][:lastIdx]
						splits[numSubscales-1] = append(
							splits[numSubscales-1],
							moved,
						)
						break
					}
				}
			}
		}
		pairSplits[pair] = splits
	}

	// Now pairSplits has the bucket splits for each src-dst pair. Each pair
	// has a list of bucket splits [][]int that are supposed to be applied in
	// each subscale

	// Validate the total number of migrating buckets should be same in original
	// bucketOwnerChanges and the sum across all splits
	totalBucketsOriginal := 0
	for _, dstMap := range bucketOwnerChanges {
		for _, buckets := range dstMap {
			totalBucketsOriginal += len(buckets)
		}
	}
	totalBucketsSplit := 0
	for _, splits := range pairSplits {
		for _, splitList := range splits {
			totalBucketsSplit += len(splitList)
		}
	}
	if totalBucketsOriginal != totalBucketsSplit {
		log.Fatalf(
			"Bucket split mismatch: original=%d vs split=%d",
			totalBucketsOriginal,
			totalBucketsSplit,
		)
	}

	// Now construct the final subscale metadata list
	res := make([]*DRRSSubscaleMetadata, 0, numSubscales)

	// Build each subscale metadata
	for i := range numSubscales {

		// first subscale: include all new workers
		// last subscale: include all removed workers
		var subNew []*ManagedWorker
		var subRemoved []*ManagedWorker
		if i == 0 {
			subNew = append(subNew, newWorkers...)
		}
		if i == numSubscales-1 {
			subRemoved = append(subRemoved, removedWorkers...)
		}

		// Compute updatedWorkers for this subscale:
		// - include original existing workers
		// - always include newWorkers (once added in first subscale they
		// persist)
		// - remove removedWorkers only in the last subscale
		subUpdatedMap := make(map[uint16]*ManagedWorker)
		maps.Copy(subUpdatedMap, existingMap)
		for _, w := range newWorkers {
			subUpdatedMap[w.WorkerId] = w
		}
		for _, w := range subRemoved {
			delete(subUpdatedMap, w.WorkerId)
		}
		subUpdated := make([]*ManagedWorker, 0, len(subUpdatedMap))

		// Convert map to slice in the same order as original updatedWorkers
		for _, w := range updatedWorkers {
			if _, ok := subUpdatedMap[w.WorkerId]; ok {
				subUpdated = append(subUpdated, w)
				delete(subUpdatedMap, w.WorkerId)
			}
		}

		// For scale down case, handle the removed workers
		for _, w := range removedWorkers {
			if _, ok := subUpdatedMap[w.WorkerId]; ok {
				subUpdated = append(subUpdated, w)
				delete(subUpdatedMap, w.WorkerId)
			}
		}

		if len(subUpdatedMap) != 0 {
			log.Fatalf(
				"Some updated workers missing in subscale %d: %+v",
				i,
				subUpdatedMap,
			)
		}

		// Build bucketOwnerChanges for this subscale
		boc := make(map[uint16]map[uint16][]int)
		for pair, splits := range pairSplits {

			src := pair.src
			dst := pair.dst

			if len(splits[i]) > 0 {
				if _, ok := boc[src]; !ok {
					boc[src] = make(map[uint16][]int)
				}
				boc[src][dst] = splits[i]
			}
		}

		res = append(res, &DRRSSubscaleMetadata{
			UpdatedWorkers:     subUpdated,
			NewWorkers:         subNew,
			RemovedWorkers:     subRemoved,
			BucketOwnerChanges: boc,
		})
	}
	return res
}

// [DRRS] Notify all sender tasks to start state migration
func (s *APIServer) notifySendersToMigrateStateDRRS(
	bucketOwnerChanges map[uint16]map[uint16][]int,
) {
	start := time.Now()
	if len(bucketOwnerChanges) == 0 {
		log.Fatalf("[DRRS] Bucket owner changes not initialized\n")
	}

	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	// migrationPlan: map[src worker][]*pb.MigrationInfo
	migrationPlan := s.generateDRRSMigrationPlan(bucketOwnerChanges)

	var wg sync.WaitGroup
	for srcWorkerId, migrationInfoList := range migrationPlan {

		// Send the StateMigration request to the src worker which will push
		// the state to dest workers
		srcWorker := s.Coordinator.WorkerManager.GetWorker(srcWorkerId)
		wg.Add(1)
		go srcWorker.PushStateMigrationDRRS(migrationInfoList, &wg)
	}
	wg.Wait()

	log.Printf(
		"[DRRS Rescale INFO] State migration started - time taken: %v\n",
		time.Since(start),
	)
}

// [DRRS] Generate migration plan for DRRS protocol.
// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
// migrationPlan: map[src worker][]*pb.MigrationInfo
func (s *APIServer) generateDRRSMigrationPlan(
	bucketOwnerChanges map[uint16]map[uint16][]int,
) map[uint16][]*pb.MigrationInfo {

	migrationPlan := make(map[uint16][]*pb.MigrationInfo)
	for sourceWorker, destWorkerMap := range bucketOwnerChanges {
		var migrationInfoList []*pb.MigrationInfo
		for destWorker, bucketIndices := range destWorkerMap {

			// bucketIndices is a sorted list of bucket indices. Now we want to
			// convert it to a list of bucket ranges. For example, if
			// bucketIndices = [0, 1, 2, 4, 5], we want to convert it to
			// [0, 2], [4, 5]. We do this by iterating through the list and
			// checking if the next index is equal to the current index + 1.
			var bucketRanges []*pb.BucketRange
			start, end := bucketIndices[0], bucketIndices[0]
			for i := 1; i < len(bucketIndices); i++ {
				if bucketIndices[i] == end+1 {
					end = bucketIndices[i]
				} else {
					bucketRanges = append(
						bucketRanges,
						&pb.BucketRange{
							LowerBucketIdx: int64(start),
							UpperBucketIdx: int64(end),
						},
					)
					start, end = bucketIndices[i], bucketIndices[i]
				}
			}
			// Add the last bucket range
			bucketRanges = append(
				bucketRanges,
				&pb.BucketRange{
					LowerBucketIdx: int64(start),
					UpperBucketIdx: int64(end),
				},
			)

			// Append the migration info for a src worker to the list
			migrationInfoList = append(
				migrationInfoList,
				&pb.MigrationInfo{
					TargetWorkerAddr: s.Coordinator.WorkerManager.GetWorkerStateCommAddr(
						destWorker,
					),
					BucketRanges: bucketRanges,
				},
			)
		}
		migrationPlan[sourceWorker] = migrationInfoList
	}
	return migrationPlan
}

// [DRRS] Initialize all tasks for DRRS
// 1. Start new tasks if needed
// 2. Send required metadata to all target tasks
func (s *APIServer) initializeTargetTasksDRRS(
	targetOperator dataflow.Operator,
	newWorkers []*ManagedWorker,
	removedWorkers []*ManagedWorker,
	bucketOwnerChanges map[uint16]map[uint16][]int,
	subScaleId int,
) ([]*ManagedWorker, []uint16) {
	start := time.Now()
	targetOperatorName := targetOperator.GetName()

	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	// migrationPlan: map[dest worker][]*pb.MigrationInfo
	migrationPlan := s.GenerateMigrationPlan(bucketOwnerChanges)

	// Get downstream info of target operator
	downstreamInfoList, keybyCollectorRoutingTable := s.getDownstreamInfoList(
		targetOperatorName,
	)

	// Notify all tasks of the target operator to initialize
	var wg sync.WaitGroup

	// Step 1: deploy new tasks with lazy init metadata
	upstreamTasks, numUpstreamOperators := s.getUpstreamList(targetOperatorName)
	numUpstreams := len(upstreamTasks)
	expectedDrainBarriers := int32(numUpstreams)
	for _, worker := range newWorkers {

		srcTaskList, ok := migrationPlan[worker.WorkerId]
		if !ok {
			log.Fatalln("No key space partitioned to new task")
		}
		expectedNumUpstreams := numUpstreams + len(srcTaskList)
		expectedInboundPeers := int32(len(srcTaskList) * numUpstreamOperators)

		wg.Add(1)
		go worker.DeployTask(&pb.TaskAssignment{
			TaskId:                     targetOperatorName,
			DownstreamInfoList:         downstreamInfoList,
			ExpectedNumUpstream:        int32(expectedNumUpstreams),
			KeybyCollectorRoutingTable: keybyCollectorRoutingTable,
			// Lazy reconfig metadata
			ExpectedDrainBarriers: &expectedDrainBarriers,
			ExpectedInboundPeers:  &expectedInboundPeers,
			// DRRS
			DrrsMigrationInfo: &pb.StateMigration{
				MigrationInfoList: srcTaskList,
			},
		}, &wg)
	}

	// Step 2: send init metadata to existing tasks
	removedWorkerMap := make(map[uint16]bool)
	for _, worker := range removedWorkers {
		removedWorkerMap[worker.WorkerId] = true
	}

	curWorkers := s.Coordinator.TaskPlacementPlan[targetOperatorName]
	for _, worker := range curWorkers {

		// Check if this task is a receiver task
		expectedInboundPeers := int32(-1)
		var migrationInfoList []*pb.MigrationInfo
		srcTaskList, ok := migrationPlan[worker.WorkerId]
		if ok {
			expectedInboundPeers = int32(
				len(srcTaskList) * numUpstreamOperators,
			)
			migrationInfoList = srcTaskList
		}

		taskToBeRemoved := false
		if _, ok := removedWorkerMap[worker.WorkerId]; ok {
			taskToBeRemoved = true
		}

		wg.Add(1)
		go worker.InitLazyReconfig(&pb.InitLazyReconfig{
			ExpectedDrainBarriers: expectedDrainBarriers,
			ExpectedInboundPeers:  expectedInboundPeers,
			IsShuttingDown:        taskToBeRemoved,
			PeerStateService:      &pb.PeerStateService{}, // Not used by DRRS
			// DRRS
			DrrsMigrationInfo: &pb.StateMigration{
				MigrationInfoList: migrationInfoList,
			},
		}, &wg)
	}
	wg.Wait()

	// Build receiver task list for future use
	var receiverTaskList []uint16
	for destWorkerId := range migrationPlan {
		receiverTaskList = append(receiverTaskList, destWorkerId)
	}

	log.Printf(
		"[DRRS INFO][Subscale %d] Step 1: DRRS Init - time taken: %v\n",
		subScaleId,
		time.Since(start),
	)
	return upstreamTasks, receiverTaskList
}

// [DRRS] Update the key partition table in the coordinator
func (s *APIServer) updateKeyPartitionTableDRRS(
	targetOperator dataflow.Operator,
	bucketOwnerChanges map[uint16]map[uint16][]int,
) {
	start := time.Now()
	s.Coordinator.KeyPartitions[targetOperator.GetName()].UpdateBucketOwnersDRRS(
		bucketOwnerChanges,
	)
	log.Printf(
		"[DRRS INFO] Step 0: Key partition table updated for subscale - time taken: %v\n",
		time.Since(start),
	)
}
