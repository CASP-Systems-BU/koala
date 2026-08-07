package coordinator

import (
	"log"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
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
	case "consistent-hashing-v2":
		policy = partition.NewHashPartitionPolicyV2(s.Coordinator.Config)
	case "uniform":
		policy = partition.NewUniformPartitionPolicy(s.Coordinator.Config)
	case "consistent-even":
		policy = partition.NewEvenPartitionPolicy(s.Coordinator.Config)
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

	// Reconfigure() internally updates the current key partition
	// bucketOwnerChanges: map[source worker]map[dest worker][]bucketIdx
	bucketOwnerChanges := curKeyPartition.Reconfigure(updatedWorkerIds, policy)
	if len(bucketOwnerChanges) == 0 {
		log.Fatalf("Repartition has no effect\n")
	}

	// [eventual migration for cancelling task] Update bucket ownership history
	// with new owners from this reconfiguration
	// history: map[bucketIdx]set(workerId)
	history := s.Coordinator.BucketOwnerHistory[targetOperator.GetName()]
	for _, destMap := range bucketOwnerChanges {
		for destWorker, bucketIndices := range destMap {
			for _, bucketIdx := range bucketIndices {
				history[bucketIdx][destWorker] = true
			}
		}
	}

	// [Logging] log the bucket owner changes for debugging
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
	return bucketOwnerChanges
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
) ([]*pb.DownstreamInfo, *pb.SerializedPartitionTable) {

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
	var keybyCollectorRoutingTable *pb.SerializedPartitionTable
	if s.Coordinator.Dataflow.Operators[downstreamOperatorIds[0]].IsStatefulOperator() {
		keybyCollectorRoutingTable = &pb.SerializedPartitionTable{
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
	var keybyCollectorRoutingTable *pb.SerializedPartitionTable
	if targetOperator.IsStatefulOperator() {
		updatedKeyPartition := s.Coordinator.KeyPartitions[targetOperator.GetName()]
		keybyCollectorRoutingTable = &pb.SerializedPartitionTable{
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

	var wg sync.WaitGroup
	for _, task := range upstreamTasks {
		wg.Go(task.Pause)
	}
	wg.Wait()

	// Wait for the data drained notification from all target operator tasks
	targetTasks := s.Coordinator.TaskPlacementPlan[targetOperatorName]
	for _, task := range targetTasks {
		wg.Go(task.WaitDataDrain)
	}
	wg.Wait()

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

	var wg sync.WaitGroup
	for _, task := range upstreamTasks {
		wg.Go(task.Resume)
	}
	wg.Wait()

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

	// Notify all tasks of the target operator to initialize for lazy protocol
	// including existing tasks, new tasks, and tasks to be removed. There are
	// two types of control messages to send:
	// 1. TaskAssignemnt: deploy a new task along with lazy metadata
	// 2. InitLazyReconfig: only send lazy metadata to existing tasks
	var wg sync.WaitGroup

	/**************************************************************************
			  Step 1: deploy new tasks (if any) with lazy init metadata
	**************************************************************************/

	// State service key lookup table for to start new tasks - align with
	// updated key partition table
	stateLookupTable := &pb.SerializedPartitionTable{
		BucketRanges: s.Coordinator.KeyPartitions[targetOperatorName].Serialize(),
	}

	// [Deprecated] Remove other lazy versions and only keep Lazy-by-key
	if s.Coordinator.Config.LazyProtocolVersion != "by-key" {
		stateLookupTable = &pb.SerializedPartitionTable{
			BucketRanges: s.Coordinator.StateLookupTables[targetOperatorName].Serialize(),
		}
	}

	// Build list of peer state service info for new tasks. The list includes:
	// 1. All existing tasks before reconfig (including tasks to be removed)
	// 2. New tasks to be added
	// 3. (Temporary) workers that previously hosted the target operator tasks
	//    but have been removed - this is to keep access to remaining state that
	//    has not been migrated away from removed workers
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
	// [Temporary] Add previously removed workers to access remaining state
	previouslyRemovedWorkers, exist := s.Coordinator.RemovedWorkers[targetOperatorName]
	if exist {
		for _, worker := range previouslyRemovedWorkers {
			allPeerStateService.PeerStateServiceInfoList = append(
				allPeerStateService.PeerStateServiceInfoList,
				&pb.PeerStateServiceInfo{
					WorkerId:         uint32(worker.WorkerId),
					StateServiceAddr: worker.StateCommAddr,
				},
			)
		}
	}

	// Get downstream info of target operator
	downstreamInfoList, keybyCollectorRoutingTable := s.getDownstreamInfoList(
		targetOperatorName,
	)

	// Get upstream info of target operator
	upstreamTasks, numUpstreamOperators := s.getUpstreamList(targetOperatorName)
	numUpstreams := len(upstreamTasks)
	expectedDrainBarriers := int32(numUpstreams)

	// Send TaskAssignment message to all new tasks
	for _, worker := range newWorkers {

		// We assume all new tasks should be assigned some key space
		// after repartition - this is not 100% true in some rare
		// consistent hashing case, but we report error if this happens
		srcTaskList, ok := migrationPlan[worker.WorkerId]
		if !ok {
			log.Fatalln("No key space partitioned to new task")
		}

		// Sender task will establish a separate peer connection for each
		// logical upstream operator
		expectedInboundPeers := int32(len(srcTaskList) * numUpstreamOperators)
		expectedNumUpstreams := expectedDrainBarriers + expectedInboundPeers

		wg.Add(1)
		go worker.DeployTask(&pb.TaskAssignment{
			TaskId:                     targetOperatorName,
			DownstreamInfoList:         downstreamInfoList,
			ExpectedNumUpstream:        int32(expectedNumUpstreams),
			KeybyCollectorRoutingTable: keybyCollectorRoutingTable,
			StateLookupTable:           stateLookupTable,
			PeerStateService:           allPeerStateService,
			// Lazy reconfig metadata
			ExpectedDrainBarriers: &expectedDrainBarriers,
			ExpectedInboundPeers:  &expectedInboundPeers,
		}, &wg)
		log.Printf(
			"[InitLazyReconfig] New task with lazy metadata sent to worker %d\n",
			worker.WorkerId,
		)
	}

	/**************************************************************************
				Step 2: send lazy init metadata to existing tasks
	**************************************************************************/

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
		if srcTaskList, ok := migrationPlan[worker.WorkerId]; ok {
			expectedInboundPeers = int32(
				len(srcTaskList) * numUpstreamOperators,
			)
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
			PeerStateService:      newPeerStateService,
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
	updatedRoutingTable := &pb.SerializedPartitionTable{
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

// [lazy-by-key][eventual migration for cancelling task] Notify all remaining
// tasks that need to eventually fetch all state from cancelling tasks
func (s *APIServer) notifyEventualStateMigration(
	targetOperator dataflow.Operator,
	removedWorkers []*ManagedWorker,
) {
	start := time.Now()
	targetOperatorName := targetOperator.GetName()

	// Build set of cancelling worker IDs for quick lookup
	cancellingWorkerSet := make(map[uint16]bool)
	for _, worker := range removedWorkers {
		cancellingWorkerSet[worker.WorkerId] = true
	}

	// Get the current key partition (post-reconfiguration) and bucket history
	curPartition := s.Coordinator.KeyPartitions[targetOperatorName]
	history := s.Coordinator.BucketOwnerHistory[targetOperatorName]

	// For each bucket, check if any cancelling worker ever owned it. If so,
	// the bucket is affected and its current owner needs to fetch state from
	// those cancelling workers.
	// Group affected buckets by current owner worker.
	// affectedByOwner: map[current owner worker ID] -> []*pb.AffectedBucketInfo
	// Each AffectedBucketInfo contains the bucket ID and the list of cancelling
	// worker IDs that ever owned this bucket.
	affectedByOwner := make(map[uint16][]*pb.AffectedBucketInfo)
	for bucketIdx, ownerHistorySet := range history {

		// Find which cancelling workers ever owned this bucket
		var cancellingWorkerIds []uint32
		for workerId := range ownerHistorySet {
			if cancellingWorkerSet[workerId] {
				cancellingWorkerIds = append(
					cancellingWorkerIds,
					uint32(workerId),
				)
			}
		}
		if len(cancellingWorkerIds) == 0 {
			continue
		}

		// Current owner of this bucket after reconfiguration
		currentOwner := curPartition.Buckets[bucketIdx]

		affectedByOwner[currentOwner] = append(
			affectedByOwner[currentOwner],
			&pb.AffectedBucketInfo{
				BucketId:            int64(bucketIdx),
				CancellingWorkerIds: cancellingWorkerIds,
			},
		)
	}

	// This function is only called when there are removed workers and eventual
	// migration is enabled — affected buckets must exist
	if len(affectedByOwner) == 0 {
		log.Fatalf(
			"[Eventual Migration] No affected buckets found but removed workers exist\n",
		)
	}

	// Send EventualStateMigration to each affected remaining worker
	var wg sync.WaitGroup
	for ownerWorkerId, affectedBuckets := range affectedByOwner {
		worker := s.Coordinator.WorkerManager.GetWorker(ownerWorkerId)
		wg.Add(1)
		go worker.EventualStateMigration(&pb.EventualStateMigration{
			AffectedBuckets: affectedBuckets,
		}, &wg)
		log.Printf(
			"[Eventual Migration] Control msg sent to worker %d with %d affected buckets\n",
			ownerWorkerId,
			len(affectedBuckets),
		)
	}
	wg.Wait()

	log.Printf(
		"[Lazy Rescale INFO] Step 4.5: Eventual state migration done for cancelling tasks - time taken: %v\n",
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

	// [Temporary] Add the removed workers to the RemovedWorker map
	for _, worker := range removedWorkers {
		s.Coordinator.RemovedWorkers[targetOperator.GetName()] = append(
			s.Coordinator.RemovedWorkers[targetOperator.GetName()],
			worker,
		)
	}

	log.Printf(
		"[Lazy Rescale INFO] Step 6: Removed tasks terminated - time taken: %v\n",
		time.Since(start),
	)
}
