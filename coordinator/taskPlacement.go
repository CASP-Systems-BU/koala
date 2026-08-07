package coordinator

import (
	"bufio"
	"log"
	"os"
	"sort"
	"strings"
	"sync"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
)

// Generate task placement plan upon new job submission
func (c *Coordinator) GetTaskPlacementPlan() {

	// Check task placement policy
	switch c.Config.TaskPlacementPolicy {
	case "random":
		log.Println("[Placement policy] Use random task placement policy")
		c.setRandomTaskPlacementPlan()
	case "custom":
		log.Println("[Placement policy] Use custom task placement policy")
		c.setCustomTaskPlacementPlan()
	default:
		log.Fatalf(
			"Invalid task placement policy: %s\n",
			c.Config.TaskPlacementPolicy,
		)
	}
}

// Init KeyPartition tables for stateful operators based on task placement plan
func (c *Coordinator) InitKeyPartitions() {

	// KeyPartition is dependent on task placement plan
	if c.TaskPlacementPlan == nil {
		log.Fatalf("Task placement plan is not available\n")
	}

	for operatorId, workers := range c.TaskPlacementPlan {

		// If the operator is stateful, create a KeyPartition table
		if c.Dataflow.Operators[operatorId].IsStatefulOperator() {

			// Get the list of worker IDs
			workerIds := make([]uint16, 0, len(workers))
			for _, worker := range workers {
				workerIds = append(workerIds, worker.WorkerId)
			}

			// Set the partitioning policy
			var policy partition.PartitionPolicy
			switch c.Config.PartitionPolicy {
			case "consistent-hashing":
				policy = partition.NewHashPartitionPolicy(c.Config)
			case "consistent-hashing-v2":
				policy = partition.NewHashPartitionPolicyV2(c.Config)
			case "uniform":
				policy = partition.NewUniformPartitionPolicy(c.Config)
			case "consistent-even":
				policy = partition.NewEvenPartitionPolicy(c.Config)
			default:
				log.Fatalf(
					"Unsupported partition policy: %s\n",
					c.Config.PartitionPolicy,
				)
			}

			c.KeyPartitions[operatorId] = keyby.NewPartitionTable(
				policy,
				workerIds,
				c.Config,
			)

			// Print out the number of buckets assigned to each worker
			table := c.KeyPartitions[operatorId]
			bucketCountPerWorker := make(map[uint16]int)
			for _, br := range table.Buckets {
				bucketCountPerWorker[br] += 1
			}

			log.Printf(
				"[Bucket Partition INFO] bucket count per worker: %+v\n",
				bucketCountPerWorker,
			)

			// [eventual migration for cancelling task] Record initial bucket
			// ownership in history
			history := make([]map[uint16]bool, len(table.Buckets))
			for i, ownerWorker := range table.Buckets {
				history[i] = map[uint16]bool{ownerWorker: true}
			}
			c.BucketOwnerHistory[operatorId] = history

			// [lazy protocol] Initialize state lookup tables
			// StateLookupTable can differ from KeyPartition based on actual
			// state location
			if c.Config.ReconfigProtocol == "lazy" {
				c.StateLookupTables[operatorId] = keyby.NewPartitionTable(
					policy,
					workerIds,
					c.Config,
				)
			}
		}
	}
}

// DeployTasks deploys tasks to workers based on the task placement plan
func (c *Coordinator) StartJob() {

	// Deploy tasks to workers based on the task placement plan
	var wg sync.WaitGroup

	// Each iteration deploy 1 operator
	for operatorId, workers := range c.TaskPlacementPlan {

		// Each operator has at most 1 downstream and upstream operator

		// Get the upstream info: number of expected upstream tasks
		numUpstream := int32(0)
		if len(c.Dataflow.Streams[operatorId].UpstreamOperators) > 0 {

			// Go over all upstream operators and sum up their parallelism
			for _, upOpId := range c.Dataflow.Streams[operatorId].UpstreamOperators {
				numUpstream += int32(
					c.Dataflow.Operators[upOpId].GetParallelism(),
				)
			}
		}

		// Get the downstream info: worker ids and data plane addresses
		downstreamInfoList := []*pb.DownstreamInfo{}
		downstreamOpIds := c.Dataflow.Streams[operatorId].DownstreamOperators
		if len(downstreamOpIds) > 0 {

			for _, worker := range c.TaskPlacementPlan[downstreamOpIds[0]] {
				downstreamInfoList = append(
					downstreamInfoList,
					&pb.DownstreamInfo{
						WorkerId:      uint32(worker.WorkerId),
						DataPlaneAddr: worker.DataPlaneAddr,
					},
				)
			}
		}

		// Get downstream key partition table for operators with keyed output
		// stream
		var downstreamKeyPartition *pb.SerializedPartitionTable
		if len(downstreamOpIds) > 0 {
			// This is a non-sink operator. Now we guarantee there is at most 1
			// downstream operator - at idx 0
			downstreamOpId := downstreamOpIds[0]
			if c.Dataflow.Operators[downstreamOpId].IsStatefulOperator() {
				keyPartition, exists := c.KeyPartitions[downstreamOpId]
				if !exists {
					log.Fatalf(
						"KeyPartition is not available for downstream stateful operator\n",
					)
				}
				downstreamKeyPartition = &pb.SerializedPartitionTable{
					BucketRanges: keyPartition.Serialize(),
				}
			}
		}

		// Send StateLookupTable and PeerStateService for lazy protocol and
		// stateful operator
		var stateLookupTable *pb.SerializedPartitionTable
		var peerStateService *pb.PeerStateService
		if c.Config.ReconfigProtocol == "lazy" &&
			c.Dataflow.Operators[operatorId].IsStatefulOperator() {

			// Get the state lookup table
			stateLookupTable = &pb.SerializedPartitionTable{
				BucketRanges: c.StateLookupTables[operatorId].Serialize(),
			}

			// Get the state service addr for all workers
			peerStateService = &pb.PeerStateService{}
			for _, worker := range workers {
				peerStateService.PeerStateServiceInfoList = append(
					peerStateService.PeerStateServiceInfoList,
					&pb.PeerStateServiceInfo{
						WorkerId:         uint32(worker.WorkerId),
						StateServiceAddr: worker.StateCommAddr,
					},
				)
			}
		}

		// if the operator is a source operator, set the source replica id
		isSource := c.Dataflow.Operators[operatorId].IsSource()

		// Deploy tasks to workers
		for i, worker := range workers {
			wg.Add(1)
			taskAssignment := &pb.TaskAssignment{
				TaskId:                     operatorId,
				DownstreamInfoList:         downstreamInfoList,
				ExpectedNumUpstream:        numUpstream,
				KeybyCollectorRoutingTable: downstreamKeyPartition,
				StateLookupTable:           stateLookupTable,
				PeerStateService:           peerStateService,
			}
			if isSource {
				id := int64(i)
				taskAssignment.SourceReplicaId = &id
			}
			go worker.DeployTask(taskAssignment, &wg)
		}
	}
	wg.Wait()

	log.Printf("All tasks deployed and running!\n")
}

/******************************************************************************
								 Helper utils
******************************************************************************/

// Get task placement plan under random task placement
func (c *Coordinator) setRandomTaskPlacementPlan() {

	// A task placement plan maps operator ID to a list of workers
	taskPlacementPlan := make(map[string][]*ManagedWorker)

	// Traverse each operator of the dataflow and allocate workers for placement
	for id, operator := range c.Dataflow.Operators {

		// Allocate #parallelism workers for each operator
		allocatedWorkers := c.WorkerManager.AllocateRandomWorkers(
			operator.GetParallelism(),
		)

		taskPlacementPlan[id] = allocatedWorkers
	}

	c.TaskPlacementPlan = taskPlacementPlan
}

// Get task placement plan under custom task placement
func (c *Coordinator) setCustomTaskPlacementPlan() {

	// A task placement plan maps operator ID to a list of workers
	taskPlacementPlan := make(map[string][]*ManagedWorker)

	// loadedPlan: map[operatorName] -> map[ip addr] -> count
	loadedPlan := c.loadCustomTaskPlacementPlan()

	// Enforce deterministic order for traversing all operators
	operatorNameList := make([]string, 0, len(c.Dataflow.Operators))
	for operatorName := range c.Dataflow.Operators {
		operatorNameList = append(operatorNameList, operatorName)
	}
	sort.Strings(operatorNameList)

	// Traverse each operator of the dataflow and allocate workers for placement
	for _, operatorName := range operatorNameList {

		operator := c.Dataflow.Operators[operatorName]
		targetAddrs, ok := loadedPlan[operatorName]
		if !ok {
			log.Fatalf(
				"Custom task placement plan is not available for operator %s\n",
				operatorName,
			)
		}

		// Allocate #parallelism workers for each operator
		allocatedWorkers := c.WorkerManager.AllocateSpecifiedWorkers(
			operator,
			targetAddrs,
		)
		taskPlacementPlan[operatorName] = allocatedWorkers
	}

	// Log the final task placement plan
	log.Println()
	log.Println("[Custom Placement INFO] Placement plan:")
	for i, operatorName := range operatorNameList {
		workerList := taskPlacementPlan[operatorName]
		log.Printf("  %d. Operator: %s\n", i+1, operatorName)
		for _, worker := range workerList {
			log.Printf(
				"   - Worker ID: %d, Address: %s\n",
				worker.WorkerId,
				worker.DataPlaneAddr,
			)
		}
	}
	log.Println()
	c.TaskPlacementPlan = taskPlacementPlan
}

// [custom task placement] Read the custom task placement file and load the
// task placement plan. Export this method for testing purposes
func (c *Coordinator) loadCustomTaskPlacementPlan() map[string]map[string]int {

	planPath := c.Config.CustomTaskPlacementFile
	file, err := os.Open(planPath)
	if err != nil {
		log.Fatalf(
			"Failed to open custom task placement file: %v\n",
			err,
		)
	}
	defer file.Close()

	// Store specified plan in the following format:
	// map[operatorId] -> map[ip addr] -> count
	// An operator can be deployed on multiple ip addresses and each ip can
	// hold multiple instances
	loadedPlan := make(map[string]map[string]int)

	// Scan the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			log.Fatalf(
				"Invalid line in custom task placement file: %s\n",
				line,
			)
		}

		operator := strings.TrimSpace(parts[0])
		addrs := strings.TrimSpace(parts[1])
		if _, ok := loadedPlan[operator]; !ok {
			loadedPlan[operator] = make(map[string]int)
		}
		loadedPlan[operator][addrs] += 1
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf(
			"Error reading custom task placement file: %v\n",
			err,
		)
	}

	return loadedPlan
}
