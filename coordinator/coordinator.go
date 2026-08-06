package coordinator

import (
	"sync/atomic"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/keyby"
)

type Coordinator struct {

	// Global config
	Config *configuration.Configuration

	// User-defined dataflow: 1 Coordinator per dataflow
	Dataflow *dataflow.Dataflow

	// WorkerManager for managing workers
	WorkerManager *WorkerManager

	// Task placement plan
	// Map: Operator ID -> list of workers hosting the operator tasks
	TaskPlacementPlan map[string][]*ManagedWorker

	// [Temporary] Workers whose previously deployed tasks are removed
	// This is a temporary (to be deleted) structure for lazy protocol to keep
	// acces to the remaining state on removed workers
	// Map: Operator ID -> list of removed workers that previously hosted the
	// operator tasks
	RemovedWorkers map[string][]*ManagedWorker

	// KeyPartitions: key space partitioning for each stateful operator
	// Map: Operator ID -> key space partition table
	// Only stateful operators are presnet in this map
	KeyPartitions map[string]*keyby.PartitionTable

	// [Lazy protocol] State Lookup Table: actual state location
	// Maintain a centralized view of state lookup table for all stateful
	// operators. This table can differ from KeyPartitions based on actual
	// state location. Note that the view Coordinator maintains can be stale
	// and the ground truth of state location is owned by local workers based
	// on actual state migration
	// [Lazy-by-key] Same as KeyPartitions since coordinator will not keep
	// track of actual state location for keys. Consider deprecate this
	StateLookupTables map[string]*keyby.PartitionTable

	// Atomic flag to indicate if there is an ongoing reconfiguration. We do
	// not allow concurrent reconfigurations
	ReconfigInProgress atomic.Bool
}

func NewCoordinator(
	config *configuration.Configuration,
	df *dataflow.Dataflow,
) *Coordinator {

	coordinator := &Coordinator{
		Config:         config,
		Dataflow:       df,
		WorkerManager:  NewWorkerManager(),
		RemovedWorkers: make(map[string][]*ManagedWorker),
		KeyPartitions:  make(map[string]*keyby.PartitionTable),
	}

	// Need to maintain a centralized view of state lookup tables if lazy
	// protocol is used
	if config.ReconfigProtocol == "lazy" {
		coordinator.StateLookupTables = make(map[string]*keyby.PartitionTable)
	}

	return coordinator
}

func (c *Coordinator) Run() {

	// Start the control plane service for Worker registration
	go c.StartControlPlaneService()

	// Start the user API service
	go c.StartAPIService()

	// Start the metric collector service
	go c.StartMetricCollectorService()

	// Wait
	wait := make(chan struct{})
	<-wait
}
