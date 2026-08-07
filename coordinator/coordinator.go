package coordinator

import (
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
)

type Coordinator struct {

	// Global config
	Config *configuration.Configuration

	// User-defined dataflow: 1 Coordinator per dataflow
	Dataflow *dataflow.Dataflow

	// WorkerManager for managing workers
	WorkerManager *WorkerManager

	// Task placement plan
	// Key: Operator ID, Value: List of selected workers
	TaskPlacementPlan map[string][]*ManagedWorker

	// KeyPartitions: expected key space partition for each stateful operator
	// Key: Stateful operator ID, Value: Key space partition
	// Only stateful operators are presnet in this map
	KeyPartitions map[string]*keyby.KeyLookupTable

	// [Lazy protocol] State Lookup Table: actual state location
	// Maintain a centralized view of state lookup table for all stateful
	// operators. This table can differ from KeyPartitions based on actual
	// state location
	StateLookupTables map[string]*keyby.KeyLookupTable
}

func NewCoordinator(
	config *configuration.Configuration,
	df *dataflow.Dataflow,
) *Coordinator {

	coordinator := &Coordinator{
		Config:        config,
		Dataflow:      df,
		WorkerManager: NewWorkerManager(),
		KeyPartitions: make(map[string]*keyby.KeyLookupTable),
	}

	// Need to maintain a centralized view of state lookup tables if lazy
	// protocol is used
	if config.ReconfigProtocol == "lazy" {
		coordinator.StateLookupTables = make(map[string]*keyby.KeyLookupTable)
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
