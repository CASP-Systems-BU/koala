package multiSourceTest

import (
	"log"
	"testing"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// In this test, we test when have multiple source setup
// each source should get assigned a unique replica id

var NUM_SOURCE = 10
var sourceChan = make(chan int, NUM_SOURCE)

func TestMultiSource(t *testing.T) {

	sourceChan = make(chan int, NUM_SOURCE)

	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 11
	_, workers, _ := testutils.DeployJob(numWorkers, multiSourceQuery, config)

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check sink
	seen := make(map[int64]bool)
	for _, worker := range workers {
		if worker.AssignedTask.IsSource() {
			replicaID := worker.AssignedTask.(*dataflow.Source[*tuple.Tuple2[string, int64]]).ReplicaID
			if replicaID == -1 {
				t.Errorf("Source replica id is not assigned")
			}
			if _, ok := seen[replicaID]; ok {
				t.Errorf(
					"Source replica id is not unique, get=%d",
					replicaID,
				)
			}
			seen[replicaID] = true
		}
	}

	// Test if replicaID 0 - 9 appears in seen
	for i := 0; i < NUM_SOURCE; i++ {
		if _, ok := seen[int64(i)]; !ok {
			t.Errorf("Source replica id is not assigned")
		}
	}

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func multiSourceQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewSource[*tuple.Tuple2[string, int64]](
		"source",
		func(co collector.Collector) {},
	)
	source.SetParallelism(NUM_SOURCE)
	dataflow.AddOperator(query, source)

	// Define sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[string, int64]) {},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, sink)

	return query
}
