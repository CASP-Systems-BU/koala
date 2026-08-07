package metricCollectorTest

import (
	"database/sql"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
)

// TestMetricCollector tests the metric collector
// It test if the metric collector is able to collect the metrics from the
// workers and store them in the database
var numWorkers = 3

func TestMetricCollector(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	testutils.DeployJob(numWorkers, query, config)

	// Wait for the test to be compeleted
	time.Sleep(6 * time.Second)

	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkTotalRecordsProcessed(t)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func query() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define Source
	source := dataflow.NewSource[*tuple.Tuple2[int, int]](
		"source",
		func(co collector.Collector) {
			startTime := time.Now()
			// First send the target keys, each key is repeated REPEAT times
			for time.Since(startTime) < 6*time.Second {
				co.Emit(&tuple.Tuple2[int, int]{
					V1: 1,
					V2: 1,
				})
				time.Sleep(50 * time.Millisecond)
			}
		},
	)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Define Sink
	sink := dataflow.NewSink("sink", func(in *tuple.Tuple2[int, int]) {})
	sink.SetParallelism(2)
	dataflow.AddOperator(query, sink)

	// Connect Source -> Sink
	dataflow.Add1To1Stream(query, source, sink)

	return query
}

func checkTotalRecordsProcessed(t *testing.T) {
	//open the database
	metricDb, err := sql.Open("sqlite3", "data/metricCollector.db")
	if err != nil {
		t.Fatalf("Error opening database: %v\n", err)
	}
	defer metricDb.Close()

	// Query the database group by operator_id and metric_type to get the
	// average value for each metric per operator
	query := "SELECT operator_id, metric_type, AVG(metric_value) as average_metric_value " +
		"FROM metrics " +
		"GROUP BY operator_id, metric_type; "
	rows, err := metricDb.Query(query)
	if err != nil {
		t.Fatalf("Error querying database: %v\n", err)
	}
	defer rows.Close()

	type metricKey struct {
		opID     string
		workerID string
	}
	metricMap := make(
		map[metricKey]map[string]float64,
	) // map[(opID, workerID)][metricType] = avgValue

	// traverse the rows
	for rows.Next() {
		var operatorId, metricType string
		var avgValue float64
		if err := rows.Scan(&operatorId, &metricType, &avgValue); err != nil {
			t.Errorf("Failed to scan row: %v", err)
			continue
		}

		// Validate format: opID:workerID
		splitOpId := strings.Split(operatorId, ":")
		opID := splitOpId[0]
		workerID := splitOpId[1]

		key := metricKey{opID, workerID}
		if _, exists := metricMap[key]; !exists {
			metricMap[key] = make(map[string]float64)
		}
		metricMap[key][metricType] = avgValue

		log.Printf(
			"Operator: %s, Worker: %s, Metric: %s, Avg: %.2f",
			opID,
			workerID,
			metricType,
			avgValue,
		)
	}

	if len(metricMap) == 0 {
		t.Fatal("No metrics found")
	}

	// Check multiple workers are represented
	workerCount := make(map[string]bool)
	for k := range metricMap {
		workerCount[k.workerID] = true
	}

	// verify we have metrics from each worker
	if len(workerCount) != numWorkers || len(metricMap) != numWorkers {
		t.Errorf(
			"Expected metrics from all workers, got only %d, %d",
			len(workerCount),
			len(metricMap),
		)
	}

	// Check each op-worker pair has multiple metrics
	for key, metrics := range metricMap {
		if len(metrics) < 2 {
			t.Errorf(
				"Expected multiple metrics for operator %s on worker %s, got only %d",
				key.opID,
				key.workerID,
				len(metrics),
			)
		}
	}
}
