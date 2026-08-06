package coordinator_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Test the timeout setup for metric collector database concurrent writes

// DB config: request timeout if database is locked -> fail if timeouts
const timeoutIntervalMS = 1000

// Test 1000 concurrent writers - simulate number of workers
const numWriters = 1000

// Number of writes per routine
const writesPerRoutine = 10

// Sleep duration between two consecutive writes
const sleepDurationSecond = 1 * time.Second

func TestMetricDBConcurrentWrites(t *testing.T) {

	// Create the directory if it does not exist
	metricDBPath := "data/metricDB/test_metricCollector.db"
	dirPath := filepath.Dir(metricDBPath)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating directory: %v\n", err)
	}

	// Open a database with busy timeout of 1000 milliseconds
	metricDb, err := sql.Open(
		"sqlite3",
		metricDBPath+"?_journal_mode=WAL&_busy_timeout="+
			fmt.Sprintf("%d", timeoutIntervalMS),
	)
	if err != nil {
		t.Fatalf("Error opening database: %v\n", err)
	}

	// Create the metrics table
	createMetricsTable := `CREATE TABLE IF NOT EXISTS metrics (operator_id text,timestamp datetime, metric_type text, metric_value float);`
	_, err = metricDb.Exec(createMetricsTable)
	if err != nil {
		log.Fatalf("Error creating metrics table: %v\n", err)
	}

	// Create 4 sample records to write
	valueStrings := make([]string, 0, 4)
	valueArgs := make([]any, 0, 4*4)
	for i := range 4 {
		valueStrings = append(valueStrings, "(?,?,?,?)")
		valueArgs = append(
			valueArgs,
			"operator_1",
			time.Now(),
			"cpu_usage",
			0.5+float64(i),
		)
	}

	stmtStr := fmt.Sprintf(
		"INSERT INTO metrics (operator_id, timestamp, metric_type, metric_value) VALUES %s;",
		strings.Join(valueStrings, ","),
	)

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := range numWriters {
		go writeExecutor(i, metricDb, stmtStr, valueArgs, &wg, t)
	}
	wg.Wait()

	// Remove the db file
	err = metricDb.Close()
	if err != nil {
		t.Fatalf("Error closing database: %v\n", err)
	}
	removeDbDir()
}

func writeExecutor(
	writerIdx int,
	db *sql.DB,
	stmtStr string,
	valueArgs []any,
	wg *sync.WaitGroup,
	t *testing.T,
) {
	defer wg.Done()

	for range writesPerRoutine {
		now := time.Now()
		stmt, err := db.Prepare(stmtStr)
		if err != nil {
			t.Errorf("Error preparing statement: %v\n", err)
			log.Fatalln()
		}
		defer stmt.Close()

		_, err = stmt.Exec(valueArgs...)
		if err != nil {
			t.Errorf("Error executing statement: %v\n", err)
			log.Fatalln()
		}

		// Only log the 1st writer for debugging
		if writerIdx == 0 {
			elapsed := time.Since(now)
			fmt.Printf(
				"Writer %d: Inserted records in %v\n",
				writerIdx,
				elapsed,
			)
		}

		// Sleep for a short duration to simulate 2 consecutive metric reports
		time.Sleep(sleepDurationSecond)
	}
}

func removeDbDir() {
	err := os.RemoveAll("data/")
	if err != nil {
		log.Fatalf("Error removing database directory: %v\n", err)
	}
}
