package coordinator

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"database/sql"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

// MetricCollectorServer is for communication between the Metric Collector
// Service and Workers One directional RPC channel from Workers to metric
// collector service is used for all messages

// Note: we do not have graceful termination now. So db.Close() is not called
// when the server is stopped, and data is not guaranteed flushed in db file.
// However, The un-flushed data is guaranteed stored in WAL file - when start
// the DB for metric retrieval, the data in WAL file will be replayed e.g. it
// works for both go client and python client when we need to open the db file
// to retrieve metrics

type MetricCollectorServer struct {
	pb.UnimplementedMetricCollectorServiceServer
	metricDb *sql.DB
}

func (coord *Coordinator) StartMetricCollectorService() {

	// Start listening on the metric collector port
	lisAddr := coord.Config.CoordinatorMetricCollectorAddr
	metricInterval := coord.Config.MetricsInterval
	metricCollectorServer := &MetricCollectorServer{metricDb: nil}

	// Setup the gRPC server
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		log.Fatalf("Errow start Metric collector server: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterMetricCollectorServiceServer(grpcServer, metricCollectorServer)
	metricCollectorServer.StartMetricCollectorDatabase(metricInterval)

	log.Printf(
		"Metric collector service starts listening on %s ...\n",
		lisAddr,
	)

	grpcServer.Serve(lis)
}

func (s *MetricCollectorServer) StartMetricCollectorDatabase(
	metricInterval time.Duration,
) {

	// Hardcode the metric db path
	metricDBPath := "data/metricDB/metricCollector.db"

	// Create the directory if it does not exist
	dirPath := filepath.Dir(metricDBPath)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating directory: %v\n", err)
	}

	// Open the SQLite database in WAL mode with busy timeout duration as
	// metricInterval. WAL enables high concurrency.
	// Busy timeout defines how long to wait if there is another concurrent
	// write. We only allow timeout to be < metricInterval to avoid blocking
	// the next batch of metrics. But in WAL mode, this timeout duration
	// shouldn't be reached
	metricDb, err := sql.Open(
		"sqlite3",
		metricDBPath+"?_journal_mode=WAL&_busy_timeout="+fmt.Sprintf(
			"%d",
			metricInterval.Milliseconds(),
		),
	)
	if err != nil {
		log.Fatalf("Error opening database: %v\n", err)
	}
	s.metricDb = metricDb

	// Create the metrics table
	createMetricsTable := `CREATE TABLE IF NOT EXISTS metrics (operator_id text,timestamp datetime, metric_type text, metric_value float);`
	_, err = metricDb.Exec(createMetricsTable)
	if err != nil {
		log.Fatalf("Error creating metrics table: %v\n", err)
	}
}

func (s *MetricCollectorServer) CollectMetrics(
	stream pb.MetricCollectorService_CollectMetricsServer,
) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {

			// Allow graceful termination when worker closes the stream
			log.Printf(
				"[Coordinator metrics service] A task has closed the metric collector stream\n",
			)
			err = stream.SendAndClose(&pb.MetricCollectionResponse{
				Message: "Metric collector shutdown received",
			})
			if err != nil {
				log.Fatalf("Error sending shutdown ack: %v\n", err)
			}
			return nil
		}
		if err != nil {
			log.Fatalf("Error receiving message: %v\n", err)
		}

		// Process the received message
		s.storeMetrics(in)
	}
}

func (s *MetricCollectorServer) storeMetrics(metrics *pb.MetricBatch) {
	if len(metrics.Metrics) == 0 {
		return
	}

	operatorId := metrics.OperatorId
	timestamp := time.Now()

	valueStrings := make([]string, 0, len(metrics.Metrics))
	valueArgs := make([]any, 0, len(metrics.Metrics)*4)
	for _, m := range metrics.Metrics {
		valueStrings = append(valueStrings, "(?,?,?,?)")
		valueArgs = append(
			valueArgs,
			operatorId,
			timestamp,
			m.MetricType,
			m.MetricValue,
		)
	}

	stmtStr := fmt.Sprintf(
		"INSERT INTO metrics (operator_id, timestamp, metric_type, metric_value) VALUES %s;",
		strings.Join(valueStrings, ","),
	)

	stmt, err := s.metricDb.Prepare(stmtStr)
	if err != nil {
		log.Fatalln(
			"[ERROR] Couldn't prepare statement to insert metrics into the db:",
			err,
		)
	}

	_, err = stmt.Exec(valueArgs...)
	if err != nil {
		log.Fatalln(
			"[ERROR] Insert metrics failed:",
			err,
		)
	}
	stmt.Close()
}
