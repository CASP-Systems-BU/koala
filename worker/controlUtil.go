package worker

import (
	"context"
	"log"
	"time"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Request a remote worker to pull state for migration
func (w *Worker) requestMigration(migrationInfo *pb.MigrationInfo) {

	// Establish connection to remote state service and pull state
	conn, err := grpc.NewClient(
		migrationInfo.TargetWorkerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// The default message size limit is 4 MB. Increase it to 100 MB for
		// large message size
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(500*1024*1024), // 100 MB
			grpc.MaxCallSendMsgSize(500*1024*1024), // 100 MB
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the remote state service: %v", err)
	}
	defer conn.Close()
	client := pb.NewStateCommServiceClient(conn)

	bucketsToBePulled := &pb.BucketsToBePulled{
		BucketRanges: migrationInfo.BucketRanges,
	}

	// Migrate in-memory task metadata
	taskMetadata, err := client.MigrateTaskMetadata(
		context.Background(),
		bucketsToBePulled,
	)
	if err != nil {
		log.Fatalf(
			"Failed to migrate in-memory task metadata from the remote worker: %v",
			err,
		)
	}

	if len(taskMetadata.Metadata) > 0 {
		if w.AssignedTask == nil {

			// If this is new task to be started, store the metadata in worker
			// memory for later setup. The task can concurrently fetch metadata
			// from multiple remote workers - sync the access
			w.TaskInitMetadata = append(
				w.TaskInitMetadata,
				taskMetadata.Metadata,
			)
		} else {

			// This is an existing task being reconfigured - directly insert the
			// migrated metadata
			w.AssignedTask.InsertMigratedMetadata(taskMetadata.Metadata)
		}

		log.Printf(
			"[Stop-and-restart metadata] Worker %d fetched in-memory task metadata: %d Bytes\n",
			w.WorkerId,
			len(taskMetadata.Metadata),
		)
	}

	// For non-remote state backends, pull state partitions
	if w.Config.StateBackendType != "tikv" {
		w.applyStateMigration(
			client,
			bucketsToBePulled,
			migrationInfo.TargetWorkerAddr,
		)
	}
}

func (w *Worker) applyStateMigration(
	client pb.StateCommServiceClient,
	bucketsToBePulled *pb.BucketsToBePulled,
	targetWorkerAddr string,
) {

	// Migrate state from state service
	stream, err := client.PullStatePartition(
		context.Background(),
		bucketsToBePulled,
	)
	if err != nil {
		log.Fatalf(
			"Failed to create stream to the remote state service: %v",
			err,
		)
	}

	// Read a state chunk at an iteration
	totalFetchedStateBytes := 0
	for {

		chunk, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive state chunk: %v", err)
		}

		// Check the end of stream message
		if chunk.EndOfStream {
			break
		}

		log.Printf(
			"[State migration] Received a state chunk with %d records\n",
			len(chunk.Keys),
		)

		// Write the state chunk to the local state store
		keys := chunk.Keys
		values := chunk.Values
		if len(keys) != len(values) || len(keys) == 0 {
			log.Fatalf("Invalid state chunk: %v\n", chunk)
		}

		w.StateService.SetMany(keys, values)

		// Sum up total number of bytes fetched
		for _, key := range keys {
			totalFetchedStateBytes += len(key)
		}
		for _, value := range values {
			totalFetchedStateBytes += len(value)
		}
	}

	// Log the fetched state size
	log.Printf(
		"[Stop-and-restart INFO] Fetched state from worker %s: %f MB\n",
		targetWorkerAddr,
		float64(totalFetchedStateBytes)/1024.0/1024.0,
	)
}

// [DRRS] Background routine to push state to target workers for migration
func (w *Worker) pushStateMigrationDRRS(migrationInfo *pb.MigrationInfo) {

	start := time.Now()
	// Establish connection to remote state service
	conn, err := grpc.NewClient(
		migrationInfo.TargetWorkerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// The default message size limit is 4 MB. Increase it to 100 MB for
		// large message size
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(500*1024*1024), // 100 MB
			grpc.MaxCallSendMsgSize(500*1024*1024), // 100 MB
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the remote state service: %v", err)
	}
	defer conn.Close()
	client := pb.NewStateCommServiceClient(conn)

	stream, err := client.PushStateMigrationDRRS(
		context.Background(),
	)
	if err != nil {
		log.Fatalf(
			"Failed to create stream to the remote state service: %v",
			err,
		)
	}

	// Iterate over buckets to be migrated
	totalStateSentBytes := 0
	for _, bucketRange := range migrationInfo.BucketRanges {
		for bucketId := bucketRange.LowerBucketIdx; bucketId <= bucketRange.UpperBucketIdx; bucketId++ {

			// Get all states for the bucket
			keys, values := w.StateService.ReadLocalBucketRange(
				bucketId,
				bucketId,
			)

			// Send the bucket state in a message
			bucketState := &pb.BucketState{
				BucketId: bucketId,
				Keys:     keys,
				Values:   values,
			}
			if err := stream.Send(bucketState); err != nil {
				log.Fatalf(
					"Failed to send bucket state for bucket %d: %v",
					bucketId,
					err,
				)
			}

			// Sum up total number of bytes sent
			for _, key := range keys {
				totalStateSentBytes += len(key)
			}
			for _, value := range values {
				totalStateSentBytes += len(value)
			}
		}
	}

	// Close the stream and receive response
	reply, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf(
			"Failed to receive response after pushing state migration: %v",
			err,
		)
	}
	if reply.Info != "All state buckets received" {
		log.Fatalf(
			"Unexpected response after pushing state migration: %s",
			reply.Info,
		)
	}

	// End of state migration for current Push channel
	log.Printf(
		"[Worker %d DRRS Migration Push INFO] Pushed state to worker %s in %v. State sent: %f MB\n",
		w.WorkerId,
		migrationInfo.TargetWorkerAddr,
		time.Since(start),
		float64(totalStateSentBytes)/1024.0/1024.0,
	)
}
