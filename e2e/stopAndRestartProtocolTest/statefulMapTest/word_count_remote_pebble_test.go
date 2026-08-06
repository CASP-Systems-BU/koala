package statefulMapTest

import (
	"context"
	"log"

	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
	"github.com/mus-format/mus-go/varint"
)

func TestStateMigrationWordCountRemotePebble(t *testing.T) {

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	config.StateBackendType = "remote-pebble"

	numWorkers := 5

	// start the remote pebble servers
	remotePebbleAddrs, remotePebbleBackends, stopRemotePebbleServers := testutils.StartRemotePebbleTestServers(
		2,
	)
	defer stopRemotePebbleServers()
	config.RemotePebbleAddrs = remotePebbleAddrs

	client, _, _ := testutils.DeployJob(
		numWorkers,
		func() *dataflow.Dataflow { return query(2) },
		config,
	)

	// Wait for 10s before rescaling
	time.Sleep(10 * time.Second)
	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   "statefulMapper",
		TargetParallelism: 3,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}
	log.Printf("Job rescale response: %v\n", resp.Info)

	// Wait for the test to be compeleted
	time.Sleep(40 * time.Second)

	/*************************************************
			CHECK CORRECTNESS
	*************************************************/
	checkCorrectnessRemotePebble(t, remotePebbleBackends)
	t.Logf("Sink recieved %d messages", sinkCounter)

	/*************************************************
			CLEANUP
	*************************************************/
	testutils.CleanUpDataFolder()
}

func checkCorrectnessRemotePebble(
	t *testing.T,
	remotePebbleBackends []*stateBackend.PebbleStateBackend,
) {

	// Get the stateful mappers
	iters := make([]stateBackend.StateIterator, 0, len(remotePebbleBackends))
	for _, backend := range remotePebbleBackends {
		iter := backend.GetIterator()
		iters = append(iters, iter)
		defer iter.Close()
	}

	// We track all existing keys in the map
	results := make(map[int64]int)

	// Iterate the state
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			keyI, _, _ := varint.UnmarshalInt64(key[constant.KeyPrefixSize:])
			valueI, _, _ := varint.UnmarshalInt(value)

			_, exist := results[keyI]
			if !exist {
				results[keyI] = valueI
			} else {
				// shouldn't happen since all keys are stored in remote pebble
				t.Errorf("Duplicate key %d found during state migration check", keyI)
			}
		}
	}

	// Check the number of appeared keys
	if len(results) != NUM_KEYS {
		t.Error("Expect ", NUM_KEYS, " keys, but got ", len(results))
	}

	// Check if the count for each key is correct
	for k, v := range results {
		if v != REPEAT {
			t.Error(
				"Expect ",
				REPEAT,
				" for key ",
				k,
				", but got ",
				v,
			)
		}
	}
}
