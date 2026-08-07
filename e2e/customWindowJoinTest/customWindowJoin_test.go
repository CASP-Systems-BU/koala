package customWindowJoinTest

import (
	"log"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/koala/api/timestampAssigner"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	testutils "github.com/CASP-Systems-BU/koala/e2e/testUtils"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/constants"
	"github.com/CASP-Systems-BU/koala/state/stateBackend"
	"github.com/CASP-Systems-BU/koala/worker"
	"github.com/mus-format/mus-go/varint"
)

// Test CustomWindowJoin with ListState

// Total number of keys in the test
const Numn_Keys = 100

// Each time bucket generates all NUMKEYS keys (1 key per record). Each record
// is 10 time units apart e.g. 0, 10, 20, ..., 990
const Bucket_Span = Numn_Keys * 10

/*
To simplify the test, we construct the following data source pattern:

Stream 1:
| all timers fire here |-----------------|-----------------| generate records |

|		bucket 4	   |	 bucket 3	 |	  bucket 2	   |	 bucket 1

Stream 2:
|----------------------| generate records| generate records| generate records |

|		bucket 4	   |	 bucket 3	 |	  bucket 2	   |	 bucket 1

In this example, Num_Buckets_Per_Period = 4 buckets. "---" means no records are
records are generated in that time period. If the bucket generates records,
they generate same keys in every bucket. Each record is 10 time units apart.
Only stream 1 will register timers - scheduled all timers to the next
(Num_Buckets_Per_Period - 1) bucket. Stream 2 will just generate keys without
registering any timers.
Num_Buckets_Per_Period must be >= 2.
*/
const Num_Buckets_Per_Period = 4
const Period_Span = Num_Buckets_Per_Period * Bucket_Span

// Number of periods discussed above
const Num_Period = 1500

// Map for storing results for checking correctness
// map[period][key] = [sum1, sum2]
var results = make(map[int]map[int][2]int)

func TestCustomWindowJoin(t *testing.T) {

	if Num_Buckets_Per_Period < 2 {
		t.Error("Num_Buckets_Per_Period must be >= 2")
	}

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 4
	_, workers, _ := testutils.DeployJob(numWorkers, query, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(Period_Span * Num_Period)
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait to reach to the ending watermark
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t, workers)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func checkCorrectness(t *testing.T, workers []*worker.Worker) {

	// Expected results:
	// For each key in each period, the expected sum1 and sum2 are:
	// sum1 = period
	// sum2 = period * (Num_Buckets_Per_Period - 1)
	for period_id, keys_map := range results {

		// Each period should have all keys
		if len(keys_map) != Numn_Keys {
			t.Errorf(
				"Period %d: expect %d keys, got %d keys",
				period_id,
				Numn_Keys,
				len(keys_map),
			)
		}

		// Check sum1 and sum2 for each key
		for _, sums := range keys_map {
			if sums[0] != period_id {
				t.Errorf(
					"Period %d: expect sum1 %d, got %d",
					period_id,
					period_id,
					sums[0],
				)
			}
			if sums[1] != period_id*(Num_Buckets_Per_Period-1) {
				t.Errorf(
					"Period %d: expect sum2 %d, got %d",
					period_id,
					period_id*(Num_Buckets_Per_Period-1),
					sums[1],
				)
			}
		}
	}

	// Get all workers for join operator
	iters := make([]stateBackend.StateIterator, 0)
	for _, w := range workers {
		// Skip the sink and source
		if w.AssignedTask.IsSink() || w.AssignedTask.IsSource() {
			continue
		}

		// Get the state
		iters = append(iters, w.StateService.StateBackendImpl.GetIterator())
	}
	if len(iters) != 1 {
		t.Errorf("Expect %d join workers, got %d", 1, len(iters))
	}

	// A single key with 2 states can have 2 count in totalKeys
	totalKeys := 0
	leftKeys := make(map[int]bool)
	for _, iter := range iters {
		for iter.First(); iter.Valid(); iter.Next() {
			totalKeys++

			serializedKey := iter.Key()
			key, _, _ := varint.UnmarshalInt(
				serializedKey[constants.KeyPrefixSize:],
			)
			leftKeys[key] = true
		}
	}
	// There should be only 1 key left at the end: last record (-1) for
	// triggerring the final watermark from each source
	if totalKeys != 2 || len(leftKeys) != 1 || !leftKeys[-1] {
		t.Errorf("Expect only -1 key in state at the end, got %d", totalKeys)
	}
}

func query() *dataflow.Dataflow {
	query := dataflow.NewDataflow()

	// Define source 1
	source1 := dataflow.NewSource[*tuple.Tuple3[int, int, int64]](
		"source1",
		func(co collector.Collector) {

			for period := range Num_Period {

				// Each period, only the 1st bucket generates records
				base := int64(period * Period_Span)
				for key := range Numn_Keys {
					timestamp := base + int64(key*10)
					// Output record:
					// V1: key
					// V2: current period id
					// V3: timestamp
					co.Emit(&tuple.Tuple3[int, int, int64]{
						V1: key,
						V2: period,
						V3: timestamp,
					})
				}
			}

			// Now send a record with ending timestamp to trigger the watermark
			// for final timers
			co.Emit(&tuple.Tuple3[int, int, int64]{
				V1: -1,
				V2: -1,
				V3: int64(Period_Span * Num_Period),
			})
			log.Println("   SOUECE1: all data emitted")
		},
	)
	tsAssigner1 := ta.NewTimestampAssigner(
		func(t *tuple.Tuple3[int, int, int64]) int64 {
			return t.V3
		},
	)
	source1.AssignTimestampAndWatermark(tsAssigner1, 100*time.Millisecond, 0)
	source1.SetParallelism(1)
	dataflow.AddOperator(query, source1)

	// Define source 2
	source2 := dataflow.NewSource[*tuple.Tuple3[int, int, int64]](
		"source2",
		func(co collector.Collector) {

			for period := range Num_Period {

				// Only the first (Num_Buckets_Per_Period - 1) buckets generate
				// records
				period_base := int64(period * Period_Span)
				for bucket := range Num_Buckets_Per_Period - 1 {
					base := period_base + int64(bucket*Bucket_Span)

					for key := range Numn_Keys {
						timestamp := base + int64(key*10)
						// Output record:
						// V1: key
						// V2: current period id
						// V3: timestamp
						co.Emit(&tuple.Tuple3[int, int, int64]{
							V1: key,
							V2: period,
							V3: timestamp,
						})
					}
				}
			}

			// Now send a record with ending timestamp to trigger the watermark
			// for final timers
			co.Emit(&tuple.Tuple3[int, int, int64]{
				V1: -1,
				V2: -1,
				V3: int64(Period_Span * Num_Period),
			})
			log.Println("   SOUECE2: all data emitted")
		},
	)
	tsAssigner2 := ta.NewTimestampAssigner(
		func(t *tuple.Tuple3[int, int, int64]) int64 {
			return t.V3
		},
	)
	source2.AssignTimestampAndWatermark(tsAssigner2, 100*time.Millisecond, 0)
	source2.SetParallelism(1)
	dataflow.AddOperator(query, source2)

	// KeyAssigner for source1 output
	keyAssigner1 := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[int, int, int64]) int {
			return t.V1
		},
	)

	// KeyAssigner for source2 output
	keyAssigner2 := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[int, int, int64]) int {
			return t.V1
		},
	)

	// Define CustomWindowJoin operator
	customWindowJoin := dataflow.NewCustomWindowJoin[*tuple.Tuple4[int, int, int, int]](
		"customWindowJoin",
		/************************* 1st input stream **************************/
		source1,
		keyAssigner1,
		// UDF for processing each input record from the 1st upstream
		func(
			in *tuple.Tuple3[int, int, int64],
			// State1: list of records from stream 1
			// V1: key of the record
			// V2: period id of the record
			// V3: timestamp of the record
			state1 *stateType.ListState[*tuple.Tuple3[int, int, int64]],
			// State2: list of records from stream 2
			// V1: key of the record
			// V2: period id of the record
			// V3: timestamp of the record
			state2 *stateType.ListState[*tuple.Tuple3[int, int, int64]],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {

			// Register timer to the last bucket of the current period
			firing_timestamp := in.V3 + int64(
				(Num_Buckets_Per_Period-1)*Bucket_Span,
			)
			timerService.RegisterTimer(firing_timestamp)

			// Add the record to state1
			state1.Add(
				&tuple.Tuple3[int, int, int64]{V1: in.V1, V2: in.V2, V3: in.V3},
			)
		},
		/************************* 2nd input stream **************************/
		source2,
		keyAssigner2,
		// UDF for processing each input record from the 2nd upstream
		func(
			in *tuple.Tuple3[int, int, int64],
			state1 *stateType.ListState[*tuple.Tuple3[int, int, int64]],
			state2 *stateType.ListState[*tuple.Tuple3[int, int, int64]],
			timerService dataflow.TimerService,
			co collector.Collector,
		) {

			// Add the record to state2
			state2.Add(
				&tuple.Tuple3[int, int, int64]{V1: in.V1, V2: in.V2, V3: in.V3},
			)
		},
		/****************************** OnTimer ******************************/
		func(timestamp int64, state1 *stateType.ListState[*tuple.Tuple3[int, int, int64]], state2 *stateType.ListState[*tuple.Tuple3[int, int, int64]], co collector.Collector) {

			// Upon timer firing, join all records in state1 and state2
			list1 := state1.Get()
			list2 := state2.Get()

			// We only care about the state within the period time window
			// Calculate the period that this timer belongs to
			period_id := int(timestamp / Period_Span)
			startTime := int64(period_id * Period_Span)
			endTime := startTime + int64(Period_Span)

			// Sum up all period ids in list1
			sum1 := 0
			for _, rec1 := range list1 {
				if rec1.V3 >= startTime && rec1.V3 < endTime {
					sum1 += rec1.V2
				}
			}

			// Sum up all period ids in list2
			sum2 := 0
			for _, rec2 := range list2 {
				if rec2.V3 >= startTime && rec2.V3 < endTime {
					sum2 += rec2.V2
				}
			}

			// Clear the states at the last timer for testing purposes
			if timestamp >= int64((Num_Period-1)*Period_Span) {
				state1.Clear()
				state2.Clear()
			}

			// Output one record with the sums
			// V1: key
			// V2: period id of this timer
			// V3: sum1
			// V4: sum2
			co.Emit(&tuple.Tuple4[int, int, int, int]{
				V1: list1[0].V1, // both list must not be empty
				V2: period_id,
				V3: sum1,
				V4: sum2,
			})
		},
	)
	customWindowJoin.SetParallelism(1)
	dataflow.AddOperator(query, customWindowJoin)

	// Define sink
	sink := dataflow.NewSink(
		"sink",
		// Input record:
		// V1: key
		// V2: period id
		// V3: sum of period ids from state1
		// V4: sum of period ids from state2
		func(t *tuple.Tuple4[int, int, int, int]) {

			// Store the results
			key := t.V1
			period := t.V2
			sum1 := t.V3
			sum2 := t.V4

			if _, exist := results[period]; !exist {
				results[period] = make(map[int][2]int)
			}
			if _, exist := results[period][key]; exist {
				log.Fatalln(
					"Duplicate key in results:",
					"period",
					period,
					"key",
					key,
				)
			}
			results[period][key] = [2]int{sum1, sum2}
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect the operators
	dataflow.Add2To1Stream(query, source1, source2, customWindowJoin)
	dataflow.Add1To1Stream(query, customWindowJoin, sink)

	return query
}
