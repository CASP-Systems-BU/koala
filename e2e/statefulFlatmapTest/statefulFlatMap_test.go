package statefulFlatmapTest

import (
	"crypto/sha256"
	"encoding/binary"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	testutils "github.com/CASP-Systems-BU/disaggregated-streaming/e2e/testUtils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
)

// In this test, we define a dataflow to count the number of occurrences of
// each word in a stream of sentences.

var REPEAT = 100
var result map[string]int
var resultCount map[string]int64

func TestWordCountKey(t *testing.T) {
	result = make(map[string]int)
	resultCount = make(map[string]int64)
	//************************************************************
	// DEPLOYMENT
	//************************************************************

	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 3
	_, workers, _ := testutils.DeployJob(numWorkers, wordCountQuery, config)

	// Monitor Sink watermark progress to detect the end of the test
	// This flatmap query doesn't need watermark and timestamp assigner
	// We add them here only to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(10_000_000)

	// Sync channel to signal the end of the test
	done := make(chan struct{})

	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	checkCorrectness(t, result, resultCount)

	//************************************************************
	// CLEANUP
	//************************************************************
	testutils.CleanUpDataFolder()
}

func wordCountQuery() *dataflow.Dataflow {
	// Define query
	query := dataflow.NewDataflow()

	// Define source
	source := dataflow.NewSource[*tuple.Tuple2[string, int64]](
		"source",
		func(co collector.Collector) {
			passage := getPassage()
			for range REPEAT {
				for _, sentence := range passage {
					co.Emit(&tuple.Tuple2[string, int64]{
						V1: sentence,
						V2: 1,
					})
				}
			}

			co.Emit(&tuple.Tuple2[string, int64]{V1: "END", V2: 10_000_000})
		},
	)

	// We add watarmark and timestamp assigner here only to detect the end of
	// the test
	tsAssigner := ta.NewTimestampAssigner(
		func(t *tuple.Tuple2[string, int64]) int64 {
			return t.V2
		},
	)
	source.AssignTimestampAndWatermark(tsAssigner, 200*time.Millisecond, 0)
	source.SetParallelism(1)
	dataflow.AddOperator(query, source)

	// Define flatmap
	keyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple2[string, int64]) int64 {
			hash := sha256.Sum256([]byte(t.V1))
			return int64(binary.BigEndian.Uint64(hash[:8]))
		},
	)
	flatmap := dataflow.NewStatefulFlatmap[*tuple.Tuple2[string, int64], *tuple.Tuple2[string, int64]](
		"flatmap",
		keyAssigner,
		func(
			t *tuple.Tuple2[string, int64],
			state *stateType.ValueState[*tuple.Tuple1[int64]],
			co collector.Collector,
		) {
			words := strings.Split(t.V1, " ")

			// use the state to keep track of the count of each sentence
			count, exist := state.Get()
			if !exist {
				count = tuple.NewTuple1(int64(0))
			}
			count.V1 += 1
			state.Set(count)

			for _, word := range words {
				co.Emit(&tuple.Tuple2[string, int64]{
					V1: word,
					V2: count.V1,
				})
			}
		},
	)
	flatmap.SetParallelism(1)
	dataflow.AddOperator(query, flatmap)

	// Define sink
	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple2[string, int64]) {
			// Count the number of occurrences of each word
			word := t.V1
			result[word]++

			// Store counter from state
			resultCount[word] = max(t.V2, resultCount[word])
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(query, sink)

	// Connect OperatorBase
	dataflow.Add1To1Stream(query, source, flatmap)
	dataflow.Add1To1Stream(query, flatmap, sink)

	return query
}

// Passage from The Tale of Two Cities
// Source https://www.gutenberg.org/cache/epub/98/pg98-images.html
func getPassage() []string {
	return []string{
		"It was the best of times",
		"it was the worst of times",
		"it was the age of wisdom",
		"it was the age of foolishness",
		"it was the epoch of belief",
		"it was the epoch of incredulity",
		"it was the season of Light",
		"it was the season of Darkness",
		"it was the spring of hope",
		"it was the winter of despair",
		"we had everything before us",
		"we had nothing before us",
		"we were all going direct to Heaven",
		"we were all going direct the other way",
		"in short, the period was so far like the present period",
		"that some of its noisiest authorities insisted on its being received",
		"for good or for evil",
		"in the superlative degree of comparison only",
		"There were a king with a large jaw and a queen with a plain face",
		"on the throne of England",
		"there were a king with a large jaw and a queen with a fair face",
		"on the throne of France",
		"in both countries it was clearer than crystal to the lords of the State preserves of loaves and fishes",
		"that things in general were settled for ever",
	}
}

// Correct word count for the passage
func passageWordCount() map[string]int {
	passage := getPassage()
	wordCount := make(map[string]int)
	for _, sentence := range passage {
		words := strings.Split(sentence, " ")
		for _, word := range words {
			wordCount[word]++
		}
	}
	return wordCount
}

func checkCorrectness(
	t *testing.T,
	result map[string]int,
	resultCount map[string]int64,
) {

	correct := passageWordCount()
	for word, count := range result {
		if word == "END" {
			continue
		}
		if count != correct[word]*REPEAT {
			t.Errorf(
				"Word count mismatch for word %s. Expected %d, got %d",
				word,
				correct[word],
				count,
			)
		}
	}

	// Check if counter from state is correct
	// The counter for each word should be REPEAT
	for word, count := range resultCount {
		if word == "END" {
			continue
		}
		if count != int64(REPEAT) {
			t.Errorf(
				"Word count mismatch for word %s. Expected %d, got %d",
				word,
				REPEAT,
				count,
			)
		}
	}
}
