package query

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/kafka"
	"github.com/CASP-Systems-BU/koala/query/borg/models"
)

// BusyMachineWarmup pre-populates the state backend with incompressible dummy
// records so that state access latencies reflect steady-state behaviour from
// the first real event. No real computation or meaningful output is produced.
func BusyMachineWarmup(configFile string) *dataflow.Dataflow {
	rng := rand.New(rand.NewSource(1024))
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config BorgConfig
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}

	kafkaConfig := kafka.DefaultKafkaConsumerConfig()
	err = kafkaConfig.SetKey(
		"bootstrap.servers",
		config.KafkaClusterIPs[0]+":9092",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Make source parallelism the same as number of producers(partitions)
	sourceParallelism := len(config.ProducerIPs)

	src := dataflow.NewKafkaSource[*models.TaskEvent](
		"source",
		"borg-task",
		kafkaConfig,
	)
	src.SetParallelism(sourceParallelism)
	dataflow.AddOperator(df, src)

	jobEventKeyAssigner := ka.NewKeyAssigner(
		func(t *models.TaskEvent) string {
			return strconv.FormatInt(
				t.V3,
				10,
			) + "_" + strconv.FormatInt(
				t.V4,
				10,
			)
		},
	)

	maxListSize := config.ListStateSize

	jobEventStatsWarmup := dataflow.NewStatefulMapper(
		"jobEventStats",
		jobEventKeyAssigner,
		func(
			in *models.TaskEvent,
			state *stateType.ListState[*TaskRecord],
		) *TaskRecord {
			list := state.Get()

			// Already warmed — skip
			if len(list) > 0 {
				return &TaskRecord{}
			}

			keyHash := in.V3
			for _, c := range in.V6 {
				keyHash = keyHash*31 + int64(c)
			}

			startIndex := rng.Intn(len(dummyTaskRecords))
			list = make([]*TaskRecord, maxListSize)
			for i := range list {
				dummy := dummyTaskRecords[(startIndex+i)%len(dummyTaskRecords)]
				list[i] = &TaskRecord{
					V1:  dummy.V1 ^ (keyHash * int64(i+1)),
					V2:  dummy.V2 ^ (keyHash * int64(i+2)),
					V3:  dummy.V3 ^ (keyHash * int64(i+3)),
					V4:  dummy.V4 ^ (keyHash * int64(i+4)),
					V5:  dummy.V5,
					V6:  dummy.V6 + float64(keyHash*31+int64(i)),
					V7:  dummy.V7 + float64(keyHash*37+int64(i)),
					V8:  dummy.V8 + float64(keyHash*41+int64(i)),
					V9:  dummy.V9,
					V10: dummy.V10,
					V11: dummy.V11,
					V12: dummy.V12,
					V13: dummy.V13,
					V14: dummy.V14,
					V15: dummy.V15,
					V16: dummy.V16,
					V17: dummy.V17,
					V18: dummy.V18,
				}
				// Ensure V1 (timestamp) stays negative for IsWarmupRecord()
				if list[i].V1 >= 0 {
					list[i].V1 = -(list[i].V1) - 1
				}
			}

			state.Update(list)

			return &TaskRecord{}
		},
	)
	jobEventStatsWarmup.SetParallelism(config.JobEventStatsParallelism)
	dataflow.AddOperator(df, jobEventStatsWarmup)

	sink := dataflow.NewSink(
		"sink",
		func(t *TaskRecord) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, jobEventStatsWarmup)
	dataflow.Add1To1Stream(df, jobEventStatsWarmup, sink)

	return df
}
