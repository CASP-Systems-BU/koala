package query

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/models"
)

func CPUReadingsWarmup(configFile string) *dataflow.Dataflow {
	rand := rand.New(rand.NewSource(1024))
	df := dataflow.NewDataflow()

	// Reading config info from json file
	var config AzureConfig
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

	src := dataflow.NewKafkaSource[*models.AzureEvent](
		"source",
		"azure",
		kafkaConfig,
	)
	src.SetParallelism(sourceParallelism)
	dataflow.AddOperator(df, src)

	vmKeyAssigner := ka.NewKeyAssigner(
		func(t *models.AzureEvent) string {
			return t.V2
		},
	)

	maxListSize := config.ListStateSize

	cpuReadingStatsWarmup := dataflow.NewStatefulMapper(
		"cpuReadingStats",
		vmKeyAssigner,
		func(
			in *models.AzureEvent,
			state *stateType.ListState[*CpuRecord],
		) *CpuRecord {
			list := state.Get()

			if len(list) > 0 {
				return &CpuRecord{}
			}

			list = make([]*CpuRecord, maxListSize)
			
			for i := range list {
				startIndex := rand.Intn(len(dummyCpuRecords))
				d := dummyCpuRecords[(startIndex+i)%len(dummyCpuRecords)]
				v1 := d.V1 
				v9 := d.V9 
				// Ensure all int fields stay negative for IsWarmupRecord()
				if v1 >= 0 {
					v1 = ^v1
				}

				list[i] = &CpuRecord{
					V1: v1,
					V2: d.V2,
					V3: d.V3,
					V4: d.V4,
					V5: d.V5,
					V6: d.V6,
					V7: d.V7,
					V8: d.V8,
					V9: v9,
				}
			}

			state.Update(list)
			return &CpuRecord{}
		},
	)
	cpuReadingStatsWarmup.SetParallelism(config.CpuReadingStatsParallelism)
	dataflow.AddOperator(df, cpuReadingStatsWarmup)

	// No-op sink — discard all output
	sink := dataflow.NewSink(
		"sink",
		func(t *CpuRecord) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, cpuReadingStatsWarmup)
	dataflow.Add1To1Stream(df, cpuReadingStatsWarmup, sink)

	return df
}
