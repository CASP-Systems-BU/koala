package query

import (
	"encoding/json"
	"log"
	"os"
	"sort"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	ka "github.com/CASP-Systems-BU/koala/api/keyAssigner"
	"github.com/CASP-Systems-BU/koala/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/kafka"
	"github.com/CASP-Systems-BU/koala/query/azure/models"
)

type AzureConfig struct {
	// We use the number of producers to be the sourceParallelism to gurantee
	// that each source task will only read from a single kafka partition.
	ProducerIPs                []string
	CpuReadingStatsParallelism int
	SinkParallelism            int
	KafkaClusterIPs            []string
	ListStateSize              int
}

// Override the UnmarshalJSON function to set default values
func (cfg *AzureConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = AzureConfig{
		ProducerIPs:                []string{},
		CpuReadingStatsParallelism: 1,
		SinkParallelism:            1,
		KafkaClusterIPs:            []string{"localhost"},
		ListStateSize:              100,
	}

	// Use alias to avoid infinite recursion
	type alias AzureConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// V1 timestamp, V2 vmID, V3 minCpu, V4 maxCpu, V5 avgCpu
// Stats: V6-V8 median
type CpuRecord = tuple.Tuple9[int64, string, float64, float64, float64, float64, float64, float64, int64]

// IsWarmupRecord returns true if the record is a dummy/warmup record.
func IsWarmupRecord(r *CpuRecord) bool {
	return r.V1 < 0
}

func medianFromFiltered(
	filtered []*CpuRecord,
	getVal func(*CpuRecord) float64,
) float64 {
	if len(filtered) == 0 {
		return 0.0
	}
	vals := make([]float64, len(filtered))
	for i, r := range filtered {
		vals[i] = getVal(r)
	}
	sort.Float64s(vals)
	return vals[len(vals)/2]
}

// CPUReadings query keys Azure events by vmID,
// maintains a bounded list of (minCpu, maxCpu, avgCpu) per key, and computes
// running medians, means, and max for all three metrics.
// During normal operation, real events are appended, sorted by timestamp,
// and oldest entries are evicted once the list exceeds ListStateSize.
func CPUReadings(configFile string) *dataflow.Dataflow {
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

	getMinCpu := func(r *CpuRecord) float64 { return r.V3 }
	getMaxCpu := func(r *CpuRecord) float64 { return r.V4 }
	getAvgCpu := func(r *CpuRecord) float64 { return r.V5 }

	cpuReadingStats := dataflow.NewStatefulMapper(
		"cpuReadingStats",
		vmKeyAssigner,
		func(
			in *models.AzureEvent,
			state *stateType.ListState[*CpuRecord],
		) *CpuRecord {
			list := state.Get()

			rec := &CpuRecord{
				V1: in.V1,
				V2: in.V2,
				V3: in.V3,
				V4: in.V4,
				V5: in.V5,
			}
			list = append(list, rec)

			// Sort by timestamp descending - most recent first
			sort.Slice(list, func(i, j int) bool {
				return list[i].V1 > list[j].V1
			})
			if len(list) > maxListSize {
				list = list[:maxListSize]
			}

			var filtered []*CpuRecord
			for _, r := range list {
				if !IsWarmupRecord(r) {
					filtered = append(filtered, r)
				}
			}
			n := len(filtered)

			medianMin := medianFromFiltered(filtered, getMinCpu)
			medianMax := medianFromFiltered(filtered, getMaxCpu)
			medianAvg := medianFromFiltered(filtered, getAvgCpu)

			rec.V6, rec.V7, rec.V8 = medianMin, medianMax, medianAvg
			rec.V9 = int64(n)

			state.Update(list)

			return &CpuRecord{
				V1: in.V1,
				V2: in.V2,
				V3: in.V3,
				V4: in.V4,
				V5: in.V5,
				V6: medianMin,
				V7: medianMax,
				V8: medianAvg,
				V9: int64(n),
			}
		},
	)
	cpuReadingStats.SetParallelism(config.CpuReadingStatsParallelism)
	dataflow.AddOperator(df, cpuReadingStats)

	sink := dataflow.NewSink(
		"sink",
		func(*CpuRecord) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, cpuReadingStats)
	dataflow.Add1To1Stream(df, cpuReadingStats, sink)

	return df
}
