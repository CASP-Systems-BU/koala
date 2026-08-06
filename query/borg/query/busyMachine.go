package query

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/borg/models"
)

type BorgConfig struct {
	// We use the number of producers to be the sourceParallelism to gurantee
	// that each source task will only read from a single
	ProducerIPs              []string
	JobEventStatsParallelism int
	SinkParallelism          int
	KafkaClusterIPs          []string
	ListStateSize            int
}

// Override the UnmarshalJSON function to set default values
func (cfg *BorgConfig) UnmarshalJSON(data []byte) error {

	// Start with the default configuration
	*cfg = BorgConfig{
		ProducerIPs:              []string{},
		JobEventStatsParallelism: 1,
		SinkParallelism:          1,
		KafkaClusterIPs:          []string{"localhost"},
		ListStateSize:            100,
	}

	// Use alias to avoid infinite recursion
	type alias BorgConfig

	// Decode JSON and override any values that are set
	return json.Unmarshal(data, (*alias)(cfg))
}

// V1-V8: event data (timestamp, jobID, taskIndex, machineID, eventType,
// cpuRequest, ramRequest, localDiskRequest) V9-V18: stats (medianCpu,
// medianRam, medianDisk, count, meanCpu, meanRam, meanDisk, maxCpu, maxRam,
// maxDisk)
type TaskRecord = tuple.Tuple18[int64, int64, int64, int64, string, float64, float64, float64, float64, float64, float64, int64, float64, float64, float64, float64, float64, float64]

// IsWarmupRecord returns true if the record is a dummy/warmup record.
func IsWarmupRecord(r *TaskRecord) bool {
	return r.V1 < 0 // timestamp negative = warmup
}

// MedianFromFiltered computes the median of a field across filtered records.
func MedianFromFiltered(
	filtered []*TaskRecord,
	getVal func(*TaskRecord) float64,
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

// MeanFromFiltered computes the mean of a field across filtered records.
func MeanFromFiltered(
	filtered []*TaskRecord,
	getVal func(*TaskRecord) float64,
) float64 {
	if len(filtered) == 0 {
		return 0.0
	}
	sum := 0.0
	for _, r := range filtered {
		sum += getVal(r)
	}
	return sum / float64(len(filtered))
}

// MaxFromFiltered computes the max of a field across filtered records.
func MaxFromFiltered(
	filtered []*TaskRecord,
	getVal func(*TaskRecord) float64,
) float64 {
	if len(filtered) == 0 {
		return 0.0
	}
	m := math.Inf(-1)
	for _, r := range filtered {
		v := getVal(r)
		if v > m {
			m = v
		}
	}
	return m
}

// BusyMachine query keys task events by (jobID, eventType),
// maintains a bounded list of (cpuRequest, ramRequest, localDiskRequest) per
// key,
// and computes running medians, means, and max for all three resource types.
// Output is per job ID per event type.
//
// During normal operation, real events are appended, sorted by timestamp,
// and oldest entries (including any remaining dummies) are evicted once the
// list exceeds ListStateSize.
func BusyMachine(configFile string) *dataflow.Dataflow {
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
	// type TaskEvent = tuple.Tuple13[
	// int64,   // timestamp
	// string,  // missingInfo
	// int64,   // jobID
	// int64,   // taskIndex
	// int64,   // machineID
	// string,  // eventType
	// string,  // userName
	// int64,   // schedulingClass
	// int64,   // priority
	// float64, // cpuRequest
	// float64, // RAMRequest
	// float64, // localDiskRequest
	// bool,    //  constraint
	// ]
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

	getCpu := func(r *TaskRecord) float64 { return r.V6 }
	getRam := func(r *TaskRecord) float64 { return r.V7 }
	getDisk := func(r *TaskRecord) float64 { return r.V8 }
	jobEventStats := dataflow.NewStatefulMapper(
		"jobEventStats",
		jobEventKeyAssigner,
		func(
			in *models.TaskEvent,
			state *stateType.ListState[*TaskRecord],
		) *TaskRecord {
			list := state.Get()
			if len(list) == 0 {
				log.Println("New key", strconv.FormatInt(in.V3, 10)+"_"+in.V6)
			}

			rec := &TaskRecord{
				V1: in.V1,  // timestamp
				V2: in.V3,  // jobID
				V3: in.V4,  // taskIndex
				V4: in.V5,  // machineID
				V5: in.V6,  // eventType
				V6: in.V10, // cpuRequest
				V7: in.V11, // ramRequest
				V8: in.V12, // localDiskRequest
			}
			list = append(list, rec)

			sort.Slice(list, func(i, j int) bool {
				return list[i].V1 > list[j].V1 // descending by timestamp - most recent first
			})
			if len(list) > maxListSize {
				list = list[:maxListSize]
			}
			var filtered []*TaskRecord
			for _, r := range list {
				if !IsWarmupRecord(r) {
					filtered = append(filtered, r)
				}
			}
			n := len(filtered)

			medianCpu := MedianFromFiltered(filtered, getCpu)
			medianRam := MedianFromFiltered(filtered, getRam)
			medianDisk := MedianFromFiltered(filtered, getDisk)

			// Store stats in the record — entire event + stats persisted to
			// Pebble Store stats in the record — entire event + stats
			// persisted to Pebble
			rec.V9, rec.V10, rec.V11 = medianCpu, medianRam, medianDisk
			rec.V12 = int64(n)
			rec.V12 = int64(n)
			state.Update(list)

			return rec
		},
	)
	jobEventStats.SetParallelism(config.JobEventStatsParallelism)
	dataflow.AddOperator(df, jobEventStats)

	sink := dataflow.NewSink(
		"sink",
		func(t *TaskRecord) {},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, src, jobEventStats)
	dataflow.Add1To1Stream(df, jobEventStats, sink)

	return df
}
