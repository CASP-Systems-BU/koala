package taxi

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	ka "github.com/CASP-Systems-BU/disaggregated-streaming/api/keyAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/stateClient/stateType"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/kafka"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/models"
	taxiUtils "github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/utils"
)

var dummyTaxiTrip []models.RouteInfo

// Find the top 10 most frequent routes during the last 30 minute.
func TaxiWarmup(configFile string) *dataflow.Dataflow {
	rand := rand.New(rand.NewSource(1024))
	df := dataflow.NewDataflow()
	// Reading config info from json file
	var config TaxiConfig
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

	taxiTripSource := dataflow.NewKafkaSource[*models.TaxiTrip](
		"source",
		"taxi-trip",
		kafkaConfig,
	)

	taxiTripSource.SetParallelism(sourceParallelism)
	dataflow.AddOperator(df, taxiTripSource)

	// CalculateCellId based on latitude and longitude
	calculateCellId := dataflow.NewFlatmap[*models.TaxiTrip, *models.RouteInfo](
		"calculateCellId",
		func(t *models.TaxiTrip, co collector.Collector) {
			startCellId, insideArea := taxiUtils.GetCellID(t.V11, t.V12)
			if insideArea {
				endCellId, insideArea := taxiUtils.GetCellID(t.V13, t.V14)
				if insideArea {
					route := models.RouteInfo{
						V1:  t.V1,
						V2:  t.V2,
						V3:  t.V3,
						V4:  t.V4,
						V5:  t.V5,
						V6:  t.V6,
						V7:  t.V7,
						V8:  t.V8,
						V9:  t.V9,
						V10: t.V10,
						V11: t.V11,
						V12: t.V12,
						V13: t.V13,
						V14: t.V14,
						V15: startCellId,
						V16: endCellId,
						// Calculate unique routeId
						V17: 90600*startCellId + endCellId,
					}
					co.Emit(&route)
				}
			}
		},
	)
	calculateCellId.SetParallelism(config.CalculateCellIdParallelism)
	dataflow.AddOperator(df, calculateCellId)

	//Key by routeId and vendorID
	routeKeyAssigner := ka.NewKeyAssigner(
		func(t *models.RouteInfo) string {
			return strconv.FormatInt(t.V17, 10) + "Vendor" + t.V3
		},
	)

	movingMedianTripTime := dataflow.NewStatefulMapper(
		"movingMedianTripTime",
		routeKeyAssigner,
		func(in *models.RouteInfo,
			// Store Route Info
			state *stateType.ListState[*models.RouteInfo],
		) *tuple.Tuple3[string, int64, int64] {
			input := &models.RouteInfo{
				V1:  in.V1,
				V2:  in.V2,
				V3:  in.V3,
				V4:  in.V4,
				V5:  in.V5,
				V6:  in.V6,
				V7:  in.V7,
				V8:  in.V8,
				V9:  in.V9,
				V10: in.V10,
				V11: in.V11,
				V12: in.V12,
				V13: in.V13,
				V14: in.V14,
				V15: in.V15,
				V16: in.V16,
				V17: in.V17,
			}
			rawList := state.Get()
			mostRecentTrips := make([]*models.RouteInfo, 0, config.ListLength+1)
			for _, oldTrip := range rawList {
				if oldTrip != nil {
					newTrip := *oldTrip
					mostRecentTrips = append(mostRecentTrips, &newTrip)
				}
			}
			mostRecentTrips = append(mostRecentTrips, input)
			startIndex := rand.Intn(len(dummyTaxiTrip))
			for i := len(mostRecentTrips); i < config.ListLength; i++ {
				dummyInput := dummyTaxiTrip[(startIndex+i)%len(dummyTaxiTrip)]
				dummyState := &models.RouteInfo{
					V1:  dummyInput.V1,
					V2:  dummyInput.V2,
					V3:  dummyInput.V3,
					V4:  dummyInput.V4,
					V5:  dummyInput.V5,
					V6:  dummyInput.V6,
					V7:  dummyInput.V7,
					V8:  dummyInput.V8,
					V9:  dummyInput.V9,
					V10: dummyInput.V10,
					V11: dummyInput.V11,
					V12: dummyInput.V12,
					V13: dummyInput.V13,
					V14: dummyInput.V14,
					V15: dummyInput.V15,
					V16: dummyInput.V16,
					V17: dummyInput.V17,
				}
				mostRecentTrips = append(mostRecentTrips, dummyState)
			}
			sort.Slice(mostRecentTrips, func(i, j int) bool {
				if mostRecentTrips[i].V7 == mostRecentTrips[j].V7 {
					return mostRecentTrips[i].V17 > mostRecentTrips[j].V17
				}
				return mostRecentTrips[i].V7 > mostRecentTrips[j].V7
			})

			state.Update(mostRecentTrips[:config.ListLength])
			return &tuple.Tuple3[string, int64, int64]{
				V1: strconv.FormatInt(in.V17, 10),
				V2: int64(0),
				V3: int64(0),
			}
		},
	)
	movingMedianTripTime.SetParallelism(config.MovingMedianTripTimeParallelism)
	dataflow.AddOperator(df, movingMedianTripTime)

	sink := dataflow.NewSink(
		"sink",
		func(t *tuple.Tuple3[string, int64, int64]) {
		},
	)
	sink.SetParallelism(config.SinkParallelism)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, taxiTripSource, calculateCellId)
	dataflow.Add1To1Stream(df, calculateCellId, movingMedianTripTime)
	//dataflow.Add1To1Stream(df, movingMedianTripTime, top10Route)
	dataflow.Add1To1Stream(df, movingMedianTripTime, sink)

	return df
}
