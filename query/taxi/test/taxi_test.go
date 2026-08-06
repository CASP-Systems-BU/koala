package query7Test

import (
	"log"
	"sort"
	"strconv"
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
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/models"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/utils"
)

// [Note] UPDATE THE QUERY ACCORDINGLY IN THIS FILE IF Taxi LOGIC CHANGES

// In this test, we use a fixed input set to test the correctness of taxi, by
// comparing output of the query with expected results

// type TaxiTrip = tuple.Tuple14[
// string,  // V1: medallion
// string,  // V2: hackLicense
// string,  // V3: vendorID
// int64,   // V4: rateCode
// string,  // V5: storeAndFwdFlag
// int64,   // V6: pickupDatetime
// int64,   // V7: dropoffDatetime
// int64,   // V8: passengerCount
// int64,   // V9: tripTimeSecs
// float64, // V10: tripDistance
// float64, // V11: pickupLongitude
// float64, // V12: pickupLatitude
// float64, // V13: dropoffLongitude
// float64, // V14: dropoffLatitude
// ]

var SampleInput []models.TaxiTrip = []models.TaxiTrip{
	// Window [0,400]
	// RouteId 27393107
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  0,
		V8:  1,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74.907599,
		V14: 41,
	},
	// RouteId 27393107
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  40,
		V8:  2,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74.907599,
		V14: 41,
	},
	// RouteId 27393259
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  50,
		V8:  3,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41,
	},
	// RouteId 27386637
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  60,
		V8:  4,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.1,
	},
	// RouteId 27380015
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  70,
		V8:  5,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.2,
	},
	// RouteId 27376704
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  80,
		V8:  6,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.25,
	},
	// RouteId 27375801
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  90,
		V8:  7,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.26,
	},
	// RouteId 27393107
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  100,
		V8:  8,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74.907599,
		V14: 41,
	},
	// RouteId 27375199
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  110,
		V8:  9,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.27,
	},
	// RouteId 27374597
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  120,
		V8:  10,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.28,
	},
	// RouteId 27373995
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  130,
		V8:  11,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.29,
	},
	// RouteId 27373092
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  140,
		V8:  12,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.30,
	},
	// RouteId 27393107
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  150,
		V8:  13,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74.907599,
		V14: 41,
	},
	// RouteId 27386637
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  160,
		V8:  14,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.1,
	},
	// RouteId 27380015
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  170,
		V8:  15,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.2,
	},
	// RouteId 27376704
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  180,
		V8:  16,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.25,
	},
	// RouteId 27375801
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  190,
		V8:  17,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.26,
	},
	// RouteId 27393259
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  200,
		V8:  18,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41,
	},
	// RouteId 27375199
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  210,
		V8:  19,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.27,
	},
	// RouteId 27374597
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  220,
		V8:  20,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.28,
	},
	// RouteId 27373995
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  230,
		V8:  21,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.29,
	},
	// RouteId 27373092
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  240,
		V8:  22,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.30,
	},
	// RouteId 27393107
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  370,
		V8:  23,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74.907599,
		V14: 41,
	},
	// RouteId 27372490
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  380,
		V8:  24,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.31,
	},
	// RouteId 27371888
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  390,
		V8:  25,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.32,
	},

	// Window[400, 800)
	// RouteId 27371888
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  410,
		V8:  26,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.32,
	},
	// RouteId 27371888
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  420,
		V8:  27,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.32,
	},
	// RouteId 27373995
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  730,
		V8:  28,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.29,
	},

	// Window[800, 1200)
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  810,
		V8:  29,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.32,
	},
	// RouteId 27371888
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  820,
		V8:  30,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.32,
	},
	// RouteId 27373995
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  1130,
		V8:  31,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.29,
	},

	// Ending watermark
	{
		V1:  "m01",
		V2:  "h01",
		V3:  "vendor",
		V4:  1,
		V5:  "N",
		V6:  0,
		V7:  1600,
		V8:  1,
		V9:  600,
		V10: 2.1,
		V11: -74.913585,
		V12: 41.474937,
		V13: -74,
		V14: 41.29,
	},
}

// Expected Result
// V1 to V10: RouteId of top 10 frequent routes

var SampleResults []tuple.Tuple20[
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64] = []tuple.Tuple20[
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64]{
	// Window [0,400)
	{
		V1:  "27393107Round0",
		V2:  8,
		V3:  "27393259Round0",
		V4:  3,
		V5:  "27386637Round0",
		V6:  4,
		V7:  "27380015Round0",
		V8:  5,
		V9:  "27376704Round0",
		V10: 6,
		V11: "27375801Round0",
		V12: 7,
		V13: "27375199Round0",
		V14: 9,
		V15: "27374597Round0",
		V16: 10,
		V17: "27373995Round0",
		V18: 11,
		V19: "27373092Round0",
		V20: 12,
	},
	// Window [400,800)
	{
		V1:  "27373995Round0",
		V2:  21,
		V3:  "27371888Round0",
		V4:  26,
		V5:  "0Round0",
		V6:  0,
		V7:  "0Round0",
		V8:  0,
		V9:  "0Round0",
		V10: 0,
		V11: "0Round0",
		V12: 0,
		V13: "0Round0",
		V14: 0,
		V15: "0Round0",
		V16: 0,
		V17: "0Round0",
		V18: 0,
		V19: "0Round0",
		V20: 0,
	},
	// Window [800,1200)
	{
		V1:  "27373995Round0",
		V2:  21,
		V3:  "27371888Round1",
		V4:  29,
		V5:  "0Round0",
		V6:  0,
		V7:  "0Round0",
		V8:  0,
		V9:  "0Round0",
		V10: 0,
		V11: "0Round0",
		V12: 0,
		V13: "0Round0",
		V14: 0,
		V15: "0Round0",
		V16: 0,
		V17: "0Round0",
		V18: 0,
		V19: "0Round0",
		V20: 0,
	},
}

var results []tuple.Tuple20[
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64,
	string, int64, string, int64]

func TestTaxiCorrectness(t *testing.T) {
	results = make([]tuple.Tuple20[
		string, int64, string, int64,
		string, int64, string, int64,
		string, int64, string, int64,
		string, int64, string, int64,
		string, int64, string, int64],
		0,
	)

	//************************************************************
	// DEPLOYMENT
	//************************************************************
	// Sync channel to signal the end of the test
	done := make(chan struct{})
	log.Println("[E2E] Starting the deployment")
	config := configuration.Default()
	numWorkers := 5
	_, workers, _ := testutils.DeployJob(numWorkers, Taxi, config)

	// Monitor Sink watermark progress to detect the end of the test
	var sink dataflow.Operator
	for _, w := range workers {
		if w.AssignedTask.IsSink() {
			sink = w.AssignedTask
			break
		}
	}
	expectedWM := int64(1600)
	// Wait till we receive the ending watermark
	go testutils.MonitorEndOfTest(sink, done, expectedWM)

	// Wait for the test to be completed
	<-done
	log.Println("[E2E] Test completed")

	//************************************************************
	// CHECK CORRECTNESS
	//************************************************************

	// Check number of result
	expectedCount := len(SampleResults)
	if len(results) != expectedCount {
		t.Errorf(
			"Incorrect amount of results=%d, expect=%d",
			len(results),
			expectedCount,
		)
	}

	// Check if the result exists
	for i, r := range results {
		if SampleResults[i].V1 != r.V1 ||
			SampleResults[i].V2 != r.V2 ||
			SampleResults[i].V3 != r.V3 ||
			SampleResults[i].V4 != r.V4 ||
			SampleResults[i].V5 != r.V5 ||
			SampleResults[i].V6 != r.V6 ||
			SampleResults[i].V7 != r.V7 ||
			SampleResults[i].V8 != r.V8 ||
			SampleResults[i].V9 != r.V9 ||
			SampleResults[i].V10 != r.V10 ||
			SampleResults[i].V11 != r.V11 ||
			SampleResults[i].V12 != r.V12 ||
			SampleResults[i].V13 != r.V13 ||
			SampleResults[i].V14 != r.V14 ||
			SampleResults[i].V15 != r.V15 ||
			SampleResults[i].V16 != r.V16 ||
			SampleResults[i].V17 != r.V17 ||
			SampleResults[i].V17 != r.V17 ||
			SampleResults[i].V19 != r.V19 ||
			SampleResults[i].V20 != r.V20 {
			t.Errorf(
				"Incorrect result, expected %v, got %v ",
				SampleResults[i],
				r,
			)
		}
	}
	log.Println("Actual results:", results)

	//************************************************************
	// CLEANUP
	//************************************************************

	testutils.CleanUpDataFolder()
}

func Taxi() *dataflow.Dataflow {
	df := dataflow.NewDataflow()
	taxiTripSource := dataflow.NewSource[*models.TaxiTrip](
		"source",
		func(co collector.Collector) {
			for _, event := range SampleInput {
				time.Sleep(200 * time.Millisecond)
				co.Emit(&event)
			}
		},
	)
	taxiTripSource.SetParallelism(1)
	// type TaxiTrip = tuple.Tuple14[
	// string,  // V1: medallion
	// string,  // V2: hackLicense
	// string,  // V3: vendorID
	// int64,   // V4: rateCode
	// string,  // V5: storeAndFwdFlag
	// int64,   // V6: pickupDatetime
	// int64,   // V7: dropoffDatetime
	// int64,   // V8: passengerCount
	// int64,   // V9: tripTimeSecs
	// float64, // V10: tripDistance
	// float64, // V11: pickupLongitude
	// float64, // V12: pickupLatitude
	// float64, // V13: dropoffLongitude
	// float64, // V14: dropoffLatitude
	// ]
	// Use dropoffDatetime as timestamp
	taxiTripTimestampAssigner := ta.NewTimestampAssigner(
		func(t *models.TaxiTrip) int64 {
			return t.V7
		},
	)
	taxiTripSource.AssignTimestampAndWatermark(
		taxiTripTimestampAssigner,
		200*time.Millisecond,
		0,
	)
	dataflow.AddOperator(df, taxiTripSource)

	// CalculateCellId based on latitude and longitude
	calculateCellId := dataflow.NewFlatmap[*models.TaxiTrip, *models.RouteInfo](
		"calculateCellId",
		func(t *models.TaxiTrip, co collector.Collector) {
			startCellId, insideArea := utils.GetCellID(t.V11, t.V12)
			if insideArea {
				endCellId, insideArea := utils.GetCellID(t.V13, t.V14)
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
	calculateCellId.SetParallelism(1)
	dataflow.AddOperator(df, calculateCellId)

	//Key by routeId and round
	routeKeyAssigner := ka.NewKeyAssigner(
		func(t *models.RouteInfo) string {
			return strconv.FormatInt(t.V17, 10)
		},
	)

	movingMedianTripTime := dataflow.NewStatefulMapper(
		"movingMedianTripTime",
		routeKeyAssigner,
		func(in *models.RouteInfo,
			// Store Route Info
			state2 *stateType.ListState[*models.RouteInfo],
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
			state2.Add(input)
			rawList := state2.Get()
			currentRouteInfo := make([]*models.RouteInfo, len(rawList))
			copy(currentRouteInfo, rawList)

			sort.Slice(currentRouteInfo, func(i, j int) bool {
				if currentRouteInfo[i].V8 == currentRouteInfo[j].V8 {
					return currentRouteInfo[i].V17 > currentRouteInfo[j].V17
				}
				return currentRouteInfo[i].V8 > currentRouteInfo[j].V8
			})
			n := len(currentRouteInfo)
			passengerCountMedian := currentRouteInfo[n/2]

			return &tuple.Tuple3[string, int64, int64]{
				V1: strconv.FormatInt(in.V17, 10),
				V2: passengerCountMedian.V8,
				V3: int64(n),
			}
		},
	)
	movingMedianTripTime.SetParallelism(1)
	dataflow.AddOperator(df, movingMedianTripTime)

	// Keyby window ending time
	movingMedianTripTimeKeyAssigner := ka.NewKeyAssigner(
		func(t *tuple.Tuple3[string, int64, int64]) int64 {
			return t.GetTimestamp() / 400
		},
	)

	// Calculate top 10 frequent routes inside a window
	top10RouteAggregator := dataflow.NewAggregator(
		// Store routeId, passengerCountMedian, frequencyCount
		// V1: routeId
		// V2: passengerCountMedian
		// V3: frequencyCount
		func() *stateType.ListState[*tuple.Tuple3[string, int64, int64]] {
			tupleList := []*tuple.Tuple3[string, int64, int64]{}
			return stateType.NewListState(tupleList)
		},

		// If there are less than 10 routes, add routes info into listState,
		// else only stores the top 10 frequent routes in listState
		func(
			acc *stateType.ListState[*tuple.Tuple3[string, int64, int64]],
			// Input: routeId, movingMedianTripTime, routeGroupId
			// V1: routeId
			// V2: passengerCountMedian
			// V3: movingMedianTripTime
			in *tuple.Tuple3[string, int64, int64],
		) *stateType.ListState[*tuple.Tuple3[string, int64, int64]] {
			allRoutesRaw := acc.Get()
			allRoutes := make(
				[]*tuple.Tuple3[string, int64, int64],
				len(allRoutesRaw),
			)
			copy(allRoutes, allRoutesRaw)
			routeInfo := &tuple.Tuple3[string, int64, int64]{
				V1: in.V1,
				V2: in.V2,
				V3: in.V3,
			}

			// Check if the same routeId is already in the listState
			alreadySeen := false
			for _, route := range allRoutes {
				if route.V1 == in.V1 {
					route.V2 = routeInfo.V2
					route.V3 = routeInfo.V3
					alreadySeen = true
				}
			}

			if !alreadySeen {
				allRoutes = append(allRoutes, routeInfo)
			}

			sort.Slice(allRoutes, func(i, j int) bool {
				if allRoutes[i].V3 == allRoutes[j].V3 {
					return allRoutes[i].V1 > allRoutes[j].V1
				}
				return allRoutes[i].V3 > allRoutes[j].V3
			})
			if len(allRoutes) < 10 {
				acc.Update(allRoutes)
			} else {
				// Only keep top 10 frequent routes
				allRoutes = allRoutes[:10]
				acc.Update(allRoutes)
			}
			return acc
		},

		// Output top10 frequent routeId
		func(
			acc *stateType.ListState[*tuple.Tuple3[string, int64, int64]],
		) *tuple.Tuple20[
			string, int64, string, int64,
			string, int64, string, int64,
			string, int64, string, int64,
			string, int64, string, int64,
			string, int64, string, int64,
		] {
			val := acc.Get()
			// If there's not enough routes, we use routeId 0 to fill the output
			if len(val) < 10 {
				for i := len(val); i < 10; i++ {
					val = append(val, &tuple.Tuple3[string, int64, int64]{
						V1: "0Round0",
						V2: 0,
						V3: 0,
					})
				}
			}
			return tuple.NewTuple20(
				val[0].V1,
				val[0].V2,
				val[1].V1,
				val[1].V2,
				val[2].V1,
				val[2].V2,
				val[3].V1,
				val[3].V2,
				val[4].V1,
				val[4].V2,
				val[5].V1,
				val[5].V2,
				val[6].V1,
				val[6].V2,
				val[7].V1,
				val[7].V2,
				val[8].V1,
				val[8].V2,
				val[9].V1,
				val[9].V2,
			)
		},

		func(
			acc1 *stateType.ListState[*tuple.Tuple3[string, int64, int64]],
			acc2 *stateType.ListState[*tuple.Tuple3[string, int64, int64]],
		) *stateType.ListState[*tuple.Tuple3[string, int64, int64]] {
			val1 := acc1.Get()
			val2 := acc2.Get()
			allRoutes := append(val1, val2...)
			// Sort by movingMedianTripTime
			sort.Slice(allRoutes, func(i, j int) bool {
				if allRoutes[i].V3 == allRoutes[j].V3 {
					return allRoutes[i].V1 > allRoutes[j].V1
				}
				return allRoutes[i].V3 > allRoutes[j].V3
			})
			// Only keep top 10 frequent routes
			allRoutes = allRoutes[:10]
			acc1.Update(allRoutes)
			return acc1
		},
	)

	top10Route := dataflow.NewTumblingWindow(
		"top10Route",
		movingMedianTripTimeKeyAssigner,
		top10RouteAggregator,
		400,
	)

	top10Route.SetParallelism(1)
	dataflow.AddOperator(df, top10Route)

	//Do-nothing sink
	sink := dataflow.NewSink(
		"sink",
		func(
			t *tuple.Tuple20[
				string, int64, string, int64,
				string, int64, string, int64,
				string, int64, string, int64,
				string, int64, string, int64,
				string, int64, string, int64]) {
			results = append(results, *t)
		},
	)
	sink.SetParallelism(1)
	dataflow.AddOperator(df, sink)

	dataflow.Add1To1Stream(df, taxiTripSource, calculateCellId)
	dataflow.Add1To1Stream(df, calculateCellId, movingMedianTripTime)
	dataflow.Add1To1Stream(df, movingMedianTripTime, top10Route)
	dataflow.Add1To1Stream(df, top10Route, sink)

	return df
}
