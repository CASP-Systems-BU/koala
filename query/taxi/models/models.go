package models

import (
	"github.com/CASP-Systems-BU/koala/api/tuple"
)

type TaxiTrip = tuple.Tuple14[
	string,  // medallion
	string,  // hackLicense
	string,  // vendorID
	int64,   // rateCode
	string,  // storeAndFwdFlag
	int64,   // pickupDatetime
	int64,   // dropoffDatetime
	int64,   // passengerCount
	int64,   // tripTimeSecs
	float64, // tripDistance
	float64, // pickupLongitude
	float64, // pickupLatitude
	float64, // dropoffLongitude
	float64, // dropoffLatitude
]

type RouteInfo = tuple.Tuple17[
	string,  // medallion
	string,  // hackLicense
	string,  // vendorID
	int64,   // rateCode
	string,  // storeAndFwdFlag
	int64,   // pickupDatetime
	int64,   // dropoffDatetime
	int64,   // passengerCount
	int64,   // tripTimeSecs
	float64, // tripDistance
	float64, // pickupLongitude
	float64, // pickupLatitude
	float64, // dropoffLongitude
	float64, // dropoffLatitude
	int64,   // startCellId
	int64,   // endCellId
	int64,   // routeId
]
