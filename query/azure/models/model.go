package models

import "github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"

type AzureEvent = tuple.Tuple5[
	int64,   // timestamp
	string,  // vmID
	float64, // minCpu
	float64, // maxCpu
	float64, // avgCpu
]
