package models

import "github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"

type TaskEvent = tuple.Tuple13[
	int64,   // timestamp
	string,  // missingInfo
	int64,   // jobID
	int64,   // taskIndex
	int64,   // machineID
	string,  // eventType
	string,  // userName
	int64,   // schedulingClass
	int64,   // priority
	float64, // cpuRequest
	float64, // ramRequest
	float64, // localDiskRequest
	bool,    //  constraint
]
