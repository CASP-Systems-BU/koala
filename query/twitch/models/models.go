package models

import "github.com/CASP-Systems-BU/koala/api/tuple"

type TwitchEvent = tuple.Tuple5[
	string, //userId
	string, //streamerId
	string, //streamerName
	int64,  //eventTime
	string, //eventType
]
