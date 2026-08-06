package models

import "github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"

type AuctionEvent = tuple.Tuple10[
	int64,  // auction id
	string, // item name
	string, // description
	int64,  // initial bid
	int64,  // reserve
	int64,  // dateTime (unix nanoseconds)
	int64,  // expires (unix nanoseconds)
	int64,  // seller
	int64,  // category
	string, // extra
]

type BidEvent = tuple.Tuple5[
	int64,  // auction id (foriegn key)
	int64,  // bidder id (foriegn key)
	int64,  // price
	int64,  // dateTime (unix nanoseconds)
	string, // extra
]

type PersonEvent = tuple.Tuple8[
	int64,  // id
	string, // name
	string, // email
	string, // creditCard
	string, // city
	string, // state
	int64,  // dateTime (unix nanoseconds)
	string, // extra
]

type ClosedAuctionEvent = tuple.Tuple6[
	int64,  // id
	int64,  // sellerId
	int64,  // finalPrice
	int64,  // category
	int64,  // dateTime (unix nanoseconds)
	string, // extra
]
