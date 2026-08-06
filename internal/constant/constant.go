package constant

// Define system-wide constants

/******************************************************************************
								  Bucket size
******************************************************************************/

// Number of bytes used to store bucket ID
const BucketIdxSize = 4 // uint32

/******************************************************************************
					   			State key prefix
******************************************************************************/

/*
Key prefix related constants. For each encoded key in the state store, we prefix
it with multiple metadata fields with fixed size:

|    Operator ID   |     Bucket ID    |     State ID     |       Key       |
| uint16 (2 bytes) | uint32 (4 bytes) | uint16 (2 bytes) | variable length |
*/

const OperatorIDSize = 2 // uint16
const StateIDSize = 2    // uint16
const KeyPrefixSize = OperatorIDSize + StateIDSize + BucketIdxSize

/******************************************************************************
						     gRPC message size limit
******************************************************************************/

// Max allowed gRPC message size for state comm data transfer. Default gRPC
// message size limit is 4 MB
const RpcMaxMessageSize = 100 * 1024 * 1024 // 100 MB

/******************************************************************************
						  Custom TCP framing constants
******************************************************************************/

const MagicStart = uint32(0xABCDEF01)
const MagicEnd = uint32(0x10FEDCBA)

// Max allowed TCP message size for state comm data transfer
const TcpMaxMessageSize = 50 * 1024 * 1024 // 50 MB

// TCP message types for lazy-by-key state comm protocol
// 1. Request for needed keys per batch
const TcpMsgTypeKeyedFetch uint8 = 0x01

// 2. Request for extra keys for eventual state migration
const TcpMsgTypeAdditionalFetch uint8 = 0x02

// [Eventual migration] Max number of keys per GetMany call on the server side
// when serving additional key fetch requests
const AdditionalKeyGetManyBatchSize = 1000
