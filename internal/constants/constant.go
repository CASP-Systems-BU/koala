package constants

// Define system-wide constants

// Bucket ID size
const BucketIdxSize = 4 // uint32

/*
Key prefix related constants. For each encoded key in the state store, we prefix
it with multiple metadata fields with fixed size:

|    Operator ID   |     Bucket ID    |     State ID     |       Key       |
| uint16 (2 bytes) | uint32 (4 bytes) | uint16 (2 bytes) | variable length |
*/

const OperatorIDSize = 2 // uint16
const StateIDSize = 2    // uint16
const KeyPrefixSize = OperatorIDSize + StateIDSize + BucketIdxSize
