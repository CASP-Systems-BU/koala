package stateClient

import (
	"encoding/binary"
	"log"
	"unsafe"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constants"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

/*
For each encoded key in the state store, we prefix it with multiple metadata
fields with fixed size:

|    Operator ID   |     Bucket ID    |     State ID     |       Key       |
| uint16 (2 bytes) | uint32 (4 bytes) | uint16 (2 bytes) | variable length |
*/

// Offsets in the key encoding
const OperatorIDOffset = 0
const BucketIDOffset = constants.OperatorIDSize
const StateIDOffset = constants.OperatorIDSize + constants.BucketIdxSize
const KeyOffset = constants.KeyPrefixSize

// Encode the simple key and return the bucket id this key belongs to.
func (sc *StateClient[K]) encodeSimpleKey(
	key K,
	stateID uint16,
) ([]byte, int64) {

	bufSize := constants.KeyPrefixSize + sc.keySizer(unsafe.Pointer(&key))
	buf := make([]byte, bufSize)

	// Encode the key and its prefix
	sc.keyEncoder(unsafe.Pointer(&key), buf[KeyOffset:])
	bucketID := sc.encodeKeyPrefix(buf, buf[KeyOffset:], stateID)

	return buf, bucketID
}

// Encode the simple key and return the bucket id this key belongs to.
// Window state key appends 1 more field to the serialized key:
// | op_id | bucket_id | state_id | key | window_start_time |
func (sc *StateClient[K]) encodeWindowKey(
	key K,
	windowStartTime int64,
	stateID uint16,
) ([]byte, int64) {

	bufSize := constants.KeyPrefixSize + sc.keySizer(
		unsafe.Pointer(&key),
	) + network.SizeInt64(
		unsafe.Pointer(&windowStartTime),
	)
	buf := make([]byte, bufSize)

	// Encode the key
	offset := sc.keyEncoder(unsafe.Pointer(&key), buf[KeyOffset:])

	// Encode the timestamp
	windowStartTimeOffset := KeyOffset + offset
	network.EncodeInt64(
		unsafe.Pointer(&windowStartTime),
		buf[windowStartTimeOffset:],
	)

	// Encode the prefix
	bucketID := sc.encodeKeyPrefix(
		buf,
		buf[KeyOffset:windowStartTimeOffset],
		stateID,
	)

	return buf, bucketID
}

func (sc *StateClient[K]) encodeKeyPrefix(
	buf []byte,
	key []byte,
	stateID uint16,
) int64 {

	// 1. Encode operator id
	binary.BigEndian.PutUint16(buf[OperatorIDOffset:], sc.operatorID)

	// 2. Encode bucket id
	// Safe to truncate numBuckets: the max value is limited by uint32
	bucketIdx := sc.keyHashFunc.Hash(key) % uint64(sc.numBuckets)
	binary.BigEndian.PutUint32(buf[BucketIDOffset:], uint32(bucketIdx))

	// 3. Encode state id
	sc.encodeStateID(buf, stateID)

	return int64(bucketIdx)
}

func (sc *StateClient[K]) encodeStateID(buf []byte, stateID uint16) {
	binary.BigEndian.PutUint16(buf[StateIDOffset:], stateID)
}

type KeyAndStartTime[K comparable] struct {
	Key       K
	StartTime int64
}

// Validate requested state IDs
func (sc *StateClient[K]) validateStateIDs(stateIDs []uint16) {

	if len(stateIDs) == 0 {
		log.Fatalf("Empty state IDs to fetch")
	}

	for _, stateID := range stateIDs {
		if _, exists := sc.cache[stateID]; !exists {
			log.Fatalln(
				"[Wrong state ID] requested State ID not registered in state client:",
				stateID,
			)
		}
	}
}

// Serialize keys if more than 1 state is requested. This is called for
// multi-state operators.
func (sc *StateClient[K]) serializeMultiStateKeys(
	serializedKeyTemplate []byte,
	stateIDs []uint16,
) {

	// The field op_id, bucket_id, and key are the same as
	// serializedKeyTemplate, we simply override state_id for each state.
	for _, stateID := range stateIDs {

		serializedKeyCopy := make([]byte, len(serializedKeyTemplate))
		copy(serializedKeyCopy, serializedKeyTemplate)

		// Only state_id needs to be override for the new key
		sc.encodeStateID(serializedKeyCopy, stateID)
		sc.serializedKeys[stateID] = append(
			sc.serializedKeys[stateID],
			serializedKeyCopy,
		)
	}
}
