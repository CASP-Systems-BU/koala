package keyby

import (
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/grpc"
	"github.com/CASP-Systems-BU/koala/internal/keyby/hash"
	"github.com/CASP-Systems-BU/koala/internal/keyby/partition"
	"github.com/mus-format/mus-go/raw"
)

// KeyLookupTableV2 maintains a KeyMap per bucket for fine-grained key lookup
type KeyLookupTableV2 struct {

	// Key hash function
	H hash.HashFunc

	// Global key space organized into buckets
	Buckets []partition.BucketV2
}

func NewKeyLookupTableV2(
	policy partition.PartitionPolicy,
	workers []uint16,
	config *configuration.Configuration,
) *KeyLookupTableV2 {

	return &KeyLookupTableV2{
		// Coordinator doesn't need the key hash function H for routing keys
		// We still generate it here for testing purposes. When the Coordiantor
		// sends the KeyLookupTableV2 to workers, we only send the Buckets field
		// while the H field is consistently constructed in the workers
		H: hash.GetKeyHashFunc(config),

		// Generate bucket list based on the partitioning policy
		Buckets: policy.GenerateBucketsV2(workers),
	}
}

/******************************************************************************
					KeyLookupTableV2 API exposed to Worker
******************************************************************************/

// Deserialize KeyLookupTable from gRPC
func DeserializeKeyLookupTableV2(
	bucketRanges []*grpc.BucketRange,
	config *configuration.Configuration,
	localWorkerId uint16,
) *KeyLookupTableV2 {

	// Deserialize bucket ranges
	buckets := make([]partition.BucketV2, config.NumBuckets)
	for _, bucketRange := range bucketRanges {
		for i := bucketRange.LowerBucketIdx; i <= bucketRange.UpperBucketIdx; i++ {

			workerId := uint16(bucketRange.WorkerId)

			// Initialize the per-key map only for local worker
			// PreAllocate with 10000 keys
			var keyMap map[string]uint16
			if workerId == localWorkerId {
				keyMap = make(map[string]uint16, 10000)
			}

			buckets[i] = partition.BucketV2{
				WorkerID: workerId,
				Map:      keyMap,
			}
		}
	}

	// Validate the length of deserialized buckets
	if int64(len(buckets)) != config.NumBuckets {
		log.Fatalf("Deserialized bucket length is not equal to NumBuckets\n")
	}

	return &KeyLookupTableV2{
		H:       hash.GetKeyHashFunc(config),
		Buckets: buckets,
	}
}

// Key lookup: key and its bucket id -> worker ID
func (table *KeyLookupTableV2) GetWorkerID(key []byte, bucketId int64) uint16 {

	bucket := table.Buckets[bucketId]

	// Key lookup is only allowed for buckets that belong to the local worker.
	// Note that the key location might still be a remote worker but the bucket
	// ownership must be local
	if bucket.Map == nil {
		log.Fatalf("Key map nil for requesting key with bucket %d\n", bucketId)
	}

	ownerWorkerId, exists := bucket.Map[string(key)]
	if !exists {

		// If key not found, this is a new key that should still belong to the
		// local worker
		return bucket.WorkerID
	}
	return ownerWorkerId
}

// Update the key address
func (table *KeyLookupTableV2) UpdateKey(
	key []byte,
	bucketId int64,
	updatedWorkerId uint16,
) {
	bucket := table.Buckets[bucketId]

	// Validate that the bucket belongs to the local worker
	if bucket.Map == nil {
		log.Fatalf("Key map nil for updating key with bucket %d\n", bucketId)
	}

	// Validate that the key already exists in the key map
	_, exists := bucket.Map[string(key)]
	if !exists {
		log.Fatalf("Key not found in key map for updating key with bucket %d\n",
			bucketId)
	}

	// Update the key address
	bucket.Map[string(key)] = updatedWorkerId
}

// Insert or update the key address
func (table *KeyLookupTableV2) InsertOrUpdateKey(
	key []byte,
	bucketId int64,
	workerId uint16,
) {
	bucket := table.Buckets[bucketId]

	// Validate that the bucket belongs to the local worker
	if bucket.Map == nil {
		log.Fatalf("Key map nil for inserting/updating key with bucket %d\n",
			bucketId)
	}

	// Insert or update the key address
	bucket.Map[string(key)] = workerId
}

// Remove the key from the key map
func (table *KeyLookupTableV2) RemoveKey(
	key []byte,
	bucketId int64,
) {
	bucket := table.Buckets[bucketId]

	// Validate that the bucket belongs to the local worker
	if bucket.Map == nil {
		log.Fatalf("Key map nil for removing key with bucket %d\n", bucketId)
	}

	// Validate that the key already exists in the key map
	_, exists := bucket.Map[string(key)]
	if !exists {
		log.Fatalf("Key not found in key map for removing key with bucket %d\n",
			bucketId)
	}

	// Remove the key from the key map
	delete(bucket.Map, string(key))
}

/******************************************************************************
			    KeyLookupTableV2 (de)serialization for transfer
******************************************************************************/

// Deserialize migrating key maps from peer channels and insert them into the
// local KeyLookupTableV2. Passed-in buffer starts from the bucket length field
func (table *KeyLookupTableV2) DeserializeMigratingKeyMaps(
	serializedKeyMaps []byte,
	localWorkerId uint16,
) {

	numBuckets := binary.BigEndian.Uint32(serializedKeyMaps[:4])
	offset := 4
	var wg sync.WaitGroup
	for i := 0; i < int(numBuckets); i++ {

		// Read bucket data length
		bucketLen := binary.BigEndian.Uint32(
			serializedKeyMaps[offset : offset+4],
		)
		offset += 4

		// Extract bucket data
		bucketBytes := serializedKeyMaps[offset : offset+int(bucketLen)]
		offset += int(bucketLen)

		// Deserialize bucket in a separate goroutine
		wg.Add(1)
		go func(data []byte) {
			defer wg.Done()

			// Parse data
			// data[0:2] -> Operator ID
			// data[2:6] -> Bucket ID
			// data[6:10] -> Number of keys

			// Read Bucket ID
			bucketID := int64(binary.BigEndian.Uint32(data[2:6]))

			// Read Num Keys
			numKeys := int(binary.BigEndian.Uint32(data[6:10]))

			// The receiving bucket must have a nil (existing task) or empty
			// (new task) key map in the local key table because they did not
			// belong to this worker before
			if table.Buckets[int(bucketID)].Map != nil &&
				len(table.Buckets[int(bucketID)].Map) != 0 {
				log.Fatalf(
					"Bucket %d already has a non-empty key map when receiving migrating keys\n",
					bucketID,
				)
			}
			keyMap := make(map[string]uint16, numKeys)
			table.Buckets[int(bucketID)].Map = keyMap

			// Update bucket owner to local worker
			table.Buckets[int(bucketID)].WorkerID = localWorkerId

			dataOffset := 10
			for range numKeys {
				// Key Length
				keyLen := int(
					binary.BigEndian.Uint16(data[dataOffset : dataOffset+2]),
				)
				dataOffset += 2

				// Reconstruct full key
				fullKey := make([]byte, 6+keyLen)
				copy(fullKey[:6], data[0:6])
				copy(fullKey[6:], data[dataOffset:dataOffset+keyLen])
				dataOffset += keyLen

				// Owner Worker ID
				owner := binary.BigEndian.Uint16(
					data[dataOffset : dataOffset+2],
				)
				dataOffset += 2

				keyMap[string(fullKey)] = owner
			}
		}(bucketBytes)
	}
	wg.Wait()
}

// Serialize the affected portion of KeyLookupTableV2 for migrating key maps
// over the peer channel. Construct the serialized key maps grouped by updated
// destination workers
func (table *KeyLookupTableV2) SerializeMigratingKeyMaps(
	localWorkerId uint16,
	fastForwardRoutingTable *PartitionTable,
) map[uint16]*buffer.MigratingKeyMap {

	start := time.Now()
	// Init utils for constructing serialized key maps
	serializedKeyMapFactory := NewSerializedKeyMapFactory()

	// How to identify the bucket to be migrated: for those buckets that
	// originally belong to the local worker in the key lookup table, check
	// if they are reassigned in the routing table. If so, we need to send
	// these key maps to the new owner
	localBuckets := table.Buckets
	for bucketId := range localBuckets {

		if localBuckets[bucketId].WorkerID == localWorkerId {
			newOwner := fastForwardRoutingTable.Buckets[bucketId]
			if newOwner != localWorkerId {

				// Serialize this bucket key map to be transferred to new owner
				serializedKeyMapFactory.SerializeBucket(
					uint32(bucketId),
					newOwner,
					localBuckets[bucketId].Map,
				)

				// Update the ownership in the local key lookup table and remove
				// the key map to keep a single copy as the ground truth
				localBuckets[bucketId].WorkerID = newOwner
				localBuckets[bucketId].Map = nil
			}
		}
	}
	serializedKeyMap := serializedKeyMapFactory.Done()
	duration := time.Since(start)
	log.Println("Serialize migrating key maps takes", duration)
	return serializedKeyMap
}

/******************************************************************************
					        Utils for key map codec
******************************************************************************/

// Max allowed size for a serialized key map work unit
var MaxKeyMapSize = 100 * 1024 * 1024 // 50 MB

// Serialized key map list under construction (per destination worker)
// Serialized key map format:
//  1. WorkUnit type (2 bytes uint16) - MigratingKeyMapWorkUnit
//  2. Length of total bytes for the rest of the message (4 bytes uint32)
//  3. Number of total buckets included (4 bytes uint32)
//  4. For each bucket:
//     4.1 Length of the bucket data (4 bytes uint32) - excluding this field
//     4.2 Operator ID (2 bytes uint16)
//     4.3 Bucket ID (4 bytes uint32)
//     4.4 Number of key value pairs in this bucket (4 bytes uint32)
//     4.5 For each key value pair:
//     - 4.5.1 Length of the key (2 bytes uint16)
//     - 4.5.2 Key bytes (variable length byte[])
//     - 4.5.3 owner worker ID (2 bytes uint16)
type SerializedKepMap struct {
	offset     int
	numBuckets int
	data       []byte
}

func NewSerializedKeyMap() *SerializedKepMap {

	initialMap := SerializedKepMap{
		offset:     0,
		numBuckets: 0,
		data:       make([]byte, MaxKeyMapSize),
	}

	// Write the work unit type (uint16) - use raw.MarshalUint16()
	raw.MarshalUint16(
		uint16(buffer.MigratingKeyMapWorkUnit),
		initialMap.data[initialMap.offset:initialMap.offset+2],
	)
	initialMap.offset += 2

	// Reserve total length field (uint32)
	initialMap.offset += 4

	// Reserve number of buckets field (uint32)
	initialMap.offset += 4

	return &initialMap
}

func (skm *SerializedKepMap) AddBucket(
	bucketId uint32,
	keyMap map[string]uint16,
) {

	// We could migrate an empty bucket - no keys yet inserted into the bucket
	numKeys := len(keyMap)

	// Reserve space for total bucket length in bytes (4.1)
	bucketLengthOffset := skm.offset
	skm.offset += 4

	// Init the total length of this bucket data in bytes
	totalBucketLenInBytes := 0
	totalBucketLenInBytes += 2 + 4 + 4         // 4.2, 4.3, 4.4
	totalBucketLenInBytes += numKeys * (2 + 2) // 4.5.1, 4.5.3

	// Encode operator ID (4.2)
	// If this is an empty bucket key map, the operator ID field is not used,
	// we just need to fill it with dummy bytes to keep the format consistent
	if numKeys == 0 {
		copy(skm.data[skm.offset:skm.offset+2], []byte{0, 0})
	} else {
		// Write operator ID (uint16) from any key in the map (first 2 bytes of
		// the key) (4.2)
		for key := range keyMap {
			copy(skm.data[skm.offset:skm.offset+2], []byte(key)[:2])
			break
		}
	}
	skm.offset += 2

	// Encode the bucket ID (4.3)
	binary.BigEndian.PutUint32(
		skm.data[skm.offset:skm.offset+4],
		bucketId,
	)
	skm.offset += 4

	// Write number of key-value pairs (uint32) (4.4)
	binary.BigEndian.PutUint32(
		skm.data[skm.offset:skm.offset+4],
		uint32(numKeys),
	)
	skm.offset += 4

	// Write each key-value pair (4.5)
	for key, val := range keyMap {

		// Write length of the key (uint16) (4.5.1)
		keyLen := len(key) - 6 // exclude the first 6 bytes used above
		binary.BigEndian.PutUint16(
			skm.data[skm.offset:skm.offset+2],
			uint16(keyLen),
		)
		skm.offset += 2

		// Write key bytes (byte[]) (4.5.2)
		copy(skm.data[skm.offset:skm.offset+keyLen], []byte(key)[6:])
		skm.offset += keyLen

		// Write owner worker ID (uint16) (4.5.3)
		binary.BigEndian.PutUint16(
			skm.data[skm.offset:skm.offset+2],
			val,
		)
		skm.offset += 2

		// Update total bucket length in bytes
		totalBucketLenInBytes += keyLen
	}

	// Encode the total bytes for this bucket
	binary.BigEndian.PutUint32(
		skm.data[bucketLengthOffset:bucketLengthOffset+4],
		uint32(totalBucketLenInBytes),
	)
	skm.numBuckets += 1

	// Validate the current offset shouldn't exceed the max allowed size
	// Note that system will crash if offset exceeds MaxKeyMapSize in any case
	// above when we try to write to the data slice
	if skm.offset > MaxKeyMapSize {
		log.Fatalf(
			"Serialized key map size exceeds the max allowed size: %d bytes\n",
			skm.offset,
		)
	}
}

func (skm *SerializedKepMap) Done() *buffer.MigratingKeyMap {

	// Encode the length of the serialized key map data - excluding the first
	// 6 bytes (work unit type and length field)
	binary.BigEndian.PutUint32(
		skm.data[2:6],
		uint32(skm.offset-6),
	)

	// Encode the number of buckets included
	binary.BigEndian.PutUint32(
		skm.data[6:10],
		uint32(skm.numBuckets),
	)

	return &buffer.MigratingKeyMap{
		SerializedKeyMap: skm.data[:skm.offset],
	}
}

type SerializedKeyMapFactory struct {
	keyMaps map[uint16]*SerializedKepMap
}

func NewSerializedKeyMapFactory() *SerializedKeyMapFactory {
	return &SerializedKeyMapFactory{
		keyMaps: make(map[uint16]*SerializedKepMap),
	}
}

func (skmf *SerializedKeyMapFactory) SerializeBucket(
	bucketId uint32,
	destWorkerId uint16,
	keyMap map[string]uint16,
) {
	if keyMap == nil {
		log.Fatalf("Bucket %d key map should not be nil\n", bucketId)
	}
	serializedKeyMap, exists := skmf.keyMaps[destWorkerId]
	if !exists {
		serializedKeyMap = NewSerializedKeyMap()
		skmf.keyMaps[destWorkerId] = serializedKeyMap
	}
	serializedKeyMap.AddBucket(bucketId, keyMap)
}

func (skmf *SerializedKeyMapFactory) Done() map[uint16]*buffer.MigratingKeyMap {
	result := make(map[uint16]*buffer.MigratingKeyMap)
	for workerId, serializedKeyMap := range skmf.keyMaps {
		result[workerId] = serializedKeyMap.Done()
	}
	return result
}
