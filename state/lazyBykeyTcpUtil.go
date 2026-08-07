package state

import (
	"encoding/binary"
	"log"
	"net"
	"sync"

	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/internal/network"
)

// [Lazy-by-key] Now we only support TCP API for Lazy-by-key protocol
type LazyByKeyTcpConn struct {
	Conn net.Conn

	// Pre-allocated buffer for reading/sending TCP messages
	Buf []byte

	// [Eventual migration for cancelling task] Pre-allocated buffer for
	// sending/receiving additional key fetch TCP messages. This is separated
	// from Buf to allow concurrent fetch of additional keys
	AdditionalBuf []byte
}

// [TCP] Fetch remote state over TCP API
func (s *StateService) fetchStateByTcp(
	remoteWorkerId uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
) {

	// Get TCP connection to the remote state service
	tcpConn := s.getByKeyMigrationTcpConn(remoteWorkerId)
	conn := tcpConn.Conn
	buf := tcpConn.Buf

	/**************************************************************************
					   Construct and send the request frame
	**************************************************************************/

	/*
	   Request TCP Frame Format (needed keys):

	   	| MAGIC_START (4B) | uint32: used for validation
	   	| Total number of bytes (4B) | uint32: total size of this request frame
	   	| Message type (1B) | uint8: 0x01 for needed key fetch
	   	| Number of state requested (2B) | uint16

	   	| 1st state id (2B) | uint16: the state ID for the first state
	   	| Number of keys for 1st state (4B) | uint32: number of keys for 1st state
	   	[
	   	- | Num bytes (4B) | uint32: the size of the serialized key
	   	- | KEY | the serialized key bytes
	   	] * Number of keys for 1st state

	   	| 2nd state id (2B) | optional if there are multiple states
	   	... ...

	   	| MAGIC_END (4B) | uint32: used for validation
	*/

	bufOffset := 0

	// Reserve space for MAGIC_START and total length
	bufOffset += 8

	// Message type: request for needed keys
	buf[bufOffset] = constant.TcpMsgTypeKeyedFetch
	bufOffset += 1

	// Number of state requested
	binary.BigEndian.PutUint16(buf[bufOffset:], uint16(len(keys)))
	bufOffset += 2

	for stateID, keyList := range keys {

		// State ID
		binary.BigEndian.PutUint16(buf[bufOffset:], stateID)
		bufOffset += 2

		// Number of keys
		binary.BigEndian.PutUint32(buf[bufOffset:], uint32(len(keyList)))
		bufOffset += 4

		for _, key := range keyList {
			keyLen := len(key)

			// Key Length
			binary.BigEndian.PutUint32(buf[bufOffset:], uint32(keyLen))
			bufOffset += 4

			// Key Bytes
			copy(buf[bufOffset:], key)
			bufOffset += keyLen
		}
	}

	// MAGIC_END
	binary.BigEndian.PutUint32(buf[bufOffset:], constant.MagicEnd)
	bufOffset += 4

	// Fill header: MAGIC_START
	binary.BigEndian.PutUint32(buf[0:4], constant.MagicStart)
	// Total length (excluding header)
	binary.BigEndian.PutUint32(buf[4:8], uint32(bufOffset-8))

	// Send request
	err := network.WriteAll(conn, buf[:bufOffset])
	if err != nil {
		log.Fatalf(
			"Error sending lazy-by-key TCP request to worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	/**************************************************************************
					 Receive and deserialize the response frame
	**************************************************************************/

	/*
	   Response TCP Frame Format (needed keys):

	   	| MAGIC_START (4B) | uint32: used for validation
	   	| Total number of bytes (4B) | uint32: total size of this response frame
	   	| Number of state in the response (2B) | uint16

	   	| 1st state id (2B) | uint16: the state ID for the first state
	   	| Number of values for 1st state (4B) | uint32
	   	[
	   	- | Num bytes (4B) | uint32: the size of the serialized value
	   	- | VALUE | the serialized value bytes
	   	] * Number of values for 1st state

	   	| 2nd state id (2B) | optional if there are multiple states
	   	... ...

	   	| MAGIC_END (4B) | uint32: used for validation
	*/

	// Reset offset to reuse buffer for reading
	bufOffset = 0

	// Read header (MAGIC_START + Total Length)
	err = network.ReadAll(conn, buf, 8)
	if err != nil {
		log.Fatalf(
			"Error reading lazy-by-key TCP response header from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Validate MAGIC_START
	if binary.BigEndian.Uint32(buf[0:4]) != constant.MagicStart {
		log.Fatalf(
			"Invalid MAGIC_START in TCP response from worker %d\n",
			remoteWorkerId,
		)
	}

	// Get response length
	respLen := binary.BigEndian.Uint32(buf[4:8])
	bufOffset += 8

	// Report bytes transferred
	s.MetricCollector.UpdateNumBytesTransferred(uint64(respLen))

	// Validate length
	if respLen > uint32(constant.TcpMaxMessageSize)-8 {
		log.Fatalf(
			"TCP response length %d exceeds max message size\n",
			respLen,
		)
	}

	// Read body
	err = network.ReadAll(conn, buf[bufOffset:], uint64(respLen))
	if err != nil {
		log.Fatalf(
			"Error reading lazy-by-key TCP response body from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Validate MAGIC_END
	if binary.BigEndian.Uint32(
		buf[bufOffset+int(respLen)-4:bufOffset+int(respLen)],
	) != constant.MagicEnd {
		log.Fatalf(
			"Invalid MAGIC_END in TCP response from worker %d\n",
			remoteWorkerId,
		)
	}

	// Deserialize response
	numStates := binary.BigEndian.Uint16(buf[bufOffset : bufOffset+2])
	bufOffset += 2

	for i := 0; i < int(numStates); i++ {

		// State ID
		stateID := binary.BigEndian.Uint16(buf[bufOffset : bufOffset+2])
		bufOffset += 2

		// Number of values
		numValues := binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4])
		bufOffset += 4

		values := make([][]byte, numValues)
		for j := 0; j < int(numValues); j++ {
			// Value length
			valLen := binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4])
			bufOffset += 4

			// Value bytes - zero-copy read: just point to the buffer
			values[j] = buf[bufOffset : bufOffset+int(valLen)]
			bufOffset += int(valLen)
		}
		res[stateID] = values
	}
}

// [eventual migration for cancelling task] Fetch additional keys from a remote
// worker over TCP. All keys are sent in a single request, and the server
// replies
// with one or more response messages (chunked if the total value size exceeds
// TcpMaxMessageSize). For each received chunk, SetMany is called synchronously
// before reading the next chunk, allowing zero-copy buffer reuse. UpdateKey
// runs in background goroutines.
//
// Request TCP Frame Format (additional keys):
//
//	| MAGIC_START (4B) | Total bytes (4B) | MsgType (1B) = 0x02 |
//	| Number of keys (4B) |
//	[
//	  | Key size (4B) | KEY bytes |
//	] * Number of keys
//	| MAGIC_END (4B) |
//
// Response TCP Frame Format (additional keys, one or more messages):
//
//	| MAGIC_START (4B) | Total bytes (4B) |
//	| Number of values (4B) |
//	[
//	  | Value size (4B) | VALUE bytes |
//	] * Number of values
//	| MAGIC_END (4B) |
func (s *StateService) fetchAdditionalKeysByTcp(
	remoteWorkerId uint16,
	keys [][]byte,
	bucketIDs []int64,
	ioWg *sync.WaitGroup,
) {

	tcpConn := s.getByKeyMigrationTcpConn(remoteWorkerId)
	conn := tcpConn.Conn
	buf := tcpConn.AdditionalBuf

	// Send ALL keys in one request
	s.sendAdditionalKeysByTcp(conn, buf, remoteWorkerId, keys)

	// Receive values in chunks. The server splits the response into multiple
	// messages if the total value size exceeds TcpMaxMessageSize. For each
	// chunk, do SetMany synchronously before receiving the next so the buffer
	// can be safely reused (zero-copy slicing).
	keyOffset := 0
	for keyOffset < len(keys) {

		// Read one response message and parse values (zero-copy from buf)
		values := s.receiveAdditionalValueChunkByTcp(
			conn, buf, remoteWorkerId,
		)
		numValues := len(values)

		// Slice keys and bucketIDs for this chunk
		chunkKeys := keys[keyOffset : keyOffset+numValues]
		chunkBucketIDs := bucketIDs[keyOffset : keyOffset+numValues]

		// Flush to local state backend synchronously - after this returns,
		// buf is safe to overwrite with the next chunk
		s.StateBackendImpl.SetMany(chunkKeys, values)

		// Update key lookup table in background goroutine
		ioWg.Add(1)
		go func(chunkKeys [][]byte, chunkBucketIDs []int64) {
			defer ioWg.Done()
			for i, key := range chunkKeys {
				bucketId := chunkBucketIDs[i]
				lock, ok := s.eventualMigrationBucketLocks[bucketId]
				if !ok {
					log.Fatalf(
						"Per-bucket lock not found for bucket %d during eventual migration\n",
						bucketId,
					)
				}
				lock.Lock()
				s.StateLookupTableV2.UpdateKey(key, bucketId, s.WorkerID)
				lock.Unlock()
			}
		}(chunkKeys, chunkBucketIDs)

		keyOffset += numValues
	}
}

// Send all additional keys over TCP in a single request message
func (s *StateService) sendAdditionalKeysByTcp(
	conn net.Conn,
	buf []byte,
	remoteWorkerId uint16,
	keys [][]byte,
) {

	bufOffset := 0

	// Reserve space for MAGIC_START and total length
	bufOffset += 8

	// Message type: additional key fetch
	buf[bufOffset] = constant.TcpMsgTypeAdditionalFetch
	bufOffset += 1

	// Number of keys
	binary.BigEndian.PutUint32(buf[bufOffset:], uint32(len(keys)))
	bufOffset += 4

	for _, key := range keys {
		keyLen := len(key)

		// Key length
		binary.BigEndian.PutUint32(buf[bufOffset:], uint32(keyLen))
		bufOffset += 4

		// Key bytes
		copy(buf[bufOffset:], key)
		bufOffset += keyLen
	}

	// MAGIC_END
	binary.BigEndian.PutUint32(buf[bufOffset:], constant.MagicEnd)
	bufOffset += 4

	// Fill header
	binary.BigEndian.PutUint32(buf[0:4], constant.MagicStart)
	binary.BigEndian.PutUint32(buf[4:8], uint32(bufOffset-8))

	// Send request
	err := network.WriteAll(conn, buf[:bufOffset])
	if err != nil {
		log.Fatalf(
			"Error sending additional key TCP request to worker %d: %v\n",
			remoteWorkerId, err,
		)
	}
}

// Receive one chunk of additional key values from TCP. Returns values as
// zero-copy slices from the buffer - caller must complete SetMany before the
// next receive call overwrites the buffer.
func (s *StateService) receiveAdditionalValueChunkByTcp(
	conn net.Conn,
	buf []byte,
	remoteWorkerId uint16,
) [][]byte {

	// Read header (MAGIC_START + Total Length)
	err := network.ReadAll(conn, buf, 8)
	if err != nil {
		log.Fatalf(
			"Error reading additional key TCP response header from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Validate MAGIC_START
	if binary.BigEndian.Uint32(buf[0:4]) != constant.MagicStart {
		log.Fatalf(
			"Invalid MAGIC_START in additional key TCP response from worker %d\n",
			remoteWorkerId,
		)
	}

	// Get response length
	respLen := binary.BigEndian.Uint32(buf[4:8])
	bufOffset := 8

	// Report bytes transferred
	s.MetricCollector.UpdateNumBytesTransferred(uint64(respLen))

	if respLen > uint32(constant.TcpMaxMessageSize)-8 {
		log.Fatalf(
			"Additional key TCP response length %d exceeds max message size\n",
			respLen,
		)
	}

	// Read body
	err = network.ReadAll(conn, buf[bufOffset:], uint64(respLen))
	if err != nil {
		log.Fatalf(
			"Error reading additional key TCP response body from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Validate MAGIC_END
	if binary.BigEndian.Uint32(
		buf[bufOffset+int(respLen)-4:bufOffset+int(respLen)],
	) != constant.MagicEnd {
		log.Fatalf(
			"Invalid MAGIC_END in additional key TCP response from worker %d\n",
			remoteWorkerId,
		)
	}

	// Deserialize values with zero-copy slicing from buffer
	numValues := int(binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4]))
	bufOffset += 4

	values := make([][]byte, numValues)
	for i := 0; i < numValues; i++ {
		valLen := binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4])
		bufOffset += 4
		values[i] = buf[bufOffset : bufOffset+int(valLen)]
		bufOffset += int(valLen)
	}

	return values
}
