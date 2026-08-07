package state

import (
	"encoding/binary"
	"log"
	"net"

	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/internal/network"
)

// [Lazy-by-key] Now we only support TCP API for Lazy-by-key protocol
type LazyByKeyTcpConn struct {
	Conn net.Conn

	// Pre-allocate a buffer for reading/sending TCP messages
	Buf []byte
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
	   Request TCP Frame Format:

	   	| MAGIC_START (4B) | uint32: used for validation
	   	| Total number of bytes (4B) | uint32: total size of this request frame
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
	   Response TCP Frame Format (same as request but for values):

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
