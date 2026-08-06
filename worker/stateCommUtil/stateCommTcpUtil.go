package stateCommUtil

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

// [Lazy-by-key] Now we only support TCP API for Lazy-by-key protocol

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

func ReadTcpRequest(conn net.Conn, buf []byte) map[uint16][][]byte {

	bufOffset := 0

	// Read the MAGIC_START and total request length first
	err := network.ReadAll(conn, buf, 8)
	if err != nil {
		// [temp] postpone failure to let end-of-warmup operation complete
		// e.g. wait to flush key lookup table at end of warmup
		time.Sleep(5 * time.Second)
		log.Fatalf("Error reading TCP magic start: %v\n", err)
	}

	// Validate MAGIC_START
	if binary.BigEndian.Uint32(buf[0:4]) != constant.MagicStart {
		log.Fatalf("Invalid MAGIC_START in TCP request")
	}

	// Get total length of this request frame (excluding the MAGIC_START
	// and total length field)
	reqLen := binary.BigEndian.Uint32(buf[4:8])
	bufOffset += 8

	// Validate the request length
	if reqLen > uint32(constant.TcpMaxMessageSize)-8 {
		log.Fatalf(
			"TCP request length %d exceeds max message size\n",
			reqLen,
		)
	}

	// Read entire request frame
	err = network.ReadAll(conn, buf[bufOffset:], uint64(reqLen))
	if err != nil {
		log.Fatalf("Error reading TCP request frame: %v\n", err)
	}

	// Validate MAGIC_END
	if binary.BigEndian.Uint32(
		buf[bufOffset+int(reqLen)-4:bufOffset+int(reqLen)],
	) != constant.MagicEnd {
		log.Fatalf("Invalid MAGIC_END in TCP request")
	}

	// Now deserialize the request
	keyMap := make(map[uint16][][]byte)

	// Number of states requested
	numStates := binary.BigEndian.Uint16(buf[bufOffset : bufOffset+2])
	bufOffset += 2

	// For each state, read the state ID and the keys
	for i := 0; i < int(numStates); i++ {

		// State ID
		stateID := binary.BigEndian.Uint16(buf[bufOffset : bufOffset+2])
		bufOffset += 2

		// Number of keys for this state
		numKeys := binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4])
		if numKeys <= 0 {
			log.Fatalf(
				"Invalid number of keys %d for state %d in TCP request\n",
				numKeys,
				stateID,
			)
		}
		bufOffset += 4

		// Read each key
		keys := make([][]byte, numKeys)
		for j := 0; j < int(numKeys); j++ {

			// Key size
			keySize := binary.BigEndian.Uint32(buf[bufOffset : bufOffset+4])
			bufOffset += 4

			// Zero-copy read the key bytes - slice the read buffer
			keys[j] = buf[bufOffset : bufOffset+int(keySize)]
			bufOffset += int(keySize)
		}
		keyMap[stateID] = keys
	}
	return keyMap
}

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

// TODO: now if value total length exceeds TcpMaxMessageSize, bufOffset will
// exceed the buffer capacity and panic - add explicit capacity check
func SendTcpResponse(conn net.Conn, valueMap map[uint16][][]byte, buf []byte) {

	bufOffset := 0

	// Reserve space for MAGIC_START and total length
	bufOffset += 8

	// Number of states in the response
	binary.BigEndian.PutUint16(buf[bufOffset:], uint16(len(valueMap)))
	bufOffset += 2

	for stateID, values := range valueMap {
		// State ID
		binary.BigEndian.PutUint16(buf[bufOffset:], stateID)
		bufOffset += 2

		// Number of values for this state
		binary.BigEndian.PutUint32(buf[bufOffset:], uint32(len(values)))
		bufOffset += 4

		for _, val := range values {
			valLen := len(val)

			// Value size
			binary.BigEndian.PutUint32(buf[bufOffset:], uint32(valLen))
			bufOffset += 4

			// Value bytes
			copy(buf[bufOffset:], val)
			bufOffset += valLen
		}
	}

	// MAGIC_END
	binary.BigEndian.PutUint32(buf[bufOffset:], constant.MagicEnd)
	bufOffset += 4

	// Fill in the header
	// MAGIC_START
	binary.BigEndian.PutUint32(buf[0:4], constant.MagicStart)
	// Total length (excluding header)
	binary.BigEndian.PutUint32(buf[4:8], uint32(bufOffset-8))

	// Send all bytes
	err := network.WriteAll(conn, buf[:bufOffset])
	if err != nil {
		log.Fatalf("Error sending TCP response: %v\n", err)
	}
}
