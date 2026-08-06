package stateCommUtil

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

// [Lazy-by-key] TCP API for state comm protocol. Supports two message types:
// (1) needed key fetch (keyed by state ID), and (2) additional key fetch for
// eventual migration (flat key list without state IDs).

// ReadTcpRequestHeader reads the common TCP frame header (MAGIC_START, total
// length), reads the full body into buf, validates MAGIC_END, and returns the
// message type byte and body start offset (after message type).
func ReadTcpRequestHeader(
	conn net.Conn,
	buf []byte,
) (msgType uint8, bodyOffset int) {

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
	bodyOffset = 8

	// Validate the request length
	if reqLen > uint32(constant.TcpMaxMessageSize)-8 {
		log.Fatalf(
			"TCP request length %d exceeds max message size\n",
			reqLen,
		)
	}

	// Read entire request frame
	err = network.ReadAll(conn, buf[bodyOffset:], uint64(reqLen))
	if err != nil {
		log.Fatalf("Error reading TCP request frame: %v\n", err)
	}

	// Validate MAGIC_END
	if binary.BigEndian.Uint32(
		buf[bodyOffset+int(reqLen)-4:bodyOffset+int(reqLen)],
	) != constant.MagicEnd {
		log.Fatalf("Invalid MAGIC_END in TCP request")
	}

	// Read message type
	msgType = buf[bodyOffset]
	bodyOffset += 1

	return msgType, bodyOffset
}

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

// DeserializeKeyedRequest deserializes a needed key fetch request from the
// buffer starting at the given offset (after message type).
func DeserializeKeyedRequest(buf []byte, offset int) map[uint16][][]byte {

	keyMap := make(map[uint16][][]byte)

	// Number of states requested
	numStates := binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	// For each state, read the state ID and the keys
	for i := 0; i < int(numStates); i++ {

		// State ID
		stateID := binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2

		// Number of keys for this state
		numKeys := binary.BigEndian.Uint32(buf[offset : offset+4])
		if numKeys <= 0 {
			log.Fatalf(
				"Invalid number of keys %d for state %d in TCP request\n",
				numKeys,
				stateID,
			)
		}
		offset += 4

		// Read each key
		keys := make([][]byte, numKeys)
		for j := 0; j < int(numKeys); j++ {

			// Key size
			keySize := binary.BigEndian.Uint32(buf[offset : offset+4])
			offset += 4

			// Zero-copy read the key bytes - slice the read buffer
			keys[j] = buf[offset : offset+int(keySize)]
			offset += int(keySize)
		}
		keyMap[stateID] = keys
	}
	return keyMap
}

/*
Request TCP Frame Format (additional keys):

	| MAGIC_START (4B) | Total bytes (4B) | MsgType (1B) = 0x02 |
	| Number of keys (4B) |
	[
	  | Key size (4B) | KEY bytes |
	] * Number of keys
	| MAGIC_END (4B) |
*/

// DeserializeAdditionalKeysRequest deserializes an additional key fetch request
// from the buffer starting at the given offset (after message type).
func DeserializeAdditionalKeysRequest(buf []byte, offset int) [][]byte {

	// Number of keys
	numKeys := binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4

	keys := make([][]byte, numKeys)
	for i := 0; i < int(numKeys); i++ {

		// Key size
		keySize := binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4

		// Zero-copy read the key bytes - slice the read buffer
		keys[i] = buf[offset : offset+int(keySize)]
		offset += int(keySize)
	}
	return keys
}

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

/*
Response TCP Frame Format (additional keys):

	| MAGIC_START (4B) | Total bytes (4B) |
	| Number of values (4B) |
	[
	  | Value size (4B) | VALUE bytes |
	] * Number of values
	| MAGIC_END (4B) |
*/

// SendAdditionalKeysResponse sends values for additional key fetch. If the
// total
// serialized size exceeds TcpMaxMessageSize, values are split across multiple
// TCP messages.
func SendAdditionalKeysResponse(conn net.Conn, values [][]byte, buf []byte) {

	// Max usable space: buffer minus fixed overhead for MAGIC_END
	maxBufOffset := constant.TcpMaxMessageSize - 4

	// Each iteration sends a chunk of values that fit in one TCP message
	start := 0
	for {
		bufOffset := 8 // skip MAGIC_START + total length

		// Reserve space for number of values - fill in later
		numValuesOffset := bufOffset
		bufOffset += 4

		// Serialize values in a single pass until buffer is full
		end := start
		for end < len(values) {
			valLen := len(values[end])
			entrySize := 4 + valLen
			if bufOffset+entrySize > maxBufOffset {
				if end == start {
					log.Fatalf(
						"Single value size %d exceeds max TCP message payload\n",
						valLen,
					)
				}
				break
			}
			binary.BigEndian.PutUint32(buf[bufOffset:], uint32(valLen))
			bufOffset += 4
			copy(buf[bufOffset:], values[end])
			bufOffset += valLen
			end++
		}

		// Fill in number of values
		binary.BigEndian.PutUint32(buf[numValuesOffset:], uint32(end-start))

		// MAGIC_END
		binary.BigEndian.PutUint32(buf[bufOffset:], constant.MagicEnd)
		bufOffset += 4

		// Fill header
		binary.BigEndian.PutUint32(buf[0:4], constant.MagicStart)
		binary.BigEndian.PutUint32(buf[4:8], uint32(bufOffset-8))

		// Send
		err := network.WriteAll(conn, buf[:bufOffset])
		if err != nil {
			log.Fatalf("Error sending additional keys TCP response: %v\n", err)
		}

		start = end
		if start >= len(values) {
			break
		}
	}
}
