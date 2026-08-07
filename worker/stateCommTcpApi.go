package worker

import (
	"log"
	"net"

	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/worker/stateCommUtil"
)

// [Lazy-by-key] TCP API for state comm protocol. Dispatches based on message
// type: (1) needed key fetch (keyed by state ID), and (2) additional key fetch
// for eventual migration (flat key list without state IDs).
func (w *Worker) handleStateCommTcpConnection(conn net.Conn) {

	// Pre-allocate buffers for reading and sending TCP messages. Use a
	// separate send buffer for additional key fetch to avoid overwriting
	// the request keys in the read buffer during batched responses.
	buf := make([]byte, constant.TcpMaxMessageSize)
	sendBuf := make([]byte, constant.TcpMaxMessageSize)

	for {

		// Read TCP frame header and get message type
		msgType, bodyOffset := stateCommUtil.ReadTcpRequestHeader(conn, buf)

		switch msgType {

		case constant.TcpMsgTypeKeyedFetch:
			// Needed key fetch: keyed by state ID
			keyMap := stateCommUtil.DeserializeKeyedRequest(buf, bodyOffset)

			valueMap := make(map[uint16][][]byte)
			for stateID, keys := range keyMap {
				valueMap[stateID] = w.StateService.StateBackendImpl.GetMany(
					keys,
				)
			}

			stateCommUtil.SendTcpResponse(conn, valueMap, buf)

		case constant.TcpMsgTypeAdditionalFetch:
			// Additional key fetch for eventual migration: flat key list.
			// Keys are zero-copy slices from buf, so use sendBuf for
			// responses to keep keys valid across batched GetMany calls.
			keys := stateCommUtil.DeserializeAdditionalKeysRequest(
				buf,
				bodyOffset,
			)

			// Process keys in batches to limit memory usage. For each
			// batch, GetMany and immediately send the values back.
			for start := 0; start < len(keys); start += constant.AdditionalKeyGetManyBatchSize {
				end := min(
					start+constant.AdditionalKeyGetManyBatchSize,
					len(keys),
				)
				values := w.StateService.StateBackendImpl.GetMany(
					keys[start:end],
				)
				stateCommUtil.SendAdditionalKeysResponse(conn, values, sendBuf)
			}

		default:
			log.Fatalf(
				"Unknown TCP message type: %d\n",
				msgType,
			)
		}
	}
}
