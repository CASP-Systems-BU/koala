package worker

import (
	"net"

	"github.com/CASP-Systems-BU/koala/internal/constant"
	"github.com/CASP-Systems-BU/koala/worker/stateCommUtil"
)

// [Lazy-by-key] Now we only support TCP API for Lazy-by-key protocol
func (w *Worker) handleStateCommTcpConnection(conn net.Conn) {

	// Pre-allocate a buffer for reading/sending TCP messages
	buf := make([]byte, constant.TcpMaxMessageSize)

	for {

		// Read and deserialize a state fetch TCP request from the connection
		keyMap := stateCommUtil.ReadTcpRequest(conn, buf)

		// Read the requested keys from local state backend. buf is unsafe to
		// overwrite during local read since keys have references to buf
		valueMap := make(map[uint16][][]byte)
		for stateID, keys := range keyMap {
			valueMap[stateID] = w.StateService.StateBackendImpl.GetMany(keys)
		}

		// Serialize and send the response back to the requester
		stateCommUtil.SendTcpResponse(conn, valueMap, buf)
	}
}
