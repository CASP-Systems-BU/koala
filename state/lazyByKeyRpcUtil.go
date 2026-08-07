package state

import (
	"log"

	pb "github.com/CASP-Systems-BU/koala/internal/grpc"
)

// [gRPC] Fetch remote state over gRPC API
func (s *StateService) fetchStateByRpc(
	remoteWorkerId uint16,
	keys map[uint16][][]byte,
	res map[uint16][][]byte,
) {

	// Get gRPC connection to the remote state service
	remoteFetchStream := s.getByKeyMigrationConn(remoteWorkerId)

	// Construct the state fetch request
	stateKeyLists := make([]*pb.StateKeyList, 0, len(keys))
	for stateID, keyList := range keys {
		stateKeyLists = append(stateKeyLists, &pb.StateKeyList{
			StateId: uint32(stateID),
			Keys:    keyList,
		})
	}
	req := &pb.LazyByKeyStateRequest{
		StateKeyLists: stateKeyLists,
	}

	// Send the request
	if err := remoteFetchStream.Send(req); err != nil {
		log.Fatalf(
			"Failed to send lazy-by-key gRPC state fetch request to worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Receive the KeyResponse message
	response, err := remoteFetchStream.Recv()
	if err != nil {
		log.Fatalf(
			"Failed to receive lazy-by-key gRPC key fetch response from worker %d: %v\n",
			remoteWorkerId,
			err,
		)
	}

	// Populate the result map with received values
	for _, stateValueList := range response.StateValueLists {
		stateId := uint16(stateValueList.StateId)
		res[stateId] = stateValueList.Values
	}
}
