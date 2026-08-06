package worker

import (
	"log"

	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"google.golang.org/protobuf/proto"
)

/******************************************************************************
					      Utils for sending StateChunk
******************************************************************************/

// gRPC Stream that responds with StateChunk-related messages
type StateChunkStream[T proto.Message] interface {
	Send(T) error
}

// Create a StateChunk message - used by stop-and-restart and lazy-basic
func newStateChunkMsg(keys [][]byte, values [][]byte) *pb.StateChunk {
	if keys == nil {
		return &pb.StateChunk{
			EndOfStream: true,
		}
	}
	return &pb.StateChunk{
		Keys:   keys,
		Values: values,
	}
}

// Create an AsyncBucketMigrationResponse message - used by lazy-optimized
func newAsyncBucketMigrationResMsg(
	keys [][]byte,
	values [][]byte,
) *pb.AsyncBucketMigrationResponse {
	if keys == nil {
		return &pb.AsyncBucketMigrationResponse{
			Message: &pb.AsyncBucketMigrationResponse_StateChunk{
				StateChunk: &pb.StateChunk{
					EndOfStream: true,
				},
			},
		}
	}
	return &pb.AsyncBucketMigrationResponse{
		Message: &pb.AsyncBucketMigrationResponse_StateChunk{
			StateChunk: &pb.StateChunk{
				Keys:   keys,
				Values: values,
			},
		},
	}
}

// StateChunkSender sends state in chunks over a stream
type StateChunkSender[T proto.Message] struct {
	stream StateChunkStream[T]

	// Max allowed chunk size
	maxChunkSize uint64

	// Handler to generate a response message
	constructResponseMsg func(keys [][]byte, values [][]byte) T

	// Current pending chunk (active buffer)
	curChunkKeys   [][]byte
	curChunkValues [][]byte
	curChunkSize   uint64
}

func NewStateChunkSender[T proto.Message](
	stream StateChunkStream[T],
	maxChunkSize uint64,
	constructResponseMsg func(keys [][]byte, values [][]byte) T,
) *StateChunkSender[T] {
	return &StateChunkSender[T]{
		stream:               stream,
		maxChunkSize:         maxChunkSize,
		constructResponseMsg: constructResponseMsg,
	}
}

// Send list of keys and values in chunks
func (s *StateChunkSender[T]) Send(keys [][]byte, values [][]byte) {

	var recSize uint64
	for i, key := range keys {

		recSize = uint64(len(key) + len(values[i]))
		if s.curChunkSize+recSize > s.maxChunkSize {

			// Send the current chunk
			err := s.stream.Send(
				s.constructResponseMsg(s.curChunkKeys, s.curChunkValues),
			)
			if err != nil {
				log.Fatalf("Failed to send state chunk: %v", err)
			}

			// Reset the current chunk
			s.resetChunk()
		}

		// Append the record to the current chunk
		s.curChunkKeys = append(s.curChunkKeys, key)
		s.curChunkValues = append(s.curChunkValues, values[i])
		s.curChunkSize += recSize
	}
}

// Flush the remaining states in the buffer and notify end of stream
func (s *StateChunkSender[T]) Flush() {

	// Send the remaining data in last chunk
	if len(s.curChunkKeys) > 0 {
		err := s.stream.Send(
			s.constructResponseMsg(s.curChunkKeys, s.curChunkValues),
		)
		if err != nil {
			log.Fatalf("Failed to send state chunk: %v", err)
		}
	}

	// Send the end of stream signal
	err := s.stream.Send(s.constructResponseMsg(nil, nil))
	if err != nil {
		log.Fatalf("Failed to send end of stream signal: %v", err)
	}

	// Reset the current chunk to prepare for the future request
	s.resetChunk()
}

func (s *StateChunkSender[T]) resetChunk() {
	s.curChunkKeys = [][]byte{}
	s.curChunkValues = [][]byte{}
	s.curChunkSize = 0
}

/******************************************************************************
								  Other Utils
******************************************************************************/

// Deduplicate states from the transferredKeys map
func dedupStates(
	keys [][]byte,
	values [][]byte,
	transferredKeys map[string]struct{},
) ([][]byte, [][]byte) {

	dedupedKeys := make([][]byte, 0, len(keys))
	dedupedValues := make([][]byte, 0, len(values))

	for i, key := range keys {
		if _, exists := transferredKeys[string(key)]; !exists {
			dedupedKeys = append(dedupedKeys, key)
			dedupedValues = append(dedupedValues, values[i])
		}
	}
	return dedupedKeys, dedupedValues
}
