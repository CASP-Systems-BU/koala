package lazy

import (
	"bytes"
	"encoding/json"
	"log"
	"reflect"
	"unsafe"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/keyby"
	"github.com/CASP-Systems-BU/koala/internal/network"
	"github.com/mus-format/mus-go/raw"
)

// Metadata needed by lazy protocol for fast forwarding. This is embedded in
// StatefulOperatorBase since only stateful operators are involved in lazy
// protocol. Note that PeerCollector is not included here - it's included in
// StatefulOperatorBaseXUpstream since it needs upstream generic data type

type LazyProtocolBase[K comparable] struct {

	// Keep a copy of current worker ID. Used for fast forward addressing
	WorkerId uint16

	// Codec for key serialization/deserialization. Used by key lookup for
	// fast forwarding
	KeyEncoder network.EncodeFunc
	KeySizer   network.SizeFunc

	// Signal channel from main routing to control plane: this is to indicate
	// the main routine has entered the reconciliation phase - all following
	// input is aware of the ownership transfer. StartFastForward api returns
	// only after this signal is received - this guarantees coordinator that
	// no more local state access for keys that have been transferred
	ReconfigStartChan chan struct{}

	// Routing Table to identify the key ownership: key([]byte) -> workerId.
	// Used for fast forwarding. This is set upon StartFastForward message
	RoutingTable *keyby.PartitionTable
}

// Initialize LazyProtocolBase during stateful operator Setup() under lazy
// protocol
func NewLazyProtocolBase[K comparable](
	workerId uint16,
) *LazyProtocolBase[K] {

	var key K
	keyEncoder := network.EncodeFuncFromKind(reflect.TypeOf(key).Kind())
	keySizer := network.TupleSizeFuncFromKind(reflect.TypeOf(key).Kind())

	return &LazyProtocolBase[K]{
		WorkerId:          workerId,
		KeyEncoder:        keyEncoder,
		KeySizer:          keySizer,
		ReconfigStartChan: make(chan struct{}),
	}
}

/******************************************************************************
							Utils for lazy protocol
******************************************************************************/

// Query the new owner worker ID for a key during fast forward.
// 1. Serialize the key
// 2. Lookup the RoutingTable to get the new owner worker ID
func (l *LazyProtocolBase[K]) GetOwnerWorkerUponReconfig(key K) uint16 {

	ptrToKey := unsafe.Pointer(&key)
	serializedKey := make([]byte, l.KeySizer(ptrToKey))
	l.KeyEncoder(ptrToKey, serializedKey)
	return l.RoutingTable.KeyToWorkerID(serializedKey)
}

// Serialize any in-memory data structure as FastForwardMetadata workunit for
// transferring over the network
func (l *LazyProtocolBase[K]) SerializeToFastForwardMetadata(
	data any,
) *buffer.FastForwardMetadata {

	// Declare the buffer writer
	var buf bytes.Buffer

	// Encode workunit type (uint16) + length of serialized metadata
	// (uint32). First write 2 bytes for workunit type and reserve 4 bytes
	// as placeholder to store length after serialization
	header := make([]byte, 6)
	raw.MarshalUint16(uint16(buffer.FastForwardMetadataWorkUnit), header)
	if _, err := buf.Write(header); err != nil {
		log.Fatalln("Failed to write workunit header to buffer:", err)
	}

	// Encode the metadata (this starts after the header)
	enc := json.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		log.Fatalln("Failed to encode FastForwardMetadata:", err)
	}

	// Encode the length of serialized metadata
	raw.MarshalUint32(uint32(buf.Len()-6), buf.Bytes()[2:])

	return buffer.NewFastForwardMetadata(buf.Bytes())
}
