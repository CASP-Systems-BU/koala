package buffer

// Metadata for fast-forwarding in the lazy protocol. This is the first message
// sent over the peer channels. Operator needs to handle the (de)serialization
// for the metadata

type FastForwardMetadata struct {

	// Serialized data for fast-forward metadata [workunit_type, bytes]
	SerializedMetadata []byte
}

var _ WorkUnit = (*FastForwardMetadata)(nil)

func NewFastForwardMetadata(buf []byte) *FastForwardMetadata {
	return &FastForwardMetadata{
		SerializedMetadata: buf,
	}
}

// Implement WorkUnit interface
func (i *FastForwardMetadata) GetType() WorkUnitType {
	return FastForwardMetadataWorkUnit
}
