package buffer

// MigratingKeyMap is used during lazy-by-key reconfiguration to transfer
// the key lookup table (KeyMap) for affected buckets from the old worker
// to the new worker

type MigratingKeyMap struct {

	// Serialized key map data [workunit_type, bytes]
	SerializedKeyMap []byte
}

var _ WorkUnit = (*MigratingKeyMap)(nil)

func NewMigratingKeyMap(buf []byte) *MigratingKeyMap {
	return &MigratingKeyMap{
		SerializedKeyMap: buf,
	}
}

// Implement WorkUnit interface
func (i *MigratingKeyMap) GetType() WorkUnitType {
	return MigratingKeyMapWorkUnit
}
