package buffer

import "github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"

// A batch workunit for DRRS protocol - it's stored in the wait buffer
type DRRSBatch[T tuple.Tuple] struct {
	Records []T

	// Bucket IDs corresponding to each record
	BucketIds []uint64

	NumRecords int

	// Indicates whether the batch is from a peer
	IsFromPeer bool

	// Upstream sub-supplier name
	SubSupplierName string
}

var _ WorkUnit = (*DRRSBatch[tuple.Tuple])(nil)

// Implement WorkUnit interface
func (b *DRRSBatch[T]) GetType() WorkUnitType {
	return DRRSBatchWorkUnit
}

/******************************************************************************
						DRRSBatch specific methods
******************************************************************************/

// Constructor for DRRS batch
func NewDRRSBatch[T tuple.Tuple](
	isFromPeer bool,
	subSupplierName string,
) *DRRSBatch[T] {

	return &DRRSBatch[T]{
		Records:         make([]T, 0),
		BucketIds:       make([]uint64, 0),
		NumRecords:      0,
		IsFromPeer:      isFromPeer,
		SubSupplierName: subSupplierName,
	}
}

// Add a record to the DRRS batch - wait in the WaitBuffer
func (b *DRRSBatch[T]) AddRecord(record T, bucketId uint64) {
	b.Records = append(b.Records, record)
	b.BucketIds = append(b.BucketIds, bucketId)
	b.NumRecords++
}
