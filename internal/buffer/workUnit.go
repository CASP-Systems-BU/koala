package buffer

// WorkUnit is an element in the buffer. It has multiple types:
// 1. *Batch[T]
// 2. *DrainBarrier
// 3. *Watermark
// 4. *CheckpointBarrier - TODO
// 5. *InflightBarrier (lazy protocol)
// 6. *TerminationSignal (lazy protocol)
// 7. *FastForwardMetadata (lazy protocol)
// 8. *DRRSBatch (DRRS protocol)

type WorkUnitType uint16

const (
	BatchWorkUnit WorkUnitType = iota + 1
	DrainBarrierWorkUnit
	WatermarkWorkUnit
	CheckpointBarrierWorkUnit
	InflightBarrierWorkUnit
	TerminationSignalWorkUnit
	FastForwardMetadataWorkUnit
	DRRSBatchWorkUnit
)

type WorkUnit interface {

	// Get the type of the WorkUnit
	GetType() WorkUnitType
}
