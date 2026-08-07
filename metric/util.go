package metric

import (
	"log"
	"time"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
)

// BatchWorkUnit is used to only expose the GetNumRecords,
// GetInputBufferInsertTime methods for a batch
// This is to avoid explicit type conversion to Batch[T] in metric collector
type BatchWorkUnit interface {
	GetNumRecords() uint64
	GetInputBufferInsertTime() time.Time
}

// Convert WorkUnit to BatchWorkUnit
func getBatchWorkUnit(workUnit buffer.WorkUnit) BatchWorkUnit {
	batchWorkUnit, ok := workUnit.(BatchWorkUnit)
	if !ok {
		log.Fatalf("Failed to convert WorkUnit to BatchWorkUnit")
	}
	return batchWorkUnit
}
