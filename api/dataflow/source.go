package dataflow

import (
	"log"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/watermark"
)

type Source[OUT tuple.Tuple] struct {
	*OperatorBase

	// UDF that takes collector to emit records
	F func(collector.Collector)

	// Source replica id, assigned during deployment
	// to identify the source replica
	ReplicaID int64
}

// Type validation at compile time
var _ OperatorWith1OutputStream[tuple.Tuple] = (*Source[tuple.Tuple])(nil)

// API exposed to users
// This should be a long-running function
// Define operator ID and source function
func NewSource[OUT tuple.Tuple](
	id string,
	f func(collector.Collector),
) *Source[OUT] {

	source := &Source[OUT]{
		OperatorBase: NewOperatorBase(id),
		F:            f,
		ReplicaID:    -1, // default value
	}

	// Supplier is not needed for Source

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	source.Collector = collector.NewRoundRobinCollector[OUT](id)

	return source
}

// Implement the interface method RunSource()
func (source *Source[OUT]) RunSource() {

	// Pass in the collector for users to emit records
	source.F(source.Collector)
}

// Source-specific Setup() method that override the OperatorBase.Setup()
// Set source replica ID from OperatorSetupParas
func (source *Source[OUT]) Setup(paras *utils.OperatorSetupParas) {

	if paras == nil {
		log.Fatalf("OperatorSetupParas is nil")
	}

	if paras.SourceReplicaID < 0 {
		log.Fatalf("Invalid source replica ID: %d", paras.SourceReplicaID)
	}

	source.ReplicaID = paras.SourceReplicaID
	log.Printf("[info] Setup Source with replica ID: %d", source.ReplicaID)

	// Set up OperatorBase
	source.OperatorBase.Setup(paras)
}

/******************************************************************************
						Source specific APIs to users
******************************************************************************/

// Assign timestamp and watermark generation related parameters
// We only have periodic watermark generator for now
func (source *Source[OUT]) AssignTimestampAndWatermark(
	tsAssigner *ta.TimestampAssigner[OUT],
	watermarkInterval time.Duration,
	maxAllowedDelay time.Duration,
) {

	if tsAssigner == nil {
		log.Fatalf("TimestampAssigner is nil")
	}

	periodicGenerator := watermark.NewPeriodicWatermarkGenerator(
		watermarkInterval,
		maxAllowedDelay,
	)

	source.Collector.SetTsAssignerAndWatermarkGenerator(
		tsAssigner,
		periodicGenerator,
	)
}

// Validate output type at compile time
func (source *Source[OUT]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
