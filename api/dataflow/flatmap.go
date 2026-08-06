package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
)

type Flatmap[IN, OUT tuple.Tuple] struct {
	*OperatorBase

	// UDF that takes 1 input record and allows user to
	// output 0 or more output records through the collector
	F func(IN, collector.Collector)
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*Flatmap[tuple.Tuple, tuple.Tuple])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*Flatmap[tuple.Tuple, tuple.Tuple])(
	nil,
)

// API exposed to users
// Define operator name and flatmapper function
func NewFlatmap[IN, OUT tuple.Tuple](
	name string,
	f func(IN, collector.Collector),
) *Flatmap[IN, OUT] {

	flatmapper := &Flatmap[IN, OUT]{
		OperatorBase: NewOperatorBase(name),
		F:            f,
	}

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	flatmapper.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	flatmapper.Collector = collector.NewRoundRobinCollector[OUT](
		name,
	)

	return flatmapper
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (flatmap *Flatmap[IN, OUT]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	for _, record := range batch.Records[0:batch.TotalNumRecords] {

		flatmap.Collector.SetCurrentTimestamp(record.GetTimestamp())

		// Execute UDF to process the input record
		flatmap.F(record, flatmap.Collector)
	}
}

// Validate input type at compile time
func (flatmap *Flatmap[IN, OUT]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (flatmap *Flatmap[IN, OUT]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
