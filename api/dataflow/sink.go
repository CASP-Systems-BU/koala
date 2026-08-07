package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
)

type Sink[IN tuple.Tuple] struct {
	*OperatorBase

	// UDF that takes 1 input record
	F func(IN)
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*Sink[tuple.Tuple])(nil)

// API exposed to users
// Define operator name and sink function
func NewSink[IN tuple.Tuple](name string, f func(IN)) *Sink[IN] {

	sink := &Sink[IN]{
		OperatorBase: NewOperatorBase(name),
		F:            f,
	}

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	sink.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Collector is not needed for sink

	return sink
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (sink *Sink[IN]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	for _, record := range batch.Records[0:batch.TotalNumRecords] {

		// Execute UDF to process the input record
		sink.F(record)
	}
}

// Validate input type at compile time
func (sink *Sink[IN]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}
