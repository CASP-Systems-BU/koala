package dataflow

import (
	"log"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/supplier"
)

type Mapper[IN, OUT tuple.Tuple] struct {
	*OperatorBase

	// UDF that takes 1 input record and output 1 output record
	F func(IN) OUT
}

// Type validation at compile time
var _ OperatorWith1InputStream[tuple.Tuple] = (*Mapper[tuple.Tuple, tuple.Tuple])(
	nil,
)

var _ OperatorWith1OutputStream[tuple.Tuple] = (*Mapper[tuple.Tuple, tuple.Tuple])(
	nil,
)

// API exposed to users
// Define operator name and mapper function
func NewMapper[IN, OUT tuple.Tuple](
	name string,
	f func(IN) OUT,
) *Mapper[IN, OUT] {

	mapper := &Mapper[IN, OUT]{
		OperatorBase: NewOperatorBase(name),
		F:            f,
	}

	// Default supplier is RoundRobinSupplier - 1 upstream operator expected
	mapper.Supplier = supplier.NewRoundRobinSupplier(name, 1)

	// Default collector is RoundRobinCollector
	// It can be reset to KeybyCollector by calling keyby operation
	mapper.Collector = collector.NewRoundRobinCollector[OUT](name)

	return mapper
}

// Implement the interface method ProcessBatch(buffer.WorkUnit)
func (mapper *Mapper[IN, OUT]) ProcessBatch(
	workUnit buffer.WorkUnit,
	subSupplierName string,
) {

	batch, ok := workUnit.(*buffer.Batch[IN])
	if !ok {
		log.Fatalln("Input to ProcessBatch() should be Batch[T]")
	}

	var output OUT
	for _, record := range batch.Records[0:batch.TotalNumRecords] {

		// Execute UDF to process the input record
		output = mapper.F(record)

		// Set the event time for the output record
		mapper.Collector.SetCurrentTimestamp(record.GetTimestamp())

		// Call collector to push the output record to the output buffer
		mapper.Collector.Emit(output)
	}
}

// Validate input type at compile time
func (mapper *Mapper[IN, OUT]) InputTupleType1() IN {
	panic("Uninvokable: this is for compile time check")
}

// Validate output type at compile time
func (mapper *Mapper[IN, OUT]) OutputTupleType1() OUT {
	panic("Uninvokable: this is for compile time check")
}
