package supplier

import (
	"math"
	"net"
	"reflect"

	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/internal/network"
	"github.com/CASP-Systems-BU/koala/metric"
)

// SubSupplier defines a single upstream operator inside the Supplier.

type SubSupplier interface {

	// Setup SubSupplier at task placement time
	Setup()

	// Get the next WorkUnit from upstreams. This is a non-blocking read
	GetWorkUnit() (buffer.WorkUnit, bool)

	// Get the name of the SubSupplier (upstream operator name)
	GetOperatorName() string

	// Add a new upstream connection to the SubSupplier
	AddUpstream(
		conn net.Conn,
		config *configuration.Configuration,
		metricCollector *metric.MetricCollector,
		isPeer bool,
		upstreamWorkerId uint16,
	)

	// Get the number of upstreams in this SubSupplier
	GetNumUpstreams() int

	// Removes an upstream connection from the SubSupplier
	RemoveUpstream()

	// Notify the SubSupplier that all upstream connections are shutting down
	NotifyShuttingDown()

	// Notify the SubSupplier that this is a warmup run
	NotifyIsWarmUpRun()

	// Handle a received watermark workunit - update corresponding upstream
	// channel watermark
	UpdateWatermark(*buffer.Watermark)

	// Get the SubSupplier's current watermark. Return false if any upstream's
	// watermark is unset.
	GetSubSupplierWM() (*buffer.Watermark, bool)
}

// Base implementation of SubSupplier
type SubSupplierBase[T tuple.Tuple] struct {

	// Name of the upstream operator
	OperatorName string

	// List of upstreams belonging to this upstream operator
	Upstreams []*network.Upstream[T]

	// [safety check] Flag to indicate all input channels are terminating
	IsShuttingDown bool

	// Flag to indicate if this is a warmup run
	IsWarmUpRun bool

	// Decoding function
	Decoder network.TupleDecoder
}

// Constructor at compile time
func NewSubSupplierBase[T tuple.Tuple](
	operatorName string,
) *SubSupplierBase[T] {
	return &SubSupplierBase[T]{
		OperatorName: operatorName,
	}
}

// Setup SubSupplier at task placement time
func (ss *SubSupplierBase[T]) Setup() {

	ss.Upstreams = make([]*network.Upstream[T], 0)

	// Create the Decoding function based on type T
	var zero T
	decoder := network.CreateDecoder(reflect.TypeOf(zero).Elem())
	ss.Decoder = decoder
}

/******************************************************************************
                   	 	SubSupplier API base implementation
******************************************************************************/

// Get the name of the SubSupplier (upstream operator name)
func (ss *SubSupplierBase[T]) GetOperatorName() string {
	return ss.OperatorName
}

// Add a new upstream to the SubSupplier
func (ss *SubSupplierBase[T]) AddUpstream(
	conn net.Conn,
	config *configuration.Configuration,
	metricCollector *metric.MetricCollector,
	isPeer bool,
	upstreamWorkerId uint16,
) {

	newUpstream := network.NewUpstream[T](
		conn,
		config,
		metricCollector,
		upstreamWorkerId,
	)

	// Start network routine for this new upstream
	// TODO: Add configurable network routines - now 1 per upstream
	go newUpstream.SupplyInputBuffer(ss.Decoder, isPeer)

	ss.Upstreams = append(ss.Upstreams, newUpstream)
}

// Get number of upstreams in this SubSupplier
func (ss *SubSupplierBase[T]) GetNumUpstreams() int {
	return len(ss.Upstreams)
}

// Traverse all upstreams of this SubSupplier and calculate the min watermark.
// If any upstream's watermark is unset, return false.
func (ss *SubSupplierBase[T]) GetSubSupplierWM() (*buffer.Watermark, bool) {

	if len(ss.Upstreams) == 0 {
		return nil, false
	}

	curMinWM := buffer.NewWatermark(math.MaxInt64)
	for _, upstream := range ss.Upstreams {

		// If any upstream's watermark is unset, return false.
		if upstream.WM.Unset() {
			return nil, false
		}

		curMinWM.SetMin(upstream.WM)
	}

	return curMinWM, true
}

// Notify the SubSupplier that all upstream connections are shutting down
func (ss *SubSupplierBase[T]) NotifyShuttingDown() {
	ss.IsShuttingDown = true
}

// Notify the SubSupplier that this is a warmup run
func (ss *SubSupplierBase[T]) NotifyIsWarmUpRun() {
	ss.IsWarmUpRun = true
}
