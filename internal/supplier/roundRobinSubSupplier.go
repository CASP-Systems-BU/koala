package supplier

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
)

// RoundRobinSubSupplier implements the round-robin traversal among all upstream
// connections.

type RoundRobinSubSupplier[T tuple.Tuple] struct {
	*SubSupplierBase[T]

	// Idx of the next upstream to consume (round-robin)
	NextUpstreamIdx int
}

var _ SubSupplier = (*RoundRobinSubSupplier[tuple.Tuple])(nil)

// Constructor at compile time
func NewRoundRobinSubSupplier[T tuple.Tuple](
	operatorName string,
) *RoundRobinSubSupplier[T] {
	return &RoundRobinSubSupplier[T]{
		SubSupplierBase: NewSubSupplierBase[T](operatorName),
	}
}

// Setup SubSupplier at task placement time
func (rss *RoundRobinSubSupplier[T]) Setup() {
	rss.SubSupplierBase.Setup()
	rss.NextUpstreamIdx = -1
}

/******************************************************************************
						Implement SubSupplier interface
******************************************************************************/

// Read the next WorkUnit from the next upstream in round-robin order
// Return false if no upstreams are connected or all upstream channels are empty
func (rss *RoundRobinSubSupplier[T]) GetWorkUnit() (buffer.WorkUnit, bool, bool) {

	if len(rss.Upstreams) == 0 {
		return nil, false, false
	}

	var nextUpstream *network.Upstream[T]
	var nextWorkUnit buffer.WorkUnit
	var ok bool

	// Traverse all upstreams of this SubSupplier in round-robin order.
	// Jump to the next upstream if the current upstream input channel is empty.
	// Unblocking read is necessary here to avoid deadlock in some cases.
	for i := 0; i < len(rss.Upstreams); i++ {

		nextUpstream = rss.incrementNextUpstreamIdx()

		// [DRRS] Check if we can only consume peer channels. This happens when
		// upstream wait buffer are full and we use this way to block upstream
		// input channels
		if rss.OnlyConsumePeers && !nextUpstream.IsPeer {
			continue
		}

		// [DRRS] If this channel has received in-flight barrier but not aligned
		// yet, we cannot consume data from it
		if nextUpstream.InflightBarrierReceived {
			continue
		}

		nextWorkUnit, ok = nextUpstream.ReadFromBufferNonblock()
		if ok {
			return nextWorkUnit, nextUpstream.IsPeer, true
		}
	}

	// We have traversed all input channels of this SubSupplier and they are all
	// empty.
	return nil, false, false
}

// Receive a watermark workunit, update the watermark for the corresponding
// upstream data channel. The current NextUpstreamIdx must point to the
// upstream channel that received this watermark.
func (rss *RoundRobinSubSupplier[T]) UpdateWatermark(
	watermark *buffer.Watermark,
) {

	rss.Upstreams[rss.NextUpstreamIdx].WM.Update(watermark)
}

// [DRRS]
func (rss *RoundRobinSubSupplier[T]) SetInflightBarrierReceived() {
	if rss.Upstreams[rss.NextUpstreamIdx].InflightBarrierReceived {
		log.Fatalf(
			"SubSupplier [%s] SetInflightBarrierReceived: upstream %d already set\n",
			rss.OperatorName,
			rss.NextUpstreamIdx,
		)
	}
	if rss.Upstreams[rss.NextUpstreamIdx].IsPeer {
		log.Fatalf(
			"SubSupplier [%s] SetInflightBarrierReceived: upstream %d is peer\n",
			rss.OperatorName,
			rss.NextUpstreamIdx,
		)
	}
	rss.Upstreams[rss.NextUpstreamIdx].InflightBarrierReceived = true
}

// [DRRS] Reset and unblock this upstream channel
func (rss *RoundRobinSubSupplier[T]) ResetInflightBarrier() {

	for _, upstream := range rss.Upstreams {
		if upstream.IsPeer {
			continue
		}
		if !upstream.InflightBarrierReceived {
			log.Fatalf(
				"SubSupplier [%s] ResetInflightBarrier: upstream not set\n",
				rss.OperatorName,
			)
		}
		upstream.InflightBarrierReceived = false
	}
}

// Current upstream is terminated, release resources and remove it from the
// Upstreams list. The current NextUpstreamIdx must point to the upstream
// channel that is terminated.
func (rss *RoundRobinSubSupplier[T]) RemoveUpstream() {

	// Case that shouldn't happen
	if rss.NextUpstreamIdx < 0 || rss.NextUpstreamIdx >= len(rss.Upstreams) {
		log.Fatalf(
			"SubSupplier [%s] RemoveUpstream: invalid index\n",
			rss.OperatorName,
		)
	}

	if len(rss.Upstreams) < 2 && !rss.IsShuttingDown {
		log.Fatalf(
			"SubSupplier [%s] RemoveUpstream: shouldn't remove the last upstream\n",
			rss.OperatorName,
		)
	}

	// Release the input buffer
	rss.Upstreams[rss.NextUpstreamIdx].InputBuffer = nil

	// In place remove the current upstream and slide the rest of the list
	// to the left by 1 ~ O(n)
	copy(
		rss.Upstreams[rss.NextUpstreamIdx:],
		rss.Upstreams[rss.NextUpstreamIdx+1:],
	)

	// resize the list
	rss.Upstreams = rss.Upstreams[:len(rss.Upstreams)-1]

	// Move the pointer to the previous upstream
	if rss.NextUpstreamIdx == 0 {
		rss.NextUpstreamIdx = len(rss.Upstreams) - 1
	} else {
		rss.NextUpstreamIdx -= 1
	}

	log.Printf(
		"[SubSupplier %s] Upstream connection closed with %d upstreams remaining\n",
		rss.OperatorName,
		len(rss.Upstreams),
	)
}

/******************************************************************************
                            RoundRobinSubSupplier utils
******************************************************************************/

func (rss *RoundRobinSubSupplier[T]) incrementNextUpstreamIdx() *network.Upstream[T] {

	// Move forward the upstream pointer - round robin
	rss.NextUpstreamIdx += 1
	if rss.NextUpstreamIdx >= len(rss.Upstreams) {
		rss.NextUpstreamIdx = 0
	}
	nextUpstream := rss.Upstreams[rss.NextUpstreamIdx]

	return nextUpstream
}
