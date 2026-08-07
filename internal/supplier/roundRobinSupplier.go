package supplier

import (
	"time"

	"github.com/CASP-Systems-BU/koala/internal/buffer"
	"github.com/CASP-Systems-BU/koala/internal/utils"
)

// RoundRobinSupplier implements Supplier interface. It applies the
// round-robin traversal among all upstream operators (SubSuppliers).

type RoundRobinSupplier struct {
	*SupplierBase

	// Idx of next SubSupplier to consume (round-robin)
	NextSubSupplierIdx int

	// Index list for round-robin traversal of SubSuppliers: list of upstream
	// operator names
	SubSupplierList []string
}

var _ Supplier = (*RoundRobinSupplier)(nil)

// Constructor called at compile time
func NewRoundRobinSupplier(
	operatorName string,
	expectedNumSubSuppliers int,
) *RoundRobinSupplier {

	return &RoundRobinSupplier{
		SupplierBase: NewSupplierBase(operatorName, expectedNumSubSuppliers),
	}
}

/******************************************************************************
                          Implement Supplier interface
******************************************************************************/

// Setup at task placement time
func (rs *RoundRobinSupplier) Setup(para *utils.OperatorSetupParas) {

	rs.SupplierBase.Setup(para)

	// Init the SubSupplierList for round-robin traversal
	rs.SubSupplierList = make([]string, 0, len(rs.SubSuppliers))
	for _, subSupplier := range rs.SubSuppliers {
		rs.SubSupplierList = append(
			rs.SubSupplierList,
			subSupplier.GetOperatorName(),
		)
	}
	rs.NextSubSupplierIdx = -1
}

/*
Return the next WorkUnit and which upstream operator it comes from.
Read the next WorkUnit from the next SubSupplier in round-robin order.
This API pre-processes the WorkUnit within the Supplier if needed e.g. check
watermark progression, drainBarrier alignment, inflightBarrier alignment, etc.

If all SubSuppliers have no input data at the moment, this API will sleep for a
while and retry. The sleep interval allows new upstreams to connect and new data
to arrive.
*/
func (rs *RoundRobinSupplier) GetWorkUnit() (buffer.WorkUnit, string) {

	sleepInterval := rs.Config.BufferSleepInterval

	// Track the number of failed attempts to read from SubSuppliers
	numEmptySubSupplierRead := 0

	rs.Lock()
	defer rs.Unlock()
	for {

		nextSubSupplier, subSupplierName := rs.incrementNextSubSupplierIdx()

		if nextWorkUnitToProcess, ok := nextSubSupplier.GetWorkUnit(); ok {

			// Successfully read from SubSupplier, reset the empty read counter
			numEmptySubSupplierRead = 0

			// Report 0 idlerate to ensure there is data point reported
			rs.MetricCollector.UpdateIdleTime(0)

			if nextWorkUnitToReturn, ok := rs.preprocessWorkUnit(nextWorkUnitToProcess, subSupplierName); ok {
				return nextWorkUnitToReturn, subSupplierName
			}
			continue
		}

		numEmptySubSupplierRead++
		if numEmptySubSupplierRead >= len(rs.SubSuppliers) {
			numEmptySubSupplierRead = 0
			// All SubSuppliers are empty for now, sleep for a while to wait
			// for new upstreams to connect or new data to arrive. Update the
			// IdleTime metric to capture this sleep time
			rs.Unlock()
			time.Sleep(sleepInterval)
			rs.MetricCollector.UpdateIdleTime(sleepInterval)
			rs.Lock()
		} else {
			// Report 0 to ensure there is data point reported
			rs.MetricCollector.UpdateIdleTime(0)
		}
	}
}

/******************************************************************************
                            RoundRobinSupplier utils
******************************************************************************/

// Increment the next SubSupplier index in round-robin order
func (rs *RoundRobinSupplier) incrementNextSubSupplierIdx() (SubSupplier, string) {

	rs.NextSubSupplierIdx += 1
	if rs.NextSubSupplierIdx >= len(rs.SubSuppliers) {
		rs.NextSubSupplierIdx = 0
	}

	subSupplierName := rs.SubSupplierList[rs.NextSubSupplierIdx]

	return rs.SubSuppliers[subSupplierName], subSupplierName
}
