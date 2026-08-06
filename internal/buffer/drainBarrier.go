package buffer

type DrainBarrier struct {
}

var _ WorkUnit = (*DrainBarrier)(nil)

func NewDrainBarrier() *DrainBarrier {
	return &DrainBarrier{}
}

// Implement WorkUnit interface
func (d *DrainBarrier) GetType() WorkUnitType {
	return DrainBarrierWorkUnit
}

/*****************************************************************************
						  DrainBarrier specific methods
******************************************************************************/
