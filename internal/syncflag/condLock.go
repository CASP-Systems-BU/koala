package syncflag

import (
	"sync"
	"sync/atomic"
)

// Note: this structure is not used anymore: we now use regular mutex locks.

// [WARNING] Conditional lock here only works for 2-routines scenario.
// Specifically, we have a main routine constantly accessing the shared data,
// and we have another routine ocasionally accessing the shared data. We want to
// avoid mutex locks for main routine when another routine is not active.

type CondLock struct {

	// 0: Not in race condition and main is not using the data
	// 1: Not in race condition but main is using the data
	// -1: In race condition and lock is required
	flag int32

	//
	lock sync.Mutex
}

func NewCondLock() *CondLock {
	return &CondLock{
		flag: 0,
		lock: sync.Mutex{},
	}
}

// This is called by main routine to regularly access the shared data
func (cl *CondLock) ConditionalLock() {

	// If true, it means another routine is currently accessing the data
	// Here flag can only be 0 or -1
	inRaceCondition := !atomic.CompareAndSwapInt32(&cl.flag, 0, 1)

	if inRaceCondition {
		cl.lock.Lock()
	}
}

// Follow the ConditionalLock() to unlock the data
func (cl *CondLock) ConditionalUnlock() {

	// If true, it means it was in race condition and lock was acquired by
	// ConditionalLock(). Here flag can be 1, 0, or -1.
	inRaceCondition := !atomic.CompareAndSwapInt32(&cl.flag, 1, 0)

	if inRaceCondition {
		cl.lock.Unlock()
	}
}

// This is called by side routine to ocasionally access the shared data
func (cl *CondLock) ForceLock() {

	// Keep trying to start race condition
	// Only success when no one is currenly accessing the data (flag == 0)
	for {
		if atomic.CompareAndSwapInt32(&cl.flag, 0, -1) {
			break
		}
	}

	cl.lock.Lock()
}

// Follow the ForceLock() to unlock the data
func (cl *CondLock) ForceUnlock() {

	cl.lock.Unlock()
	atomic.StoreInt32(&cl.flag, 0)
}
