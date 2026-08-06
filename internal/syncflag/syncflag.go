package syncflag

import (
	"sync"
	"sync/atomic"
)

// SyncFlag is used for synchronization and signaling

type SyncFlag struct {
	// 0 = not signaled (blocked), 1 = signaled (unblocked)
	flag int32
	// Condition variable for waiting and signaling
	cond *sync.Cond
}

func NewSyncFlag() *SyncFlag {
	return &SyncFlag{
		flag: 0,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Wait blocks until the it is signaled.
func (sf *SyncFlag) Wait() {

	sf.cond.L.Lock()
	defer sf.cond.L.Unlock()
	for atomic.LoadInt32(&sf.flag) == 0 {
		sf.cond.Wait()
	}
}

// Quick check if it's blocked
func (sf *SyncFlag) IsBlocked() bool {
	return atomic.LoadInt32(&sf.flag) == 0
}

// Signal sets the flag as signaled and wakes up all waiting goroutines.
func (sf *SyncFlag) Signal() {
	// Set the flag to signaled state
	atomic.StoreInt32(&sf.flag, 1)

	// Wake up all waiting goroutines
	sf.cond.L.Lock()
	sf.cond.Broadcast()
	sf.cond.L.Unlock()
}

// Reset sets the flag back to unsignaled state.
func (sf *SyncFlag) Reset() {
	// Reset the flag to unsignaled state
	atomic.StoreInt32(&sf.flag, 0)
}
