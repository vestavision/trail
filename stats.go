package trail

import "sync/atomic"

var counters struct {
	written       atomic.Uint64
	dropped       atomic.Uint64
	buffered      atomic.Uint64
	publishErrors atomic.Uint64
}

type Statistics struct {
	Written       uint64
	Dropped       uint64
	Buffered      uint64
	PublishErrors uint64
}

// Stats returns a lock-free snapshot. Monotonic counters cover the process
// lifetime; Buffered is the current active-writer gauge.
func Stats() Statistics {
	return Statistics{
		Written:       counters.written.Load(),
		Dropped:       counters.dropped.Load(),
		Buffered:      counters.buffered.Load(),
		PublishErrors: counters.publishErrors.Load(),
	}
}
