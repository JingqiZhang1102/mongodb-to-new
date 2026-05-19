package migration

import (
	"time"
)

// LagTracker tracks replication lag measurements (mutex-free, relies on parent locking)
type LagTracker struct {
	totalLag time.Duration
	count    int64
}

// NewLagTracker creates a new instance of LagTracker
func NewLagTracker() *LagTracker {
	return &LagTracker{}
}

// RecordLags records the replication lag for a batch of write operations
func (lt *LagTracker) RecordLags(ops []WriteOperation, processedTime time.Time) {
	for _, op := range ops {
		if !op.EventTime.IsZero() {
			lt.totalLag += processedTime.Sub(op.EventTime)
			lt.count++
		}
	}
}

// Flush returns the accumulated total lag and count, then resets them to 0
func (lt *LagTracker) Flush() (time.Duration, int64) {
	total := lt.totalLag
	count := lt.count
	lt.totalLag = 0
	lt.count = 0
	return total, count
}
