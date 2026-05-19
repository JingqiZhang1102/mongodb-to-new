package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

// StatsManager coordinates statistics and replication lag tracking thread-safely
type StatsManager struct {
	mu                   sync.Mutex
	eventsSinceLastStats int
	lastStatsTime        time.Time
	lagTracker           *LagTracker
	log                  *logger.Logger
	statsInterval        time.Duration
}

// NewStatsManager creates a new StatsManager
func NewStatsManager(log *logger.Logger, interval time.Duration) *StatsManager {
	return &StatsManager{
		lastStatsTime: time.Now(),
		lagTracker:    NewLagTracker(),
		log:           log,
		statsInterval: interval,
	}
}

// RecordLags records processing lag for a batch of operations and increments processed events count
func (sm *StatsManager) RecordLags(ops []WriteOperation, processedTime time.Time) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordLags(ops, processedTime)
	}
	sm.eventsSinceLastStats += len(ops)
}


// Start begins the periodic statistics reporting loop
func (sm *StatsManager) Start(ctx context.Context) {
	ticker := time.NewTicker(sm.statsInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sm.ReportStats()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ReportStats logs the accumulated change stream and lag statistics, resetting internal counters
func (sm *StatsManager) ReportStats() {
	sm.mu.Lock()
	eventCount := sm.eventsSinceLastStats
	duration := time.Since(sm.lastStatsTime)
	sm.eventsSinceLastStats = 0
	sm.lastStatsTime = time.Now()

	var totalLag time.Duration
	var count int64
	if sm.lagTracker != nil {
		totalLag, count = sm.lagTracker.Flush()
	}
	sm.mu.Unlock()

	var avgLagStr string
	if count > 0 {
		avgLag := totalLag / time.Duration(count)
		avgLagStr = fmt.Sprintf(", average processing lag: %v", avgLag.Round(time.Millisecond))
	} else {
		avgLagStr = ", average processing lag: N/A"
	}

	if duration > 0 && eventCount > 0 {
		rate := float64(eventCount) / duration.Seconds()
		sm.log.Infof("Change stream statistics: Processed %d events in the last %v (%.2f events/second)%s",
			eventCount, duration.Round(time.Second), rate, avgLagStr)
	} else if eventCount > 0 {
		sm.log.Infof("Change stream statistics: Processed %d events since last report%s", eventCount, avgLagStr)
	} else {
		sm.log.Info("Change stream statistics: No events processed since last report")
	}
}
