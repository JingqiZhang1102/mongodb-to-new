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
	mu                             sync.Mutex
	lastStatsTime                  time.Time
	lagTracker                     *LagTracker
	log                            *logger.Logger
	statsInterval                  time.Duration
	updateDocMissingSinceLastStats int

	// Maps for Received, Processed, and Failed counts by operationType string
	receivedSinceLastStats  map[string]int
	processedSinceLastStats map[string]int
	failedSinceLastStats    map[string]int
}

// NewStatsManager creates a new StatsManager
func NewStatsManager(log *logger.Logger, interval time.Duration) *StatsManager {
	return &StatsManager{
		lastStatsTime:           time.Now(),
		lagTracker:              NewLagTracker(),
		log:                     log,
		statsInterval:           interval,
		receivedSinceLastStats:  make(map[string]int),
		processedSinceLastStats: make(map[string]int),
		failedSinceLastStats:    make(map[string]int),
	}
}

// RecordLags records processing lag for a batch of operations and increments processed events count
func (sm *StatsManager) RecordLags(ops []WriteOperation, processedTime time.Time) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordLags(ops, processedTime)
	}
	for _, op := range ops {
		sm.processedSinceLastStats[op.OpType]++
	}
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

// IncrementUpdateDocMissing increments the count of updates skipped due to missing fullDocument thread-safely
func (sm *StatsManager) IncrementUpdateDocMissing() {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.updateDocMissingSinceLastStats++
}

// IncrementEventsReceived increments the count of received input events by operation type thread-safely
func (sm *StatsManager) IncrementEventsReceived(opType string) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.receivedSinceLastStats[opType]++
}

// IncrementEventsFailed increments the count of failed write operations by operation type thread-safely
func (sm *StatsManager) IncrementEventsFailed(opType string) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.failedSinceLastStats[opType]++
}

// ReportStats logs the accumulated change stream and lag statistics, resetting internal counters
func (sm *StatsManager) ReportStats() {
	sm.mu.Lock()
	getTypeCounts := func(m map[string]int) (int, int, int, int) {
		inserts := m["insert"]
		updates := m["update"] + m["replace"]
		deletes := m["delete"]
		total := 0
		for _, v := range m {
			total += v
		}
		return inserts, updates, deletes, total
	}

	insertsReceived, updatesReceived, deletesReceived, eventsReceived := getTypeCounts(sm.receivedSinceLastStats)
	insertsProcessed, updatesProcessed, deletesProcessed, eventsProcessed := getTypeCounts(sm.processedSinceLastStats)
	insertsFailed, updatesFailed, deletesFailed, eventsFailed := getTypeCounts(sm.failedSinceLastStats)

	updateDocMissing := sm.updateDocMissingSinceLastStats

	duration := time.Since(sm.lastStatsTime)

	sm.receivedSinceLastStats = make(map[string]int)
	sm.processedSinceLastStats = make(map[string]int)
	sm.failedSinceLastStats = make(map[string]int)
	sm.updateDocMissingSinceLastStats = 0
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

	var rateReceived, rateProcessed, rateFailed, rateUpdateDocMissing float64
	var rateInsertsReceived, rateUpdatesReceived, rateDeletesReceived float64
	var rateInsertsProcessed, rateUpdatesProcessed, rateDeletesProcessed float64
	var rateInsertsFailed, rateUpdatesFailed, rateDeletesFailed float64

	if duration.Seconds() > 0 {
		rateReceived = float64(eventsReceived) / duration.Seconds()
		rateProcessed = float64(eventsProcessed) / duration.Seconds()
		rateFailed = float64(eventsFailed) / duration.Seconds()
		rateUpdateDocMissing = float64(updateDocMissing) / duration.Seconds()

		rateInsertsReceived = float64(insertsReceived) / duration.Seconds()
		rateUpdatesReceived = float64(updatesReceived) / duration.Seconds()
		rateDeletesReceived = float64(deletesReceived) / duration.Seconds()

		rateInsertsProcessed = float64(insertsProcessed) / duration.Seconds()
		rateUpdatesProcessed = float64(updatesProcessed) / duration.Seconds()
		rateDeletesProcessed = float64(deletesProcessed) / duration.Seconds()

		rateInsertsFailed = float64(insertsFailed) / duration.Seconds()
		rateUpdatesFailed = float64(updatesFailed) / duration.Seconds()
		rateDeletesFailed = float64(deletesFailed) / duration.Seconds()
	}

	msg := fmt.Sprintf("Change stream statistics: Received %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], Processed %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], Failed %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], updateDocMissing %d (%.2f events/sec)%s in the last %v",
		eventsReceived, rateReceived, insertsReceived, rateInsertsReceived, updatesReceived, rateUpdatesReceived, deletesReceived, rateDeletesReceived,
		eventsProcessed, rateProcessed, insertsProcessed, rateInsertsProcessed, updatesProcessed, rateUpdatesProcessed, deletesProcessed, rateDeletesProcessed,
		eventsFailed, rateFailed, insertsFailed, rateInsertsFailed, updatesFailed, rateUpdatesFailed, deletesFailed, rateDeletesFailed,
		updateDocMissing, rateUpdateDocMissing, avgLagStr, duration.Round(time.Second))

	sm.log.Info(msg)
}

// GetProcessedCount returns the count of processed events of the given type thread-safely
func (sm *StatsManager) GetProcessedCount(opType string) int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.processedSinceLastStats[opType]
}

// GetReceivedCount returns the count of received events of the given type thread-safely
func (sm *StatsManager) GetReceivedCount(opType string) int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.receivedSinceLastStats[opType]
}

// GetFailedCount returns the count of failed events of the given type thread-safely
func (sm *StatsManager) GetFailedCount(opType string) int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.failedSinceLastStats[opType]
}
