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
	sequentialRetriesSinceLastStats int
	orderedBulkWritesSinceLastStats     int
	orderedBulkWritesSizeSinceLastStats int
	orderedSizesHistogram               []int
	unorderedBulkWritesSinceLastStats   int
	unorderedBulkWritesSizeSinceLastStats int
	unorderedSizesHistogram             []int
	timeoutFlushesSinceLastStats        int

	// Maps for Received, Processed, and Failed counts by operationType string
	receivedSinceLastStats  map[string]int
	processedSinceLastStats map[string]int
	failedSinceLastStats    map[string]int

	totalQueueLatency     map[string]time.Duration
	queueLatencyCount     map[string]int64
	totalBulkWriteLatency map[string]time.Duration
	bulkWriteLatencyCount map[string]int64
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
		orderedSizesHistogram:   make([]int, 4096),
		unorderedSizesHistogram: make([]int, 4096),
		totalQueueLatency:      make(map[string]time.Duration),
		queueLatencyCount:      make(map[string]int64),
		totalBulkWriteLatency:  make(map[string]time.Duration),
		bulkWriteLatencyCount:  make(map[string]int64),
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

// IncrementSequentialRetries increments the count of sequential retries thread-safely
func (sm *StatsManager) IncrementSequentialRetries(count int) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sequentialRetriesSinceLastStats += count
}



// GetSequentialRetries returns the sequential retries count thread-safely
func (sm *StatsManager) GetSequentialRetries() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.sequentialRetriesSinceLastStats
}

// GetOrderedBulkWrites returns the ordered bulk writes count thread-safely
func (sm *StatsManager) GetOrderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.orderedBulkWritesSinceLastStats
}

// GetOrderedBulkWritesSize returns the ordered bulk writes total size thread-safely
func (sm *StatsManager) GetOrderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.orderedBulkWritesSizeSinceLastStats
}

// GetUnorderedBulkWrites returns the unordered bulk writes count thread-safely
func (sm *StatsManager) GetUnorderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.unorderedBulkWritesSinceLastStats
}

// GetUnorderedBulkWritesSize returns the unordered bulk writes total size thread-safely
func (sm *StatsManager) GetUnorderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.unorderedBulkWritesSizeSinceLastStats
}

// RecordBulkWrite records a bulk write execution with the given size and ordered status thread-safely
func (sm *StatsManager) RecordBulkWrite(size int, isOrdered bool) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Clamp size to prevent array out of bounds if write batch size is larger than 4095
	clampedSize := size
	if clampedSize < 0 {
		clampedSize = 0
	} else if clampedSize >= 4096 {
		clampedSize = 4095
	}

	if isOrdered {
		sm.orderedBulkWritesSinceLastStats++
		sm.orderedBulkWritesSizeSinceLastStats += size
		sm.orderedSizesHistogram[clampedSize]++
	} else {
		sm.unorderedBulkWritesSinceLastStats++
		sm.unorderedBulkWritesSizeSinceLastStats += size
		sm.unorderedSizesHistogram[clampedSize]++
	}
}

// RecordLatency records both queue buffering and bulk write execution latency by operation type thread-safely
func (sm *StatsManager) RecordLatency(opType string, ops []WriteOperation, issueTime time.Time, bulkOpLatency time.Duration) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var oldestReceiveTime time.Time
	for _, op := range ops {
		if op.ReceiveTime.IsZero() {
			continue
		}
		if oldestReceiveTime.IsZero() || op.ReceiveTime.Before(oldestReceiveTime) {
			oldestReceiveTime = op.ReceiveTime
		}
	}

	if !oldestReceiveTime.IsZero() {
		sm.totalQueueLatency[opType] += issueTime.Sub(oldestReceiveTime)
		sm.queueLatencyCount[opType]++
	}

	sm.totalBulkWriteLatency[opType] += bulkOpLatency
	sm.bulkWriteLatencyCount[opType]++
}

// GetAvgQueueLatency returns the average queue buffering latency for a specific operation type thread-safely
func (sm *StatsManager) GetAvgQueueLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	count := sm.queueLatencyCount[opType]
	if count == 0 {
		return 0
	}
	return sm.totalQueueLatency[opType] / time.Duration(count)
}

// GetAvgBulkWriteLatency returns the average bulk write execution latency for a specific operation type thread-safely
func (sm *StatsManager) GetAvgBulkWriteLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	count := sm.bulkWriteLatencyCount[opType]
	if count == 0 {
		return 0
	}
	return sm.totalBulkWriteLatency[opType] / time.Duration(count)
}

// IncrementTimeoutFlushes increments the count of timeout flushes thread-safely
func (sm *StatsManager) IncrementTimeoutFlushes() {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.timeoutFlushesSinceLastStats++
}

// GetTimeoutFlushes returns the timeout flushes count thread-safely
func (sm *StatsManager) GetTimeoutFlushes() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.timeoutFlushesSinceLastStats
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
	sequentialRetries := sm.sequentialRetriesSinceLastStats
	orderedWrites := sm.orderedBulkWritesSinceLastStats
	orderedSize := sm.orderedBulkWritesSizeSinceLastStats
	orderedSizes := sm.orderedSizesHistogram
	unorderedWrites := sm.unorderedBulkWritesSinceLastStats
	unorderedSize := sm.unorderedBulkWritesSizeSinceLastStats
	unorderedSizes := sm.unorderedSizesHistogram
	timeoutFlushes := sm.timeoutFlushesSinceLastStats
	queueLatency := sm.totalQueueLatency
	queueLatencyCount := sm.queueLatencyCount
	bulkWriteLatency := sm.totalBulkWriteLatency
	bulkWriteLatencyCount := sm.bulkWriteLatencyCount

	duration := time.Since(sm.lastStatsTime)

	sm.receivedSinceLastStats = make(map[string]int)
	sm.processedSinceLastStats = make(map[string]int)
	sm.failedSinceLastStats = make(map[string]int)
	sm.updateDocMissingSinceLastStats = 0
	sm.sequentialRetriesSinceLastStats = 0
	sm.orderedBulkWritesSinceLastStats = 0
	sm.orderedBulkWritesSizeSinceLastStats = 0
	sm.orderedSizesHistogram = make([]int, 4096)
	sm.unorderedBulkWritesSinceLastStats = 0
	sm.unorderedBulkWritesSizeSinceLastStats = 0
	sm.unorderedSizesHistogram = make([]int, 4096)
	sm.timeoutFlushesSinceLastStats = 0
	sm.totalQueueLatency = make(map[string]time.Duration)
	sm.queueLatencyCount = make(map[string]int64)
	sm.totalBulkWriteLatency = make(map[string]time.Duration)
	sm.bulkWriteLatencyCount = make(map[string]int64)
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
	var rateSequentialRetries, rateOrderedWrites, rateUnorderedWrites float64
	var rateTimeoutFlushes float64

	if duration.Seconds() > 0 {
		rateReceived = float64(eventsReceived) / duration.Seconds()
		rateProcessed = float64(eventsProcessed) / duration.Seconds()
		rateFailed = float64(eventsFailed) / duration.Seconds()
		rateUpdateDocMissing = float64(updateDocMissing) / duration.Seconds()
		rateSequentialRetries = float64(sequentialRetries) / duration.Seconds()
		rateOrderedWrites = float64(orderedWrites) / duration.Seconds()
		rateUnorderedWrites = float64(unorderedWrites) / duration.Seconds()
		rateTimeoutFlushes = float64(timeoutFlushes) / duration.Seconds()

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

	var avgOrderedSizeStr string
	if orderedWrites > 0 {
		p50, p90, p100 := calculatePercentiles(orderedSizes)
		avgOrderedSizeStr = fmt.Sprintf(", average ordered bulk size: %.1f ops (p50=%d, p90=%d, p100=%d)", float64(orderedSize)/float64(orderedWrites), p50, p90, p100)
	} else {
		avgOrderedSizeStr = ", avg ordered bulk size: N/A"
	}

	var avgUnorderedSizeStr string
	if unorderedWrites > 0 {
		p50, p90, p100 := calculatePercentiles(unorderedSizes)
		avgUnorderedSizeStr = fmt.Sprintf(", average unordered bulk size: %.1f ops (p50=%d, p90=%d, p100=%d)", float64(unorderedSize)/float64(unorderedWrites), p50, p90, p100)
	} else {
		avgUnorderedSizeStr = ", avg unordered bulk size: N/A"
	}

	getLatencyStr := func(latencyMap map[string]time.Duration, countMap map[string]int64, opType string) string {
		total := latencyMap[opType]
		count := countMap[opType]
		if count > 0 {
			avg := total / time.Duration(count)
			return avg.Round(time.Millisecond).String()
		}
		return "N/A"
	}

	avgInsertQueue := getLatencyStr(queueLatency, queueLatencyCount, "insert")
	avgUpdateQueue := getLatencyStr(queueLatency, queueLatencyCount, "update")
	avgDeleteQueue := getLatencyStr(queueLatency, queueLatencyCount, "delete")
	avgReplaceQueue := getLatencyStr(queueLatency, queueLatencyCount, "replace")

	avgInsertDb := getLatencyStr(bulkWriteLatency, bulkWriteLatencyCount, "insert")
	avgUpdateDb := getLatencyStr(bulkWriteLatency, bulkWriteLatencyCount, "update")
	avgDeleteDb := getLatencyStr(bulkWriteLatency, bulkWriteLatencyCount, "delete")
	avgReplaceDb := getLatencyStr(bulkWriteLatency, bulkWriteLatencyCount, "replace")

	msg := fmt.Sprintf("Change stream statistics: Received %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], Processed %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], Failed %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)], updateDocMissing %d (%.2f events/sec), Ordered BulkWrites %d (%.2f/sec)%s, Unordered BulkWrites %d (%.2f/sec)%s, Sequential Retries %d (%.2f/sec), Timeout Flushes %d (%.2f/sec)%s, Queue Latency [insert: %s, update: %s, delete: %s, replace: %s], BulkWrite Execution Latency [insert: %s, update: %s, delete: %s, replace: %s] in the last %v",
		eventsReceived, rateReceived, insertsReceived, rateInsertsReceived, updatesReceived, rateUpdatesReceived, deletesReceived, rateDeletesReceived,
		eventsProcessed, rateProcessed, insertsProcessed, rateInsertsProcessed, updatesProcessed, rateUpdatesProcessed, deletesProcessed, rateDeletesProcessed,
		eventsFailed, rateFailed, insertsFailed, rateInsertsFailed, updatesFailed, rateUpdatesFailed, deletesFailed, rateDeletesFailed,
		updateDocMissing, rateUpdateDocMissing, orderedWrites, rateOrderedWrites, avgOrderedSizeStr, unorderedWrites, rateUnorderedWrites, avgUnorderedSizeStr, sequentialRetries, rateSequentialRetries, timeoutFlushes, rateTimeoutFlushes, avgLagStr, avgInsertQueue, avgUpdateQueue, avgDeleteQueue, avgReplaceQueue, avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, duration.Round(time.Second))

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

// calculatePercentiles calculates the p50, p90, and p100 percentiles for a histogram slice
func calculatePercentiles(histogram []int) (p50, p90, p100 int) {
	total := 0
	for _, count := range histogram {
		total += count
	}
	if total == 0 {
		return 0, 0, 0
	}

	target50 := total * 50 / 100
	target90 := total * 90 / 100
	target100 := total - 1

	p50 = -1
	p90 = -1
	p100 = -1

	cumulative := 0
	for size, count := range histogram {
		if count == 0 {
			continue
		}
		cumulative += count
		if p50 == -1 && cumulative > target50 {
			p50 = size
		}
		if p90 == -1 && cumulative > target90 {
			p90 = size
		}
		if cumulative > target100 {
			p100 = size
			break
		}
	}
	return p50, p90, p100
}
