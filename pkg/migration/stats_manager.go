package migration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

// StatsManager coordinates statistics and replication lag tracking thread-safely
type StatsManager struct {
	mu            sync.Mutex
	lastStatsTime time.Time
	lagTracker    *LagTracker
	log           *logger.Logger
	statsInterval time.Duration

	// Scalar counters updated lock-freely via atomic package
	updatedThenDeletedSinceLastStats     int64
	sequentialRetriesSinceLastStats      int64
	orderedBulkWritesSinceLastStats      int64
	orderedBulkWritesSizeSinceLastStats int64
	unorderedBulkWritesSinceLastStats    int64
	unorderedBulkWritesSizeSinceLastStats int64
	timeoutFlushesSinceLastStats         int64

	// Group flush reasons (atomically tracked)
	groupFlushesOpType    int64
	groupFlushesBatchFull int64
	groupFlushesNamespace int64
	groupFlushesCollision int64

	// Received counts (atomically tracked)
	receivedInserts  int64
	receivedUpdates  int64
	receivedDeletes  int64
	receivedReplaces int64
	receivedMixed    int64

	// Processed counts (atomically tracked)
	processedInserts  int64
	processedUpdates  int64
	processedDeletes  int64
	processedReplaces int64
	processedMixed    int64

	// Failed counts (atomically tracked)
	failedInserts  int64
	failedUpdates  int64
	failedDeletes  int64
	failedReplaces int64
	failedMixed    int64

	// Queue Latency (atomically tracked)
	totalQueueLatencyInserts  int64 // stored as nanoseconds
	totalQueueLatencyUpdates  int64
	totalQueueLatencyDeletes  int64
	totalQueueLatencyReplaces int64
	totalQueueLatencyMixed    int64
	queueLatencyCountInserts  int64
	queueLatencyCountUpdates  int64
	queueLatencyCountDeletes  int64
	queueLatencyCountReplaces int64
	queueLatencyCountMixed    int64

	// BulkWrite Latency (atomically tracked)
	totalBulkWriteLatencyInserts  int64 // stored as nanoseconds
	totalBulkWriteLatencyUpdates  int64
	totalBulkWriteLatencyDeletes  int64
	totalBulkWriteLatencyReplaces int64
	totalBulkWriteLatencyMixed    int64
	bulkWriteLatencyCountInserts  int64
	bulkWriteLatencyCountUpdates  int64
	bulkWriteLatencyCountDeletes  int64
	bulkWriteLatencyCountReplaces int64
	bulkWriteLatencyCountMixed    int64

	// Histograms (atomically tracked using fixed length arrays)
	orderedSizesHistogram   [4096]int64
	unorderedSizesHistogram [4096]int64

	// Worker processed tracking
	workerMu                      sync.Mutex
	workerProcessedSinceLastStats map[int]int64
}

// NewStatsManager creates a new StatsManager
func NewStatsManager(log *logger.Logger, interval time.Duration) *StatsManager {
	return &StatsManager{
		lastStatsTime:                 time.Now(),
		lagTracker:                    NewLagTracker(),
		log:                           log,
		statsInterval:                 interval,
		workerProcessedSinceLastStats: make(map[int]int64),
	}
}

// RecordLags records processing lag for a batch of operations and increments processed events count
func (sm *StatsManager) RecordLags(ops []WriteOperation, processedTime time.Time) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordLags(ops, processedTime)
	}
	sm.mu.Unlock()

	for _, op := range ops {
		sm.IncrementEventsProcessed(op.OpType, 1)
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

// IncrementUpdatedThenDeleted increments the count of updates skipped due to missing fullDocument thread-safely and lock-freely, and counts it in worker processed QPS.
func (sm *StatsManager) IncrementUpdatedThenDeleted(workerID int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.updatedThenDeletedSinceLastStats, 1)

	sm.workerMu.Lock()
	sm.workerProcessedSinceLastStats[workerID]++
	sm.workerMu.Unlock()
}

// GetUpdatedThenDeleted returns the updatedThenDeleted count thread-safely and lock-freely
func (sm *StatsManager) GetUpdatedThenDeleted() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.updatedThenDeletedSinceLastStats))
}

// GetGroupFlushReasonCount returns the count of group flushes for a specific reason thread-safely and lock-freely
func (sm *StatsManager) GetGroupFlushReasonCount(reason string) int {
	if sm == nil {
		return 0
	}
	var count int64
	switch reason {
	case "optype":
		count = atomic.LoadInt64(&sm.groupFlushesOpType)
	case "batchfull":
		count = atomic.LoadInt64(&sm.groupFlushesBatchFull)
	case "namespace":
		count = atomic.LoadInt64(&sm.groupFlushesNamespace)
	case "collision":
		count = atomic.LoadInt64(&sm.groupFlushesCollision)
	}
	return int(count)
}

// IncrementSequentialRetries increments the count of sequential retries thread-safely and lock-freely
func (sm *StatsManager) IncrementSequentialRetries(count int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.sequentialRetriesSinceLastStats, int64(count))
}

// GetSequentialRetries returns the sequential retries count thread-safely and lock-freely
func (sm *StatsManager) GetSequentialRetries() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.sequentialRetriesSinceLastStats))
}

// GetOrderedBulkWrites returns the ordered bulk writes count thread-safely and lock-freely
func (sm *StatsManager) GetOrderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.orderedBulkWritesSinceLastStats))
}

// GetOrderedBulkWritesSize returns the ordered bulk writes total size thread-safely and lock-freely
func (sm *StatsManager) GetOrderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.orderedBulkWritesSizeSinceLastStats))
}

// GetUnorderedBulkWrites returns the unordered bulk writes count thread-safely and lock-freely
func (sm *StatsManager) GetUnorderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.unorderedBulkWritesSinceLastStats))
}

// GetUnorderedBulkWritesSize returns the unordered bulk writes total size thread-safely and lock-freely
func (sm *StatsManager) GetUnorderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.unorderedBulkWritesSizeSinceLastStats))
}

// RecordBulkWrite records a bulk write execution with the given size and ordered status thread-safely and lock-freely
func (sm *StatsManager) RecordBulkWrite(size int, isOrdered bool) {
	if sm == nil {
		return
	}

	clampedSize := size
	if clampedSize < 0 {
		clampedSize = 0
	} else if clampedSize >= 4096 {
		clampedSize = 4095
	}

	if isOrdered {
		atomic.AddInt64(&sm.orderedBulkWritesSinceLastStats, 1)
		atomic.AddInt64(&sm.orderedBulkWritesSizeSinceLastStats, int64(size))
		atomic.AddInt64(&sm.orderedSizesHistogram[clampedSize], 1)
	} else {
		atomic.AddInt64(&sm.unorderedBulkWritesSinceLastStats, 1)
		atomic.AddInt64(&sm.unorderedBulkWritesSizeSinceLastStats, int64(size))
		atomic.AddInt64(&sm.unorderedSizesHistogram[clampedSize], 1)
	}
}

// RecordLatency records operation latency metrics thread-safely and lock-freely
func (sm *StatsManager) RecordLatency(opType string, ops []WriteOperation, issueTime time.Time, bulkOpLatency time.Duration, workerID int) {
	if sm == nil {
		return
	}

	var oldestReceiveTime time.Time
	for _, op := range ops {
		if oldestReceiveTime.IsZero() || op.ReceiveTime.Before(oldestReceiveTime) {
			oldestReceiveTime = op.ReceiveTime
		}
	}

	if !oldestReceiveTime.IsZero() {
		qLatency := int64(issueTime.Sub(oldestReceiveTime))
		switch opType {
		case "insert":
			atomic.AddInt64(&sm.totalQueueLatencyInserts, qLatency)
			atomic.AddInt64(&sm.queueLatencyCountInserts, 1)
		case "update":
			atomic.AddInt64(&sm.totalQueueLatencyUpdates, qLatency)
			atomic.AddInt64(&sm.queueLatencyCountUpdates, 1)
		case "delete":
			atomic.AddInt64(&sm.totalQueueLatencyDeletes, qLatency)
			atomic.AddInt64(&sm.queueLatencyCountDeletes, 1)
		case "replace":
			atomic.AddInt64(&sm.totalQueueLatencyReplaces, qLatency)
			atomic.AddInt64(&sm.queueLatencyCountReplaces, 1)
		case "mixed":
			atomic.AddInt64(&sm.totalQueueLatencyMixed, qLatency)
			atomic.AddInt64(&sm.queueLatencyCountMixed, 1)
		}
	}

	dbLatency := int64(bulkOpLatency)
	switch opType {
	case "insert":
		atomic.AddInt64(&sm.totalBulkWriteLatencyInserts, dbLatency)
		atomic.AddInt64(&sm.bulkWriteLatencyCountInserts, 1)
	case "update":
		atomic.AddInt64(&sm.totalBulkWriteLatencyUpdates, dbLatency)
		atomic.AddInt64(&sm.bulkWriteLatencyCountUpdates, 1)
	case "delete":
		atomic.AddInt64(&sm.totalBulkWriteLatencyDeletes, dbLatency)
		atomic.AddInt64(&sm.bulkWriteLatencyCountDeletes, 1)
	case "replace":
		atomic.AddInt64(&sm.totalBulkWriteLatencyReplaces, dbLatency)
		atomic.AddInt64(&sm.bulkWriteLatencyCountReplaces, 1)
	case "mixed":
		atomic.AddInt64(&sm.totalBulkWriteLatencyMixed, dbLatency)
		atomic.AddInt64(&sm.bulkWriteLatencyCountMixed, 1)
	}

	// Track successful writes processed by this worker thread-safely
	sm.workerMu.Lock()
	sm.workerProcessedSinceLastStats[workerID] += int64(len(ops))
	sm.workerMu.Unlock()
}

// GetAvgQueueLatency returns the average queue buffering latency for a specific operation type thread-safely and lock-freely
func (sm *StatsManager) GetAvgQueueLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	var total, count int64
	switch opType {
	case "insert":
		total = atomic.LoadInt64(&sm.totalQueueLatencyInserts)
		count = atomic.LoadInt64(&sm.queueLatencyCountInserts)
	case "update":
		total = atomic.LoadInt64(&sm.totalQueueLatencyUpdates)
		count = atomic.LoadInt64(&sm.queueLatencyCountUpdates)
	case "delete":
		total = atomic.LoadInt64(&sm.totalQueueLatencyDeletes)
		count = atomic.LoadInt64(&sm.queueLatencyCountDeletes)
	case "replace":
		total = atomic.LoadInt64(&sm.totalQueueLatencyReplaces)
		count = atomic.LoadInt64(&sm.queueLatencyCountReplaces)
	case "mixed":
		total = atomic.LoadInt64(&sm.totalQueueLatencyMixed)
		count = atomic.LoadInt64(&sm.queueLatencyCountMixed)
	}
	if count == 0 {
		return 0
	}
	return time.Duration(total / count)
}

// GetAvgBulkWriteLatency returns the average bulk write execution latency for a specific operation type thread-safely and lock-freely
func (sm *StatsManager) GetAvgBulkWriteLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	var total, count int64
	switch opType {
	case "insert":
		total = atomic.LoadInt64(&sm.totalBulkWriteLatencyInserts)
		count = atomic.LoadInt64(&sm.bulkWriteLatencyCountInserts)
	case "update":
		total = atomic.LoadInt64(&sm.totalBulkWriteLatencyUpdates)
		count = atomic.LoadInt64(&sm.bulkWriteLatencyCountUpdates)
	case "delete":
		total = atomic.LoadInt64(&sm.totalBulkWriteLatencyDeletes)
		count = atomic.LoadInt64(&sm.bulkWriteLatencyCountDeletes)
	case "replace":
		total = atomic.LoadInt64(&sm.totalBulkWriteLatencyReplaces)
		count = atomic.LoadInt64(&sm.bulkWriteLatencyCountReplaces)
	case "mixed":
		total = atomic.LoadInt64(&sm.totalBulkWriteLatencyMixed)
		count = atomic.LoadInt64(&sm.bulkWriteLatencyCountMixed)
	}
	if count == 0 {
		return 0
	}
	return time.Duration(total / count)
}

// IncrementTimeoutFlushes increments the count of timeout flushes thread-safely and lock-freely
func (sm *StatsManager) IncrementTimeoutFlushes() {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.timeoutFlushesSinceLastStats, 1)
}

// GetTimeoutFlushes returns the timeout flushes count thread-safely and lock-freely
func (sm *StatsManager) GetTimeoutFlushes() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.timeoutFlushesSinceLastStats))
}

// IncrementGroupFlushReason increments the count of group flushes by reason thread-safely and lock-freely
func (sm *StatsManager) IncrementGroupFlushReason(reason string) {
	if sm == nil {
		return
	}
	switch reason {
	case "optype":
		atomic.AddInt64(&sm.groupFlushesOpType, 1)
	case "batchfull":
		atomic.AddInt64(&sm.groupFlushesBatchFull, 1)
	case "namespace":
		atomic.AddInt64(&sm.groupFlushesNamespace, 1)
	case "collision":
		atomic.AddInt64(&sm.groupFlushesCollision, 1)
	}
}

// IncrementWorkerProcessed increments the count of successfully processed events by a specific worker thread-safely
func (sm *StatsManager) IncrementWorkerProcessed(workerID int, count int) {
	if sm == nil {
		return
	}
	sm.workerMu.Lock()
	sm.workerProcessedSinceLastStats[workerID] += int64(count)
	sm.workerMu.Unlock()
}

// IncrementEventsReceived increments the count of received input events by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsReceived(opType string) {
	if sm == nil {
		return
	}
	switch opType {
	case "insert":
		atomic.AddInt64(&sm.receivedInserts, 1)
	case "update":
		atomic.AddInt64(&sm.receivedUpdates, 1)
	case "delete":
		atomic.AddInt64(&sm.receivedDeletes, 1)
	case "replace":
		atomic.AddInt64(&sm.receivedReplaces, 1)
	case "mixed":
		atomic.AddInt64(&sm.receivedMixed, 1)
	}
}

// IncrementEventsProcessed increments the count of successfully processed events by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsProcessed(opType string, count int64) {
	if sm == nil {
		return
	}
	switch opType {
	case "insert":
		atomic.AddInt64(&sm.processedInserts, count)
	case "update":
		atomic.AddInt64(&sm.processedUpdates, count)
	case "delete":
		atomic.AddInt64(&sm.processedDeletes, count)
	case "replace":
		atomic.AddInt64(&sm.processedReplaces, count)
	case "mixed":
		atomic.AddInt64(&sm.processedMixed, count)
	}
}

// IncrementEventsFailed increments the count of failed write operations by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsFailed(opType string) {
	if sm == nil {
		return
	}
	switch opType {
	case "insert":
		atomic.AddInt64(&sm.failedInserts, 1)
	case "update":
		atomic.AddInt64(&sm.failedUpdates, 1)
	case "delete":
		atomic.AddInt64(&sm.failedDeletes, 1)
	case "replace":
		atomic.AddInt64(&sm.failedReplaces, 1)
	case "mixed":
		atomic.AddInt64(&sm.failedMixed, 1)
	}
}

// ReportStats logs the accumulated change stream and lag statistics, resetting internal counters
func (sm *StatsManager) ReportStats() {
	// Reset and load atomic counters atomically using atomic.SwapInt64
	updatedThenDeleted := atomic.SwapInt64(&sm.updatedThenDeletedSinceLastStats, 0)
	sequentialRetries := atomic.SwapInt64(&sm.sequentialRetriesSinceLastStats, 0)
	orderedWrites := atomic.SwapInt64(&sm.orderedBulkWritesSinceLastStats, 0)
	orderedWritesSize := atomic.SwapInt64(&sm.orderedBulkWritesSizeSinceLastStats, 0)
	unorderedWrites := atomic.SwapInt64(&sm.unorderedBulkWritesSinceLastStats, 0)
	unorderedWritesSize := atomic.SwapInt64(&sm.unorderedBulkWritesSizeSinceLastStats, 0)
	timeoutFlushes := atomic.SwapInt64(&sm.timeoutFlushesSinceLastStats, 0)

	groupFlushesOpType := atomic.SwapInt64(&sm.groupFlushesOpType, 0)
	groupFlushesBatchFull := atomic.SwapInt64(&sm.groupFlushesBatchFull, 0)
	groupFlushesNamespace := atomic.SwapInt64(&sm.groupFlushesNamespace, 0)
	groupFlushesCollision := atomic.SwapInt64(&sm.groupFlushesCollision, 0)

	receivedInserts := atomic.SwapInt64(&sm.receivedInserts, 0)
	receivedUpdates := atomic.SwapInt64(&sm.receivedUpdates, 0)
	receivedDeletes := atomic.SwapInt64(&sm.receivedDeletes, 0)
	receivedReplaces := atomic.SwapInt64(&sm.receivedReplaces, 0)
	receivedMixed := atomic.SwapInt64(&sm.receivedMixed, 0)

	processedInserts := atomic.SwapInt64(&sm.processedInserts, 0)
	processedUpdates := atomic.SwapInt64(&sm.processedUpdates, 0)
	processedDeletes := atomic.SwapInt64(&sm.processedDeletes, 0)
	processedReplaces := atomic.SwapInt64(&sm.processedReplaces, 0)
	processedMixed := atomic.SwapInt64(&sm.processedMixed, 0)

	failedInserts := atomic.SwapInt64(&sm.failedInserts, 0)
	failedUpdates := atomic.SwapInt64(&sm.failedUpdates, 0)
	failedDeletes := atomic.SwapInt64(&sm.failedDeletes, 0)
	failedReplaces := atomic.SwapInt64(&sm.failedReplaces, 0)
	failedMixed := atomic.SwapInt64(&sm.failedMixed, 0)

	totalQueueLatencyInserts := atomic.SwapInt64(&sm.totalQueueLatencyInserts, 0)
	queueLatencyCountInserts := atomic.SwapInt64(&sm.queueLatencyCountInserts, 0)
	totalQueueLatencyUpdates := atomic.SwapInt64(&sm.totalQueueLatencyUpdates, 0)
	queueLatencyCountUpdates := atomic.SwapInt64(&sm.queueLatencyCountUpdates, 0)
	totalQueueLatencyDeletes := atomic.SwapInt64(&sm.totalQueueLatencyDeletes, 0)
	queueLatencyCountDeletes := atomic.SwapInt64(&sm.queueLatencyCountDeletes, 0)
	totalQueueLatencyReplaces := atomic.SwapInt64(&sm.totalQueueLatencyReplaces, 0)
	queueLatencyCountReplaces := atomic.SwapInt64(&sm.queueLatencyCountReplaces, 0)
	totalQueueLatencyMixed := atomic.SwapInt64(&sm.totalQueueLatencyMixed, 0)
	queueLatencyCountMixed := atomic.SwapInt64(&sm.queueLatencyCountMixed, 0)

	totalBulkWriteLatencyInserts := atomic.SwapInt64(&sm.totalBulkWriteLatencyInserts, 0)
	bulkWriteLatencyCountInserts := atomic.SwapInt64(&sm.bulkWriteLatencyCountInserts, 0)
	totalBulkWriteLatencyUpdates := atomic.SwapInt64(&sm.totalBulkWriteLatencyUpdates, 0)
	bulkWriteLatencyCountUpdates := atomic.SwapInt64(&sm.bulkWriteLatencyCountUpdates, 0)
	totalBulkWriteLatencyDeletes := atomic.SwapInt64(&sm.totalBulkWriteLatencyDeletes, 0)
	bulkWriteLatencyCountDeletes := atomic.SwapInt64(&sm.bulkWriteLatencyCountDeletes, 0)
	totalBulkWriteLatencyReplaces := atomic.SwapInt64(&sm.totalBulkWriteLatencyReplaces, 0)
	bulkWriteLatencyCountReplaces := atomic.SwapInt64(&sm.bulkWriteLatencyCountReplaces, 0)
	totalBulkWriteLatencyMixed := atomic.SwapInt64(&sm.totalBulkWriteLatencyMixed, 0)
	bulkWriteLatencyCountMixed := atomic.SwapInt64(&sm.bulkWriteLatencyCountMixed, 0)

	// Extract worker processed metrics under worker lock
	sm.workerMu.Lock()
	workerProcessed := sm.workerProcessedSinceLastStats
	sm.workerProcessedSinceLastStats = make(map[int]int64)
	sm.workerMu.Unlock()

	// Load histograms under lock
	var orderedSizes [4096]int64
	var unorderedSizes [4096]int64
	for i := 0; i < 4096; i++ {
		orderedSizes[i] = atomic.SwapInt64(&sm.orderedSizesHistogram[i], 0)
		unorderedSizes[i] = atomic.SwapInt64(&sm.unorderedSizesHistogram[i], 0)
	}

	sm.mu.Lock()
	now := time.Now()
	duration := now.Sub(sm.lastStatsTime)
	sm.lastStatsTime = now

	var avgLag time.Duration
	if sm.lagTracker != nil {
		totalLag, count := sm.lagTracker.Flush()
		if count > 0 {
			avgLag = totalLag / time.Duration(count)
		}
	}
	sm.mu.Unlock()

	eventsReceived := receivedInserts + receivedUpdates + receivedDeletes + receivedReplaces + receivedMixed
	eventsProcessed := processedInserts + processedUpdates + processedDeletes + processedReplaces + processedMixed + updatedThenDeleted
	eventsFailed := failedInserts + failedUpdates + failedDeletes + failedReplaces + failedMixed

	insertsReceived := receivedInserts
	updatesReceived := receivedUpdates + receivedReplaces
	deletesReceived := receivedDeletes

	insertsProcessed := processedInserts
	updatesProcessed := processedUpdates + processedReplaces
	deletesProcessed := processedDeletes

	insertsFailed := failedInserts
	updatesFailed := failedUpdates + failedReplaces
	deletesFailed := failedDeletes

	var rateReceived, rateProcessed, rateFailed, rateUpdatedThenDeleted float64
	var rateInsertsProcessed, rateUpdatesProcessed, rateDeletesProcessed float64
	var rateInsertsFailed, rateUpdatesFailed, rateDeletesFailed float64
	var rateSequentialRetries, rateOrderedWrites, rateUnorderedWrites float64
	var rateTimeoutFlushes, rateGroupFlushesOpType, rateGroupFlushesBatchFull, rateGroupFlushesNamespace, rateGroupFlushesCollision float64

	var rateInsertsReceived, rateUpdatesReceived, rateDeletesReceived float64

	if duration.Seconds() > 0 {
		rateReceived = float64(eventsReceived) / duration.Seconds()
		rateProcessed = float64(eventsProcessed) / duration.Seconds()
		rateFailed = float64(eventsFailed) / duration.Seconds()
		rateUpdatedThenDeleted = float64(updatedThenDeleted) / duration.Seconds()
		rateSequentialRetries = float64(sequentialRetries) / duration.Seconds()
		rateOrderedWrites = float64(orderedWrites) / duration.Seconds()
		rateUnorderedWrites = float64(unorderedWrites) / duration.Seconds()
		rateTimeoutFlushes = float64(timeoutFlushes) / duration.Seconds()

		rateGroupFlushesOpType = float64(groupFlushesOpType) / duration.Seconds()
		rateGroupFlushesBatchFull = float64(groupFlushesBatchFull) / duration.Seconds()
		rateGroupFlushesNamespace = float64(groupFlushesNamespace) / duration.Seconds()
		rateGroupFlushesCollision = float64(groupFlushesCollision) / duration.Seconds()

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

	// Calculate ordered and unordered write percentiles
	var orderedSizesInt []int
	var unorderedSizesInt []int
	for i := 0; i < 4096; i++ {
		orderedSizesInt = append(orderedSizesInt, int(orderedSizes[i]))
		unorderedSizesInt = append(unorderedSizesInt, int(unorderedSizes[i]))
	}
	p50Ordered, p90Ordered, p100Ordered := calculatePercentiles(orderedSizesInt)
	p50Unordered, p90Unordered, p100Unordered := calculatePercentiles(unorderedSizesInt)

	avgOrderedSizeStr := ""
	if orderedWrites > 0 {
		avgOrderedSizeStr = fmt.Sprintf(" (avg size: %.1f, p50: %d, p90: %d, p100: %d)", float64(orderedWritesSize)/float64(orderedWrites), p50Ordered, p90Ordered, p100Ordered)
	}
	avgUnorderedSizeStr := ""
	if unorderedWrites > 0 {
		avgUnorderedSizeStr = fmt.Sprintf(" (avg size: %.1f, p50: %d, p90: %d, p100: %d)", float64(unorderedWritesSize)/float64(unorderedWrites), p50Unordered, p90Unordered, p100Unordered)
	}

	getLatencyStr := func(total int64, count int64) string {
		if count > 0 {
			avg := time.Duration(total / count)
			return avg.Round(time.Millisecond).String()
		}
		return "N/A"
	}

	avgInsertQueue := getLatencyStr(totalQueueLatencyInserts, queueLatencyCountInserts)
	avgUpdateQueue := getLatencyStr(totalQueueLatencyUpdates, queueLatencyCountUpdates)
	avgDeleteQueue := getLatencyStr(totalQueueLatencyDeletes, queueLatencyCountDeletes)
	avgReplaceQueue := getLatencyStr(totalQueueLatencyReplaces, queueLatencyCountReplaces)
	avgMixedQueue := getLatencyStr(totalQueueLatencyMixed, queueLatencyCountMixed)

	avgInsertDb := getLatencyStr(totalBulkWriteLatencyInserts, bulkWriteLatencyCountInserts)
	avgUpdateDb := getLatencyStr(totalBulkWriteLatencyUpdates, bulkWriteLatencyCountUpdates)
	avgDeleteDb := getLatencyStr(totalBulkWriteLatencyDeletes, bulkWriteLatencyCountDeletes)
	avgReplaceDb := getLatencyStr(totalBulkWriteLatencyReplaces, bulkWriteLatencyCountReplaces)
	avgMixedDb := getLatencyStr(totalBulkWriteLatencyMixed, bulkWriteLatencyCountMixed)

	// Calculate active workers and percentiles
	var workerQps []float64
	for _, count := range workerProcessed {
		if count > 0 {
			qps := float64(count) / duration.Seconds()
			workerQps = append(workerQps, qps)
		}
	}
	activeWorkers := len(workerQps)

	var wQpsP50, wQpsP70, wQpsP90, wQpsP100 float64
	if activeWorkers > 0 {
		sort.Float64s(workerQps)
		wQpsP50 = workerQps[int(float64(activeWorkers)*0.50)]
		wQpsP70 = workerQps[int(float64(activeWorkers)*0.70)]
		wQpsP90 = workerQps[int(float64(activeWorkers)*0.90)]
		wQpsP100 = workerQps[activeWorkers-1]
	}

	avgLagStr := "N/A"
	if avgLag > 0 {
		avgLagStr = avgLag.Round(time.Millisecond).String()
	}

	msg := fmt.Sprintf("Change stream statistics (last %v):\n"+
		"  - Received:  %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)]\n"+
		"  - Processed: %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)]\n"+
		"  - Failed:    %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Updates: %d (%.2f/sec), Deletes: %d (%.2f/sec)]\n"+
		"  - updatedThenDeleted: %d (%.2f events/sec)\n"+
		"  - Ordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Unordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Sequential Retries: %d (%.2f/sec)\n"+
		"  - Timeout Flushes: %d (%.2f/sec)\n"+
		"  - Group Flushes: [optype: %d (%.2f/sec), batchfull: %d (%.2f/sec), namespace: %d (%.2f/sec), collision: %d (%.2f/sec)]\n"+
		"  - Queue Latency: [insert: %s, update: %s, delete: %s, replace: %s, mixed: %s]\n"+
		"  - BulkWrite Execution Latency: [insert: %s, update: %s, delete: %s, replace: %s, mixed: %s]\n"+
		"  - Workers: [Active: %d]\n"+
		"  - Worker QPS: [p50: %.2f, p70: %.2f, p90: %.2f, p100: %.2f]\n"+
		"  - Average processing lag: %s",
		duration.Round(time.Second),
		eventsReceived, rateReceived, insertsReceived, rateInsertsReceived, updatesReceived, rateUpdatesReceived, deletesReceived, rateDeletesReceived,
		eventsProcessed, rateProcessed, insertsProcessed, rateInsertsProcessed, updatesProcessed, rateUpdatesProcessed, deletesProcessed, rateDeletesProcessed,
		eventsFailed, rateFailed, insertsFailed, rateInsertsFailed, updatesFailed, rateUpdatesFailed, deletesFailed, rateDeletesFailed,
		updatedThenDeleted, rateUpdatedThenDeleted, orderedWrites, rateOrderedWrites, avgOrderedSizeStr, unorderedWrites, rateUnorderedWrites, avgUnorderedSizeStr, sequentialRetries, rateSequentialRetries, timeoutFlushes, rateTimeoutFlushes, groupFlushesOpType, rateGroupFlushesOpType, groupFlushesBatchFull, rateGroupFlushesBatchFull, groupFlushesNamespace, rateGroupFlushesNamespace, groupFlushesCollision, rateGroupFlushesCollision, avgInsertQueue, avgUpdateQueue, avgDeleteQueue, avgReplaceQueue, avgMixedQueue, avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, avgMixedDb,
		activeWorkers,
		wQpsP50, wQpsP70, wQpsP90, wQpsP100,
		avgLagStr)

	sm.log.Info(msg)
}

// GetProcessedCount returns the count of processed events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetProcessedCount(opType string) int {
	if sm == nil {
		return 0
	}
	var count int64
	switch opType {
	case "insert":
		count = atomic.LoadInt64(&sm.processedInserts)
	case "update":
		count = atomic.LoadInt64(&sm.processedUpdates)
	case "delete":
		count = atomic.LoadInt64(&sm.processedDeletes)
	case "replace":
		count = atomic.LoadInt64(&sm.processedReplaces)
	case "mixed":
		count = atomic.LoadInt64(&sm.processedMixed)
	}
	return int(count)
}

// GetReceivedCount returns the count of received events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetReceivedCount(opType string) int {
	if sm == nil {
		return 0
	}
	var count int64
	switch opType {
	case "insert":
		count = atomic.LoadInt64(&sm.receivedInserts)
	case "update":
		count = atomic.LoadInt64(&sm.receivedUpdates)
	case "delete":
		count = atomic.LoadInt64(&sm.receivedDeletes)
	case "replace":
		count = atomic.LoadInt64(&sm.receivedReplaces)
	case "mixed":
		count = atomic.LoadInt64(&sm.receivedMixed)
	}
	return int(count)
}

// GetFailedCount returns the count of failed events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetFailedCount(opType string) int {
	if sm == nil {
		return 0
	}
	var count int64
	switch opType {
	case "insert":
		count = atomic.LoadInt64(&sm.failedInserts)
	case "update":
		count = atomic.LoadInt64(&sm.failedUpdates)
	case "delete":
		count = atomic.LoadInt64(&sm.failedDeletes)
	case "replace":
		count = atomic.LoadInt64(&sm.failedReplaces)
	case "mixed":
		count = atomic.LoadInt64(&sm.failedMixed)
	}
	return int(count)
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
