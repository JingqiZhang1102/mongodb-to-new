package migration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/event"
)

const (
	opInsert  = 0
	opUpdate  = 1
	opDelete  = 2
	opReplace = 3
	opMixed   = 4
)

func opIndex(opType string) int {
	switch opType {
	case "insert":
		return opInsert
	case "update":
		return opUpdate
	case "delete":
		return opDelete
	case "replace":
		return opReplace
	case "mixed":
		return opMixed
	}
	return -1
}

type opStats struct {
	received              int64
	processed             int64
	failed                int64
	dlq                   int64
	totalBulkWriteLatency int64
	bulkWriteLatencyCount int64
	sequentialRetries     int64
}

type poolMonitorStats struct {
	opened       int64
	closed       int64
	open         int64
	inUse        int64
	succeeded    int64
	failed       int64
	returned     int64
	waitDuration int64
}

// StatsManager coordinates statistics and replication lag tracking thread-safely
type StatsManager struct {
	mu                   sync.Mutex
	lastStatsTime        time.Time
	lagTracker           *LagTracker
	log                  *logger.Logger
	statsInterval        time.Duration
	groupOpsByDistinctId bool
	DontApply            bool
	DryRun               bool

	// Unified operation statistics
	opsStats [5]opStats

	// Scalar counters updated lock-freely via atomic package
	updatedThenDeletedSinceLastStats      int64
	sequentialRetriesSinceLastStats       int64
	orderedBulkWritesSinceLastStats       int64
	orderedBulkWritesSizeSinceLastStats   int64
	unorderedBulkWritesSinceLastStats     int64
	unorderedBulkWritesSizeSinceLastStats int64
	timeoutFlushesSinceLastStats          int64
	duplicateKeysSinceLastStats           int64

	// Read latency and size tracking (atomically updated)
	totalReadLatencyNs int64
	readCount          int64
	totalReadSizeBytes int64


	// Group flush reasons (atomically tracked)
	groupFlushesOpType    int64
	groupFlushesBatchFull int64
	groupFlushesNamespace int64
	groupFlushesCollision int64

	// Histograms (atomically tracked using fixed length arrays)
	orderedSizesHistogram   [4096]int64
	unorderedSizesHistogram [4096]int64

	// Worker processed tracking (lock-free fixed-size array)
	workerProcessedSinceLastStats [4096]int64

	// DLQ and failure breakdown tracking
	failureMu        sync.Mutex
	failureBreakdown map[string]map[string]int64 // opType -> simplified error message -> count

	// Connection Pool stats (atomically tracked)
	sourcePool poolMonitorStats
	targetPool poolMonitorStats
}

// NewStatsManager creates a new StatsManager
func NewStatsManager(log *logger.Logger, interval time.Duration, groupOpsByDistinctId ...bool) *StatsManager {
	distinctId := false
	if len(groupOpsByDistinctId) > 0 {
		distinctId = groupOpsByDistinctId[0]
	}
	return &StatsManager{
		lastStatsTime:        time.Now(),
		lagTracker:           NewLagTracker(),
		log:                  log,
		statsInterval:        interval,
		groupOpsByDistinctId: distinctId,
		failureBreakdown:     make(map[string]map[string]int64),
	}
}

// RecordLags records processing lag for a batch of operations and increments processed events count
func (sm *StatsManager) RecordLags(ops []WriteOperation) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordLags(ops)
	}
	sm.mu.Unlock()

	for _, op := range ops {
		if !op.SuccessTime.IsZero() {
			sm.IncrementEventsProcessed(op.OpType, 1)
		} else {
			sm.IncrementEventsFailed(op.OpType, op.DLQed, op.Error)
		}
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
				if sm.DryRun {
					sm.ReportDryRunStats()
				} else {
					sm.ReportStats()
				}
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

	if workerID >= 0 && workerID < 4096 {
		atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], 1)
	}
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
func (sm *StatsManager) IncrementSequentialRetries(opType string, count int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.sequentialRetriesSinceLastStats, int64(count))

	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].sequentialRetries, int64(count))
	}
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

// RecordLatency records operation execution latency metrics thread-safely and lock-freely
func (sm *StatsManager) RecordLatency(opType string, ops []WriteOperation, bulkOpLatency time.Duration, workerID int, success bool) {
	if sm == nil || len(ops) == 0 {
		return
	}

	dbLatency := int64(bulkOpLatency)
	if sm.groupOpsByDistinctId {
		atomic.AddInt64(&sm.opsStats[opMixed].totalBulkWriteLatency, dbLatency)
		atomic.AddInt64(&sm.opsStats[opMixed].bulkWriteLatencyCount, 1)
	} else {
		idx := opIndex(opType)
		if idx >= 0 {
			atomic.AddInt64(&sm.opsStats[idx].totalBulkWriteLatency, dbLatency)
			atomic.AddInt64(&sm.opsStats[idx].bulkWriteLatencyCount, 1)
		}
	}

	if success {
		if workerID >= 0 && workerID < 4096 {
			atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], int64(len(ops)))
		}
	}
}

// GetAvgBulkWriteLatency returns the average bulk write execution latency for a specific operation type thread-safely and lock-freely
func (sm *StatsManager) GetAvgBulkWriteLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	var total, count int64
	idx := opIndex(opType)
	if idx >= 0 {
		total = atomic.LoadInt64(&sm.opsStats[idx].totalBulkWriteLatency)
		count = atomic.LoadInt64(&sm.opsStats[idx].bulkWriteLatencyCount)
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

// IncrementDuplicateKeys increments the count of duplicate key errors thread-safely and lock-freely
func (sm *StatsManager) IncrementDuplicateKeys(count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.duplicateKeysSinceLastStats, count)
}

// GetDuplicateKeys returns the duplicate keys count thread-safely and lock-freely
func (sm *StatsManager) GetDuplicateKeys() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.duplicateKeysSinceLastStats))
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
	if workerID >= 0 && workerID < 4096 {
		atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], int64(count))
	}
}

// IncrementEventsReceived increments the count of received input events by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsReceived(opType string) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].received, 1)
	}
}

// RecordReadMetric records changeStream.Next latency and document size metrics thread-safely and lock-freely
func (sm *StatsManager) RecordReadMetric(latency time.Duration, sizeBytes int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.totalReadLatencyNs, int64(latency))
	atomic.AddInt64(&sm.readCount, 1)
	atomic.AddInt64(&sm.totalReadSizeBytes, int64(sizeBytes))
}

// IncrementEventsProcessed increments the count of successfully processed events by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsProcessed(opType string, count int64) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].processed, count)
	}
}

func simplifyError(errStr string) string {
	if strings.Contains(errStr, "connection(") && strings.Contains(errStr, "socket was unexpectedly closed: EOF") {
		start := strings.Index(errStr, "connection(")
		if start != -1 {
			end := strings.Index(errStr[start:], ")")
			if end != -1 {
				return "connection(...)" + errStr[start+end+1:]
			}
		}
	}
	return errStr
}

// IncrementEventsFailed increments the count of failed write operations by operation type thread-safely and lock-freely
func (sm *StatsManager) IncrementEventsFailed(opType string, dlqed bool, err error) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].failed, 1)
		if dlqed {
			atomic.AddInt64(&sm.opsStats[idx].dlq, 1)
		}
	}

	sm.failureMu.Lock()
	defer sm.failureMu.Unlock()

	if sm.failureBreakdown == nil {
		sm.failureBreakdown = make(map[string]map[string]int64)
	}

	if err != nil {
		errMsg := simplifyError(err.Error())
		if sm.failureBreakdown[opType] == nil {
			sm.failureBreakdown[opType] = make(map[string]int64)
		}
		sm.failureBreakdown[opType][errMsg]++
	}
}

// GetDLQCount returns the count of DLQ'ed documents of the given type thread-safely and lock-freely
func (sm *StatsManager) GetDLQCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].dlq))
	}
	return 0
}

// GetSourcePoolMonitor returns a MongoDB client PoolMonitor for the source connection pool.
func (sm *StatsManager) GetSourcePoolMonitor() *event.PoolMonitor {
	if sm == nil {
		return nil
	}
	return sm.createPoolMonitor(&sm.sourcePool)
}

// GetTargetPoolMonitor returns a MongoDB client PoolMonitor for the target connection pool.
func (sm *StatsManager) GetTargetPoolMonitor() *event.PoolMonitor {
	if sm == nil {
		return nil
	}
	return sm.createPoolMonitor(&sm.targetPool)
}

func (sm *StatsManager) createPoolMonitor(stats *poolMonitorStats) *event.PoolMonitor {
	return &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			switch evt.Type {
			case event.ConnectionCreated:
				atomic.AddInt64(&stats.opened, 1)
				atomic.AddInt64(&stats.open, 1)
			case event.ConnectionClosed:
				atomic.AddInt64(&stats.closed, 1)
				atomic.AddInt64(&stats.open, -1)
			case event.GetSucceeded:
				atomic.AddInt64(&stats.inUse, 1)
				atomic.AddInt64(&stats.succeeded, 1)
				atomic.AddInt64(&stats.waitDuration, int64(evt.Duration))
			case event.GetFailed:
				atomic.AddInt64(&stats.failed, 1)
				atomic.AddInt64(&stats.waitDuration, int64(evt.Duration))
			case event.ConnectionReturned:
				atomic.AddInt64(&stats.inUse, -1)
				atomic.AddInt64(&stats.returned, 1)
			}
		},
	}
}

func (sm *StatsManager) formatPoolStats(stats *poolMonitorStats, name string, duration time.Duration) string {
	opened := atomic.SwapInt64(&stats.opened, 0)
	closed := atomic.SwapInt64(&stats.closed, 0)
	open := atomic.LoadInt64(&stats.open)
	inUse := atomic.LoadInt64(&stats.inUse)
	succeeded := atomic.SwapInt64(&stats.succeeded, 0)
	failed := atomic.SwapInt64(&stats.failed, 0)
	returned := atomic.SwapInt64(&stats.returned, 0)
	waitDuration := atomic.SwapInt64(&stats.waitDuration, 0)

	var rateOpened, rateClosed, rateSucceeded, rateFailed, rateReturned float64
	if duration.Seconds() > 0 {
		rateOpened = float64(opened) / duration.Seconds()
		rateClosed = float64(closed) / duration.Seconds()
		rateSucceeded = float64(succeeded) / duration.Seconds()
		rateFailed = float64(failed) / duration.Seconds()
		rateReturned = float64(returned) / duration.Seconds()
	}

	attempts := succeeded + failed
	avgWaitStr := "N/A"
	if attempts > 0 {
		avgWaitStr = (time.Duration(waitDuration) / time.Duration(attempts)).Round(time.Microsecond).String()
	}

	idle := open - inUse
	if idle < 0 {
		idle = 0
	}

	return fmt.Sprintf("%s: Connections: [Opened: %d (%.2f/sec), Closed: %d (%.2f/sec), Open: %d, In-Use: %d, Idle: %d] Checkouts: [Succeeded: %d (%.2f/sec), Failed: %d (%.2f/sec), Returned: %d (%.2f/sec), Avg Wait: %s]",
		name, opened, rateOpened, closed, rateClosed, open, inUse, idle,
		succeeded, rateSucceeded, failed, rateFailed, returned, rateReturned, avgWaitStr)
}

// GetProcessedCount returns the count of processed events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetProcessedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].processed))
	}
	return 0
}


// GetReceivedCount returns the count of received events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetReceivedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].received))
	}
	return 0
}

// GetFailedCount returns the count of failed events of the given type thread-safely and lock-freely
func (sm *StatsManager) GetFailedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].failed))
	}
	return 0
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
	duplicateKeys := atomic.SwapInt64(&sm.duplicateKeysSinceLastStats, 0)

	totalReadLatencyNs := atomic.SwapInt64(&sm.totalReadLatencyNs, 0)
	readCount := atomic.SwapInt64(&sm.readCount, 0)
	totalReadSizeBytes := atomic.SwapInt64(&sm.totalReadSizeBytes, 0)

	groupFlushesOpType := atomic.SwapInt64(&sm.groupFlushesOpType, 0)
	groupFlushesBatchFull := atomic.SwapInt64(&sm.groupFlushesBatchFull, 0)
	groupFlushesNamespace := atomic.SwapInt64(&sm.groupFlushesNamespace, 0)
	groupFlushesCollision := atomic.SwapInt64(&sm.groupFlushesCollision, 0)

	sm.failureMu.Lock()
	breakdown := sm.failureBreakdown
	sm.failureBreakdown = make(map[string]map[string]int64)
	sm.failureMu.Unlock()

	var swapped [5]opStats
	for i := 0; i < 5; i++ {
		swapped[i].received = atomic.SwapInt64(&sm.opsStats[i].received, 0)
		swapped[i].processed = atomic.SwapInt64(&sm.opsStats[i].processed, 0)
		swapped[i].failed = atomic.SwapInt64(&sm.opsStats[i].failed, 0)
		swapped[i].dlq = atomic.SwapInt64(&sm.opsStats[i].dlq, 0)
		swapped[i].totalBulkWriteLatency = atomic.SwapInt64(&sm.opsStats[i].totalBulkWriteLatency, 0)
		swapped[i].bulkWriteLatencyCount = atomic.SwapInt64(&sm.opsStats[i].bulkWriteLatencyCount, 0)
		swapped[i].sequentialRetries = atomic.SwapInt64(&sm.opsStats[i].sequentialRetries, 0)
	}

	// Extract worker processed metrics atomically without locks
	workerProcessed := make(map[int]int64)
	for i := 0; i < 4096; i++ {
		count := atomic.SwapInt64(&sm.workerProcessedSinceLastStats[i], 0)
		if count > 0 {
			workerProcessed[i] = count
		}
	}

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

	var lagRes LagFlushResult
	if sm.lagTracker != nil {
		lagRes = sm.lagTracker.Flush()
	}
	sm.mu.Unlock()

	eventsReceived := swapped[opInsert].received + swapped[opUpdate].received + swapped[opDelete].received + swapped[opReplace].received + swapped[opMixed].received
	eventsFailed := swapped[opInsert].failed + swapped[opUpdate].failed + swapped[opDelete].failed + swapped[opReplace].failed + swapped[opMixed].failed

	insertsReceived := swapped[opInsert].received
	updatesReceived := swapped[opUpdate].received + swapped[opReplace].received
	deletesReceived := swapped[opDelete].received

	insertsFailed := swapped[opInsert].failed
	updatesFailed := swapped[opUpdate].failed + swapped[opReplace].failed
	deletesFailed := swapped[opDelete].failed

	var eventsProcessed, eventsApplied int64
	var insertsProcessed, updatesProcessed, deletesProcessed int64

	eventsProcessed = swapped[opInsert].processed + swapped[opUpdate].processed + swapped[opDelete].processed + swapped[opReplace].processed + swapped[opMixed].processed + updatedThenDeleted
	eventsApplied = swapped[opInsert].processed + swapped[opUpdate].processed + swapped[opDelete].processed + swapped[opReplace].processed + swapped[opMixed].processed

	insertsProcessed = swapped[opInsert].processed
	updatesProcessed = swapped[opUpdate].processed + swapped[opReplace].processed
	deletesProcessed = swapped[opDelete].processed

	avgReadLatency := time.Duration(0)
	avgReadSize := 0.0
	if readCount > 0 {
		avgReadLatency = time.Duration(totalReadLatencyNs / readCount)
		avgReadSize = float64(totalReadSizeBytes) / float64(readCount)
	}

	var rateReceived, rateProcessed, rateApplied, rateFailed, rateUpdatedThenDeleted float64
	var rateInsertsProcessed, rateUpdatesProcessed, rateDeletesProcessed float64
	var rateInsertsFailed, rateUpdatesFailed, rateDeletesFailed float64
	var rateSequentialRetries, rateOrderedWrites, rateUnorderedWrites float64
	var rateTimeoutFlushes, rateGroupFlushesOpType, rateGroupFlushesBatchFull, rateGroupFlushesNamespace, rateGroupFlushesCollision float64
	var rateDuplicateKeys float64

	var rateInsertsReceived, rateUpdatesReceived, rateDeletesReceived float64

	if duration.Seconds() > 0 {
		rateReceived = float64(eventsReceived) / duration.Seconds()
		rateProcessed = float64(eventsProcessed) / duration.Seconds()
		rateApplied = float64(eventsApplied) / duration.Seconds()
		rateFailed = float64(eventsFailed) / duration.Seconds()
		rateUpdatedThenDeleted = float64(updatedThenDeleted) / duration.Seconds()
		rateSequentialRetries = float64(sequentialRetries) / duration.Seconds()
		rateOrderedWrites = float64(orderedWrites) / duration.Seconds()
		rateUnorderedWrites = float64(unorderedWrites) / duration.Seconds()
		rateTimeoutFlushes = float64(timeoutFlushes) / duration.Seconds()
		rateDuplicateKeys = float64(duplicateKeys) / duration.Seconds()

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

	var avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, avgMixedDb string

	avgMixedDb = getLatencyStr(swapped[opMixed].totalBulkWriteLatency, swapped[opMixed].bulkWriteLatencyCount)

	if !sm.groupOpsByDistinctId {
		avgInsertDb = getLatencyStr(swapped[opInsert].totalBulkWriteLatency, swapped[opInsert].bulkWriteLatencyCount)
		avgUpdateDb = getLatencyStr(swapped[opUpdate].totalBulkWriteLatency, swapped[opUpdate].bulkWriteLatencyCount)
		avgDeleteDb = getLatencyStr(swapped[opDelete].totalBulkWriteLatency, swapped[opDelete].bulkWriteLatencyCount)
		avgReplaceDb = getLatencyStr(swapped[opReplace].totalBulkWriteLatency, swapped[opReplace].bulkWriteLatencyCount)
	}

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

	var dbLatencyMsg string
	if sm.groupOpsByDistinctId {
		dbLatencyMsg = avgMixedDb
	} else {
		dbLatencyMsg = fmt.Sprintf("[insert: %s, update: %s, delete: %s, replace: %s, mixed: %s]",
			avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, avgMixedDb)
	}

	sequentialRetriesBreakdown := ""
	if sequentialRetries > 0 {
		sequentialRetriesBreakdown = fmt.Sprintf(" [Inserts: %d, Updates: %d, Deletes: %d, Replaces: %d]",
			swapped[opInsert].sequentialRetries, swapped[opUpdate].sequentialRetries, swapped[opDelete].sequentialRetries, swapped[opReplace].sequentialRetries)
	}

	formatLag := func(d time.Duration) string {
		if d == 0 {
			return "N/A"
		}
		return d.Round(time.Millisecond).String()
	}

	// Format DLQ stats
	dlqStatsStr := "  - DLQ'ed:"
	var dlqOps []string
	opNames := []string{"insert", "update", "delete", "replace", "mixed"}
	for _, op := range opNames {
		idx := opIndex(op)
		if idx >= 0 && swapped[idx].dlq > 0 {
			dlqOps = append(dlqOps, fmt.Sprintf("%s: %d", op, swapped[idx].dlq))
		}
	}
	if len(dlqOps) > 0 {
		dlqStatsStr += " [" + strings.Join(dlqOps, ", ") + "]"
	} else {
		dlqStatsStr += " 0"
	}

	// Extract and format connection pool stats for source and target
	sourcePoolStr := sm.formatPoolStats(&sm.sourcePool, "Source", duration)
	targetPoolStr := sm.formatPoolStats(&sm.targetPool, "Target", duration)

	poolStatsStr := fmt.Sprintf("  - Connection Pool:\n"+
		"      * %s\n"+
		"      * %s", sourcePoolStr, targetPoolStr)

	// Format Failure Breakdown
	failureBreakdownStr := "  - Failure Details (Error/Op):"
	var hasBreakdown bool
	for op, errors := range breakdown {
		if len(errors) > 0 {
			hasBreakdown = true
			failureBreakdownStr += fmt.Sprintf("\n      * %s:", op)
			for errText, count := range errors {
				failureBreakdownStr += fmt.Sprintf("\n          - %s: %d", errText, count)
			}
		}
	}
	if !hasBreakdown {
		failureBreakdownStr += " 0"
	}

	headerStr := "Change stream statistics"
	if sm.DontApply {
		headerStr += " in dont-apply live-only mode"
	}

	var processedLine string
	if sm.DontApply {
		processedLine = fmt.Sprintf("  - Processed: %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec), updatedThenDeleted: %d (%.2f/sec)]",
			eventsProcessed, rateProcessed,
			insertsProcessed, rateInsertsProcessed,
			deletesProcessed, rateDeletesProcessed,
			updatesProcessed, rateUpdatesProcessed,
			updatedThenDeleted, rateUpdatedThenDeleted)
	} else {
		processedLine = fmt.Sprintf("  - Processed: %d (%.2f events/sec) (applied: %d (%.2f/sec)) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec), updatedThenDeleted: %d (%.2f/sec)]",
			eventsProcessed, rateProcessed,
			eventsApplied, rateApplied,
			insertsProcessed, rateInsertsProcessed,
			deletesProcessed, rateDeletesProcessed,
			updatesProcessed, rateUpdatesProcessed,
			updatedThenDeleted, rateUpdatedThenDeleted)
	}


	msg := fmt.Sprintf(headerStr+" (last %v):\n"+
		"  - Received:  %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec)]\n"+
		"%s\n"+
		"  - Failed:    %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec)]\n"+
		"  - Ordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Unordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Sequential Retries: %d (%.2f/sec)%s\n"+
		"  - Duplicate Key Errors: %d (%.2f/sec)\n"+
		"  - Group Flushes: [optype: %d (%.2f/sec), namespace: %d (%.2f/sec), batchfull: %d (%.2f/sec), id collision: %d (%.2f/sec), timeout: %d (%.2f/sec)]\n"+
		"  - BulkWrite Execution Latency: %s\n"+
		"  - Worker QPS: [Active: %d] [p50: %.2f, p70: %.2f, p90: %.2f, p100: %.2f]\n"+
		"  - Lags:\n"+
		"      * ReadToEventTimeLag: %s | WorkerReceivedToReadTimeLag: %s\n"+
		"      * SuccessWithRetryLag: [from received  : %s, from event time: %s]\n"+
		"      * SuccessLag: [from received  : %s, from event time: %s]\n"+
		"  - ChangeStream Read Performance:\n"+
		"      * Next Latency: %s | Event Size: avg %.1f bytes (total %s)\n"+
		"%s\n"+
		"%s\n"+
		"%s",
		duration.Round(time.Second),
		eventsReceived, rateReceived, insertsReceived, rateInsertsReceived, deletesReceived, rateDeletesReceived, updatesReceived, rateUpdatesReceived,
		processedLine,
		eventsFailed, rateFailed, insertsFailed, rateInsertsFailed, deletesFailed, rateDeletesFailed, updatesFailed, rateUpdatesFailed,
		orderedWrites, rateOrderedWrites, avgOrderedSizeStr, unorderedWrites, rateUnorderedWrites, avgUnorderedSizeStr, sequentialRetries, rateSequentialRetries, sequentialRetriesBreakdown, duplicateKeys, rateDuplicateKeys, groupFlushesOpType, rateGroupFlushesOpType, groupFlushesNamespace, rateGroupFlushesNamespace, groupFlushesBatchFull, rateGroupFlushesBatchFull, groupFlushesCollision, rateGroupFlushesCollision, timeoutFlushes, rateTimeoutFlushes,
		dbLatencyMsg,
		activeWorkers,
		wQpsP50, wQpsP70, wQpsP90, wQpsP100,
		formatLag(lagRes.ReadToEventTimeLag),
		formatLag(lagRes.WorkerReceivedToReadTimeLag),
		formatLag(lagRes.SuccessWithRetryLagToWorkerReceivedTime),
		formatLag(lagRes.SuccessWithRetryTimeToEventTime),
		formatLag(lagRes.SuccessTimeToWorkerReceivedLag),
		formatLag(lagRes.SuccessTimeToEventTimeLag),
		avgReadLatency.Round(time.Microsecond),
		avgReadSize,
		formatBytes(totalReadSizeBytes),
		dlqStatsStr,
		failureBreakdownStr,
		poolStatsStr)

	sm.log.Info(msg)
}

// ReportDryRunStats logs the dry-run statistics, resetting dry-run specific counters.
func (sm *StatsManager) ReportDryRunStats() {
	// Reset and load dry-run atomic counters atomically
	totalReadLatencyNs := atomic.SwapInt64(&sm.totalReadLatencyNs, 0)
	readCount := atomic.SwapInt64(&sm.readCount, 0)
	totalReadSizeBytes := atomic.SwapInt64(&sm.totalReadSizeBytes, 0)

	sm.mu.Lock()
	now := time.Now()
	duration := now.Sub(sm.lastStatsTime)
	sm.lastStatsTime = now
	sm.mu.Unlock()

	var rateProcessed float64
	if duration.Seconds() > 0 {
		rateProcessed = float64(readCount) / duration.Seconds()
	}

	avgLatency := time.Duration(0)
	avgSize := 0.0
	if readCount > 0 {
		avgLatency = time.Duration(totalReadLatencyNs / readCount)
		avgSize = float64(totalReadSizeBytes) / float64(readCount)
	}

	// Format connection pool stats
	sourcePoolStr := sm.formatPoolStats(&sm.sourcePool, "Source", duration)
	targetPoolStr := sm.formatPoolStats(&sm.targetPool, "Target", duration)

	msg := fmt.Sprintf("Change stream statistics in dry-run live-only mode (last %v):\n"+
		"  - Read: %d (%.2f events/sec) | Next Latency: avg %s | Event Size: avg %.1f bytes (total %s)\n"+
		"  - Connection Pool:\n"+
		"      * %s\n"+
		"      * %s",
		duration.Round(time.Second),
		readCount, rateProcessed,
		avgLatency.Round(time.Microsecond),
		avgSize,
		formatBytes(totalReadSizeBytes),
		sourcePoolStr,
		targetPoolStr)

	sm.log.Info(msg)
}

func calculatePercentiles(values []int) (int, int, int) {
	if len(values) == 0 {
		return 0, 0, 0
	}

	var activeValues []int
	for i, count := range values {
		for j := 0; j < count; j++ {
			activeValues = append(activeValues, i)
		}
	}

	if len(activeValues) == 0 {
		return 0, 0, 0
	}

	sort.Ints(activeValues)
	p50 := activeValues[int(float64(len(activeValues))*0.50)]
	p90 := activeValues[int(float64(len(activeValues))*0.90)]
	p100 := activeValues[len(activeValues)-1]

	return p50, p90, p100
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
