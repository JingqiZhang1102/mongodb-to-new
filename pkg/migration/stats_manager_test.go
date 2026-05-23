package migration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

func TestStatsManagerEventCounting(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	now := time.Now()

	// Record batch of 5 operations of different types (SuccessTime must be set to count them as processed)
	ops5 := []WriteOperation{
		{OpType: "insert", SuccessTime: now},
		{OpType: "update", SuccessTime: now},
		{OpType: "delete", SuccessTime: now},
		{OpType: "replace", SuccessTime: now},
		{OpType: "insert", SuccessTime: now},
	}
	sm.RecordLags(ops5)
	
	totalProcessed5 := sm.GetProcessedCount("insert") + sm.GetProcessedCount("update") + sm.GetProcessedCount("replace") + sm.GetProcessedCount("delete")
	if totalProcessed5 != 5 {
		t.Errorf("expected total processed 5, got %d", totalProcessed5)
	}
	if sm.GetProcessedCount("insert") != 2 {
		t.Errorf("expected inserts processed count 2, got %d", sm.GetProcessedCount("insert"))
	}
	if sm.GetProcessedCount("update")+sm.GetProcessedCount("replace") != 2 {
		t.Errorf("expected updates+replaces processed count 2, got %d", sm.GetProcessedCount("update")+sm.GetProcessedCount("replace"))
	}
	if sm.GetProcessedCount("delete") != 1 {
		t.Errorf("expected deletes processed count 1, got %d", sm.GetProcessedCount("delete"))
	}

	// Record batch of 10 operations
	ops10 := make([]WriteOperation, 10)
	for i := range ops10 {
		ops10[i].OpType = "insert"
		ops10[i].SuccessTime = now
	}
	sm.RecordLags(ops10)
	
	totalProcessed15 := sm.GetProcessedCount("insert") + sm.GetProcessedCount("update") + sm.GetProcessedCount("replace") + sm.GetProcessedCount("delete")
	if totalProcessed15 != 15 {
		t.Errorf("expected total processed 15, got %d", totalProcessed15)
	}
	if sm.GetProcessedCount("insert") != 12 {
		t.Errorf("expected inserts processed count 12, got %d", sm.GetProcessedCount("insert"))
	}
}

func TestStatsManagerReportStats(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	now := time.Now()
	ops := []WriteOperation{
		{EventTime: now.Add(-250 * time.Millisecond), OpType: "insert", SuccessTime: now},
	}
	sm.RecordLags(ops)

	// Call ReportStats
	sm.ReportStats()

	// Counters should reset to 0 after report
	if sm.GetProcessedCount("insert") != 0 {
		t.Errorf("expected inserts processed count to reset to 0, got %d", sm.GetProcessedCount("insert"))
	}
	
	res := sm.lagTracker.Flush()
	if res.SuccessTimeToEventTimeLag != 0 {
		t.Errorf("expected lagTracker metrics to reset to 0, got %v", res.SuccessTimeToEventTimeLag)
	}
}

func TestStatsManagerRecordLagsAndStart(t *testing.T) {
	log := logger.New()
	// Use a very small interval for testing Start
	sm := NewStatsManager(log, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm.Start(ctx)

	now := time.Now()
	ops := []WriteOperation{
		{EventTime: now.Add(-100 * time.Millisecond), SuccessTime: now},
		{EventTime: now.Add(-200 * time.Millisecond), SuccessTime: now},
	}

	// Call RecordLags on StatsManager
	sm.RecordLags(ops)

	// Wait for periodic ticker to fire
	time.Sleep(25 * time.Millisecond)

	// The counters should have been flushed/reset to 0 by ReportStats inside the ticker goroutine
	sm.mu.Lock()
	res := sm.lagTracker.Flush()
	sm.mu.Unlock()

	if res.SuccessTimeToEventTimeLag != 0 {
		t.Errorf("expected lag count to reset to 0 after periodic ticker, got %v", res.SuccessTimeToEventTimeLag)
	}
}

func TestStatsManagerConcurrency(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	var wg sync.WaitGroup
	workersCount := 25
	iterations := 150
	now := time.Now()

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sm.RecordLags([]WriteOperation{
					{EventTime: now.Add(-1 * time.Millisecond), OpType: "insert", SuccessTime: now},
				})
			}
		}()
	}

	wg.Wait()

	expectedEvents := workersCount * iterations
	processedEvents := sm.GetProcessedCount("insert")
	if processedEvents != expectedEvents {
		t.Errorf("expected %d events, got %d", expectedEvents, processedEvents)
	}
	
	res := sm.lagTracker.Flush()
	if res.SuccessTimeToEventTimeLag != 1*time.Millisecond {
		t.Errorf("expected SuccessTimeToEventTimeLag 1ms, got %v", res.SuccessTimeToEventTimeLag)
	}

	res2 := sm.lagTracker.Flush()
	if res2.SuccessTimeToEventTimeLag != 0 {
		t.Errorf("expected lagTracker to reset to 0, got %v", res2.SuccessTimeToEventTimeLag)
	}
}

func TestStatsManagerMetrics(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	// Test single increment for all three
	sm.IncrementUpdatedThenDeleted(0)
	sm.IncrementEventsReceived("insert")
	sm.IncrementEventsFailed("insert", false, nil)
	sm.IncrementSequentialRetries("insert", 3)
	sm.RecordBulkWrite(42, true)
	sm.RecordBulkWrite(10, false)
	sm.IncrementTimeoutFlushes()

	missingCount := sm.GetUpdatedThenDeleted()
	inputCount := sm.GetReceivedCount("insert")
	failedCount := sm.GetFailedCount("insert")
	seqRetriesCount := sm.GetSequentialRetries()
	orderedWritesCount := sm.GetOrderedBulkWrites()
	orderedWritesSize := sm.GetOrderedBulkWritesSize()
	unorderedWritesCount := sm.GetUnorderedBulkWrites()
	unorderedWritesSize := sm.GetUnorderedBulkWritesSize()
	timeoutFlushesCount := sm.GetTimeoutFlushes()

	if missingCount != 1 {
		t.Errorf("expected updatedThenDeletedSinceLastStats 1, got %d", missingCount)
	}
	if inputCount != 1 {
		t.Errorf("expected insertsReceivedSinceLastStats 1, got %d", inputCount)
	}
	if failedCount != 1 {
		t.Errorf("expected insertsFailedSinceLastStats 1, got %d", failedCount)
	}
	if seqRetriesCount != 3 {
		t.Errorf("expected sequential retries 3, got %d", seqRetriesCount)
	}
	if orderedWritesCount != 1 {
		t.Errorf("expected ordered bulk writes 1, got %d", orderedWritesCount)
	}
	if orderedWritesSize != 42 {
		t.Errorf("expected ordered bulk writes size 42, got %d", orderedWritesSize)
	}
	if unorderedWritesCount != 1 {
		t.Errorf("expected unordered bulk writes 1, got %d", unorderedWritesCount)
	}
	if unorderedWritesSize != 10 {
		t.Errorf("expected unordered bulk writes size 10, got %d", unorderedWritesSize)
	}
	if timeoutFlushesCount != 1 {
		t.Errorf("expected timeout flushes 1, got %d", timeoutFlushesCount)
	}

	// Test concurrent increments
	var wg sync.WaitGroup
	workersCount := 10
	iterations := 100
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sm.IncrementUpdatedThenDeleted(0)
				sm.IncrementEventsReceived("insert")
				sm.IncrementEventsFailed("insert", false, nil)
			}
		}()
	}
	wg.Wait()

	missingCount = sm.GetUpdatedThenDeleted()
	inputCount = sm.GetReceivedCount("insert")
	failedCount = sm.GetFailedCount("insert")

	expectedCount := 1 + workersCount*iterations
	if missingCount != expectedCount {
		t.Errorf("expected updatedThenDeletedSinceLastStats %d, got %d", expectedCount, missingCount)
	}
	if inputCount != expectedCount {
		t.Errorf("expected insertsReceivedSinceLastStats %d, got %d", expectedCount, inputCount)
	}
	if failedCount != expectedCount {
		t.Errorf("expected insertsFailedSinceLastStats %d, got %d", expectedCount, failedCount)
	}

	// Verify ReportStats logs without panic
	sm.ReportStats()

	// Counters should reset after ReportStats
	missingCountAfter := sm.GetUpdatedThenDeleted()

	if missingCountAfter != 0 {
		t.Errorf("expected updatedThenDeletedSinceLastStats to reset to 0, got %d", missingCountAfter)
	}
	if sm.GetSequentialRetries() != 0 {
		t.Errorf("expected sequential retries to reset to 0, got %d", sm.GetSequentialRetries())
	}
	if sm.GetOrderedBulkWrites() != 0 {
		t.Errorf("expected ordered bulk writes to reset to 0, got %d", sm.GetOrderedBulkWrites())
	}
	if sm.GetOrderedBulkWritesSize() != 0 {
		t.Errorf("expected ordered bulk writes size to reset to 0, got %d", sm.GetOrderedBulkWritesSize())
	}
	if sm.GetUnorderedBulkWrites() != 0 {
		t.Errorf("expected unordered bulk writes to reset to 0, got %d", sm.GetUnorderedBulkWrites())
	}
	if sm.GetUnorderedBulkWritesSize() != 0 {
		t.Errorf("expected unordered bulk writes size to reset to 0, got %d", sm.GetUnorderedBulkWritesSize())
	}
	if sm.GetTimeoutFlushes() != 0 {
		t.Errorf("expected timeout flushes to reset to 0, got %d", sm.GetTimeoutFlushes())
	}
	if sm.GetReceivedCount("insert") != 0 || sm.GetReceivedCount("update") != 0 || sm.GetReceivedCount("delete") != 0 {
		t.Errorf("expected received counters to reset to 0")
	}
	if sm.GetProcessedCount("insert") != 0 || sm.GetProcessedCount("update") != 0 || sm.GetProcessedCount("delete") != 0 {
		t.Errorf("expected processed counters to reset to 0")
	}
	if sm.GetFailedCount("insert") != 0 || sm.GetFailedCount("update") != 0 || sm.GetFailedCount("delete") != 0 {
		t.Errorf("expected failed counters to reset to 0")
	}
}

func TestStatsManagerLatencies(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	// Test RecordLatency for insert
	now := time.Now()
	// For ops1: ReadTime = now - 150ms, WorkerReceiveTime = now - 100ms (transit lag: 50ms, buffering lag: 100ms)
	ops1 := []WriteOperation{
		{ReadTime: now.Add(-150 * time.Millisecond), WorkerReceiveTime: now.Add(-100 * time.Millisecond), SuccessTime: now},
		{ReadTime: now.Add(-120 * time.Millisecond), WorkerReceiveTime: now.Add(-50 * time.Millisecond), SuccessTime: now},
	}
	// For ops2: ReadTime = now - 350ms, WorkerReceiveTime = now - 200ms (transit lag: 150ms, buffering lag: 200ms)
	ops2 := []WriteOperation{
		{ReadTime: now.Add(-350 * time.Millisecond), WorkerReceiveTime: now.Add(-200 * time.Millisecond), SuccessTime: now},
		{ReadTime: now.Add(-300 * time.Millisecond), WorkerReceiveTime: now.Add(-150 * time.Millisecond), SuccessTime: now},
	}

	sm.RecordLatency("insert", ops1, 50*time.Millisecond, 0, true)
	sm.RecordLatency("insert", ops2, 150*time.Millisecond, 0, true)
	
	sm.RecordLags(ops1)
	sm.RecordLags(ops2)

	lagRes := sm.lagTracker.Flush()

	// Expected average transit: (50ms + 150ms) / 2 = 100ms
	if lagRes.WorkerReceivedToReadTimeLag < 90*time.Millisecond || lagRes.WorkerReceivedToReadTimeLag > 110*time.Millisecond {
		t.Errorf("expected average transit latency around 100ms, got %v", lagRes.WorkerReceivedToReadTimeLag)
	}

	// Expected average buffering: (100ms + 50ms + 200ms + 150ms) / 4 = 125ms
	if lagRes.SuccessTimeToWorkerReceivedLag < 115*time.Millisecond || lagRes.SuccessTimeToWorkerReceivedLag > 135*time.Millisecond {
		t.Errorf("expected average buffering latency around 125ms, got %v", lagRes.SuccessTimeToWorkerReceivedLag)
	}

	avgBulkOpInsert := sm.GetAvgBulkWriteLatency("insert")
	// Expected average bulk op latency: (50ms + 150ms) / 2 = 100ms
	if avgBulkOpInsert < 90*time.Millisecond || avgBulkOpInsert > 110*time.Millisecond {
		t.Errorf("expected average insert bulkwrite latency around 100ms, got %v", avgBulkOpInsert)
	}

	// Test RecordLatency for update
	ops3 := []WriteOperation{
		{ReadTime: now.Add(-500 * time.Millisecond), WorkerReceiveTime: now.Add(-400 * time.Millisecond), SuccessTime: now},
	}
	sm.RecordLatency("update", ops3, 300*time.Millisecond, 0, true)
	sm.RecordLags(ops3)

	lagResUpdate := sm.lagTracker.Flush()

	if lagResUpdate.WorkerReceivedToReadTimeLag != 100*time.Millisecond {
		t.Errorf("expected update transit latency 100ms, got %v", lagResUpdate.WorkerReceivedToReadTimeLag)
	}

	if lagResUpdate.SuccessTimeToWorkerReceivedLag != 400*time.Millisecond {
		t.Errorf("expected update buffering latency 400ms, got %v", lagResUpdate.SuccessTimeToWorkerReceivedLag)
	}

	avgBulkOpUpdate := sm.GetAvgBulkWriteLatency("update")
	if avgBulkOpUpdate != 300*time.Millisecond {
		t.Errorf("expected update bulkwrite latency 300ms, got %v", avgBulkOpUpdate)
	}

	// Test group flushes by reason metrics
	if sm.GetGroupFlushReasonCount("optype") != 0 {
		t.Errorf("expected initial optype flushes count to be 0, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	sm.IncrementGroupFlushReason("optype")
	sm.IncrementGroupFlushReason("optype")
	sm.IncrementGroupFlushReason("batchfull")
	if sm.GetGroupFlushReasonCount("optype") != 2 {
		t.Errorf("expected optype flushes count to be 2, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	if sm.GetGroupFlushReasonCount("batchfull") != 1 {
		t.Errorf("expected batchfull flushes count to be 1, got %d", sm.GetGroupFlushReasonCount("batchfull"))
	}

	// Verify ReportStats resets them
	sm.ReportStats()

	if sm.GetAvgBulkWriteLatency("insert") != 0 {
		t.Errorf("expected avg insert bulkwrite latency to reset to 0, got %v", sm.GetAvgBulkWriteLatency("insert"))
	}
	if sm.GetGroupFlushReasonCount("optype") != 0 {
		t.Errorf("expected optype flushes count to reset to 0, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	if sm.GetGroupFlushReasonCount("batchfull") != 0 {
		t.Errorf("expected batchfull flushes count to reset to 0, got %d", sm.GetGroupFlushReasonCount("batchfull"))
	}
}

// BenchmarkStatsManagerRecordLatency measures stats gathering hot-path latency under high concurrent load
func BenchmarkStatsManagerRecordLatency(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	sm := NewStatsManager(log, 5*time.Minute)
	now := time.Now()
	ops := []WriteOperation{{ReadTime: now.Add(-20 * time.Millisecond), WorkerReceiveTime: now.Add(-10 * time.Millisecond), OpType: "mixed"}}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.RecordLatency("mixed", ops, 5*time.Millisecond, i%8, true)
			i++
		}
	})
}

// BenchmarkStatsManagerIncrementReceived measures simple counter increment performance under high concurrent load
func BenchmarkStatsManagerIncrementReceived(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	sm := NewStatsManager(log, 5*time.Minute)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sm.IncrementEventsReceived("insert")
		}
	})
}

func TestStatsManagerDLQAndFailureBreakdown(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	err1 := fmt.Errorf("connection(5bc95d74-1eb9-46fa-9445-39e5b4514544.us-east4.firestore.goog:443[-27608]) socket was unexpectedly closed: EOF")
	err2 := fmt.Errorf("connection(other-conn.us-east4.firestore.goog:443[-28000]) socket was unexpectedly closed: EOF")
	err3 := fmt.Errorf("Document already exists")

	// 1. Increment failed event with DLQ = true, dynamic EOF error 1
	sm.IncrementEventsFailed("insert", true, err1)
	// 2. Increment failed event with DLQ = true, dynamic EOF error 2 (should be normalized and grouped together!)
	sm.IncrementEventsFailed("insert", true, err2)
	// 3. Increment failed event with DLQ = false, standard duplicate error
	sm.IncrementEventsFailed("update", false, err3)

	sm.failureMu.Lock()
	dlqCountInsert := sm.GetDLQCount("insert")
	dlqCountUpdate := sm.GetDLQCount("update")
	breakdownInsertEOF := sm.failureBreakdown["insert"]["connection(...) socket was unexpectedly closed: EOF"]
	breakdownUpdateDup := sm.failureBreakdown["update"]["Document already exists"]
	sm.failureMu.Unlock()

	if dlqCountInsert != 2 {
		t.Errorf("expected DLQ'ed inserts 2, got %d", dlqCountInsert)
	}
	if dlqCountUpdate != 0 {
		t.Errorf("expected DLQ'ed updates 0, got %d", dlqCountUpdate)
	}
	if breakdownInsertEOF != 2 {
		t.Errorf("expected normalized and grouped insert EOF count 2, got %d", breakdownInsertEOF)
	}
	if breakdownUpdateDup != 1 {
		t.Errorf("expected update duplicate count 1, got %d", breakdownUpdateDup)
	}

	// Verify ReportStats clears them and prints without panic
	sm.ReportStats()

	sm.failureMu.Lock()
	if sm.GetDLQCount("insert") != 0 || sm.GetDLQCount("update") != 0 || len(sm.failureBreakdown) != 0 {
		t.Errorf("expected DLQ stats and failure breakdown to reset after ReportStats")
	}
	sm.failureMu.Unlock()
}

