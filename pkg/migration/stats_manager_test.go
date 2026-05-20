package migration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

func TestStatsManagerEventCounting(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	now := time.Now()

	// Record batch of 5 operations of different types
	ops5 := []WriteOperation{
		{OpType: "insert"},
		{OpType: "update"},
		{OpType: "delete"},
		{OpType: "replace"},
		{OpType: "insert"},
	}
	sm.RecordLags(ops5, now)
	
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
	}
	sm.RecordLags(ops10, now)
	
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
		{EventTime: now.Add(-250 * time.Millisecond), OpType: "insert"},
	}
	sm.RecordLags(ops, now)

	// Call ReportStats
	sm.ReportStats()

	// Counters should reset to 0 after report
	if sm.GetProcessedCount("insert") != 0 {
		t.Errorf("expected inserts processed count to reset to 0, got %d", sm.GetProcessedCount("insert"))
	}
	if sm.lagTracker.count != 0 {
		t.Errorf("expected lagTracker count to reset to 0, got %d", sm.lagTracker.count)
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
		{EventTime: now.Add(-100 * time.Millisecond)},
		{EventTime: now.Add(-200 * time.Millisecond)},
	}

	// Call RecordLags on StatsManager
	sm.RecordLags(ops, now)

	// Wait for periodic ticker to fire
	time.Sleep(25 * time.Millisecond)

	// The counters should have been flushed/reset to 0 by ReportStats inside the ticker goroutine
	sm.mu.Lock()
	lagCount := sm.lagTracker.count
	sm.mu.Unlock()

	if lagCount != 0 {
		t.Errorf("expected lag count to reset to 0 after periodic ticker, got %d", lagCount)
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
					{EventTime: now.Add(-1 * time.Millisecond), OpType: "insert"},
				}, now)
			}
		}()
	}

	wg.Wait()

	expectedEvents := workersCount * iterations
	processedEvents := sm.GetProcessedCount("insert")
	if processedEvents != expectedEvents {
		t.Errorf("expected %d events, got %d", expectedEvents, processedEvents)
	}
	if sm.lagTracker.count != int64(expectedEvents) {
		t.Errorf("expected %d lag records, got %d", expectedEvents, sm.lagTracker.count)
	}
}

func TestStatsManagerMetrics(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	// Test single increment for all three
	sm.IncrementUpdateDocMissing()
	sm.IncrementEventsReceived("insert")
	sm.IncrementEventsFailed("insert")
	sm.IncrementSequentialRetries(3)
	sm.RecordBulkWrite(42, true)
	sm.RecordBulkWrite(10, false)
	sm.IncrementTimeoutFlushes()

	sm.mu.Lock()
	missingCount := sm.updateDocMissingSinceLastStats
	sm.mu.Unlock()
	inputCount := sm.GetReceivedCount("insert")
	failedCount := sm.GetFailedCount("insert")
	seqRetriesCount := sm.GetSequentialRetries()
	orderedWritesCount := sm.GetOrderedBulkWrites()
	orderedWritesSize := sm.GetOrderedBulkWritesSize()
	unorderedWritesCount := sm.GetUnorderedBulkWrites()
	unorderedWritesSize := sm.GetUnorderedBulkWritesSize()
	timeoutFlushesCount := sm.GetTimeoutFlushes()

	if missingCount != 1 {
		t.Errorf("expected updateDocMissingSinceLastStats 1, got %d", missingCount)
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
				sm.IncrementUpdateDocMissing()
				sm.IncrementEventsReceived("insert")
				sm.IncrementEventsFailed("insert")
			}
		}()
	}
	wg.Wait()

	sm.mu.Lock()
	missingCount = sm.updateDocMissingSinceLastStats
	sm.mu.Unlock()
	inputCount = sm.GetReceivedCount("insert")
	failedCount = sm.GetFailedCount("insert")

	expectedCount := 1 + workersCount*iterations
	if missingCount != expectedCount {
		t.Errorf("expected updateDocMissingSinceLastStats %d, got %d", expectedCount, missingCount)
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
	sm.mu.Lock()
	missingCountAfter := sm.updateDocMissingSinceLastStats
	sm.mu.Unlock()

	if missingCountAfter != 0 {
		t.Errorf("expected updateDocMissingSinceLastStats to reset to 0, got %d", missingCountAfter)
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
