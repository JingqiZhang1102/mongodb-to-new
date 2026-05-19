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

	// Record batch of 5 operations
	ops5 := make([]WriteOperation, 5)
	sm.RecordLags(ops5, now)
	if sm.eventsSinceLastStats != 5 {
		t.Errorf("expected eventsSinceLastStats 5, got %d", sm.eventsSinceLastStats)
	}

	// Record batch of 10 operations
	ops10 := make([]WriteOperation, 10)
	sm.RecordLags(ops10, now)
	if sm.eventsSinceLastStats != 15 {
		t.Errorf("expected eventsSinceLastStats 15, got %d", sm.eventsSinceLastStats)
	}
}

func TestStatsManagerReportStats(t *testing.T) {
	log := logger.New()
	sm := NewStatsManager(log, 5*time.Minute)

	now := time.Now()
	ops := []WriteOperation{
		{EventTime: now.Add(-250 * time.Millisecond)},
	}
	sm.RecordLags(ops, now)

	// Call ReportStats
	sm.ReportStats()

	// Counters should reset to 0 after report
	if sm.eventsSinceLastStats != 0 {
		t.Errorf("expected eventsSinceLastStats to reset to 0, got %d", sm.eventsSinceLastStats)
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
					{EventTime: now.Add(-1 * time.Millisecond)},
				}, now)
			}
		}()
	}

	wg.Wait()

	expectedEvents := workersCount * iterations
	if sm.eventsSinceLastStats != expectedEvents {
		t.Errorf("expected %d events, got %d", expectedEvents, sm.eventsSinceLastStats)
	}
	if sm.lagTracker.count != int64(expectedEvents) {
		t.Errorf("expected %d lag records, got %d", expectedEvents, sm.lagTracker.count)
	}
}
