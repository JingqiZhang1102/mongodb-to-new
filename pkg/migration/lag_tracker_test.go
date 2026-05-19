package migration

import (
	"testing"
	"time"
)

func TestLagTrackerAccumulation(t *testing.T) {
	tracker := NewLagTracker()

	// Test single record using RecordLags
	now := time.Now()
	tracker.RecordLags([]WriteOperation{
		{EventTime: now.Add(-500 * time.Millisecond)},
	}, now)

	if tracker.count != 1 {
		t.Errorf("expected count 1, got %d", tracker.count)
	}
	if tracker.totalLag != 500*time.Millisecond {
		t.Errorf("expected totalLag 500ms, got %v", tracker.totalLag)
	}

	// Test batch record
	ops := []WriteOperation{
		{EventTime: now.Add(-100 * time.Millisecond)},
		{EventTime: now.Add(-200 * time.Millisecond)},
		{EventTime: time.Time{}}, // should be ignored
	}

	tracker.RecordLags(ops, now)

	// Previous 1 event (500ms) + new 2 valid events (100ms + 200ms = 300ms)
	// Total events = 3
	// Total lag = 800ms
	if tracker.count != 3 {
		t.Errorf("expected count 3, got %d", tracker.count)
	}
	if tracker.totalLag != 800*time.Millisecond {
		t.Errorf("expected totalLag 800ms, got %v", tracker.totalLag)
	}
}

func TestLagTrackerFlush(t *testing.T) {
	tracker := NewLagTracker()

	// Record some lag
	now := time.Now()
	tracker.RecordLags([]WriteOperation{
		{EventTime: now.Add(-100 * time.Millisecond)},
	}, now)

	// Flush the metrics
	total, count := tracker.Flush()

	if count != 1 {
		t.Errorf("expected flushed count 1, got %d", count)
	}
	if total != 100*time.Millisecond {
		t.Errorf("expected flushed totalLag 100ms, got %v", total)
	}

	// The counters should be reset to 0 after Flush
	if tracker.count != 0 {
		t.Errorf("expected count to reset to 0, got %d", tracker.count)
	}
	if tracker.totalLag != 0 {
		t.Errorf("expected totalLag to reset to 0, got %v", tracker.totalLag)
	}
}
