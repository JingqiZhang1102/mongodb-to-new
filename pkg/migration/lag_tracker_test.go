package migration

import (
	"testing"
	"time"
)

func TestLagTrackerAccumulationAndFlush(t *testing.T) {
	tracker := NewLagTracker()

	now := time.Now()

	// op1: Succeeded after retry (SuccessAfterRetry: true)
	// EventTime = now - 500ms, ReadTime = now - 400ms, WorkerReceiveTime = now - 200ms, SuccessTime = now - 100ms
	// readToEventTimeLag: 100ms
	// workerReceivedToReadTimeLag: 200ms
	// successWithRetryTimeToEventTime: 400ms
	// successWithRetryLagToWorkerReceivedTime: 100ms
	op1 := WriteOperation{
		EventTime:         now.Add(-500 * time.Millisecond),
		ReadTime:          now.Add(-400 * time.Millisecond),
		WorkerReceiveTime: now.Add(-200 * time.Millisecond),
		SuccessTime:       now.Add(-100 * time.Millisecond),
		SuccessAfterRetry: true,
	}

	// op2: Ignored (no SuccessTime)
	op2 := WriteOperation{
		EventTime:         now.Add(-500 * time.Millisecond),
		ReadTime:          now.Add(-400 * time.Millisecond),
		WorkerReceiveTime: now.Add(-200 * time.Millisecond),
	}

	// op3: Succeeded on first attempt (SuccessAfterRetry: false)
	// EventTime = now - 1000ms, ReadTime = now - 900ms, WorkerReceiveTime = now - 700ms, SuccessTime = now - 600ms
	// readToEventTimeLag: 100ms
	// workerReceivedToReadTimeLag: 200ms
	// successTimeToWorkerReceivedLag: 100ms
	// successTimeToEventTimeLag: 400ms
	op3 := WriteOperation{
		EventTime:         now.Add(-1000 * time.Millisecond),
		ReadTime:          now.Add(-900 * time.Millisecond),
		WorkerReceiveTime: now.Add(-700 * time.Millisecond),
		SuccessTime:       now.Add(-600 * time.Millisecond),
		SuccessAfterRetry: false,
	}

	tracker.RecordLags([]WriteOperation{op1, op2, op3})

	// Flush the metrics
	res := tracker.Flush()

	// readToEventTimeLag: (100ms + 100ms) / 2 = 100ms
	if res.ReadToEventTimeLag != 100*time.Millisecond {
		t.Errorf("expected ReadToEventTimeLag 100ms, got %v", res.ReadToEventTimeLag)
	}
	// workerReceivedToReadTimeLag: (200ms + 200ms) / 2 = 200ms
	if res.WorkerReceivedToReadTimeLag != 200*time.Millisecond {
		t.Errorf("expected WorkerReceivedToReadTimeLag 200ms, got %v", res.WorkerReceivedToReadTimeLag)
	}
	
	// successTimeToWorkerReceivedLag (from op3 only): 100ms
	if res.SuccessTimeToWorkerReceivedLag != 100*time.Millisecond {
		t.Errorf("expected SuccessTimeToWorkerReceivedLag 100ms, got %v", res.SuccessTimeToWorkerReceivedLag)
	}
	// successTimeToEventTimeLag (from op3 only): 400ms
	if res.SuccessTimeToEventTimeLag != 400*time.Millisecond {
		t.Errorf("expected SuccessTimeToEventTimeLag 400ms, got %v", res.SuccessTimeToEventTimeLag)
	}
	
	// successWithRetryTimeToEventTime (from op1 only): 400ms
	if res.SuccessWithRetryTimeToEventTime != 400*time.Millisecond {
		t.Errorf("expected SuccessWithRetryTimeToEventTime 400ms, got %v", res.SuccessWithRetryTimeToEventTime)
	}
	// successWithRetryLagToWorkerReceivedTime (from op1 only): 100ms
	if res.SuccessWithRetryLagToWorkerReceivedTime != 100*time.Millisecond {
		t.Errorf("expected SuccessWithRetryLagToWorkerReceivedTime 100ms, got %v", res.SuccessWithRetryLagToWorkerReceivedTime)
	}

	// Verify counters reset after Flush
	res2 := tracker.Flush()
	if res2.ReadToEventTimeLag != 0 || res2.WorkerReceivedToReadTimeLag != 0 || res2.SuccessTimeToWorkerReceivedLag != 0 || res2.SuccessTimeToEventTimeLag != 0 || res2.SuccessWithRetryTimeToEventTime != 0 || res2.SuccessWithRetryLagToWorkerReceivedTime != 0 {
		t.Errorf("expected flushed metrics to reset to 0, got %+v", res2)
	}
}
