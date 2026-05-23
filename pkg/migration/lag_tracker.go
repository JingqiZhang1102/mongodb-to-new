package migration

import (
	"time"
)

// LagFlushResult represents the averaged lag metrics flushed from LagTracker
type LagFlushResult struct {
	ReadToEventTimeLag                      time.Duration
	WorkerReceivedToReadTimeLag             time.Duration
	SuccessTimeToWorkerReceivedLag          time.Duration
	SuccessTimeToEventTimeLag               time.Duration
	SuccessWithRetryTimeToEventTime         time.Duration
	SuccessWithRetryLagToWorkerReceivedTime time.Duration
}

// metricAccumulator unifies duration sum and event count for simple averaging
type metricAccumulator struct {
	total time.Duration
	count int64
}

func (m *metricAccumulator) Add(d time.Duration) {
	m.total += d
	m.count++
}

func (m *metricAccumulator) Average() time.Duration {
	if m.count > 0 {
		return m.total / time.Duration(m.count)
	}
	return 0
}

func (m *metricAccumulator) Reset() {
	m.total = 0
	m.count = 0
}

// LagTracker tracks replication lag measurements (mutex-free, relies on parent locking)
type LagTracker struct {
	readToEventTime                      metricAccumulator
	workerReceivedToReadTime             metricAccumulator
	successTimeToWorkerReceived          metricAccumulator
	successTimeToEventTime               metricAccumulator
	successWithRetryTimeToEventTime         metricAccumulator
	successWithRetryLagToWorkerReceivedTime metricAccumulator
}

// NewLagTracker creates a new instance of LagTracker
func NewLagTracker() *LagTracker {
	return &LagTracker{}
}

// RecordLags records the replication lag metrics for a batch of write operations
func (lt *LagTracker) RecordLags(ops []WriteOperation) {
	for _, op := range ops {
		if op.SuccessTime.IsZero() {
			continue
		}

		if !op.ReadTime.IsZero() && !op.EventTime.IsZero() {
			lt.readToEventTime.Add(op.ReadTime.Sub(op.EventTime))
		}

		if !op.WorkerReceiveTime.IsZero() && !op.ReadTime.IsZero() {
			lt.workerReceivedToReadTime.Add(op.WorkerReceiveTime.Sub(op.ReadTime))
		}

		if op.SuccessAfterRetry {
			if !op.EventTime.IsZero() {
				lt.successWithRetryTimeToEventTime.Add(op.SuccessTime.Sub(op.EventTime))
			}
			if !op.WorkerReceiveTime.IsZero() {
				lt.successWithRetryLagToWorkerReceivedTime.Add(op.SuccessTime.Sub(op.WorkerReceiveTime))
			}
		} else {
			if !op.WorkerReceiveTime.IsZero() {
				lt.successTimeToWorkerReceived.Add(op.SuccessTime.Sub(op.WorkerReceiveTime))
			}
			if !op.EventTime.IsZero() {
				lt.successTimeToEventTime.Add(op.SuccessTime.Sub(op.EventTime))
			}
		}
	}
}

// Flush returns the accumulated lag averages, then resets internal counters
func (lt *LagTracker) Flush() LagFlushResult {
	res := LagFlushResult{
		ReadToEventTimeLag:                      lt.readToEventTime.Average(),
		WorkerReceivedToReadTimeLag:             lt.workerReceivedToReadTime.Average(),
		SuccessTimeToWorkerReceivedLag:          lt.successTimeToWorkerReceived.Average(),
		SuccessTimeToEventTimeLag:               lt.successTimeToEventTime.Average(),
		SuccessWithRetryTimeToEventTime:         lt.successWithRetryTimeToEventTime.Average(),
		SuccessWithRetryLagToWorkerReceivedTime: lt.successWithRetryLagToWorkerReceivedTime.Average(),
	}

	lt.readToEventTime.Reset()
	lt.workerReceivedToReadTime.Reset()
	lt.successTimeToWorkerReceived.Reset()
	lt.successTimeToEventTime.Reset()
	lt.successWithRetryTimeToEventTime.Reset()
	lt.successWithRetryLagToWorkerReceivedTime.Reset()

	return res
}
