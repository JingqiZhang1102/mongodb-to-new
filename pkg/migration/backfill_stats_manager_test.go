package migration

import (
	"context"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

func TestBackfillStatsManager(t *testing.T) {
	log := logger.New()
	sm := NewBackfillStatsManager(log, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm.Start(ctx)

	// Record some reads
	sm.RecordRead(10*time.Millisecond, 500)
	sm.RecordRead(20*time.Millisecond, 1500) // 1.5 KB
	sm.RecordRead(100*time.Millisecond, 10240) // 10 KB

	sm.AddTargetCount(10)
	sm.RecordIngestQueueStall(100 * time.Millisecond)

	// Record worker received
	sm.RecordWorkerReceived(3)

	// Record some writes
	sm.RecordBulkWrite(50 * time.Millisecond)
	sm.RecordWriteResult(2, 1, 1, 1, 0)
	sm.IncrementSequentialRetries("replace", 1)

	// Trigger manual stats report
	sm.ReportStats(false)

	// Test stats manager with a throttler (active)
	cfgActive := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         500.0,
		RampRatePerMin:   0,
		UpdateIntervalMs: 100,
	}
	wtActive := NewWriteThrottler(cfgActive, 100)
	sm.SetThrottler(wtActive)
	sm.ReportStats(false)

	// Test stats manager with a throttler (inactive/Inf QPS)
	cfgHigh := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100000.0,
		RampRatePerMin:   0,
		UpdateIntervalMs: 100,
	}
	wtHigh := NewWriteThrottler(cfgHigh, 100)
	sm.SetThrottler(wtHigh)
	sm.ReportStats(false)

	// Wait a bit to ensure ticker fires (doesn't fail/panic)
	time.Sleep(150 * time.Millisecond)

	// Final report
	sm.ReportStats(true)
}
