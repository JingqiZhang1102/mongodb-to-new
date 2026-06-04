package migration

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"golang.org/x/time/rate"
)

// WriteThrottler wraps golang.org/x/time/rate.Limiter to dynamically
// ramp up allowed database write QPS over time indefinitely.
type WriteThrottler struct {
	limiter  *rate.Limiter
	config   config.BackfillRampUpConfig
	start    time.Time
	disabled atomic.Bool
}

// NewWriteThrottler constructs a WriteThrottler.
// If the throttler is disabled (Enabled is false), it returns nil.
func NewWriteThrottler(cfg config.BackfillRampUpConfig, burst int) *WriteThrottler {
	if !cfg.Enabled {
		return nil
	}

	startQps := cfg.StartQps
	if startQps < 0 {
		startQps = 0
	}

	// Ensure burst is at least 1
	if burst < 1 {
		burst = 1
	}

	wt := &WriteThrottler{
		limiter: rate.NewLimiter(rate.Limit(startQps), burst),
		config:  cfg,
		start:   time.Now(),
	}

	if startQps >= 100000.0 {
		wt.disabled.Store(true)
	}

	return wt
}

// IsDisabled returns whether the throttler has been disabled.
func (wt *WriteThrottler) IsDisabled() bool {
	if wt == nil {
		return true
	}
	return wt.disabled.Load()
}

// Limit returns the current QPS limit.
func (wt *WriteThrottler) Limit() rate.Limit {
	if wt == nil || wt.IsDisabled() {
		return rate.Inf
	}
	return wt.limiter.Limit()
}

// StartRampUp runs a background goroutine that updates the rate limit at fixed intervals.
func (wt *WriteThrottler) StartRampUp(ctx context.Context) {
	if wt == nil || wt.IsDisabled() {
		return
	}

	updateInterval := time.Duration(wt.config.UpdateIntervalMs) * time.Millisecond
	if updateInterval <= 0 {
		updateInterval = time.Second
	}

	ticker := time.NewTicker(updateInterval)
	go func() {
		defer ticker.Stop()
		rampRatePerSec := wt.config.RampRatePerMin / 60.0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if wt.IsDisabled() {
					return
				}
				elapsedSecs := time.Since(wt.start).Seconds()
				currentQps := wt.config.StartQps + rampRatePerSec*elapsedSecs
				if currentQps >= 100000.0 {
					wt.disabled.Store(true)
					return
				}
				wt.limiter.SetLimit(rate.Limit(currentQps))
			}
		}
	}()
}

// Wait blocks until the required number of tokens are available.
// If the context is canceled or expired, it returns the context error.
func (wt *WriteThrottler) Wait(ctx context.Context, count int) error {
	if wt == nil || wt.IsDisabled() {
		return nil
	}
	if count <= 0 {
		return nil
	}
	return wt.limiter.WaitN(ctx, count)
}
