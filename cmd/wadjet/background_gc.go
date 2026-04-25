package main

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"
)

// startBackgroundGC runs a goroutine that periodically calls runtime.GC()
// to reclaim transient garbage that wadjet's gogc=off default would
// otherwise let accumulate to GoMemLimit. Cheap fix for the SF10 catalog-
// priming-induced 11.5 GB baseline heap (project_q18_sf10_native_dag_oom).
//
// Default interval: 30s. Override with WADJET_BG_GC_INTERVAL=<duration> or
// WADJET_BG_GC_INTERVAL=off to disable. Each runtime.GC() call is ~50ms
// in steady-state — negligible vs the gigabytes of transient garbage it
// reclaims at SF10+ scale.
func startBackgroundGC(ctx context.Context, logger *slog.Logger) {
	intervalStr := os.Getenv("WADJET_BG_GC_INTERVAL")
	if strings.EqualFold(strings.TrimSpace(intervalStr), "off") {
		logger.Info("background GC disabled (WADJET_BG_GC_INTERVAL=off)")
		return
	}
	interval := 30 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d >= time.Second {
			interval = d
		}
	}
	logger.Info("background GC enabled", "interval", interval)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runtime.GC()
			}
		}
	}()
}
