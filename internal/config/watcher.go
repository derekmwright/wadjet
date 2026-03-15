package config

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// Watcher polls a config file for changes and triggers reload.
// Uses file modtime + size instead of fsnotify to avoid external dependencies.
type Watcher struct {
	path     string
	manager  *Manager
	interval time.Duration
	debounce time.Duration
	logger   *slog.Logger

	lastMod  time.Time
	lastSize int64
}

// WatcherConfig configures the file watcher.
type WatcherConfig struct {
	Path     string        // config file path
	Interval time.Duration // poll interval (default 2s)
	Debounce time.Duration // debounce window after change detected (default 500ms)
}

// NewWatcher creates a file watcher for configuration hot-reload.
func NewWatcher(cfg WatcherConfig, manager *Manager, logger *slog.Logger) *Watcher {
	if cfg.Interval == 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.Debounce == 0 {
		cfg.Debounce = 500 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}

	w := &Watcher{
		path:     cfg.Path,
		manager:  manager,
		interval: cfg.Interval,
		debounce: cfg.Debounce,
		logger:   logger,
	}

	// Capture initial file state
	if info, err := os.Stat(cfg.Path); err == nil {
		w.lastMod = info.ModTime()
		w.lastSize = info.Size()
	}

	return w
}

// Watch starts polling in a goroutine and stops when ctx is cancelled.
func (w *Watcher) Watch(ctx context.Context) {
	w.logger.Info("config watcher started", "path", w.path, "interval", w.interval)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("config watcher stopped")
			return
		case <-ticker.C:
			w.check()
		}
	}
}

func (w *Watcher) check() {
	info, err := os.Stat(w.path)
	if err != nil {
		// File might be temporarily gone during an editor save — skip
		return
	}

	mod := info.ModTime()
	size := info.Size()

	if mod.Equal(w.lastMod) && size == w.lastSize {
		return
	}

	// File changed — debounce (editors often write in multiple steps)
	w.logger.Debug("config file change detected, debouncing", "path", w.path)
	time.Sleep(w.debounce)

	// Re-stat after debounce to get final state
	info, err = os.Stat(w.path)
	if err != nil {
		return
	}

	w.lastMod = info.ModTime()
	w.lastSize = info.Size()

	w.logger.Info("reloading configuration", "path", w.path)
	if err := w.manager.Reload(w.path); err != nil {
		w.logger.Error("config reload failed", "error", err)
	}
}
