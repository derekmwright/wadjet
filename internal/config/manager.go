package config

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ChangeEvent describes what changed in a configuration update.
type ChangeEvent struct {
	Old *Config
	New *Config
}

// Subscriber is called when configuration changes.
type Subscriber func(event ChangeEvent)

// Manager provides atomic access to configuration with change notification.
// Reads are lock-free via atomic.Pointer. Writes are serialized and notify subscribers.
type Manager struct {
	current atomic.Pointer[Config]
	mu      sync.Mutex // serializes writes
	subs    []Subscriber
	logger  *slog.Logger
}

// NewManager creates a ConfigManager with the given initial config.
func NewManager(initial *Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{logger: logger}
	m.current.Store(initial)
	return m
}

// Current returns the current configuration. Lock-free.
func (m *Manager) Current() *Config {
	return m.current.Load()
}

// Subscribe registers a callback for configuration changes.
// Subscribers are called synchronously in order during Apply.
func (m *Manager) Subscribe(fn Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = append(m.subs, fn)
}

// Apply atomically replaces the configuration and notifies subscribers.
// Fields that should not be hot-reloaded (Mode, HTTP.Addr, NATS.Port) are
// preserved from the current config.
func (m *Manager) Apply(newCfg *Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.current.Load()

	// Preserve non-hot-reloadable fields from current config
	frozen := *newCfg
	frozen.Mode = old.Mode
	frozen.HTTP.Addr = old.HTTP.Addr
	frozen.NATS.Port = old.NATS.Port
	frozen.NATS.URL = old.NATS.URL
	frozen.NATS.StoreDir = old.NATS.StoreDir

	if err := validate(&frozen); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	m.current.Store(&frozen)

	event := ChangeEvent{Old: old, New: &frozen}
	for _, sub := range m.subs {
		sub(event)
	}

	m.logger.Info("configuration updated")
	return nil
}

// Reload reads the config file and applies changes.
func (m *Manager) Reload(path string) error {
	cfg, err := Load(path)
	if err != nil {
		return fmt.Errorf("reloading config: %w", err)
	}
	return m.Apply(cfg)
}

// validate checks that a config is minimally valid.
func validate(cfg *Config) error {
	switch cfg.Mode {
	case "standalone", "coordinator", "worker":
	default:
		return fmt.Errorf("invalid mode: %q", cfg.Mode)
	}
	if cfg.Worker.MaxConcurrent < 1 {
		return fmt.Errorf("worker.max_concurrent must be >= 1, got %d", cfg.Worker.MaxConcurrent)
	}
	if cfg.Worker.CacheBytes < 0 {
		return fmt.Errorf("worker.cache_bytes must be >= 0")
	}
	return nil
}
