package config

import (
	"fmt"
	"log/slog"
	"strings"
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

// subscription is a subscriber together with the config-key prefixes it
// consumes. The prefixes are what makes a key HOT-RELOADABLE: a value the
// running process re-reads only because something subscribed to it. A key
// nobody subscribed to cannot be changed at runtime, and the admin API
// refuses the write rather than reporting a change that nothing applies
// (#828).
type subscription struct {
	prefixes []string
	fn       Subscriber
}

// Manager provides atomic access to configuration with change notification.
// Reads are lock-free via atomic.Pointer. Writes are serialized and notify subscribers.
type Manager struct {
	current atomic.Pointer[Config]
	res     atomic.Pointer[Resolution]
	mu      sync.Mutex // serializes writes
	subs    []subscription
	logger  *slog.Logger
}

// NewManager creates a ConfigManager with the given initial config.
func NewManager(initial *Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{logger: logger}
	m.current.Store(initial)
	m.res.Store(&Resolution{cfg: initial, sources: map[string]Source{}})
	return m
}

// NewManagerFromResolution creates a Manager over a resolved configuration,
// keeping the per-key source so GET /v1/admin/config can report where each
// effective value came from (#828).
func NewManagerFromResolution(res *Resolution, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{logger: logger}
	m.current.Store(res.Config())
	m.res.Store(res)
	return m
}

// Resolution returns the current configuration with its per-key sources.
func (m *Manager) Resolution() *Resolution { return m.res.Load() }

// HotReloadable reports whether a runtime change to key would reach a
// consumer — that is, whether any subscriber registered a prefix covering
// it. A deferred key (registry.Key.Deferred) is never hot-reloadable.
func (m *Manager) HotReloadable(key string) bool {
	k, ok := KeyByName(key)
	if !ok {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hotReloadableLocked(k)
}

// hotReloadableLocked is HotReloadable with m.mu already held.
func (m *Manager) hotReloadableLocked(k Key) bool {
	if k.Deferred {
		return false
	}
	for _, s := range m.subs {
		for _, p := range s.prefixes {
			if p == "" || k.Name == p || strings.HasPrefix(k.Name, p+".") {
				return true
			}
		}
	}
	return false
}

// ChangedKeys returns the registry keys whose values differ between two
// configurations, in registry order.
func ChangedKeys(old, new *Config) []string {
	var out []string
	for _, k := range keys {
		if !equalValues(k, k.Get(old), k.Get(new)) {
			out = append(out, k.Name)
		}
	}
	return out
}

// Current returns the current configuration. Lock-free.
func (m *Manager) Current() *Config {
	return m.current.Load()
}

// Subscribe registers a callback for configuration changes.
// Subscribers are called synchronously in order during Apply.
//
// A bare Subscribe claims NO config keys, so it does not make anything
// hot-reloadable. Use SubscribeKeys to declare which keys the callback
// actually applies.
func (m *Manager) Subscribe(fn Subscriber) {
	m.SubscribeKeys(nil, fn)
}

// SubscribeKeys registers a callback and declares the config-key prefixes it
// applies at runtime. Those prefixes are exactly the keys the admin API will
// accept a write for; everything else is refused as not hot-reloadable.
func (m *Manager) SubscribeKeys(prefixes []string, fn Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = append(m.subs, subscription{prefixes: prefixes, fn: fn})
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
	frozen.NATS.ClusterID = old.NATS.ClusterID
	frozen.NATS.LeafRemotes = old.NATS.LeafRemotes

	if err := validate(&frozen); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	m.current.Store(&frozen)
	m.res.Store(m.res.Load().withConfig(&frozen, SourceAdmin))

	event := ChangeEvent{Old: old, New: &frozen}
	for _, sub := range m.subs {
		sub.fn(event)
	}

	m.logger.Info("configuration updated")
	return nil
}

// Reload reads the config file and applies changes.
func (m *Manager) Reload(path string) error {
	_, err := m.ReloadWithReport(path)
	return err
}

// ReloadWithReport reads the config file, applies it, and returns the
// registry keys the FILE changed that no subscriber consumes — the ones
// Apply preserved.
//
// Apply preserving them is right: the running process is not going to
// re-read a startup-only key, and the manager must report the running
// configuration. But saying nothing about the part that was ignored is how
// an operator edits `worker.max_concurrent`, sees "reloaded", and believes
// it took effect. The PUT path answers 409 naming such a key; a file reload
// cannot refuse (the file legitimately carries startup-only keys for the
// NEXT start), so it reports instead.
func (m *Manager) ReloadWithReport(path string) ([]string, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, fmt.Errorf("reloading config: %w", err)
	}
	var ignored []string
	for _, name := range ChangedKeys(m.Current(), cfg) {
		if !m.HotReloadable(name) {
			ignored = append(ignored, name)
		}
	}
	if err := m.Apply(cfg); err != nil {
		return nil, err
	}
	if len(ignored) > 0 {
		m.logger.Warn("configuration reloaded, but these keys take effect only at startup and were NOT applied",
			"path", path, "keys", strings.Join(ignored, ", "))
	}
	return ignored, nil
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
