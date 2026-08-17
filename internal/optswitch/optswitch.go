// Package optswitch is the registry of optimization kill switches (#287).
//
// Every optimization that can change the shape of query results — pruning,
// pushdown, plan rewrites, fast-path operator variants — registers a Toggle
// here, named and bound to its env kill switch. Production behavior is
// unchanged: a toggle is on unless its env var is "0", exactly the
// convention the ad-hoc `os.Getenv(...) != "0"` vars used.
//
// The registry exists so the optimization-invariance oracle
// (internal/oracle) can enumerate every switch and run each corpus query
// with optimizations individually disabled, asserting identical results.
// Adding a kill switch for new optimization work is part of its definition
// of done — registering it here extends the oracle for free.
package optswitch

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// Toggle is one registered optimization switch. On() is safe for
// concurrent readers and cheap enough for per-query (not per-row) checks.
type Toggle struct {
	Name string // short stable id, e.g. "scan-filter"
	Env  string // kill-switch env var, e.g. WADJET_SCAN_FILTER
	Desc string // one-line description of what turning it off disables
	on   atomic.Bool
}

// On reports whether the optimization is enabled.
func (t *Toggle) On() bool { return t.on.Load() }

// Set flips the toggle and returns the previous value. Test-only in
// spirit: production code never calls Set, it reads env at Register time.
func (t *Toggle) Set(v bool) (prev bool) { return t.on.Swap(v) }

var (
	mu       sync.Mutex
	registry = map[string]*Toggle{}
)

// Register creates, records, and returns a toggle. The initial value comes
// from the env var: enabled unless set to "0". Duplicate names panic —
// each optimization registers exactly once, in the package that owns it.
func Register(name, env, desc string) *Toggle {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("optswitch: duplicate toggle %q", name))
	}
	t := &Toggle{Name: name, Env: env, Desc: desc}
	t.on.Store(os.Getenv(env) != "0")
	registry[name] = t
	return t
}

// All returns every registered toggle, sorted by name. Only toggles whose
// defining packages are linked into the binary appear.
func All() []*Toggle {
	mu.Lock()
	defer mu.Unlock()
	out := make([]*Toggle, 0, len(registry))
	for _, t := range registry {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
