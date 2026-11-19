// SPDX-License-Identifier: MIT

package auth

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0050 A1: a coordinator statement decides every authorization question
// against ONE policy state and the bind verdict that belongs to it. These
// gates drive the provider's REAL install and reload paths — UpdateFromConfig,
// Update, UpdateWithEvaluator, BindToCatalog — not a model of their store
// order.

var pinTestConfig = Config{
	Enabled: true,
	APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
}

// pinCatalogWithout is a catalog that does NOT hold `Hits`, so binding the
// pb policies against it refuses (BindToCatalog's failure path).
func pinCatalogWithout(t *testing.T, ctx context.Context) *catalog.Catalog {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sch := parquet.Schema{Columns: []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}}
	if err := cat.CreateTable(ctx, "other", sch, nil); err != nil {
		t.Fatal(err)
	}
	return cat
}

// pinBoundProvider is a provider bound to cat and running a bindable set.
func pinBoundProvider(t *testing.T, ctx context.Context, cat *catalog.Catalog) *Provider {
	t.Helper()
	authn, authz := New(pinTestConfig)
	p := NewProvider(authn, authz, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "secret")))
	if err := p.BindToCatalog(ctx, cat); err != nil {
		t.Fatal(err)
	}
	return p
}

// pinRefuse installs an unbindable set through the real reload path, so the
// provider keeps its running set and refuses every statement (#802, #882).
func pinRefuse(t *testing.T, p *Provider) {
	t.Helper()
	if err := p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("nosuchrelation", "secret")...); err == nil {
		t.Fatal("an unbindable set was accepted")
	}
	if p.BindError() == nil {
		t.Fatal("a refused reload was not remembered")
	}
}

// pinWithHook runs Pin with write landing between its first state load and
// its verdict load, once — the window the plain two-load read fails open in.
func pinWithHook(p *Provider, write func()) *Provider {
	var once sync.Once
	pinReadTestHook = func() { once.Do(write) }
	defer func() { pinReadTestHook = nil }()
	return p.Pin()
}

// TestAPinIsAConsistentPairUnderTheRealReloadPaths is the deterministic half
// of the A1 gate. Each case lands a REAL writer inside the read.
func TestAPinIsAConsistentPairUnderTheRealReloadPaths(t *testing.T) {
	ctx := context.Background()
	authn, authz := New(pinTestConfig)

	// A refusing provider, and inside the read an install that CLEARS the
	// refusal. The plain read returns (the refused-against running set, no
	// refusal): a pair that held at no moment of the read and fails open.
	clearing := []struct {
		name  string
		write func(t *testing.T, p *Provider, good *catalog.Catalog)
	}{
		{"UpdateFromConfig", func(t *testing.T, p *Provider, _ *catalog.Catalog) {
			if err := p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("hits", "counterid")...); err != nil {
				t.Errorf("bindable reload refused: %v", err)
			}
		}},
		{"UpdateWithEvaluator", func(t *testing.T, p *Provider, _ *catalog.Catalog) {
			p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "counterid")))
		}},
		{"Update", func(t *testing.T, p *Provider, _ *catalog.Catalog) {
			ps, err := ParsePolicies([]PolicyConfig{{Table: "hits", Role: "analyst", RowFilter: "counterid = 1"}})
			if err != nil {
				t.Fatal(err)
			}
			p.Update(authn, authz, ps)
		}},
		{"BindToCatalog", func(t *testing.T, p *Provider, good *catalog.Catalog) {
			if err := p.BindToCatalog(ctx, good); err != nil {
				t.Errorf("bindable attach refused: %v", err)
			}
		}},
	}
	for _, c := range clearing {
		t.Run("a clearing install inside the read/"+c.name, func(t *testing.T) {
			good := pbCatalog(t, ctx)
			p := pinBoundProvider(t, ctx, good)
			if c.name == "BindToCatalog" {
				// The refusal BindToCatalog itself remembers: an attach to a
				// catalog the set does not bind against.
				if err := p.BindToCatalog(ctx, pinCatalogWithout(t, ctx)); err == nil {
					t.Fatal("attach to a catalog without the relation was accepted")
				}
			} else {
				pinRefuse(t, p)
			}
			refusedAgainst := p.state.Load()

			pin := pinWithHook(p, func() { c.write(t, p, good) })
			after := p.state.Load()
			if after == refusedAgainst {
				t.Fatalf("the clearing install did not install a new state")
			}
			if p.BindError() != nil {
				t.Fatalf("the clearing install left the provider refusing: %v", p.BindError())
			}
			got := pin.state.Load()
			if got == refusedAgainst && pin.BindError() == nil {
				t.Fatalf("FAIL-OPEN: the pin holds the set the provider was refusing beside, with " +
					"no refusal — a pair that held at no moment of the read")
			}
			if got != after || pin.BindError() != nil {
				t.Fatalf("the pin is not the provider's pair after the install: state is new=%v, "+
					"refusal=%v", got == after, pin.BindError())
			}
		})
	}

	// A refused install inside the read: the state does not move and the pin
	// takes the later moment's pair — the running set WITH the refusal.
	t.Run("a refused install inside the read fails closed", func(t *testing.T) {
		p := pinBoundProvider(t, ctx, pbCatalog(t, ctx))
		running := p.state.Load()
		pin := pinWithHook(p, func() { pinRefuse(t, p) })
		if pin.state.Load() != running {
			t.Fatal("the pin holds a state that was never installed")
		}
		if pin.BindError() == nil {
			t.Fatal("the pin missed a refusal published before its verdict load")
		}
	})
}

// TestAPinIsNeverUpdated: the copy is the state the statement was accepted
// with; a reload of the live provider, or a write aimed at the copy itself,
// does not change it (ADR-0050 A4).
func TestAPinIsNeverUpdated(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)
	p := pinBoundProvider(t, ctx, cat)
	pin := p.Pin()
	ev, st := pin.Evaluator(), pin.state.Load()
	if pin.Audit() != p.Audit() {
		t.Fatal("the pin does not share the live provider's audit logger")
	}

	// The live provider reloads, then refuses.
	if err := p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("hits", "counterid")...); err != nil {
		t.Fatal(err)
	}
	pinRefuse(t, p)
	if pin.Evaluator() != ev || pin.BindError() != nil {
		t.Fatal("a reload of the live provider reached the pinned copy")
	}

	// Writes aimed at the copy are refused and change nothing.
	authn, authz := New(pinTestConfig)
	pin.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "counterid")))
	pin.Update(authn, authz, nil)
	if err := pin.UpdateFromConfig(pinTestConfig, nil); err == nil {
		t.Error("UpdateFromConfig on a pinned copy was accepted")
	}
	if err := pin.BindToCatalog(ctx, pinCatalogWithout(t, ctx)); err == nil {
		t.Error("BindToCatalog on a pinned copy was accepted")
	}
	AttachProvider(ctx, pin, cat, nil)
	if pin.state.Load() != st || pin.BindError() != nil {
		t.Fatal("a write aimed at the pinned copy changed it")
	}
	if pin.Pin() != pin {
		t.Fatal("re-pinning a pinned copy made another copy")
	}
	var nilP *Provider
	if nilP.Pin() != nil {
		t.Fatal("Pin of a nil provider is not nil")
	}
}

// TestAPinIsNeverFailOpenUnderConcurrentReloads is the hammer: one writer
// cycles the real install paths through bindable and unbindable sets while
// readers pin. The writer is serialized so the provider's history is a
// sequence of (state, verdict) pairs with a sequence counter around each
// write; every pin that returned NO refusal must hold a pair that was the
// provider's steady pair at some point inside the pin's own window. A
// pin returning a refusal is never fail-open (transient refusals are
// reachable: an install stores the state before it clears the verdict).
func TestAPinIsNeverFailOpenUnderConcurrentReloads(t *testing.T) {
	ctx := context.Background()
	good := pbCatalog(t, ctx)
	bad := pinCatalogWithout(t, ctx)
	p := pinBoundProvider(t, ctx, good)
	authn, authz := New(pinTestConfig)

	type pair struct {
		st      *authState
		refused bool
	}
	ops := 3000
	if testing.Short() {
		ops = 600
	}
	pairs := make([]pair, ops+1)
	pairs[0] = pair{p.state.Load(), p.bindErr.Load() != nil}
	var seq atomic.Int64 // even: steady pair seq/2; odd: write (seq+1)/2 in progress
	writes := []func(){
		func() { _ = p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("hits", "secret")...) },
		func() { _ = p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("nosuchrelation", "secret")...) },
		func() { p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "counterid"))) },
		func() { p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("HITS", "secret"))) },
		func() { _ = p.BindToCatalog(ctx, bad) },
		func() { _ = p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("hits", "secret")...) }, // refused: bad attached
		func() { _ = p.BindToCatalog(ctx, good) },
		func() { p.Update(authn, authz, nil) },
	}

	type obs struct {
		s0, s1  int64
		st      *authState
		refused bool
	}
	done := make(chan struct{})
	var mu sync.Mutex
	var seen []obs
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []obs
			for {
				select {
				case <-done:
					mu.Lock()
					seen = append(seen, local...)
					mu.Unlock()
					return
				default:
				}
				s0 := seq.Load()
				pin := p.Pin()
				s1 := seq.Load()
				local = append(local, obs{s0, s1, pin.state.Load(), pin.BindError() != nil})
			}
		}()
	}
	for k := 1; k <= ops; k++ {
		seq.Add(1)
		writes[(k-1)%len(writes)]()
		pairs[k] = pair{p.state.Load(), p.bindErr.Load() != nil}
		seq.Add(1)
	}
	close(done)
	wg.Wait()

	var open, closedTransient, checked int
	for _, o := range seen {
		lo, hi := o.s0/2, (o.s1+1)/2
		if o.refused {
			// Fail closed whatever it is; count the ones no steady pair in
			// the window explains (the install's own store-order gap).
			explained := false
			for i := lo; i <= hi; i++ {
				if pairs[i].st == o.st && pairs[i].refused {
					explained = true
				}
			}
			if !explained {
				closedTransient++
			}
			continue
		}
		checked++
		ok := false
		for i := lo; i <= hi; i++ {
			if pairs[i].st == o.st && !pairs[i].refused {
				ok = true
				break
			}
		}
		if !ok {
			open++
			if open <= 3 {
				t.Errorf("FAIL-OPEN: a pin holds (a state, no refusal) that was not the provider's "+
					"pair at any moment of its read (window %d..%d)", lo, hi)
			}
		}
	}
	refusing := 0
	for _, pr := range pairs {
		if pr.refused {
			refusing++
		}
	}
	if refusing == 0 || refusing == len(pairs) {
		t.Fatalf("the writer did not alternate refusing and clean pairs (%d of %d refusing)", refusing, len(pairs))
	}
	if open > 0 {
		t.Fatalf("%d of %d unrefused pins failed open", open, checked)
	}
	t.Logf("%d pins, %d unrefused checked, 0 fail-open, %d transient fail-closed refusals; "+
		"%d of %d writer pairs refusing", len(seen), checked, closedTransient, refusing, len(pairs))
}

// TestPinRaceHammerWithConcurrentWriters runs every writer concurrently with
// pins (the CAS path in BindToCatalog included). Under -race it asserts the
// read is race-free and terminates; the ordering property is the serialized
// hammer's above.
func TestPinRaceHammerWithConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	good := pbCatalog(t, ctx)
	bad := pinCatalogWithout(t, ctx)
	p := pinBoundProvider(t, ctx, good)
	authn, authz := New(pinTestConfig)
	n := 300
	if testing.Short() {
		n = 60
	}
	var wg sync.WaitGroup
	writer := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				f()
			}
		}()
	}
	writer(func() { _ = p.UpdateFromConfig(pinTestConfig, nil, pbPolicies("hits", "secret")...) })
	writer(func() { p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("HITS", "secret"))) })
	writer(func() { _ = p.BindToCatalog(ctx, bad); _ = p.BindToCatalog(ctx, good) })
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20*n; i++ {
				pin := p.Pin()
				if pin.state.Load() == nil {
					t.Error("a pin holds no state")
					return
				}
				_ = pin.BindError()
				_ = pin.Evaluator()
			}
		}()
	}
	wg.Wait()
}

// TestWithDecisionTimeStampsOnce (ADR-0050 A2): the statement's time is the
// one every later decision reads, and a time already on the context wins.
func TestWithDecisionTimeStampsOnce(t *testing.T) {
	t0 := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	ctx := ContextWithEnvironment(context.Background(), Environment{SourceIP: "10.0.0.1:5432", Protocol: "pgwire"})
	ctx = WithDecisionTime(ctx, t0)
	time.Sleep(2 * time.Millisecond)
	env := DecisionEnvironment(ctx, "coordinator")
	if !env.Time.Equal(t0) || env.SourceIP != "10.0.0.1" || env.Protocol != "pgwire" {
		t.Fatalf("decision environment = %+v", env)
	}
	if got := DecisionEnvironment(WithDecisionTime(ctx, t0.Add(time.Hour)), "x").Time; !got.Equal(t0) {
		t.Fatalf("a second stamp replaced the statement's time: %v", got)
	}
	bare := DecisionEnvironment(WithDecisionTime(context.Background(), t0), "coordinator")
	if !bare.Time.Equal(t0) || bare.Protocol != "coordinator" || bare.SourceIP != "" {
		t.Fatalf("stamping a bare context changed more than the time: %+v", bare)
	}
}
