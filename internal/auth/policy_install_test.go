package auth

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The installer, and the two properties round 3 found missing from it.
//
// ADR-0033 rule 2 says every entry that installs a policy set against a
// catalog BINDS it. `Update` and `UpdateWithEvaluator` stored a new set and
// touched neither the binding nor `bindErr`, so on an already-bound provider
// the new set was enforced UNBOUND with `BindError()` still nil — the #882
// disclosure, reached through a setter instead of through an attach. Nothing
// shipped called them that way, which is exactly the problem: the rule held by
// caller discipline rather than by construction.
//
// And `BindToCatalog` read the running set, bound a copy, and STORED the copy,
// so a set installed inside that window was overwritten by the older snapshot
// — a retired policy set coming back.

func pbProviderWith(t *testing.T, resource, target string) *Provider {
	t.Helper()
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies(resource, target)))
	return p
}

// pbMaskTarget is the column the running set masks, or "" if no rule matched.
func pbMaskTarget(p *Provider) string {
	ev := p.Evaluator()
	if ev == nil {
		return ""
	}
	td := ev.EvaluateTableAccess(Subject{Attributes: Attributes{"role": "analyst"}},
		"Hits", ActionRead, Environment{})
	if len(td.Columns) == 0 {
		return ""
	}
	return td.Columns[0].Column
}

// TestASetInstalledOnABoundProviderIsBoundToo is B2(r3)'s gate.
func TestASetInstalledOnABoundProviderIsBoundToo(t *testing.T) {
	ctx := context.Background()

	t.Run("a bindable set installed through the setter binds", func(t *testing.T) {
		cat := pbCatalog(t, ctx)
		p := pbProviderWith(t, "hits", "secret")
		if err := p.BindToCatalog(ctx, cat); err != nil {
			t.Fatal(err)
		}
		authn, authz := New(Config{Enabled: true, APIKeys: []APIKeyDef{{Key: "k", Name: "a", Role: "analyst"}}})
		// A new set, named the FOLDED way: only a bind rewrites it to the
		// catalog's `Secret`, so the target names the binding.
		p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "secret")))
		if err := p.BindError(); err != nil {
			t.Fatalf("a bindable set installed through the setter was remembered as unbound: %v", err)
		}
		if got := pbMaskTarget(p); got != "Secret" {
			t.Fatalf("the installed set was not bound to the catalog: mask target = %q, want %q",
				got, "Secret")
		}
	})

	t.Run("an unbindable set installed through the setter does not enforce", func(t *testing.T) {
		cat := pbCatalog(t, ctx)
		p := pbProviderWith(t, "hits", "secret")
		if err := p.BindToCatalog(ctx, cat); err != nil {
			t.Fatal(err)
		}
		before := pbMaskTarget(p)
		authn, authz := New(Config{Enabled: true, APIKeys: []APIKeyDef{{Key: "k", Name: "a", Role: "analyst"}}})
		// `HITS` is neither the catalog's spelling nor the folded one, so the
		// fold-aware floor cannot reach it and the bind must refuse.
		p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("HITS", "secret")))
		if p.BindError() == nil {
			t.Fatal("a set naming no relation was installed through the setter and NOT remembered " +
				"as unbound — every statement would run beside a policy that matches nothing")
		}
		if got := pbMaskTarget(p); got != before {
			t.Fatalf("the previous set did not keep running: mask target = %q, want %q", got, before)
		}
	})

	t.Run("the legacy setter behaves the same", func(t *testing.T) {
		cat := pbCatalog(t, ctx)
		p := pbProviderWith(t, "hits", "secret")
		if err := p.BindToCatalog(ctx, cat); err != nil {
			t.Fatal(err)
		}
		authn, authz := New(Config{Enabled: true, APIKeys: []APIKeyDef{{Key: "k", Name: "a", Role: "analyst"}}})
		ps, err := ParsePolicies([]PolicyConfig{{Table: "nosuchrelation", Role: "analyst", RowFilter: "x = 1"}})
		if err != nil {
			t.Fatal(err)
		}
		p.Update(authn, authz, ps)
		if p.BindError() == nil {
			t.Fatal("a legacy set naming no relation was installed and not remembered as unbound")
		}
	})

	t.Run("with no catalog attached the set installs and the floor applies", func(t *testing.T) {
		// ADR-0033 rule 4: where nothing was attached, the fold-aware
		// comparison is all there is and it is not a refusal. An embedded
		// caller that builds an evaluator and never attaches a catalog must
		// keep answering.
		p := pbProviderWith(t, "hits", "Secret")
		if err := p.BindError(); err != nil {
			t.Fatalf("installing a set with no catalog attached refused: %v", err)
		}
		if got := pbMaskTarget(p); got != "Secret" {
			t.Fatalf("the set did not install: mask target = %q", got)
		}
	})
}

// TestAReattachOfABoundSetWritesNothing is half of P1(r3): the HTTP DML door
// re-attaches its `wadjet.Attach`ed DB on EVERY statement, so a bind that
// rewrites state is a rewrite per request — and the read-modify-write it used
// to do is the lost update the other half is about.
func TestAReattachOfABoundSetWritesNothing(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)
	p := pbProviderWith(t, "hits", "secret")
	if err := p.BindToCatalog(ctx, cat); err != nil {
		t.Fatal(err)
	}
	first := p.Evaluator()
	for i := 0; i < 200; i++ {
		if err := p.BindToCatalog(ctx, cat); err != nil {
			t.Fatalf("re-attach %d: %v", i, err)
		}
		if p.Evaluator() != first {
			t.Fatalf("re-attach %d rewrote the running set: a bound set must be idempotent to "+
				"re-attach, because the DML door re-attaches per statement", i)
		}
	}
}

// TestARebindNeverResurrectsARetiredPolicySet is the other half of P1(r3).
//
// The schedule is the one the round-3 review measured: a bind holding the
// snapshot it read while a NEW set is installed. At natural timing the window
// is microseconds and the review saw 0 of 200; with the window widened to 2 ms
// it saw 200 of 200 come back with the superseded set. A defect whose trigger
// is a SCHEDULE cannot be gated by hoping the scheduler cooperates, so the
// window is widened here on purpose (the knob pattern of
// exec.ForceAggDrainEvery).
func TestARebindNeverResurrectsARetiredPolicySet(t *testing.T) {
	ctx := context.Background()
	const rounds = 200

	bindSwapTestHook = func() { time.Sleep(2 * time.Millisecond) }
	t.Cleanup(func() { bindSwapTestHook = nil })

	lost := 0
	for i := 0; i < rounds; i++ {
		cat := pbCatalog(t, ctx)
		// The provider is not yet attached, so the first bind is the one
		// holding a snapshot across the window.
		p := pbProviderWith(t, "hits", "counterid") // the RETIRED set
		authn, authz := New(Config{Enabled: true, APIKeys: []APIKeyDef{{Key: "k", Name: "a", Role: "analyst"}}})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.BindToCatalog(ctx, cat)
		}()
		time.Sleep(200 * time.Microsecond) // land inside the widened window
		// The reload: the operator retires the old set and installs this one.
		p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "secret")))
		wg.Wait()

		if got := pbMaskTarget(p); got != "Secret" {
			lost++
			t.Errorf("round %d: the re-bind RESURRECTED the superseded set: mask target = %q, "+
				"want %q", i, got, "Secret")
			if lost > 3 {
				t.Fatalf("lost updates: %d of the first %d rounds", lost, i+1)
			}
		}
	}
}
