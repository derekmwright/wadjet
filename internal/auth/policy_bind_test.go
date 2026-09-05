package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// pbCatalog holds one relation registered under a MIXED-CASE name with
// mixed-case columns — what a parquet dataset or an Iceberg import brings.
// Every other policy fixture in this package names a lower-case relation with
// lower-case columns, where the operator's spelling, the statement's folded
// spelling and the catalog's spelling are all the SAME STRING, so none of them
// can tell a bound policy from an unbound one.
func pbCatalog(t *testing.T, ctx context.Context) *catalog.Catalog {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "Secret", Type: parquet.TypeString},
		{Name: "counterid", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, "Hits", sch, nil); err != nil {
		t.Fatal(err)
	}
	return cat
}

func pbPolicies(resource, target string) []AccessControlPolicy {
	return []AccessControlPolicy{{
		Name: "pb", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "pb-scoped", EffectStr: "allow", Priority: 10,
			Resources:   []Condition{{Attribute: "resource.name", Op: "eq", Value: resource}},
			Actions:     []Action{ActionRead},
			Obligations: []Obligation{{Type: "mask_column", Target: target, Value: "'***'"}},
		}},
	}}
}

// TestPolicyNamesBindToTheCatalogAtLoad is the gate for the second half of
// #882's position: the names a policy uses are resolved ONCE, at load, against
// the catalog, and one that does not resolve REFUSES.
//
// The first half — that a spelling mismatch can never yield "no policy
// applies" — is gated in internal/server. This half is what makes a TYPO loud:
// without it, `resource.name eq "hitz"` is indistinguishable from a relation
// that does not exist yet, and the rule carrying the obligations silently
// never matches, which beside a broad allow is a grant.
func TestPolicyNamesBindToTheCatalogAtLoad(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)

	t.Run("a folded relation and column bind to the catalog's spelling", func(t *testing.T) {
		// This is the spelling an operator copies out of their own query,
		// because an unquoted reference IS the folded one.
		pols := pbPolicies("hits", "secret")
		bound, _, err := BindPoliciesToCatalog(ctx, cat, pols, nil)
		if err != nil {
			t.Fatalf("a folded spelling of an existing relation was refused: %v", err)
		}
		// The bind returns a COPY: the caller's set is untouched, and the
		// rewrite is visible only on what it hands back (P1(r2)).
		if got := pols[0].Rules[0].Resources[0].Value; got != "hits" {
			t.Errorf("the bind rewrote the caller's set in place: %v", got)
		}
		if got := bound[0].Rules[0].Resources[0].Value; got != "Hits" {
			t.Errorf("relation bound to %q, want the catalog's spelling %q", got, "Hits")
		}
		if got := bound[0].Rules[0].Obligations[0].Target; got != "Secret" {
			t.Errorf("column bound to %q, want the schema's spelling %q", got, "Secret")
		}
	})

	t.Run("the catalog's own spelling binds unchanged", func(t *testing.T) {
		pols := pbPolicies("Hits", "Secret")
		if _, _, err := BindPoliciesToCatalog(ctx, cat, pols, nil); err != nil {
			t.Fatalf("the catalog's own spelling was refused: %v", err)
		}
	})

	t.Run("a relation the catalog does not hold refuses the load", func(t *testing.T) {
		_, _, err := BindPoliciesToCatalog(ctx, cat, pbPolicies("hitz", "Secret"), nil)
		if err == nil {
			t.Fatal("a policy naming a relation that does not exist LOADED — its scoped " +
				"rule would never match, and beside a broad allow that is a grant")
		}
		if !strings.Contains(err.Error(), "hitz") {
			t.Errorf("the refusal does not name the relation: %v", err)
		}
	})

	t.Run("a column the relation does not have refuses the load", func(t *testing.T) {
		_, _, err := BindPoliciesToCatalog(ctx, cat, pbPolicies("Hits", "sekret"), nil)
		if err == nil {
			t.Fatal("a policy masking a column that does not exist LOADED — nothing would be masked")
		}
		if !strings.Contains(err.Error(), "sekret") {
			t.Errorf("the refusal does not name the column: %v", err)
		}
	})

	t.Run("a delimited wrong-case column refuses, as it does in a query", func(t *testing.T) {
		// `SECRET` carries upper case, so it can only have been written
		// delimited, and a delimited name is byte-exact everywhere else in the
		// engine. A policy is not more permissive than the queries it polices.
		if _, _, err := BindPoliciesToCatalog(ctx, cat, pbPolicies("Hits", "SECRET"), nil); err == nil {
			t.Fatal("a delimited wrong-case column resolved; a delimited name is byte-exact")
		}
	})

	t.Run("a wildcard tables list is not a relation", func(t *testing.T) {
		// MigrateRBACToABAC emits `tables: ["*"]` for a role with no table
		// restriction. Resolving that as a relation would refuse every such
		// config at startup.
		pols := []AccessControlPolicy{{
			Name: "pb", Version: 1, Enabled: true,
			Rules: []PolicyRule{{
				ID: "pb-wild", EffectStr: "allow", Priority: 10,
				Resources: []Condition{{Attribute: "resource.name", Op: "in", Value: []string{"*"}}},
				Actions:   []Action{ActionRead},
			}},
		}}
		if _, _, err := BindPoliciesToCatalog(ctx, cat, pols, nil); err != nil {
			t.Fatalf("a wildcard tables list was refused: %v", err)
		}
	})

	t.Run("a legacy policy binds its table and columns too", func(t *testing.T) {
		ps, err := ParsePolicies([]PolicyConfig{{
			Table: "hits", Role: "analyst",
			Columns: map[string]string{"secret": "mask"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		_, boundPS, err := BindPoliciesToCatalog(ctx, cat, nil, ps)
		if err != nil {
			t.Fatalf("a folded legacy policy was refused: %v", err)
		}
		p := boundPS.Lookup("Hits", "analyst")
		if p == nil {
			t.Fatal("the bound legacy policy is not reachable under the catalog's spelling")
		}
		if p.Table != "Hits" {
			t.Errorf("legacy table bound to %q, want %q", p.Table, "Hits")
		}
		if _, ok := p.Columns["Secret"]; !ok {
			t.Errorf("legacy column did not bind to the schema's spelling: %v", p.Columns)
		}
	})

	t.Run("a legacy policy naming no relation refuses", func(t *testing.T) {
		ps, err := ParsePolicies([]PolicyConfig{{Table: "nosuch", Role: "analyst", RowFilter: "1=1"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := BindPoliciesToCatalog(ctx, cat, nil, ps); err == nil {
			t.Fatal("a legacy policy naming a relation that does not exist LOADED")
		}
	})
}

// TestPolicyBindFailureKeepsThePreviousSet is #802's contract applied to
// names: a reload that cannot be bound installs NOTHING, so the set already
// running keeps running rather than being replaced by one whose scoped rules
// silently never match.
func TestPolicyBindFailureKeepsThePreviousSet(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)

	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	})
	p := NewProvider(authn, authz, nil, nil)
	good := pbPolicies("hits", "secret")
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(good))
	if err := p.BindToCatalog(ctx, cat); err != nil {
		t.Fatalf("binding the initial set: %v", err)
	}
	before := p.Evaluator()
	if before == nil {
		t.Fatal("no evaluator after the initial bind")
	}

	// A reload naming a relation that does not exist.
	err := p.UpdateFromConfig(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	}, nil, pbPolicies("nosuchrelation", "Secret")...)
	if err == nil {
		t.Fatal("a hot reload naming a relation that does not exist was ACCEPTED")
	}
	if p.Evaluator() != before {
		t.Fatal("the refused reload replaced the running policy set — #802's contract is " +
			"that a policy that cannot be read swaps nothing")
	}
}

// TestBindToCatalogDoesNotMutateTheRunningSet is P1(r2)'s gate: the bind
// rewrites a COPY and swaps it in, so a decision in flight never reads a
// half-rewritten rule.
//
// It used to rewrite `cond.Value` inside the rules the LIVE evaluator was
// reading, which the race detector reports as four races between
// `bindRuleResources` and `PolicyEvaluator.ruleMatches`. The two shipped
// `serve` modes bound before their listeners started, so the shipped path was
// safe — but `BindToCatalog` is exported, it is the only API an embedded
// caller has for ADR-0033 rule 3, and wiring the bind into every ATTACH (which
// is what #882 round 2 required) puts it exactly where the race is live.
//
// Run this package with -race; without it the test still asserts the other
// half, which is that a FAILED bind leaves the running set untouched rather
// than partially rewritten.
func TestBindToCatalogDoesNotMutateTheRunningSet(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)

	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pbPolicies("hits", "secret")))

	subj := Subject{Attributes: Attributes{"role": "analyst"}}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// A decision in flight, reading the very rules the bind rewrites.
			if ev := p.Evaluator(); ev != nil {
				ev.EvaluateTableAccess(subj, "Hits", ActionRead, Environment{})
			}
		}
	}()
	for i := 0; i < 50; i++ {
		if err := p.BindToCatalog(ctx, cat); err != nil {
			close(stop)
			<-done
			t.Fatalf("bind %d: %v", i, err)
		}
	}
	close(stop)
	<-done

	// The bound set is the one now running, and it carries the catalog's
	// spelling — so the swap happened rather than the copy being discarded.
	td := p.Evaluator().EvaluateTableAccess(subj, "Hits", ActionRead, Environment{})
	if len(td.Columns) != 1 || td.Columns[0].Column != "Secret" {
		t.Fatalf("the bound set is not the running one: %+v", td.Columns)
	}
}

// TestAFailedBindLeavesTheRunningSetIntact is the other half of P1(r2): a bind
// that fails partway used to return mid-loop having already rewritten the
// rules it had reached, so the running set was left half-bound. #802's
// contract — a policy set that cannot be installed installs nothing — has to
// hold for a bind exactly as it holds for a reload.
func TestAFailedBindLeavesTheRunningSetIntact(t *testing.T) {
	ctx := context.Background()
	cat := pbCatalog(t, ctx)

	// Two rules: the first resolves, the second does not. A bind that mutated
	// in place would leave the first rewritten.
	pols := []AccessControlPolicy{{
		Name: "pb", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{
				ID: "good", EffectStr: "allow", Priority: 10,
				Resources:   []Condition{{Attribute: "resource.name", Op: "eq", Value: "hits"}},
				Actions:     []Action{ActionRead},
				Obligations: []Obligation{{Type: "mask_column", Target: "secret", Value: "'***'"}},
			},
			{
				ID: "bad", EffectStr: "allow", Priority: 10,
				Resources: []Condition{{Attribute: "resource.name", Op: "eq", Value: "nosuchrelation"}},
				Actions:   []Action{ActionRead},
			},
		},
	}}
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(pols))

	if err := p.BindToCatalog(ctx, cat); err == nil {
		t.Fatal("a set naming a relation that does not exist was bound")
	}
	if pols[0].Rules[0].Resources[0].Value != "hits" {
		t.Errorf("the failed bind rewrote the caller's rule in place: %v",
			pols[0].Rules[0].Resources[0].Value)
	}
	if pols[0].Rules[0].Obligations[0].Target != "secret" {
		t.Errorf("the failed bind rewrote the caller's obligation in place: %v",
			pols[0].Rules[0].Obligations[0].Target)
	}
	if p.BindError() == nil {
		t.Error("a failed bind is not remembered, so enforcement would run on the unbound set")
	}
}
