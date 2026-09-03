package expr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// fakeAccountant is a minimal, deterministic MemoryAccountant test double:
// it records every Reserve/Release call and can be told to refuse the next
// Reserve, so a test can pin exactly what InSubquery charges and returns
// without depending on internal/engine/memory's real budget arithmetic.
type fakeAccountant struct {
	used       int64
	reserved   []int64
	released   []int64
	refuseNext bool
}

var errFakeBudgetExceeded = errors.New("fake budget exceeded")

func (f *fakeAccountant) Reserve(n int64) error {
	if f.refuseNext {
		return fmt.Errorf("budget: %w", errFakeBudgetExceeded)
	}
	f.used += n
	f.reserved = append(f.reserved, n)
	return nil
}

func (f *fakeAccountant) Release(n int64) {
	f.used -= n
	f.released = append(f.released, n)
}

// TestInSubqueryChargesAndReleasesMemory is the core #528 regression: a
// large IN-list subquery's membership set is charged to Budget when it is
// built and returns exactly what it charged when Release is called.
func TestInSubqueryChargesAndReleasesMemory(t *testing.T) {
	b := testBatch()
	rows := make([]map[string]any, 0, 10_000)
	for i := int64(0); i < 10_000; i++ {
		rows = append(rows, map[string]any{"id": i})
	}
	acct := &fakeAccountant{}
	inSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM huge",
		Runner: mockRunner(rows, nil),
		Budget: acct,
	}

	// Resolving (via the first Eval) must charge a POSITIVE amount — the
	// whole point of #528 is that this used to be zero, always, no matter
	// how large the subquery's result was.
	if !inSq.EvalBool(b, 0) {
		t.Fatal("expected id=1 to be IN the subquery result")
	}
	if len(acct.reserved) != 1 {
		t.Fatalf("Reserve called %d times, want 1", len(acct.reserved))
	}
	charged := acct.reserved[0]
	if charged <= 0 {
		t.Fatalf("charged %d bytes for a 10,000-row membership set, want > 0", charged)
	}
	if acct.used != charged {
		t.Fatalf("accountant.used = %d, want %d (matching the one Reserve call)", acct.used, charged)
	}

	// A second probe (id=2, also in [0,10000)) must not charge again — the
	// set is built once (resolveSlow is idempotent) and so is its charge.
	if !inSq.EvalBool(b, 1) {
		t.Fatal("expected id=2 to be IN the subquery result")
	}
	if len(acct.reserved) != 1 {
		t.Fatalf("Reserve called %d times after a second probe, want still 1", len(acct.reserved))
	}

	// Release must return exactly what was charged, exactly once.
	inSq.Release()
	if len(acct.released) != 1 || acct.released[0] != charged {
		t.Fatalf("Release calls = %v, want exactly one call releasing %d", acct.released, charged)
	}
	if acct.used != 0 {
		t.Fatalf("accountant.used = %d after Release, want 0", acct.used)
	}

	// Release is idempotent: a second call must not double-release.
	inSq.Release()
	if len(acct.released) != 1 {
		t.Fatalf("Release calls = %d after a second Release, want still 1 (idempotent)", len(acct.released))
	}
}

// TestInSubqueryRefusesPastTheBudget pins the "never OOM" half of #451/
// ADR-0006: when the membership set does not fit the caller's budget,
// InSubquery must refuse the query (a panic the pipeline's FatalEvalPanic
// boundary converts to a query error) rather than silently building the
// unbounded map anyway.
func TestInSubqueryRefusesPastTheBudget(t *testing.T) {
	b := testBatch()
	acct := &fakeAccountant{refuseNext: true}
	inSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM huge",
		Runner: mockRunner([]map[string]any{{"id": int64(1)}}, nil),
		Budget: acct,
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("want a panic when Reserve refuses the membership set")
		}
		fe, ok := r.(fatalEval)
		if !ok {
			t.Fatalf("panic value = %T (%v), want fatalEval", r, r)
		}
		err := fe.FatalEvalError()
		if !errors.Is(err, errFakeBudgetExceeded) {
			t.Errorf("FatalEvalError() = %v, want it to wrap errFakeBudgetExceeded", err)
		}
	}()
	inSq.EvalBool(b, 0)
	t.Fatal("unreachable: EvalBool should have panicked")
}

// TestInSubqueryUnbudgetedByDefault still pins the nil case; what it no longer
// describes is production. Since #531 the planner passes WithBudget on every
// compile site that carries a subquery runner, so a query's membership set IS
// charged. A nil Budget remains the shape for every compile that does not ask
// for one — the worker, coordinator and DML sites compile with no runner at
// all, so they refuse an uncorrelated IN rather than build a set — and it must
// behave exactly as it did before #528: no panic, no charge, ordinary IN
// semantics, and a Release that is a harmless no-op.
func TestInSubqueryUnbudgetedByDefault(t *testing.T) {
	b := testBatch()
	inSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM active_users",
		Runner: mockRunner([]map[string]any{{"id": int64(1)}, {"id": int64(3)}}, nil),
	}
	if !inSq.EvalBool(b, 0) {
		t.Fatal("expected id=1 to be IN the subquery result")
	}
	if inSq.EvalBool(b, 1) {
		t.Fatal("expected id=2 to NOT be IN the subquery result")
	}
	// Release on a nil-Budget InSubquery must be a harmless no-op.
	inSq.Release()
}

// TestCompileWithBudgetChargesTheCompiledInSubquery is the compile-time wiring
// half of #528: a budget must reach the InSubquery a compile constructs, and a
// compile that does not ask for one must keep leaving it nil. Production wires
// it through the WithBudget OPTION (#531) rather than through this entry point,
// because an entry point that takes a budget and nothing else silently drops
// the options its call site was already passing — WithSubqueryDeclTypes among
// them, which is #696.
func TestCompileWithBudgetChargesTheCompiledInSubquery(t *testing.T) {
	rows := make([]map[string]any, 0, 5_000)
	for i := int64(0); i < 5_000; i++ {
		rows = append(rows, map[string]any{"id": i})
	}
	runner := mockRunner(rows, nil)
	parsed, err := plansql.Parse("SELECT 1 FROM t WHERE id IN (SELECT id FROM huge)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.WhereExpr == nil {
		t.Fatal("no WHERE clause")
	}

	acct := &fakeAccountant{}
	compiled, err := CompileWithBudget(info.WhereExpr, runner, nil, nil, nil, acct, func(*InSubquery) {})
	if err != nil {
		t.Fatalf("CompileWithBudget: %v", err)
	}
	inSq, ok := compiled.(*InSubquery)
	if !ok {
		t.Fatalf("compiled to %T, want *InSubquery", compiled)
	}
	if inSq.Budget != MemoryAccountant(acct) {
		t.Fatal("CompileWithBudget did not thread its budget onto the compiled InSubquery")
	}

	b := testBatch()
	inSq.EvalBool(b, 0)
	if len(acct.reserved) != 1 || acct.reserved[0] <= 0 {
		t.Fatalf("Reserve calls = %v, want exactly one positive charge", acct.reserved)
	}

	// The option is the shape production uses, and it must reach the same
	// node — through an entry point that keeps its other options.
	tracked := 0
	compiled3, err := CompileWithRunner(info.WhereExpr, runner,
		WithBudget(acct, func(*InSubquery) { tracked++ }),
		WithSubqueryDeclTypes(func(string) (batch.TypeID, int, int, bool) { return 0, 0, 0, false }))
	if err != nil {
		t.Fatalf("CompileWithRunner+WithBudget: %v", err)
	}
	if in3, ok := compiled3.(*InSubquery); !ok || in3.Budget != MemoryAccountant(acct) {
		t.Fatalf("WithBudget did not reach the compiled InSubquery (%T)", compiled3)
	}
	if tracked != 1 {
		t.Fatalf("WithBudget handed the caller %d nodes to release, want 1 — a charge whose "+
			"teardown point the caller never sees is the #531 failure", tracked)
	}

	// A budget with no release hook is REFUSED at construction: charging
	// without a teardown turns an unaccounted map into a permanently charged
	// one, which is worse than the bug.
	if WithBudget(acct, nil) != nil {
		t.Fatal("WithBudget accepted a nil release function")
	}

	// CompileWithRunner without the option must still leave Budget nil.
	compiled2, err := CompileWithRunner(info.WhereExpr, runner)
	if err != nil {
		t.Fatalf("CompileWithRunner: %v", err)
	}
	inSq2, ok := compiled2.(*InSubquery)
	if !ok {
		t.Fatalf("compiled to %T, want *InSubquery", compiled2)
	}
	if inSq2.Budget != nil {
		t.Fatal("CompileWithRunner must not attach a budget")
	}
}
