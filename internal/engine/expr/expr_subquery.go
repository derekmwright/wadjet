// This file holds expr subquery; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// --- Subquery expressions ---

// SubqueryRunner executes a SQL subquery and returns its result rows.
// Each row is a map of column name to value.
type SubqueryRunner func(sql string) ([]map[string]any, error)

// ScalarSubquery evaluates a subquery that returns a single scalar value.
// Example: WHERE price > (SELECT AVG(price) FROM products)
// Uncorrelated: executed once and result cached.
//
// The cache is shared by every parallel pipeline worker — one compiled
// expression tree is captured by all of them (Pipeline.runParallel) — so it
// is published the same way ColRef publishes its resolution: written under
// resolveMu, released by an atomic store, and never read before that store
// is observed. A plain `if !cached { cached = true; ... }` raced: a worker
// that saw the flag before the value was written compared against a nil
// threshold, dropped every row of its batches, and the query answered a
// different row count on every run (#398).
type ScalarSubquery struct {
	SQL    string
	Runner SubqueryRunner
	// Cols is the subquery's SELECT-list COLUMN COUNT, resolved from its own
	// plan at compile time — see refuseMultiColumnSubqueryByPlan.
	Cols SubqueryColumnsFunc
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// Decl is the DECLARED type of the subquery's single output column, and
	// DeclKnown says whether anything resolved it (#696). It carries no value
	// and changes no evaluation: it exists so the boxed comparison can read
	// this operand as the number it IS.
	//
	// A DECIMAL boxes as its rendered TEXT, so without a declaration the pair
	// `a > (SELECT AVG(a) FROM decpair)` had a proven DECIMAL on one side and
	// an unclassifiable box on the other, fell through to compare()'s
	// LEXICOGRAPHIC rule, and answered 0 rows for PostgreSQL's 4 because
	// "12.75" sorts below "7.570000". DecPrecision/DecScale go with a DECIMAL
	// Decl for the same reason every other declaration carries them.
	Decl                   batch.TypeID
	DeclKnown              bool
	DecPrecision, DecScale int
	// resolved publishes val: stored last under resolveMu, and the only
	// thing an evaluating goroutine reads before using val.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	val       any
}

func (e *ScalarSubquery) Eval(_ *batch.RecordBatch, _ int) any {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	return e.val
}

func (e *ScalarSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	// The same guard ExistsSubquery.resolveSlow carries, for the same reason:
	// a scalar subquery this evaluator runs ONCE has to be one that really
	// reads no outer row. `WHERE (SELECT COUNT(*) FROM dim WHERE dim.k =
	// u.did) > 0` over a CTE was planned here and answered a query-wide
	// constant 0 on all four arms (#535).
	refuseDanglingSubquery("scalar", e.SQL, e.Scope)
	// BEFORE THE RUN: PostgreSQL decides the column count during parse
	// analysis, so an EMPTY multi-column subquery is 42601 there too.
	refuseMultiColumnSubqueryByPlan(e.Cols, e.SQL, false)
	// TWO ROWS, not the whole result: `> 1` is the entire cardinality rule,
	// so the read stops where the answer is known (plansql.AppendRowLimit).
	// e.SQL — not the bounded text — is what every error below names, because
	// the bound is this engine's business and the query is the user's.
	rows, err := e.Runner(plansql.WithRowLimit(e.SQL, 2))
	if err != nil {
		failEval(subqueryRunFailed("scalar", e.SQL, err))
	}
	// COLUMNS BEFORE ROWS — PostgreSQL's order; see refuseMultiColumnSubquery.
	refuseMultiColumnSubquery(e.SQL, rows, false)
	if len(rows) > 1 {
		// Reported with no count: the read stopped on purpose, so this site
		// knows "more than one" and not how many more.
		failEval(&ScalarSubqueryRowsError{SQL: e.SQL})
	}
	v, cardErr := ScalarSubqueryValue(e.SQL, rows)
	if cardErr != nil {
		failEval(cardErr)
	}
	e.val = v
}

// MemoryAccountant is the minimal per-task memory-budget hook InSubquery
// uses to charge its uncorrelated membership set (ADR-0006, #528). It is
// declared here rather than importing internal/engine/memory: expr has no
// other reason to depend on that package, and *memory.Tracker already has
// exactly this method set, so a caller that holds one satisfies this
// interface with no adapter — the seam CompileWithBudget threads through
// costs no new package dependency.
type MemoryAccountant interface {
	// Reserve charges n bytes against the budget, returning an error (which
	// InSubquery treats as a query error, never a silent no-op) if doing so
	// would exceed it.
	Reserve(n int64) error
	// Release returns n previously reserved bytes.
	Release(n int64)
}

// InSubquery checks if a value is in the result set of a subquery.
// Example: WHERE user_id IN (SELECT user_id FROM active_users)
// Uncorrelated: executed once and result set cached in a hash set for O(1) lookup.
type InSubquery struct {
	// Cols is the subquery's SELECT-list COLUMN COUNT — see
	// refuseMultiColumnSubqueryByPlan.
	Cols   SubqueryColumnsFunc
	Expr   Expr
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// Budget charges the membership set resolveSlow builds to the caller's
	// per-task memory tracker (ADR-0006, #528). nil (CompileWithRunner,
	// CompileWithScope, etc.) keeps the map unbudgeted, exactly as before
	// #528 — every shape that decorrelates into a semi join never reaches
	// this type at all (its build side is already budgeted and spillable);
	// only tryDecorrelateInSubquery's DECLINED shapes do, and only a
	// computed inner select item is unbounded (a LIMIT/OFFSET or an
	// ungrouped-aggregate inner item is bounded by construction).
	//
	// Set in production by expr.WithBudget, which the physical planner
	// passes at every compile site that carries a subquery runner (#531).
	// The option also hands the planner each node it budgets, because the
	// release side is the half that matters: a charge with no teardown point
	// makes every uncorrelated IN-subquery in a task hold its bytes for the
	// task's lifetime, and a task that plans several of them runs out of
	// budget for work that has already finished. WithBudget therefore
	// refuses a nil release hook rather than construct that state; the
	// teardown point is PhysicalPlan.Cleanup.
	Budget MemoryAccountant
	// SetBound bounds the membership set in ROWS, refusing past it rather
	// than truncating — a set short by one row is a different answer, and on
	// a write door it deletes the wrong rows. Zero is unbounded.
	//
	// It lives on this construct and not in the runner because a runner sees
	// SQL text and cannot tell which construct asked for it. IN is the one
	// that wants a SET; EXISTS wants a row and a scalar subquery is an error
	// past one, and both read a bounded number of rows by construction now
	// (plansql.AppendRowLimit). Bounding all three in the runner charged
	// those two for a set neither builds.
	SetBound int
	// resolved publishes the set: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398) — an
	// unsynchronized flag let a parallel worker probe a half-built map.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	// setNull records a NULL in the subquery's result set: a probe that
	// misses such a set is UNKNOWN, not false — the `NOT IN (SELECT
	// nullable_col ...)` trap (#370).
	setNull bool
	// emptySet records that the subquery returned NOTHING — not one value,
	// not even a NULL. It is a distinct case from setNull and from a
	// populated set, because an empty set is the one where a NULL PROBE key
	// is decided rather than UNKNOWN (see EvalBoolNull).
	emptySet bool
	intSet   map[int64]struct{}
	strSet   map[string]struct{}
	// fltSet is keyed by kernel.KeyFloat64Bits, not by the raw float64: a Go
	// map can never find a NaN key (NaN != NaN), and PostgreSQL's float order
	// says NaN EQUALS itself and -0.0 equals +0.0 (ADR-0012 item 8). Keyed
	// raw, `real IN (SELECT double …)` dropped the NaN row that the same
	// predicate as a JOIN — whose key canonicalises the bits (ADR-0023
	// item 1) — matched. It is a key; it folds what the comparator folds.
	fltSet map[uint64]struct{}
	// decSet is strSet keyed by batch.CanonicalDecimalText instead of by the
	// raw rendering, and it is consulted only when the PROBE is declared
	// DECIMAL. A DECIMAL boxes as its text at its own scale, so
	// `COALESCE(a, b) IN (SELECT COALESCE(a, b) FROM t WHERE …)` compared
	// "12.75" against the set's "12.7500" and answered zero rows where
	// PostgreSQL answers four — the row-at-a-time twin of #474, and a
	// two-path split besides, since the stage DAG lowers the same predicate
	// to a semi join keyed through the columnar encoding and got it right.
	// The gate is the DECLARATION, never the box's shape: a genuine STRING
	// column holding numeric-looking text still compares AS TEXT (#504).
	decSet map[string]struct{}
	// setNumericKind records what the SET's members are, which is half of the
	// rung this predicate compares at (#615 F2). PostgreSQL resolves
	// `probe IN (SELECT …)` by the same OPERATOR ladder a join key uses —
	// int ⊕ numeric → numeric, anything ⊕ float8 → float8, real ⊕ int →
	// float8 — and this type only ever consulted the set that matched the
	// probe's own box. Every cross-rung pair therefore missed EVERY member:
	// `numeric IN (SELECT float8)` answered 0 where PostgreSQL answers 7,
	// and its NOT IN answered 7 where PostgreSQL answers 0 — inventing rows,
	// not just dropping them.
	setNumericKind inSetKind
	// probe caches the settled kind of e.Expr, the same way every other
	// declaration-driven comparison site caches its operands'.
	probe boxOperand
	vals  []any // fallback for mixed types
	// chargedBytes is exactly what was handed to Budget.Reserve, guarded by
	// resolveMu, so Release returns exactly that many bytes exactly once.
	chargedBytes int64
}

func (e *InSubquery) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *InSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *InSubquery) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	// An EMPTY set is a real answer and not an absence, and it is checked
	// BEFORE the probe's own NULL because the NULL-keyed row is exactly the
	// one the general rule gets wrong: `x IN ()` is FALSE for every row and
	// `x NOT IN ()` is TRUE for every row, the NULL-keyed one included,
	// because an empty set offers no comparison to be UNKNOWN about. This is
	// the same boundary the semi/anti lowering states (#507) and the
	// materialized IN-set renders as a constant (ADR-0021 §2); the subquery-
	// predicate route stated it nowhere and dropped the NULL-keyed row.
	if e.emptySet {
		return e.Not, false
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}
	// The RUNG first: `x IN (SELECT y …)` is `x = y` quantified, so it is
	// resolved by PostgreSQL's operator ladder over (probe, set) — not by
	// whichever typed set happens to match the probe's Go box, which is what
	// this used to do and why every cross-rung pair missed every member
	// (#615 F2).
	if e.setNumericKind != inSetOther {
		_, probeIsInt := toInt64SafeStrict(lv)
		switch inSubqueryRung(e.probe.resolve(b), probeIsInt, e.setNumericKind) {
		case inSetDecimal:
			// EXACT, at the value's own digits: numeric ⊕ integer.
			if key, ok := inSubqueryDecimalKey(lv); ok && e.decSet != nil {
				if _, found := e.decSet[key]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetFloat:
			// float8, the numeric category's preferred type. A DECIMAL probe
			// reads through its exact text, which is `numeric::float8`.
			if fv, ok := inSubqueryFloat(lv); ok && e.fltSet != nil {
				if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetInt:
			if iv, ok := toInt64Safe(lv); ok && e.intSet != nil {
				if _, found := e.intSet[iv]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		}
	}
	// Fast path: typed hash lookup
	if e.intSet != nil {
		if iv, ok := toInt64Safe(lv); ok {
			if _, found := e.intSet[iv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.strSet != nil {
		if sv, ok := lv.(string); ok {
			// A DECIMAL probe keys by VALUE, not by rendering: see decSet.
			if e.decSet != nil && e.probe.resolve(b) == boxDecimal {
				if key, ok := batch.CanonicalDecimalText(sv); ok {
					if _, found := e.decSet[key]; found {
						return !e.Not, false
					}
					return e.missAnswer()
				}
			}
			if _, found := e.strSet[sv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.fltSet != nil {
		if fv, ok := toFloat64Safe(lv); ok {
			if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	// Fallback: linear scan for mixed types
	for _, rv := range e.vals {
		if rv != nil && compare(lv, rv, CmpEq) {
			return !e.Not, false
		}
	}
	return e.missAnswer()
}

// resolveSlow runs the subquery once and builds the probe set. Idempotent.
// inSetKind is what an IN-subquery's materialised value set holds. It is the
// SET half of the operator ladder; boxOperand.resolve gives the probe half.
type inSetKind int8

const (
	inSetOther   inSetKind = iota
	inSetInt               // int64 members: an INTEGER in PostgreSQL's terms
	inSetFloat             // float64 members: float8
	inSetDecimal           // decimal TEXT members: numeric
)

// inSubqueryRung is the type this predicate compares at, by PostgreSQL's
// operator resolution — the same ladder physical.joinKeyCommonType applies to
// a join key, because `x IN (SELECT y …)` IS `x = y` quantified:
//
//	numeric ⊕ integer  -> numeric   (exact, at the value's own digits)
//	numeric ⊕ float8   -> float8
//	integer ⊕ float8   -> float8
//	real    ⊕ anything -> float8    (a real boxes as a float64 already)
//
// probeIsInt distinguishes the two boxNumber cases at the row: an int64 box
// against a decimal set is the exact rung, a float box against the same set is
// the float one.
func inSubqueryRung(probe boxKind, probeIsInt bool, set inSetKind) inSetKind {
	switch set {
	case inSetInt:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && !probeIsInt:
			return inSetFloat
		}
		return inSetInt
	case inSetDecimal:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && probeIsInt:
			return inSetDecimal
		case probe == boxNumber:
			return inSetFloat
		}
		return inSetOther
	case inSetFloat:
		if probe == boxDecimal || probe == boxNumber {
			return inSetFloat
		}
		return inSetOther
	}
	return inSetOther
}

// inSubqueryDecimalKey reads a probe box as a canonical DECIMAL key: a
// DECIMAL boxes as its rendered text, an integer as an int64, and the two are
// one numeric value to PostgreSQL.
func inSubqueryDecimalKey(lv any) (string, bool) {
	switch v := lv.(type) {
	case string:
		return batch.CanonicalDecimalText(v)
	default:
		if iv, ok := toInt64Safe(lv); ok {
			return batch.CanonicalDecimalText(strconv.FormatInt(iv, 10))
		}
		_ = v
	}
	return "", false
}

// inSubqueryFloat reads a probe box as the float64 PostgreSQL would compare
// at. A DECIMAL's text goes through ParseFloat, which is the correctly
// rounded `numeric::float8`.
func inSubqueryFloat(lv any) (float64, bool) {
	if sv, ok := lv.(string); ok {
		f, err := strconv.ParseFloat(sv, 64)
		return f, err == nil
	}
	return toFloat64Safe(lv)
}

// toInt64SafeStrict is toInt64Safe restricted to boxes that ARE integers, so
// a float64 box does not answer true for a whole-numbered value — the rung
// for `float8 IN (SELECT numeric)` is float8 whether or not the row happens
// to hold 2.0.
func toInt64SafeStrict(lv any) (int64, bool) {
	switch v := lv.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	}
	return 0, false
}

func (e *InSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	// Bound here rather than at construction so every caller gets it: the
	// write is under resolveMu and published by the Store above, the same
	// release the value set itself rides.
	e.probe.expr = e.Expr
	// The same guard the other two uncorrelated evaluators carry: a set this
	// resolves ONCE has to be one that reads no outer row (#734/#679/#535).
	refuseDanglingSubquery("IN", e.SQL, e.Scope)
	refuseMultiColumnSubqueryByPlan(e.Cols, e.SQL, true)
	rows, err := e.Runner(e.SQL)
	if err != nil {
		// NOT an empty set. Treating the failure as "every probe misses" is
		// the same fold the correlated evaluators made, one construct over:
		// a membership set that could not be built has no answer, and
		// answering FALSE for every row is a confident wrong one.
		failEval(subqueryRunFailed("IN", e.SQL, err))
	}
	if e.SetBound > 0 && len(rows) > e.SetBound {
		failEval(&InSetTooLargeError{SQL: e.SQL, Rows: len(rows), Bound: e.SetBound})
	}
	// Collect values and detect predominant type for hash set
	var rawVals []any
	{
		for _, r := range rows {
			// PostgreSQL refuses a multi-column IN subquery outright (42601,
			// `subquery has too many columns`). Taking "the first column" out
			// of a Go MAP instead built the set from a DIFFERENT column on
			// different runs of the same query, because map iteration order is
			// randomized per range statement. A one-column subquery whose
			// pipeline emitted a hidden ORDER BY key beside it is #875 and is
			// trimmed where the pipeline is built, so a row with two entries
			// here is a genuine two-column SELECT list.
			if len(r) > 1 {
				failEval(&SubqueryColumnsError{SQL: e.SQL, Columns: len(r), InPredicate: true})
			}
			for _, v := range r {
				if v != nil {
					rawVals = append(rawVals, v)
				} else {
					e.setNull = true
				}
			}
		}
		// Build typed hash set. Use toInt64Safe/toFloat64Safe to normalize
		// all integer types (int32, int64, int) and float types (float32, float64).
		if len(rawVals) > 0 {
			if _, ok := toInt64Safe(rawVals[0]); ok {
				// An INTEGER set, carried in all three spellings the ladder
				// can ask for: exactly (intSet), as canonical DECIMAL text
				// for a numeric probe, and as float64 for a float one. Every
				// conversion here is exact — an int64 that does not survive
				// float64 is the 2^53 case, and PostgreSQL rounds it too.
				e.setNumericKind = inSetInt
				e.intSet = make(map[int64]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if iv, ok := toInt64Safe(v); ok {
						e.intSet[iv] = struct{}{}
						e.fltSet[kernel.KeyFloat64Bits(float64(iv))] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(strconv.FormatInt(iv, 10)); ok {
							e.decSet[key] = struct{}{}
						}
					} else {
						e.vals = rawVals
						e.intSet, e.decSet, e.fltSet = nil, nil, nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else if _, ok := rawVals[0].(string); ok {
				e.strSet = make(map[string]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				allDecimal := true
				for _, v := range rawVals {
					if sv, ok := v.(string); ok {
						e.strSet[sv] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(sv); ok {
							e.decSet[key] = struct{}{}
						} else {
							allDecimal = false
						}
					} else {
						e.vals = rawVals
						e.strSet, e.decSet = nil, nil
						allDecimal = false
						break
					}
				}
				// A set every one of whose members is decimal text IS a
				// numeric set, and a FLOAT probe compares against it at
				// float8. ParseFloat is the correctly-rounded reading, which
				// is what `numeric::float8` does.
				if allDecimal && e.strSet != nil {
					e.setNumericKind = inSetDecimal
					e.fltSet = make(map[uint64]struct{}, len(rawVals))
					for sv := range e.strSet {
						if fv, err := strconv.ParseFloat(sv, 64); err == nil {
							e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
						}
					}
				}
			} else if _, ok := toFloat64Safe(rawVals[0]); ok {
				// A FLOAT set is float8 against everything (PostgreSQL's
				// preferred type of the numeric category), so it gets NO
				// decimal view: an exact comparison against it would be a
				// different predicate.
				e.setNumericKind = inSetFloat
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if fv, ok := toFloat64Safe(v); ok {
						e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
					} else {
						e.vals = rawVals
						e.fltSet = nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else {
				e.vals = rawVals
			}
		}
		// Nothing at all came back — not a value and not a NULL.
		e.emptySet = len(rawVals) == 0 && !e.setNull
	}
	e.chargeMemory()
}

// chargeMemory reserves the membership set's estimated heap footprint
// against Budget (ADR-0006, #528). A nil Budget (every compile path except
// CompileWithBudget) is a no-op, exactly the pre-#528 behavior — this is
// what makes the fix opt-in rather than a change to every existing caller.
//
// Called once, from inside resolveSlow's already-held resolveMu, after the
// set this query holds has been decided, so the estimate matches exactly
// what stays reachable for the life of this InSubquery.
//
// AFTER, which bounds what this can do. The map is already built and already
// resident by the time a byte is charged, so a subquery large enough to
// exhaust the machine exhausts it before Reserve is ever called: this does
// not PREVENT that OOM, and #528's issue text describing it as doing so is
// wrong. What it does do is make the set VISIBLE to the task's budget — it
// counts against every later allocation the task makes, and a set that is
// over budget on its own turns into a query error instead of a permanently
// unaccounted resident map that every other operator then has to fit
// alongside. That is worth having and is what ADR-0006 asks of a structure
// that cannot spill; it is not the same claim.
//
// Charging as the set is BUILT — a Reserve per N rows inside resolveSlow's
// accumulation loop, refusing partway — is what would actually bound peak
// resident bytes. It needs resolveSlow to have somewhere to put a partial
// failure and a caller that can act on one, so it is a change to this
// type's contract rather than to this function. Not attempted here.
func (e *InSubquery) chargeMemory() {
	if e.Budget == nil {
		return
	}
	n := inSubqueryMemBytes(e.intSet, e.strSet, e.fltSet, e.vals)
	if n <= 0 {
		return
	}
	if err := e.Budget.Reserve(n); err != nil {
		// A giant uncorrelated IN-subquery is exactly the shape ADR-0006
		// exists to refuse rather than let grow an unbudgeted map until the
		// process OOMs (#528) — raised the same way every other expr-level
		// query error is (fatal.go), since EvalBoolNull has no error return
		// to propagate one through.
		panic(fatalEval{fmt.Errorf("IN subquery membership set: %w", err)})
	}
	e.chargedBytes = n
}

// Release returns any bytes charged to Budget in resolveSlow, and is
// idempotent — a caller that does not know whether resolveSlow ever ran, or
// has already called Release, may call it any number of times.
//
// Its caller is the plan that compiled the tree: the physical planner
// registers every InSubquery compiled under a budget (expr.WithBudget's
// release hook) and PhysicalPlan.Cleanup releases them, which is the
// teardown point #531 needed and #528 left open. Wiring the charge WITHOUT
// one converts an unbudgeted map into a permanently-charged one — a worse
// failure than the one #528 set out to fix, because the bytes are returned
// to the OS by GC and never returned to the tracker — which is why
// WithBudget refuses to construct a budget with a nil release hook.
func (e *InSubquery) Release() {
	if e.Budget == nil {
		return
	}
	e.resolveMu.Lock()
	n := e.chargedBytes
	e.chargedBytes = 0
	e.resolveMu.Unlock()
	if n > 0 {
		e.Budget.Release(n)
	}
}

// inSubqueryMapEntryOverhead approximates a Go map's per-entry bookkeeping:
// a bucket holds up to 8 (key, value) pairs plus one tophash byte and an
// amortized share of an overflow-bucket pointer per entry, which rounds up
// to about this many bytes on top of the key/value payload itself. This
// does not need to be exact — Reserve's job is to catch an unbounded
// subquery before it outgrows the task's budget, not to account every byte
// precisely — and rounding up is the safe direction for a budget check.
const inSubqueryMapEntryOverhead = 16

// inSubqueryMemBytes estimates the heap footprint of whichever membership
// set resolveSlow built — at most one of intSet/strSet/fltSet/vals is
// non-nil, matching resolveSlow's mutually exclusive construction.
func inSubqueryMemBytes(intSet map[int64]struct{}, strSet map[string]struct{}, fltSet map[uint64]struct{}, vals []any) int64 {
	switch {
	case intSet != nil:
		return int64(len(intSet)) * (8 + inSubqueryMapEntryOverhead)
	case fltSet != nil:
		return int64(len(fltSet)) * (8 + inSubqueryMapEntryOverhead)
	case strSet != nil:
		var n int64
		for k := range strSet {
			n += int64(len(k)) + 16 /* string header */ + inSubqueryMapEntryOverhead
		}
		return n
	case len(vals) > 0:
		var n int64
		for _, v := range vals {
			n += 16 // interface header (type word + data word)
			if s, ok := v.(string); ok {
				n += int64(len(s))
			} else {
				n += 8 // scalar payload estimate
			}
		}
		return n
	}
	return 0
}

// missAnswer is the IN answer for a probe the set does not contain: FALSE
// (or TRUE under NOT) for a NULL-free set, UNKNOWN when the set held a NULL.
func (e *InSubquery) missAnswer() (bool, bool) {
	if e.setNull {
		return false, true
	}
	return e.Not, false
}

// ExistsSubquery evaluates to true if a subquery returns any rows.
// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
// Uncorrelated: executed once and result cached.
type ExistsSubquery struct {
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// resolved publishes exists: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398).
	resolved  atomic.Bool
	resolveMu sync.Mutex
	exists    bool
}

func (e *ExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *ExistsSubquery) EvalBool(_ *batch.RecordBatch, _ int) bool {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	if e.Not {
		return !e.exists
	}
	return e.exists
}

func (e *ExistsSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	// This evaluator runs the subquery ONCE, query-wide, and memoizes — which
	// is only sound if the subquery really is uncorrelated. When the
	// classifier missed a correlation (an EXISTS inside an aggregate ARGUMENT
	// is compiled with no outer scope at all, #734) the text still names the
	// outer relation, and standalone `ResolveColumnRef` strips the qualifier
	// and rebinds it — so this answered a query-wide CONSTANT, TRUE or FALSE
	// according to whether the two relations happened to share a column name.
	// Checked once here, where it costs one parse per query (#734/#679/#535).
	refuseDanglingSubquery("EXISTS", e.SQL, e.Scope)
	// ONE ROW. EXISTS asks whether there is a row; the first one answers it.
	rows, err := e.Runner(plansql.WithRowLimit(e.SQL, 1))
	if err != nil {
		// `err == nil && len(rows) > 0` made a failure indistinguishable
		// from an empty result. They are not the same thing.
		failEval(subqueryRunFailed("EXISTS", e.SQL, err))
	}
	e.exists = len(rows) > 0
	e.resolved.Store(true)
}
