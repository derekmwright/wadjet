package physical

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// `WHERE x IN (SELECT …)` on the stage DAG
// ------------------------------------------------------------------
//
// An IN-subquery has one distributed lowering: logical.tryDecorrelateInSubquery
// turns it into a semi/anti join. When that rewrite DECLINES — a subquery
// carrying LIMIT/OFFSET (#482), an ungrouped aggregate item, or a computed one
// (#516) — the IN stays a subquery PREDICATE, and the stage DAG had nothing to
// execute one with: the filter shipped to the worker verbatim and failed with
// "IN subquery requires a SubqueryRunner". The single-process path answered
// every one of those correctly, so it was a two-path divergence in which the
// distributed side ERRORED (#524).
//
// resolveSubqueryAST handled a scalar SubqueryNode and fell through `default:`
// for InExpr. It no longer does. An UNCORRELATED IN-subquery is a SET-valued
// producer, and the set is the whole of what the predicate needs: executed
// once on the coordinator, its rows become the literal list the expression
// layer already evaluates — with NOT IN's three-valued rule (#370), which is
// the same rule #507 gave the semi-join lowering. The subquery runs AS
// WRITTEN, so its LIMIT, OFFSET and ORDER BY mean what they say, which is
// exactly why #482 made the rewrite decline in the first place.
//
// Two bounds, and crossing either is a typed REFUSAL rather than a guess:
//
//   - The set must fit. A declined shape can be unbounded
//     (`IN (SELECT b.id + 0 FROM huge)`), and inlining a million literals into
//     a filter expression is not a plan. maxInlinedInSetRows caps it.
//   - Every value must have a literal spelling that survives the round trip
//     through the filter's text. Integers, floats, strings, booleans and NULL
//     do; a value this cannot render honestly is refused rather than
//     approximated.
//
// The refusal routes the query to the coordinator-local single-process
// pipeline, which owns IN-subquery semantics — the same handoff #359 makes for
// correlated subqueries and #466 for an unstageable DISTINCT. A slower right
// answer beats an error, and both beat a different one.

// ErrInSubqueryDistributed marks a plan the stage DAG refuses because it
// carries an IN-subquery the planner could not materialize into a literal set.
// The coordinator routes it onto the local single-process pipeline, where
// expr.InSubquery resolves the set once under resolveMu and caches it.
var ErrInSubqueryDistributed = errors.New(
	"IN subquery in this position has no distributed stage")

// maxInlinedInSetRows bounds the set an IN-subquery may be materialized into.
//
// The number is a plan-text budget, not a memory one: every row becomes a
// literal in a filter expression that is serialized into each task, so the
// cost is paid per task and shows up in dispatch size. Ten thousand keeps a
// bounded subquery (the #482 LIMIT shapes this exists for) comfortably inside
// it while refusing an unbounded one early enough to route local before the
// coordinator has read a large result into memory.
//
// WADJET_IN_SET_MAX overrides it; 0 disables materialization entirely, which
// makes every declined IN-subquery take the local route (the kill switch for
// this path). Read per call rather than at init so a test can exercise the
// refusal without a fixture large enough to cross the real bound.
const defaultInlinedInSetRows = 10000

func maxInlinedInSetRows() int {
	if v := os.Getenv("WADJET_IN_SET_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return defaultInlinedInSetRows
}

// refuseInSubquery parks the first IN-subquery refusal. resolveSubqueryAST
// runs under walkStages, which has no error path, so the refusal is recorded
// and PlanDistributed returns it — the same mechanism refuseCorrelated uses.
func (p *Planner) refuseInSubquery(err error) {
	if p.inSubqueryErr == nil {
		p.inSubqueryErr = err
	}
}

// materializeInSubquery executes an uncorrelated IN-subquery and returns the
// predicate rewritten against its result set, or ok=false when it cannot —
// having parked the refusal.
//
// An EMPTY set is a real answer, not an absence: `x IN ()` is FALSE for every
// row including a NULL x, and `x NOT IN ()` is TRUE for every row including a
// NULL one, because an empty set has nothing to be UNKNOWN about. Neither
// renders as an empty value list (nothing parses that), so they render as the
// constant they are.
func (p *Planner) materializeInSubquery(ctx context.Context, in *plansql.InExpr, subq *plansql.SubqueryNode, decls colDecls) (plansql.Node, bool) {
	// A FLOAT32 PROBE cannot take this path at all, whatever the set holds.
	// PostgreSQL's multi-element `real IN (…)` NARROWS its literals to real[]
	// (#549) while `real = ANY(<subquery>)` widens the real to float8 — so
	// materializing a subquery into a literal list silently changes the
	// comparison WIDTH, and the rows whose real value does not survive the
	// round trip appear or vanish: `r IN (SELECT double …)` answered 8 where
	// PostgreSQL answers 6 (#615 F2). inSetLiteral already declines a float32
	// SET for the mirror of this reason; this is the probe half, and it needs
	// the declared type the value list cannot show.
	if t, c := nodeDeclaredType(in.Left, decls); c == expr.Decided && t.ID == parquet.TypeFloat32 {
		p.refuseInSubquery(fmt.Errorf("%w: the probe is REAL, and PostgreSQL compares a "+
			"multi-element IN list at real width while a subquery widens it to double "+
			"precision — inlining the set would change the predicate",
			ErrInSubqueryDistributed))
		return nil, false
	}
	// A subquery that is not self-contained cannot run as a standalone
	// producer: its dangling outer reference resolves to no column and
	// evaluates NULL, which is the silent 0 #359 is about. The correlated
	// refusal owns that shape.
	if dangling := plansql.DanglingTableRefs(subq.SQL); len(dangling) > 0 {
		p.refuseCorrelated(fmt.Errorf("%w: an IN subquery references outer %s"+
			" and cannot execute as a standalone set producer",
			ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
		return nil, false
	}
	// A subquery whose FROM reads a RECURSIVE CTE has no set to inline here.
	// executeSubquery runs it through buildSubqueryPipeline, which has no
	// fixed-point cache, so a recursive reference reads ZERO rows with NO
	// error — and the len(rows)==0 branch below would take that for a genuine
	// empty set and answer `x IN ()` FALSE / `x NOT IN ()` TRUE for every row.
	// That is the silent wrong answer #F1 removes, so the shape is REFUSED and
	// routed to the coordinator-local pipeline, which materializes the
	// recursive CTE and answers it.
	if p.subqueryReadsRecursiveCTE(subq.SQL) {
		p.refuseInSubquery(fmt.Errorf("%w: the subquery reads a recursive CTE, "+
			"which has no set-producer lowering", ErrInSubqueryDistributed))
		return nil, false
	}
	bound := maxInlinedInSetRows()
	if bound == 0 {
		p.refuseInSubquery(fmt.Errorf("%w: materialization disabled (WADJET_IN_SET_MAX=0)",
			ErrInSubqueryDistributed))
		return nil, false
	}

	start := time.Now()
	rows, err := p.executeSubquery(ctx, subq.SQL)
	if err != nil {
		p.refuseInSubquery(fmt.Errorf("%w: executing the subquery as a set producer failed: %v",
			ErrInSubqueryDistributed, err))
		return nil, false
	}
	slog.Info("plan-time IN subquery materialized on coordinator",
		"duration", time.Since(start).Round(time.Millisecond), "rows", len(rows))

	if len(rows) > bound {
		p.refuseInSubquery(fmt.Errorf("%w: the subquery yields %d rows, past the %d-row inline bound",
			ErrInSubqueryDistributed, len(rows), bound))
		return nil, false
	}
	if len(rows) == 0 {
		return emptyInSetPredicate(in.Not), true
	}

	values := make([]plansql.Node, 0, len(rows))
	for _, row := range rows {
		if len(row) != 1 {
			p.refuseInSubquery(fmt.Errorf("%w: the subquery yields %d columns, not one",
				ErrInSubqueryDistributed, len(row)))
			return nil, false
		}
		for _, v := range row {
			lit, ok := inSetLiteral(v)
			if !ok {
				p.refuseInSubquery(fmt.Errorf("%w: a %T value has no literal spelling this can inline",
					ErrInSubqueryDistributed, v))
				return nil, false
			}
			values = append(values, lit)
		}
	}
	return &plansql.InExpr{Left: in.Left, Not: in.Not, Values: values}, true
}

// emptyInSetPredicate is the constant `x IN ()` / `x NOT IN ()` evaluates to.
// Rendered as a comparison of two numeric literals rather than a bare boolean,
// because every filter compiler in the engine takes a comparison.
func emptyInSetPredicate(not bool) plansql.Node {
	right := "0"
	if not {
		right = "1"
	}
	return &plansql.CmpExpr{
		Left:  &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
		Op:    "=",
		Right: &plansql.Lit{Value: right, Kind: plansql.LitNumber},
	}
}

// inSetLiteral renders one subquery result value as an AST literal, or reports
// ok=false when the value has no spelling that survives the round trip through
// the filter's TEXT. Refusing beats approximating: an inlined value that
// re-parses as something else is a wrong answer with no error attached.
func inSetLiteral(v any) (plansql.Node, bool) {
	switch val := v.(type) {
	case nil:
		return &plansql.Lit{Value: "null", Kind: plansql.LitNull}, true
	case bool:
		if val {
			return &plansql.Lit{Value: "true", Kind: plansql.LitBool}, true
		}
		return &plansql.Lit{Value: "false", Kind: plansql.LitBool}, true
	case int:
		return &plansql.Lit{Value: strconv.Itoa(val), Kind: plansql.LitNumber}, true
	case int32:
		return &plansql.Lit{Value: strconv.FormatInt(int64(val), 10), Kind: plansql.LitNumber}, true
	case int64:
		return &plansql.Lit{Value: strconv.FormatInt(val, 10), Kind: plansql.LitNumber}, true
	case float32:
		// REFUSED, and not because it cannot be rendered. The IN-literal-list
		// kernel compares a FLOAT32 column against its literals in float64
		// space while `=` narrows the literal to float32, so a set of eight
		// values that eight rows satisfy under an OR-of-equals matches NONE
		// of them through IN — measured 0 vs 8 on `float32(i)+0.1` (#549).
		// Inlining here would turn that kernel bug into a silent wrong answer
		// for a query that used to fail loudly, so a float32 set takes the
		// local route, where expr.InSubquery compares typed values instead.
		return nil, false
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			// "NaN" and "+Inf" are not numeric literals in this dialect;
			// they re-parse as identifiers or not at all.
			return nil, false
		}
		text := strconv.FormatFloat(val, 'f', -1, 64)
		// A float64 whose shortest rendering has no fractional part comes
		// back as an INTEGER literal when the worker re-parses the filter
		// text, and an integer literal is compared EXACTLY: 9007199254740992
		// then failed to match the bigint 9007199254740993 that
		// PostgreSQL — comparing at float8 — calls equal (#615 F2). The
		// decimal point is what keeps a float8 a float8 across the wire.
		if !strings.ContainsAny(text, ".eE") {
			text += ".0"
		}
		// A float that does not render EXACTLY is a different value once the
		// worker parses the filter text back. FormatFloat with precision -1
		// is round-trip exact for float64 by contract; the check costs
		// nothing and makes that a property of this code rather than of a
		// doc comment.
		if back, err := strconv.ParseFloat(text, 64); err != nil || back != val {
			return nil, false
		}
		return &plansql.Lit{Value: text, Kind: plansql.LitNumber}, true
	case string:
		return &plansql.Lit{Value: val, Kind: plansql.LitString}, true
	default:
		return nil, false
	}
}

// findInSubqueryValue returns the single SubqueryNode an IN predicate's value
// list carries, or nil when the list is literal (or holds more than one term,
// which is not a subquery IN).
func findInSubqueryValue(in *plansql.InExpr) *plansql.SubqueryNode {
	if in == nil || len(in.Values) != 1 {
		return nil
	}
	node := in.Values[0]
	for {
		switch n := node.(type) {
		case *plansql.SubqueryNode:
			return n
		case *plansql.ParenNode:
			node = n.Inner
		default:
			return nil
		}
	}
}

// NullAwareAntiForcedBroadcasts counts the joins whose build side was forced
// to REPLICATE against the size decision because the join is null-aware
// (#507): NOT IN's three-valued rule reads one fact off the WHOLE build, and a
// hash-partitioned build splits it.
//
// Exported because it is the only way to see the trade from outside: the
// answers stay right either way, and what changes is that a build the
// broadcast threshold would have refused — including one refused explicitly by
// BroadcastBytesThreshold < 0 — is now replicated to every task. #539 tracks
// the shape that removes the need.
var NullAwareAntiForcedBroadcasts atomic.Int64

// subqueryReadsRecursiveCTE reports whether the FROM of sql — at any nesting,
// including inside a derived table and through the subquery's own WITH — names
// a RECURSIVE CTE, one declared recursive in the enclosing statement (p.ctes)
// or in the subquery itself. Its logical twin is innerRelationsAreScannable's
// CTE decline: the logical pass keeps a recursive CTE out of a semi/anti
// join's build side, and this keeps it out of a materialized IN set.
func (p *Planner) subqueryReadsRecursiveCTE(sql string) bool {
	recursive := make(map[string]bool, len(p.ctes))
	for _, c := range p.ctes {
		if c.Recursive {
			recursive[strings.ToLower(c.Name)] = true
		}
	}
	return sqlReadsRecursiveCTE(sql, recursive)
}

func sqlReadsRecursiveCTE(sql string, recursive map[string]bool) bool {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return false
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return false
	}
	scope := recursive
	if len(info.CTEs) > 0 {
		scope = make(map[string]bool, len(recursive)+len(info.CTEs))
		for k := range recursive {
			scope[k] = true
		}
		for _, c := range info.CTEs {
			if c.Recursive {
				scope[strings.ToLower(c.Name)] = true
			}
		}
	}
	for _, t := range info.Tables {
		if tableRefReadsRecursiveCTE(t, scope) {
			return true
		}
	}
	for _, j := range info.Joins {
		ref := plansql.TableRef{Name: j.RightTable}
		if j.RightTableRef != nil {
			ref = *j.RightTableRef
		}
		if tableRefReadsRecursiveCTE(ref, scope) {
			return true
		}
	}
	return false
}

func tableRefReadsRecursiveCTE(t plansql.TableRef, recursive map[string]bool) bool {
	if strings.HasPrefix(t.Name, "(") {
		if len(t.Name) < 2 || !strings.HasSuffix(t.Name, ")") {
			return false
		}
		return sqlReadsRecursiveCTE(t.Name[1:len(t.Name)-1], recursive)
	}
	return recursive[strings.ToLower(t.Name)]
}
