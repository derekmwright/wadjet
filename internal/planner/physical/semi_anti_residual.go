// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// semiAntiConj is one conjunct of a semi/anti join's residual: a probe
// (enclosing-query) column, an operator, and a build (subquery) column. The
// decorrelation (logical.tryDecorrelateExists) renders every conjunct as
// "probe OP build", OP being one of the comparison operators below.
type semiAntiConj struct {
	probeCol, op, buildCol string
}

// semiAntiOps are the operators a residual conjunct may carry, longest
// spelling first so that no shorter operator matches inside a longer one.
// IS [NOT] DISTINCT FROM comes first: its words contain none of the symbols.
var semiAntiOps = []string{"is not distinct from", "is distinct from", "!=", ">=", "<=", "<>", ">", "<", "="}

// parseSemiAntiConjuncts splits a residual into its conjuncts. A conjunct with
// no operator it knows is an ERROR, never a skipped conjunct: skipping one
// answered the remaining conjuncts alone, and a residual with no known
// operator at all compiled to no filter, so the probe tested existence only
// (#1681, an IS [NOT] DISTINCT FROM residual).
func parseSemiAntiConjuncts(filter string) ([]semiAntiConj, error) {
	var conjs []semiAntiConj
	for _, part := range strings.Split(strings.ToLower(filter), " and ") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		found := false
		for _, op := range semiAntiOps {
			idx := strings.Index(part, " "+op+" ")
			if idx < 0 {
				continue
			}
			// Kept QUALIFIED. A join emits a build column under its
			// relation's qualifier whenever the bare name collides on the
			// probe side, so stripping here resolved the filter to whichever
			// relation reorderJoins put on the probe — the #527 defect one
			// layer down from the logical plan. filterColumnIndex falls back
			// to the bare name for the single-relation builds that emit it
			// that way.
			left := strings.TrimSpace(part[:idx])
			right := strings.TrimSpace(part[idx+len(op)+2:])
			if left == "" || right == "" {
				break
			}
			if op == "<>" {
				op = "!="
			}
			conjs = append(conjs, semiAntiConj{probeCol: left, op: op, buildCol: right})
			found = true
			break
		}
		if !found {
			return nil, sqlerr.New("0A000",
				"semi/anti join residual %q: the conjunct %q is not a comparison of two columns, "+
					"and the residual cannot be evaluated without it", filter, part)
		}
	}
	return conjs, nil
}

// semiAntiKernel evaluates one conjunct on a candidate pair from the typed
// slots. A kernel exists only where reading the slot IS PostgreSQL's
// comparison: both sides of one type whose storage orders as the type does.
type semiAntiKernel func(pv, bv *batch.Vector, pRow, bRow int) bool

const (
	saOpNE = iota
	saOpGT
	saOpLT
	saOpGE
	saOpLE
	saOpEQ
)

func semiAntiOpCode(op string) (int, bool) {
	switch op {
	case "!=":
		return saOpNE, true
	case ">":
		return saOpGT, true
	case "<":
		return saOpLT, true
	case ">=":
		return saOpGE, true
	case "<=":
		return saOpLE, true
	case "=":
		return saOpEQ, true
	}
	return 0, false // IS [NOT] DISTINCT FROM: NULL is a value there
}

// semiAntiTypedKernel returns the typed kernel for a conjunct over these two
// operand types, or nil when the slot does not order as PostgreSQL does and
// the conjunct must go through the engine's own comparison instead: two
// different types (int4 against int8, int against float or numeric, date
// against timestamp — the slots are different arrays), NUMERIC (a scaled
// Int128 whose scale may differ per side), BYTEA, the network types, UUID,
// BOOL, nested types, and IS [NOT] DISTINCT FROM, whose NULL is a value.
// Resolved ONCE per join, so the per-row cost is one indirect call.
func semiAntiTypedKernel(pt, bt batch.TypeID, op string) semiAntiKernel {
	code, ok := semiAntiOpCode(op)
	if !ok || pt != bt {
		return nil
	}
	switch pt {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return func(pv, bv *batch.Vector, i, j int) bool { return ordCmp(pv.Int32Data[i], bv.Int32Data[j], code) }
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return func(pv, bv *batch.Vector, i, j int) bool { return ordCmp(pv.Int64Data[i], bv.Int64Data[j], code) }
	case batch.TypeFloat64:
		return func(pv, bv *batch.Vector, i, j int) bool {
			return pgFloatCmp(pv.Float64Data[i], bv.Float64Data[j], code)
		}
	case batch.TypeFloat32:
		return func(pv, bv *batch.Vector, i, j int) bool {
			return pgFloatCmp(pv.Float32Data[i], bv.Float32Data[j], code)
		}
	case batch.TypeString:
		// Byte order is the C collation's, the reference's (ADR-0012).
		return func(pv, bv *batch.Vector, i, j int) bool {
			return ordCmp(pv.BytesData.StringValue(i), bv.BytesData.StringValue(j), code)
		}
	}
	return nil
}

func ordCmp[T int32 | int64 | string](a, b T, code int) bool {
	switch code {
	case saOpNE:
		return a != b
	case saOpGT:
		return a > b
	case saOpLT:
		return a < b
	case saOpGE:
		return a >= b
	case saOpLE:
		return a <= b
	default:
		return a == b
	}
}

// pgFloatCmp is PostgreSQL's float comparison: NaN equals NaN and orders
// above every other value (float8_cmp_internal). The IEEE comparison the
// kernel used made every comparison with a NaN false and NaN <> NaN true.
func pgFloatCmp[T float32 | float64](a, b T, code int) bool {
	aNaN, bNaN := a != a, b != b
	switch code {
	case saOpNE:
		return a != b && !(aNaN && bNaN)
	case saOpGT:
		return a > b || (aNaN && !bNaN)
	case saOpLT:
		return a < b || (bNaN && !aNaN)
	case saOpGE:
		return a >= b || aNaN
	case saOpLE:
		return a <= b || bNaN
	default:
		return a == b || (aNaN && bNaN)
	}
}

// semiAntiGeneral evaluates one conjunct through the ENGINE'S OWN comparison
// (expr.Compile, the compiler every WHERE and every outer join's ON residual
// runs), over a two-column row holding the probe operand and the build
// operand. The sides are bound here, by the residual's probe-OP-build
// convention, and never re-resolved by name. Evaluators carry per-candidate
// scratch and the join's probe workers share the filter, so they are pooled.
type semiAntiGeneral struct {
	pool sync.Pool
}

func newSemiAntiGeneral(compiled expr.Expr, probe *batch.RecordBatch, pi int, build *batch.RecordBatch, bi int) *semiAntiGeneral {
	binds := []residualBind{
		{spelled: "probe", idx: pi, field: -1},
		{spelled: "build", idx: bi, field: -1, fromBuild: true},
	}
	schema := make([]parquet.Column, 2)
	srcs := []*batch.RecordBatch{probe, build}
	for i := range binds {
		schema[i], binds[i].nested = residualSlot(srcs[i], &binds[i], semiAntiSlotName(i))
		binds[i].declared = schema[i]
	}
	g := &semiAntiGeneral{}
	g.pool.New = func() any {
		e := &residualEval{compiled: compiled, resolved: true}
		e.binds = make([]residualBind, len(binds))
		copy(e.binds, binds)
		e.row = batch.NewRecordBatch(schema, 1)
		for i := range e.binds {
			e.binds[i].dst = e.row.Columns[i]
		}
		return e
	}
	return g
}

func (g *semiAntiGeneral) eval(probe *batch.RecordBatch, pRow int, build *batch.RecordBatch, bRow int) bool {
	e := g.pool.Get().(*residualEval)
	ok := e.eval(probe, pRow, build, bRow)
	g.pool.Put(e)
	return ok
}

func semiAntiSlotName(i int) string {
	if i == 0 {
		return "_wj_sa_probe"
	}
	return "_wj_sa_build"
}

// compileSemiAntiConj compiles `probe OP build` over the two slots.
func compileSemiAntiConj(op string) (expr.Expr, error) {
	return expr.Compile(&plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: semiAntiSlotName(0)},
		Op:    op,
		Right: &plansql.ColRef{Column: semiAntiSlotName(1)},
	})
}

// buildSemiAntiFilter compiles a semi/anti join's residual (e.g.
// "l_suppkey != l_suppkey") into the predicate the probe asks of each
// candidate pair: TRUE only when every conjunct is TRUE, as SQL's WHERE inside
// the EXISTS body requires (a NULL comparison is UNKNOWN and fails the pair).
// Convention: left of the operator = probe column, right = build column.
//
// Each conjunct runs on a typed kernel where the typed slot orders exactly as
// PostgreSQL does (semiAntiTypedKernel), and through the engine's own
// comparison otherwise (semiAntiGeneral): numeric, bytea and the network types
// used to compare their printed TEXT here (12.25 below 9.25, 10.0.0.12 below
// 10.0.0.9), operands of two types read the wrong array, and IS [NOT] DISTINCT
// FROM was not recognized at all (#1681). A residual it cannot parse is an
// error the caller raises, never a dropped conjunct. A nil filter and nil
// error mean an empty residual.
//
// HashJoin's probe runs in parallel and shares this closure: indices and
// kernels are resolved once on the first call (sync.Once gives the
// happens-before), and the general evaluators are pooled.
func buildSemiAntiFilter(filter string) (func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool, error) {
	conjs, err := parseSemiAntiConjuncts(filter)
	if err != nil {
		return nil, err
	}
	if len(conjs) == 0 {
		return nil, nil
	}
	compiled := make([]expr.Expr, len(conjs))
	for i, c := range conjs {
		if compiled[i], err = compileSemiAntiConj(c.op); err != nil {
			return nil, sqlerr.New("0A000", "semi/anti join residual %q: %v", filter, err)
		}
	}

	probeIdxs := make([]int, len(conjs))
	buildIdxs := make([]int, len(conjs))
	kernels := make([]semiAntiKernel, len(conjs))
	generals := make([]*semiAntiGeneral, len(conjs))
	var resolveOnce sync.Once

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		resolveOnce.Do(func() {
			for i, c := range conjs {
				pi, bi := filterColumnIndex(probe, c.probeCol), filterColumnIndex(build, c.buildCol)
				probeIdxs[i], buildIdxs[i] = pi, bi
				if pi < 0 || bi < 0 {
					continue
				}
				kernels[i] = semiAntiTypedKernel(probe.Columns[pi].Type, build.Columns[bi].Type, c.op)
				generals[i] = newSemiAntiGeneral(compiled[i], probe, pi, build, bi)
			}
		})

		for i := range conjs {
			pi, bi := probeIdxs[i], buildIdxs[i]
			if pi < 0 || bi < 0 {
				return false
			}
			pv := probe.Columns[pi]
			bv := build.Columns[bi]
			// A VIEW's typed slot is not addressed by its row: it goes the
			// general way, which reads it through its base.
			if k := kernels[i]; k != nil && pv.Base == nil && bv.Base == nil {
				if !k(pv, bv, probeRow, buildRow) {
					return false
				}
				// A comparison with a NULL operand is UNKNOWN, and the
				// conjunction must be TRUE for the pair to match. The kernel
				// reads the slot without the bitmap — a NULL slot holds its
				// zero value — so a TRUE stands only when neither operand is
				// NULL (asked after the comparison: a FALSE fails the pair
				// whatever the bitmap says).
				if filterOperandNull(pv, probeRow) || filterOperandNull(bv, buildRow) {
					return false
				}
				continue
			}
			if !generals[i].eval(probe, probeRow, build, buildRow) {
				return false
			}
		}
		return true
	}, nil
}
