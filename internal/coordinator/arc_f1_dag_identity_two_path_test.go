package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// DAG COLUMN IDENTITY — the value gate for arc F1, on FOUR arms.
//
// One position, applied at six sites: a stage's output column is identified by
// PROVENANCE — which producer slot it came from — and a consumer binds by that
// identity, never by re-resolving a bare name against an arm's raw scan
// columns. Every Want below was measured on live postgres:17-alpine over rows
// identical to the `decpair` / `oldtab` / type-matrix fixtures, and every arm
// is asserted because each of these was right on some arm and wrong on
// another.
//
//   - #780 a join arm of BARE SCANS publishing a COMPUTED column answered the
//     enclosing query the arm's RAW column: `g.a * 3 AS a` was computed by no
//     stage at all on the DAG, so `m.a` bound `decpair.a` and the query
//     returned 12.75 where PostgreSQL returns 38.25. Silent, both DAG arms.
//   - #770 two join arms publishing ONE alias: the rename `a AS w` was
//     materialized nowhere, so a `GROUP BY x.w` over arms that both publish
//     `w` reached the worker as a key nothing emits.
//   - #745 a window PARTITIONED BY a STORED `__winkey_1` column was refused on
//     both DAG arms because the guard read the NAME rather than asking which
//     producer supplies the column.
//   - #480 a non-equi join fed by two aggregates was refused at plan time for
//     a co-location it does not need, and the build it DOES need replicated
//     was asked for nowhere.
//   - #796 a window one nesting level above its scan declared float8 where the
//     same window directly over the table declares numeric.
//   - #644 a distributed ORDER BY on a container column tied on every row.
//
// The declared TYPE is asserted beside the rows wherever the two paths ever
// disagreed about it: a right value under a wrong OID is a defect a value
// oracle cannot see (ADR-0012).

// f1Arm is one execution arm: a name and a runner that renders the answer.
type f1Arm struct {
	name  string
	run   func(sql string) (string, error)
	coord *Coordinator
}

// f1Arms stands up the four arms over the shared type-matrix corpus plus the
// reserved-name table `oldtab`, which only the catalog door can create.
func f1Arms(t *testing.T, ctx context.Context) []f1Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
	rsWriteOldTable(t, ctx, infra, infraB)
	rsIngestOldTable(t, ctx, single)
	rsIngestOldTable(t, ctx, spilled)
	return []f1Arm{
		{"single", func(s string) (string, error) { return f1RenderSingle(ctx, single, s) }, nil},
		{spilledArm, func(s string) (string, error) { return f1RenderSingle(ctx, spilled, s) }, nil},
		{"dag", func(s string) (string, error) { return f1RenderDAG(ctx, dag, s) }, dag},
		{"dagshuf", func(s string) (string, error) { return f1RenderDAG(ctx, shuf, s) }, shuf},
	}
}

// f1Case is one shape: the SQL, PostgreSQL 17's answer as f1Render writes it,
// and per-arm overrides for a divergence this arc PINS rather than fixes.
type f1Case struct {
	name string
	sql  string
	want string
	// pin overrides want for the named arm, with the mechanism in `why`.
	pin map[string]string
	why string
	// routed is the local-route counter delta each DAG arm is expected to
	// move, keyed by arm name — "" or absent meaning the arm EXECUTED the
	// query as stages. Rows alone cannot tell those apart, and both are
	// results this arc moves, so the disposition is asserted per cell.
	routed map[string]string
}

func f1Run(t *testing.T, arms []f1Arm, cases []f1Case) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := f1Counters(arm.coord)
				got, err := arm.run(tc.sql)
				if err != nil {
					got = "ERR " + err.Error()
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				// A pinned REFUSAL is matched by prefix: the message carries a
				// byte count that moves with the corpus, and what the pin
				// claims is WHICH refusal, not how many bytes were tracked.
				// A VALUE is matched exactly.
				ok := got == want
				if !ok && strings.HasPrefix(want, "ERR ") {
					ok = strings.HasPrefix(got, want)
				}
				if !ok {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s",
						tc.sql, arm.name, got, want, f1Why(tc.why))
				}
				// Rows alone cannot tell an EXECUTED query from one the DAG
				// refused and the coordinator answered locally, and both are
				// results this arc moves.
				if arm.coord != nil {
					moved := f1CounterDelta(before, f1Counters(arm.coord))
					if moved != tc.routed[arm.name] {
						t.Errorf("%s\n  arm %s disposition: got %q, want %q",
							tc.sql, arm.name, moved, tc.routed[arm.name])
					}
				}
			}
		})
	}
}

func f1Why(why string) string {
	if why == "" {
		return ""
	}
	return "\n  pinned: " + why
}

// f1RouteCounters is every local-route counter this gate asserts.
var f1RouteCounters = []struct {
	name string
	fn   func(*Coordinator) int64
}{
	{"group key", (*Coordinator).GroupKeyLocalRoutes},
	{"unreachable output", (*Coordinator).UnreachableOutputLocalRoutes},
	{"scalar projection", (*Coordinator).ScalarProjectionLocalRoutes},
}

func f1Counters(c *Coordinator) []int64 {
	if c == nil {
		return nil
	}
	out := make([]int64, len(f1RouteCounters))
	for i, rc := range f1RouteCounters {
		out[i] = rc.fn(c)
	}
	return out
}

func f1CounterDelta(before, after []int64) string {
	var moved []string
	for i := range before {
		if after[i] != before[i] {
			moved = append(moved, fmt.Sprintf("%s +%d", f1RouteCounters[i].name, after[i]-before[i]))
		}
	}
	return strings.Join(moved, ", ")
}

// f1RenderSingle and f1RenderDAG write one result as `cols=[name:TYPE …]
// rows=N | r0 | r1 …`, POSITIONALLY: a result may legally carry two columns of
// one name, and the declared TYPE is part of the answer.
func f1RenderSingle(ctx context.Context, db *wadjet.DB, sql string) (string, error) {
	out, err := db.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("cols=[")
	for i, c := range out.Columns {
		if i > 0 {
			b.WriteString(" ")
		}
		ty := "?"
		if i < len(out.ColumnMetas) {
			m := out.ColumnMetas[i]
			ty = m.TypeName
			if m.Precision > 0 {
				ty = fmt.Sprintf("%s(%d,%d)", ty, m.Precision, m.Scale)
			}
		}
		fmt.Fprintf(&b, "%s:%s", c, ty)
	}
	fmt.Fprintf(&b, "] rows=%d", len(out.Rows))
	for i := range out.Rows {
		b.WriteString(" | ")
		f1WriteRow(&b, out.Cells(i))
	}
	return b.String(), nil
}

func f1RenderDAG(ctx context.Context, c *Coordinator, sql string) (string, error) {
	out, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return "", err
	}
	if out.Error != "" {
		return "", fmt.Errorf("%s", out.Error)
	}
	var b strings.Builder
	b.WriteString("cols=[")
	sc := out.OutputSchema()
	for i, col := range out.Columns {
		if i > 0 {
			b.WriteString(" ")
		}
		ty := "?"
		if i < len(sc) {
			ty = f1TypeName(sc[i])
		}
		fmt.Fprintf(&b, "%s:%s", col, ty)
	}
	var cells [][]any
	s := out.Stream()
	defer s.Close()
	for {
		bb, err := s.Next(ctx)
		if err != nil {
			return "", err
		}
		if bb == nil {
			break
		}
		cells = append(cells, bb.ToRowValues()...)
	}
	fmt.Fprintf(&b, "] rows=%d", len(cells))
	for _, r := range cells {
		b.WriteString(" | ")
		f1WriteRow(&b, r)
	}
	return b.String(), nil
}

// f1TypeName renders a declared column the way ColumnMeta.TypeName does, so
// the two paths' renderings are comparable strings.
func f1TypeName(c parquet.Column) string {
	ty := strings.ToUpper(c.Type.String())
	if c.Precision > 0 {
		return fmt.Sprintf("%s(%d,%d)", ty, c.Precision, c.Scale)
	}
	return ty
}

func f1WriteRow(b *strings.Builder, cells []any) {
	for i, c := range cells {
		if i > 0 {
			b.WriteString(",")
		}
		if c == nil {
			b.WriteString("NULL")
			continue
		}
		fmt.Fprintf(b, "%v", c)
	}
}
