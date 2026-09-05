package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// NAMES AND SCOPES 2 — the value gate for arc E3, on FOUR arms.
//
// Five mechanisms, one fixture, one shape of assertion: every Want below was
// measured on live postgres:17-alpine over the same rows, and every arm is
// asserted because each of these was right on some arm and wrong on another.
//
//   - #851 a bare GROUP BY name binds the INPUT column, PostgreSQL's
//     precedence, INSIDE a derived table and a CTE as well as at the top
//     level. The precedence is decided by the binder, which had validated a
//     tree the logical builder then threw away, so a nested block was planned
//     from the parser's provisional substitution: ONE row where PostgreSQL
//     answers six, on every arm and in silence.
//   - #785 an aggregate ALIASED like a group key, beside a HAVING on that
//     aggregate. The aggregate's output and the key answered to ONE name and
//     the HAVING bound the key: zero rows for PostgreSQL's eight.
//   - #787 an ORDER BY over a DERIVED table's aggregate, refused loudly
//     because the walk that asks "is this term spellable over my grouping"
//     descended into somebody else's grouping.
//   - #797 a DISTINCT over a SELECT list holding an aggregate or window CALL,
//     recorded as a group key under the text the query wrote rather than the
//     slot the operator below publishes — refused and routed on both DAG arms.
//   - #629 a duplicate output name referenced by two key references AND an
//     aggregate: the cross-class M<N collision, asserted POSITIONALLY,
//     because a result keyed by name cannot hold three columns called
//     n_regionkey.
//
// The routing counters are asserted beside the rows wherever the disposition
// is a route: rows alone cannot tell "the DAG executed it" from "the DAG
// refused it and the coordinator answered locally", and both of those are
// results this arc MOVES.
type e3Arm struct {
	name  string
	run   func(sql string) ([]string, [][]any, error)
	coord *Coordinator
}

func e3Arms(t *testing.T, ctx context.Context) []e3Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	single := tmdStandalone(t, ctx)
	// The SPILLED arm is the fourth: a group-key or HAVING defect that lives
	// in the merge of drained partial state is invisible to the in-memory one
	// (ADR-0027).
	spilled := e3BudgetedStandalone(t, ctx)
	return []e3Arm{
		{"single", func(sql string) ([]string, [][]any, error) { return e3PosSingle(ctx, single, sql) }, nil},
		{spilledArm, func(sql string) ([]string, [][]any, error) { return e3PosSingle(ctx, spilled, sql) }, nil},
		{"dag", func(sql string) ([]string, [][]any, error) { return e3PosDAG(ctx, dag, sql) }, dag},
		{"dagshuf", func(sql string) ([]string, [][]any, error) { return e3PosDAG(ctx, shuf, sql) }, shuf},
	}
}

// e3ArmBudget is the memory budget every SPILLED arm in this arc runs at, and
// e3ArmBudgetName is how a gate spells it in an arm name or a pin.
//
// It is named rather than inlined because a pinned spilled answer is a claim
// ABOUT the budget: a spill is a CONDITION, not a query shape (ADR-0027), so
// raising this number makes the same query stop spilling and answer what the
// other arms answer — which would read as a fix and delete a pin that still
// holds. Anything that changes it has to change the pins with it.
const (
	e3ArmBudget     = 512 * 1024
	e3ArmBudgetName = "512 KiB budget"
)

// spilledArm is the key a per-arm pin uses for that arm. It carries the budget
// so the pin says at what the answer was measured.
var spilledArm = "spilled (" + e3ArmBudgetName + ")"

// e3BudgetedStandalone is the single-process engine under e3ArmBudget, over
// the same fixture tmdStandalone loads.
func e3BudgetedStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: e3ArmBudget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		// A reserved-name fixture goes in through the CATALOG — the DDL door
		// refuses its schema, which is the point of it (see tmdStandalone).
		if tmdStoresAReservedName(tbl) {
			if err := db.Catalog().CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
				t.Fatalf("create %s: %v", tbl.name, err)
			}
			raw := ingest.New(db.Catalog(), tbl.name, tbl.schema, nil, ingest.Config{
				MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
			})
			if err := raw.Ingest(ctx, tbl.rows); err != nil {
				t.Fatalf("ingest %s: %v", tbl.name, err)
			}
			if err := raw.FlushAll(ctx); err != nil {
				t.Fatalf("flush %s: %v", tbl.name, err)
			}
			continue
		}
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// e3PosSingle and e3PosDAG read a result POSITIONALLY. A map keyed by column
// name cannot hold two columns of one name, and #629's whole shape is three
// outputs called n_regionkey.
func e3PosSingle(ctx context.Context, db *wadjet.DB, sql string) ([]string, [][]any, error) {
	out, err := db.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	cells := make([][]any, len(out.Rows))
	for i := range out.Rows {
		cells[i] = out.Cells(i)
	}
	return out.Columns, cells, nil
}

func e3PosDAG(ctx context.Context, c *Coordinator, sql string) ([]string, [][]any, error) {
	out, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	if out.Error != "" {
		return nil, nil, fmt.Errorf("%s", out.Error)
	}
	cols := out.Columns
	var cells [][]any
	s := out.Stream()
	defer s.Close()
	for {
		b, err := s.Next(ctx)
		if err != nil {
			return cols, cells, err
		}
		if b == nil {
			return cols, cells, nil
		}
		cells = append(cells, b.ToRowValues()...)
	}
}

// e3Render is one result as a comparable string: "cols | r0c0,r0c1 | r1c0,…".
func e3Render(cols []string, rows [][]any) string {
	var b strings.Builder
	b.WriteString(strings.Join(cols, ","))
	for _, r := range rows {
		b.WriteString(" | ")
		for i, c := range r {
			if i > 0 {
				b.WriteString(",")
			}
			if c == nil {
				b.WriteString("NULL")
				continue
			}
			fmt.Fprintf(&b, "%v", c)
		}
	}
	return b.String()
}

// e3SortedRender is e3Render with the ROWS sorted, so a result can be compared
// as a multiset when only its order is in question.
func e3SortedRender(cols []string, rows [][]any) string {
	return e3SortLines(e3Render(cols, rows))
}

// e3SortLines sorts the row segments of an e3Render string, keeping the column
// header first.
func e3SortLines(rendered string) string {
	parts := strings.Split(rendered, " | ")
	if len(parts) < 2 {
		return rendered
	}
	head, body := parts[0], append([]string(nil), parts[1:]...)
	sort.Strings(body)
	return head + " | " + strings.Join(body, " | ")
}

// e3Case is one shape: the SQL, PostgreSQL 17's answer rendered by e3Render,
// and whether the DAG arms are expected to ROUTE the query to the
// coordinator's local pipeline instead of executing it as stages.
// e3RouteCounters is every local-route counter this gate asserts. A shape
// whose disposition is EXECUTION must move none of them; rows alone cannot
// tell an executed query from a refused-and-routed one.
var e3RouteCounters = []struct {
	name string
	fn   func(*Coordinator) int64
}{
	{"group key", (*Coordinator).GroupKeyLocalRoutes},
	{"unreachable output", (*Coordinator).UnreachableOutputLocalRoutes},
	{"scalar projection", (*Coordinator).ScalarProjectionLocalRoutes},
}

type e3Case struct {
	name  string
	sql   string
	want  string
	route func(*Coordinator) int64 // nil = the DAG must EXECUTE it
	// pin records what an arm answers TODAY where that is not PostgreSQL's
	// answer, keyed by arm name. Each entry names a defect OUTSIDE this arc's
	// mechanisms, reached by a shape this arc's own cell needs; the comment
	// beside it carries the mechanism. A pin that starts AGREEING fails, so
	// deleting it is the next fix's proof.
	pin map[string]string
	// pinOrder names the arms whose ROWS are PostgreSQL's but whose ORDER is
	// not. It is a pin on the ORDER alone, and it exists because pinning the
	// wrong order as a STRING would pin an unordered result: the sequence a
	// DAG arm returns when the sort key it was given is the wrong one is not
	// stable across runs, so the assertion has to be "the multiset is right
	// AND the sequence is not" rather than any particular sequence. It fails
	// the day the arm sorts correctly, which is what makes it a pin.
	pinOrder map[string]bool
}

func TestArcE3NamesAndScopesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	// The eight groups of `GROUP BY g + 1` over the type matrix: g = i%7 with
	// a NULL every thirteenth row, so keys 1..7 plus a NULL group.
	gk := map[int64]int64{}
	nullN := int64(0)
	for _, r := range typematrix.Data(typematrix.Rows) {
		if g, ok := r["g"].(int32); ok {
			gk[int64(g)+1]++
		} else {
			nullN++
		}
	}
	eight := func(render func(k, n int64) string) string {
		parts := make([]string, 0, 8)
		for k := int64(1); k <= 7; k++ {
			parts = append(parts, render(k, gk[k]))
		}
		parts = append(parts, render(-1, nullN))
		return strings.Join(parts, " | ")
	}
	// `k, "g + 1"` for the #785 shape, NULL key last (ORDER BY k puts NULLs
	// last, which is PostgreSQL's default for ASC).
	want785 := "k,g + 1 | " + eight(func(k, n int64) string {
		if k < 0 {
			return fmt.Sprintf("NULL,%d", n)
		}
		return fmt.Sprintf("%d,%d", k, n)
	})

	cases := []e3Case{
		// --- #851 -------------------------------------------------------
		// `GROUP BY g` beside `g*0 AS g`: the alias and the input column
		// share the name, and PostgreSQL binds the INPUT column — six groups
		// over the six rows, each projecting 0. The top-level spelling was
		// already right (#739); the derived and CTE ones bound the alias and
		// answered ONE row.
		{name: "851/bare", sql: "SELECT g*0 AS g, COUNT(*) AS n FROM typemx WHERE id < 6 " +
			"GROUP BY g ORDER BY 2, 1",
			want: "g,n | 0,1 | 0,1 | 0,1 | 0,1 | 0,1 | 0,1",
			// The DAG ROUTES this shape and did so before this arc: no stage
			// computes `g*0` over the aggregate's output, so the gather's
			// SELECT list is unreachable and the coordinator answers it on its
			// local pipeline. Asserted so a right→routed move — or the reverse
			// — is visible rather than invisible behind the rows.
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		{name: "851/derived", sql: "SELECT x.g, x.n FROM (SELECT g*0 AS g, COUNT(*) AS n " +
			"FROM typemx WHERE id < 6 GROUP BY g) x ORDER BY 2, 1",
			want: "g,n | 0,1 | 0,1 | 0,1 | 0,1 | 0,1 | 0,1"},
		{name: "851/derived-star", sql: "SELECT * FROM (SELECT g*0 AS g, COUNT(*) AS n " +
			"FROM typemx WHERE id < 6 GROUP BY g) x ORDER BY 2, 1",
			want:  "g,n | 0,1 | 0,1 | 0,1 | 0,1 | 0,1 | 0,1",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		{name: "851/cte", sql: "WITH x AS (SELECT g*0 AS g, COUNT(*) AS n FROM typemx " +
			"WHERE id < 6 GROUP BY g) SELECT g, n FROM x ORDER BY 2, 1",
			want: "g,n | 0,1 | 0,1 | 0,1 | 0,1 | 0,1 | 0,1"},
		{name: "851/nested-derived", sql: "SELECT y.g FROM (SELECT x.g FROM (SELECT g*0 AS g, " +
			"COUNT(*) AS n FROM typemx WHERE id < 6 GROUP BY g) x) y ORDER BY 1",
			want: "g | 0 | 0 | 0 | 0 | 0 | 0"},
		// The QUALIFIED spelling of the same key: `GROUP BY t.g` never was a
		// bare alias reference, so it binds the column either way.
		{name: "851/qualified", sql: "SELECT g*0 AS g, COUNT(*) AS n FROM typemx t " +
			"WHERE id < 6 GROUP BY t.g ORDER BY 2, 1",
			want:  "g,n | 0,1 | 0,1 | 0,1 | 0,1 | 0,1 | 0,1",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		// The OTHER TWO binding rules, on the same fixture, because a fix
		// that moved GROUP BY's rule onto them would be a new wrong answer.
		// ORDER BY prefers the OUTPUT alias — the opposite precedence — so
		// the rows come back in -g order, not in g order.
		{name: "851/order-by-prefers-the-output-alias",
			sql:   "SELECT -g AS g, COUNT(*) AS n FROM typemx WHERE id < 6 GROUP BY g ORDER BY g",
			want:  "g,n | -5,1 | -4,1 | -3,1 | -2,1 | -1,1 | 0,1",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		// PINNED on the DAG arms, and the divergence is NOT this arc's: a
		// derived table whose SELECT alias SHADOWS the input column it is
		// computed from, with its own inner ORDER BY on that name, publishes
		// the INPUT column's values on both DAG arms — 0..5 rather than
		// -5..0 — so the alias is resolved against the scan instead of
		// against the projection. The single-process path answers
		// PostgreSQL's rows. The shape is here because the ORDER BY half of
		// #851's rule needs a derived spelling; the wrong VALUES are a
		// separate mechanism in the DAG's derived-alias resolution.
		{name: "851/order-by-prefers-the-output-alias-in-a-derived-table",
			sql:  "SELECT x.g FROM (SELECT -g AS g FROM typemx WHERE id < 6 ORDER BY g) x",
			want: "g | -5 | -4 | -3 | -2 | -1 | 0",
			pin: map[string]string{
				"dag":     "g | 0 | 1 | 2 | 3 | 4 | 5",
				"dagshuf": "g | 0 | 1 | 2 | 3 | 4 | 5",
			}},
		// HAVING binds the INPUT column, like GROUP BY and unlike ORDER BY:
		// `HAVING g > 2` keeps the groups whose INPUT g is 3, 4, 5.
		{name: "851/having-binds-the-input-column",
			sql:   "SELECT -g AS g, COUNT(*) AS n FROM typemx WHERE id < 6 GROUP BY g HAVING g > 2 ORDER BY 1",
			want:  "g,n | -5,1 | -4,1 | -3,1",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		// The DAG answered these three in their BYTE order until 2026-09-04:
		// `Stage.GroupByTypes` holds the DERIVED keys only, so a projection
		// over a BARE key had no declaration, `-g` took the STRING fallback,
		// and the sort key materialized into a text vector. The rows were
		// always PostgreSQL's; the SEQUENCE was not.
		{name: "851/having-binds-the-input-column-in-a-derived-table",
			sql: "SELECT x.g FROM (SELECT -g AS g, COUNT(*) AS n FROM typemx WHERE id < 6 " +
				"GROUP BY g HAVING g > 2) x ORDER BY 1",
			want: "g | -5 | -4 | -3"},
		// The plain spelling of the same shape — a shadowing alias over a
		// bare key, ordered from OUTSIDE the derived table. It was RIGHT on
		// the DAG before this arc and the nested GROUP BY binding moved it
		// onto the byte-ordered path, which is why it is driven here: a
		// right-to-wrong move needs a cell of its own, not the sibling's pin.
		{name: "851/derived-group-by-with-a-negated-shadowing-alias",
			sql: "SELECT x.g FROM (SELECT -g AS g, COUNT(*) AS n FROM typemx WHERE id < 6 " +
				"GROUP BY g) x ORDER BY 1",
			want: "g | -5 | -4 | -3 | -2 | -1 | 0"},
		// …and the same query whose alias shadows NOTHING, which was
		// byte-ordered on the DAG at base too. Both spellings are one
		// declaration.
		{name: "851/derived-group-by-with-a-negated-alias",
			sql: "SELECT x.k FROM (SELECT -g AS k, COUNT(*) AS n FROM typemx WHERE id < 6 " +
				"GROUP BY g) x ORDER BY 1",
			want: "k | -5 | -4 | -3 | -2 | -1 | 0"},
		// The two binary spellings of the same value, which were already
		// right: they are the control that says the declaration is what
		// moved, not the sort.
		{name: "851/ctl-derived-group-by-with-a-multiplied-alias",
			sql: "SELECT x.g FROM (SELECT g*-3 AS g, COUNT(*) AS n FROM typemx WHERE id < 6 " +
				"GROUP BY g) x ORDER BY 1",
			want: "g | -15 | -12 | -9 | -6 | -3 | 0"},
		{name: "851/ctl-derived-group-by-with-a-subtracted-alias",
			sql: "SELECT x.g FROM (SELECT 0-g AS g, COUNT(*) AS n FROM typemx WHERE id < 6 " +
				"GROUP BY g) x ORDER BY 1",
			want: "g | -5 | -4 | -3 | -2 | -1 | 0"},
		// The CONTROL that keeps the precedence from swallowing #739's own
		// case: an alias that names NO input column still binds the alias,
		// so this is ONE group over the six rows.
		{name: "851/ctl-an-unambiguous-alias-still-binds-the-alias",
			sql:  "SELECT g*0 AS kk, COUNT(*) AS n FROM typemx WHERE id < 6 GROUP BY kk ORDER BY 1",
			want: "kk,n | 0,6"},
		{name: "851/ctl-an-unambiguous-alias-in-a-derived-table",
			sql: "SELECT x.kk, x.n FROM (SELECT g*0 AS kk, COUNT(*) AS n FROM typemx " +
				"WHERE id < 6 GROUP BY kk) x ORDER BY 1",
			want: "kk,n | 0,6"},

		// --- #785 -------------------------------------------------------
		// The headline: an aggregate aliased like the COMPUTED key, with a
		// HAVING on the aggregate. Zero rows on all four arms before.
		{name: "785/aggregate-aliased-like-a-computed-key",
			sql: `SELECT g + 1 AS k, COUNT(*) AS "g + 1" FROM typemx GROUP BY g + 1 ` +
				"HAVING COUNT(*) > 100 ORDER BY k",
			want: want785},
		// The same collision with a PLAIN-COLUMN key, which is where the
		// ladder is the instrument: every group has 80 rows, so `> 0`,
		// `> 1` and `> 79` must all keep three groups. Bound to the key
		// (0,1,2) they keep 2, 1 and 0.
		{name: "785/aggregate-aliased-like-a-plain-key/gt0",
			sql:  "SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g HAVING COUNT(*) > 0 ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/aggregate-aliased-like-a-plain-key/gt1",
			sql:  "SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g HAVING COUNT(*) > 1 ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/aggregate-aliased-like-a-plain-key/gt79",
			sql:  "SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g HAVING COUNT(*) > 79 ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		// The key is NOT in the select list, so nothing else asks for the
		// colliding name — and the gather resolved the aggregate's rename to
		// the KEY's column on both DAG arms, silently.
		{name: "785/aggregate-aliased-like-a-key-not-selected",
			sql:  "SELECT COUNT(*) AS g, MIN(id) AS m FROM typemx WHERE id < 6 GROUP BY g ORDER BY m",
			want: "g,m | 1,0 | 1,1 | 1,2 | 1,3 | 1,4 | 1,5"},
		// The mirror: the aggregate is aliased like the key and the key is
		// selected FIRST, so the key reference must still take the key.
		{name: "785/key-first-aggregate-aliased-like-it",
			sql:  "SELECT g AS x, SUM(h) AS g FROM collslot GROUP BY g HAVING SUM(h) > 0 ORDER BY x",
			want: "x,g | 0,120 | 1,120 | 2,120"},
		// The three CONTROLS the cell is bounded by, each right before.
		{name: "785/ctl-no-collision",
			sql: "SELECT g + 1 AS k, COUNT(*) AS c FROM typemx GROUP BY g + 1 " +
				"HAVING COUNT(*) > 100 ORDER BY k",
			want: "k,c | " + eight(func(k, n int64) string {
				if k < 0 {
					return fmt.Sprintf("NULL,%d", n)
				}
				return fmt.Sprintf("%d,%d", k, n)
			})},
		{name: "785/ctl-having-on-the-key-instead",
			sql: `SELECT g + 1 AS k, COUNT(*) AS "g + 1" FROM typemx GROUP BY g + 1 ` +
				"HAVING g + 1 > 2 ORDER BY k",
			want: fmt.Sprintf("k,g + 1 | 3,%d | 4,%d | 5,%d | 6,%d | 7,%d",
				gk[3], gk[4], gk[5], gk[6], gk[7])},
		// NESTED. A wrapper — one derived table — turns an aggregate output
		// into a plain column reference for the block above, and the gather
		// pairs a duplicate source name with the column of its own CLASS. It
		// was asking `OutputRename.IsAgg`, a property of the WRAPPER's item,
		// so both DAG arms answered the KEY under the aggregate's name.
		{name: "785/nested-in-a-derived-table",
			sql: "SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM collslot " +
				"GROUP BY g HAVING COUNT(*) > 0) u ORDER BY u.x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/nested-in-a-derived-table-no-having",
			sql: "SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM collslot " +
				"GROUP BY g) u ORDER BY u.x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		// The WRAPPED family, PINNED until arc F4 and now asserted. Two
		// different mechanisms wore one name here, and measuring them apart
		// is what closed them:
		//
		//   - TWO DERIVED TABLES put no fragment projection anywhere. The
		//     stage list is scan → final_aggregate → gather, measured, so
		//     ADR-0026 §3a's "a second wrapper puts one there just as a CTE
		//     does" was not the mechanism. What was wrong is the CLASS:
		//     `renameIsAggregateOutput` walks to the projection that defines
		//     the name, and a SELECT item written with NO ALIAS — `SELECT
		//     u.g, u.x` — was invisible to the lookup, which asked only about
		//     `Alias`. The walk answered "not an aggregate output" and the
		//     gather paired both renames with the first column of the name,
		//     which is the KEY.
		//   - A CTE really does put a fragment projection there, and that
		//     projection resolved `g` by NAME against the aggregate's output,
		//     where the key and the count both answer to it, and took the
		//     first. It addresses the SLOT now (ProjectExprSpec.SourceSlot →
		//     exec.ProjectColumn.SourceIdx), which is the addressing the
		//     single-process projection has applied since #575.
		//
		// All three members stay driven, because keeping the whole family is
		// what tells the two mechanisms apart: the CTE over the collision, a
		// CTE over a DERIVED TABLE over it, and TWO derived tables over it
		// with no CTE at all. ONE derived wrapper — the cell above — was right
		// before and is the control for all three.
		{name: "785/nested-in-a-cte",
			sql: "WITH u AS (SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g " +
				"HAVING COUNT(*) > 0) SELECT g, x FROM u ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/nested-in-a-derived-table-inside-a-cte",
			sql: "WITH z AS (SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x " +
				"FROM collslot GROUP BY g HAVING COUNT(*) > 0) u) " +
				"SELECT g, x FROM z ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/nested-two-derived-tables-deep",
			sql: "SELECT z.g, z.x FROM (SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, " +
				"g AS x FROM collslot GROUP BY g HAVING COUNT(*) > 0) u) z ORDER BY z.x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		// The boundary attempted from both sides, so neither "one more
		// wrapper" nor "the two classes in the other order" is the next
		// spelling that is silently wrong. Both measured on PostgreSQL 17.
		{name: "785/nested-three-derived-tables-deep",
			sql: "SELECT y.g, y.x FROM (SELECT z.g, z.x FROM (SELECT u.g, u.x FROM " +
				"(SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g " +
				"HAVING COUNT(*) > 0) u) z) y ORDER BY y.x",
			want: "g,x | 80,0 | 80,1 | 80,2"},
		{name: "785/nested-in-a-cte-key-first",
			sql: "WITH u AS (SELECT g AS x, COUNT(*) AS g FROM collslot GROUP BY g " +
				"HAVING COUNT(*) > 0) SELECT x, g FROM u ORDER BY x",
			want: "x,g | 0,80 | 1,80 | 2,80"},
		// The ORDER BY face of the same collision, PINNED. `ORDER BY COUNT(*)`
		// over an aggregate aliased `g` resolves the sort key to what the
		// aggregate PUBLISHES — the name `g` — and the stage then sorts by
		// the first column of that name, which is the KEY. The rows are all
		// there and every value is right; the SEQUENCE is `x` descending.
		//
		// It is the same first-match rule the gather's pairing and the
		// fragment's projection each meet, at a third consumer, and closing
		// it means the sort key carrying the aggregate's POSITION —
		// `SortKeySpec.SlotPos` exists for the positional-ORDER-BY case
		// (#557) and nothing sets it here. Pre-existing: base answers the
		// same. The non-colliding control beside it is right on every arm,
		// which is what says the collision is the trigger.
		{name: "785/order-by-the-aggregate-under-a-colliding-alias",
			sql: "SELECT COUNT(*) AS g, g AS x FROM typemx GROUP BY g " +
				"ORDER BY COUNT(*) DESC, x",
			want: "g,x | 660,0 | 660,1 | 660,6 | 659,2 | 659,3 | 659,4 | 659,5 | 384,NULL",
			pin: map[string]string{
				"dag":     "g,x | 384,NULL | 660,6 | 659,5 | 659,4 | 659,3 | 659,2 | 660,1 | 660,0",
				"dagshuf": "g,x | 384,NULL | 660,6 | 659,5 | 659,4 | 659,3 | 659,2 | 660,1 | 660,0",
			}},
		{name: "785/ctl-order-by-the-aggregate-without-a-collision",
			sql: "SELECT COUNT(*) AS c, g AS x FROM typemx GROUP BY g " +
				"ORDER BY COUNT(*) DESC, x",
			want: "c,x | 660,0 | 660,1 | 660,6 | 659,2 | 659,3 | 659,4 | 659,5 | 384,NULL"},
		{name: "785/ctl-no-having",
			sql:  "SELECT COUNT(*) AS g, g AS x FROM collslot GROUP BY g ORDER BY x",
			want: "g,x | 80,0 | 80,1 | 80,2"},

		// --- #787 -------------------------------------------------------
		// An ORDER BY over a DERIVED table's aggregate. SUM(id) per g over
		// collslot's 240 rows: 9480, 9560, 9640, so the sort is a real one.
		// The DAG ROUTES: it can carry the ordering but no fragment computes
		// the term, which is refused at plan time now instead of failing
		// three dispatch attempts in.
		{name: "787/derived-aggregate-order-by-an-expression",
			sql: "SELECT d.g, d.s FROM (SELECT g, SUM(id) AS s FROM collslot GROUP BY g) d " +
				"ORDER BY d.s * 2",
			want:  "g,s | 0,9480 | 1,9560 | 2,9640",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		{name: "787/derived-aggregate-order-by-a-window-alias",
			sql: "SELECT d.g, d.s FROM (SELECT g, SUM(id) AS s, ROW_NUMBER() OVER (ORDER BY g) " +
				"AS rn FROM collslot GROUP BY g) d ORDER BY d.rn",
			want: "g,s | 0,9480 | 1,9560 | 2,9640"},
		{name: "787/derived-aggregate-order-by-a-key-expression",
			sql: "SELECT d.g FROM (SELECT g, SUM(id) AS s FROM collslot GROUP BY g) d " +
				"ORDER BY d.g * -1",
			want:  "g | 2 | 1 | 0",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		// The pre-existing sibling the same refusal fixes: a DISTINCT lowers
		// to an aggregate, so its derived table has one too.
		{name: "787/derived-distinct-order-by-an-expression",
			sql: "SELECT u.k FROM (SELECT DISTINCT id AS k, g AS v FROM typemx WHERE id < 5) u " +
				"ORDER BY u.k * 2",
			want:  "k | 0 | 1 | 2 | 3 | 4",
			route: (*Coordinator).UnreachableOutputLocalRoutes},
		// The CONTROL: the same query without the arithmetic keys on a
		// column the aggregate emits, and stays on the DAG.
		{name: "787/ctl-order-by-the-aggregate-output-itself",
			sql: "SELECT d.g, d.s FROM (SELECT g, SUM(id) AS s FROM collslot GROUP BY g) d " +
				"ORDER BY d.s",
			want: "g,s | 0,9480 | 1,9560 | 2,9640"},

		// --- #797 -------------------------------------------------------
		// A DISTINCT over a SELECT list holding an aggregate or a window
		// CALL. Both ran on the coordinator's local pipeline before, because
		// the lowering recorded the CALL as the group key; they run as
		// stages now, which is what the absent route assertion says.
		{name: "797/distinct-over-a-wrapped-aggregate",
			sql: "SELECT DISTINCT g, COUNT(*) + 0 AS w FROM typemx GROUP BY g ORDER BY g",
			want: "g,w | " + strings.Join([]string{
				fmt.Sprintf("0,%d", gk[1]), fmt.Sprintf("1,%d", gk[2]),
				fmt.Sprintf("2,%d", gk[3]), fmt.Sprintf("3,%d", gk[4]),
				fmt.Sprintf("4,%d", gk[5]), fmt.Sprintf("5,%d", gk[6]),
				fmt.Sprintf("6,%d", gk[7]), fmt.Sprintf("NULL,%d", nullN),
			}, " | ")},
		{name: "797/distinct-over-a-wrapped-window",
			sql:  "SELECT DISTINCT id, SUM(g) OVER () + 0 AS w FROM typemx WHERE id < 4 ORDER BY id",
			want: "id,w | 0,6 | 1,6 | 2,6 | 3,6"},
		{name: "797/distinct-over-a-wrapped-aggregate-inside-a-derived-table",
			sql: "SELECT COUNT(*) AS c FROM (SELECT DISTINCT g, COUNT(*) + 0 AS w FROM typemx " +
				"GROUP BY g) u",
			want: "c | 8"},

		// --- #769, the JOIN KEY face -------------------------------------
		// A ROW field path as a join KEY. `c_row.b` looks like a qualified
		// column to the pass that decides which ON conjuncts the executor can
		// represent, so it stayed in JoinCond as a key pair — and the
		// executor matches on column NAMES, so `c_row.b` resolved to nothing
		// and every probe row matched every build row. ~10,000 rows of
		// `id, NULL` on single, spilled and broadcast, and
		// `partitioned shuffle: key "c_row.b" not in schema` on the shuffled
		// arm, where PostgreSQL 17 (spelled `ON (n.c_row).b = d.b`) answers
		// ONE row. Silent on three of the four, and pre-existing at base.
		{name: "769/a-field-path-as-a-join-key",
			sql: "SELECT n.id AS nid, d.b AS db FROM typemx_nested n JOIN decpair d " +
				"ON c_row.b = d.b ORDER BY n.id",
			want: "nid,db | 0,0.0000"},
		// PostgreSQL's own spelling of the same key.
		{name: "769/a-field-path-as-a-join-key-parenthesised",
			sql: "SELECT n.id AS nid, d.b AS db FROM typemx_nested n JOIN decpair d " +
				"ON (c_row).b = d.b ORDER BY n.id",
			want: "nid,db | 0,0.0000"},
		// The INSTRUMENT that localised it, and now the CONTROL: the same
		// path inside an ARITHMETIC operand was already right on every arm,
		// because the arithmetic made the ON-conjunct pass decline and the
		// residual route materialized the path. The fix is to decline the
		// BARE path the same way, so these two must agree.
		{name: "769/ctl-a-field-path-in-an-arithmetic-join-key",
			sql: "SELECT n.id AS nid, d.b AS db FROM typemx_nested n JOIN decpair d " +
				"ON c_row.b + 0 = d.b ORDER BY n.id",
			want: "nid,db | 0,0.0000"},
		// The CONTROL that bounds the decline: an ordinary QUALIFIED column as
		// a join key is not a field path and must stay a key pair. `n.id` is
		// not a container's field, and routing it to the residual path would
		// turn every keyed join in the corpus into a cross product.
		{name: "769/ctl-an-ordinary-qualified-join-key",
			sql:  "SELECT COUNT(*) AS n FROM typemx_nested n JOIN decpair d ON n.id = d.id",
			want: "n | 9"},
		// --- #629 -------------------------------------------------------
		// One output name referenced by TWO key references AND an aggregate.
		// Asserted positionally: the DAG used to return TWO columns for this
		// three-item SELECT list, a rename-only degradation.
		{name: "629/cross-class-duplicate-name",
			sql: "SELECT COUNT(*) AS n_regionkey, g AS n_regionkey, g AS n_regionkey " +
				"FROM collslot GROUP BY g ORDER BY 3",
			want: "n_regionkey,n_regionkey,n_regionkey | 80,0,0 | 80,1,1 | 80,2,2"},
		// The VALUES are right on every arm and the four columns are all
		// there; what is PINNED is the DAG's ORDER. A POSITIONAL ORDER BY
		// with TWO keys over duplicate output names sorts by the last key
		// only — `sortKeySlotPos` maps a position to a slot for a Sort whose
		// child is the projection itself, and the DAG's sort stage reads a
		// STAGE's output instead, which is the residual the naming arc
		// self-flagged (#557's own note). One key (the entry above) is right.
		{name: "629/cross-class-duplicate-name-two-keys",
			sql: "SELECT COUNT(*) AS c, g AS c, g AS c, h AS c FROM collslot " +
				"GROUP BY g, h ORDER BY 2, 4",
			want: "c,c,c,c | 20,0,0,0 | 20,0,0,1 | 20,0,0,2 | 20,0,0,3 | " +
				"20,1,1,0 | 20,1,1,1 | 20,1,1,2 | 20,1,1,3 | " +
				"20,2,2,0 | 20,2,2,1 | 20,2,2,2 | 20,2,2,3",
			pinOrder: map[string]bool{"dag": true, "dagshuf": true}},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				var before int64
				beforeAll := map[string]int64{}
				if arm.coord != nil {
					if c.route != nil {
						before = c.route(arm.coord)
					}
					for _, rc := range e3RouteCounters {
						beforeAll[rc.name] = rc.fn(arm.coord)
					}
				}
				cols, rows, err := arm.run(c.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				got := e3Render(cols, rows)
				// checkRoutes asserts the routing counters, and it runs for a
				// PINNED arm too. A pin says "these rows are not PostgreSQL's
				// and here is why"; it cannot also say which pipeline produced
				// them, and routing a pinned shape to the coordinator's local
				// pipeline would make its rows RIGHT — the pin would fail with
				// "it is fixed", naming a fix that never happened. So the
				// counters are asserted on the same arms and in the same
				// direction as for an unpinned cell: the pin holds a WRONG
				// answer from the DAG, not the absence of one.
				checkRoutes := func() {
					if arm.coord == nil {
						return
					}
					if c.route != nil {
						if c.route(arm.coord) == before {
							t.Errorf("%s arm did NOT route the query local, and the rows can no "+
								"longer be attributed to the pipeline that produced them\n  SQL: %s",
								arm.name, c.sql)
						}
						return
					}
					// Every counter, so a refusal widened by accident shows here
					// rather than passing as "the rows are right".
					for _, rc := range e3RouteCounters {
						if rc.fn(arm.coord) != beforeAll[rc.name] {
							t.Errorf("%s arm routed the query local on the %s counter — the DAG "+
								"is refusing a shape it executes\n  SQL: %s", arm.name, rc.name, c.sql)
						}
					}
				}
				if c.pinOrder[arm.name] {
					if got == c.want {
						t.Errorf("the %s arm now answers PostgreSQL's rows IN ORDER for a "+
							"shape this gate pins as mis-ordered. It is fixed: assert it and "+
							"delete the pin\n  %s\n  SQL: %s", arm.name, got, c.sql)
					} else if sorted := e3SortedRender(cols, rows); sorted != e3SortLines(c.want) {
						t.Errorf("the %s arm answered a different SET of rows, not merely a "+
							"different ORDER\n  got  %s\n  want %s\n  SQL: %s",
							arm.name, sorted, e3SortLines(c.want), c.sql)
					}
					checkRoutes()
					continue
				}
				if pinned, ok := c.pin[arm.name]; ok {
					switch {
					case got == c.want:
						t.Errorf("the %s arm now answers PostgreSQL's rows for a shape this "+
							"gate PINS as divergent. It is fixed: assert it and delete the "+
							"pin\n  %s\n  SQL: %s", arm.name, got, c.sql)
					case got != pinned:
						t.Errorf("the %s arm answered\n  %s\nthis pin records\n  %s\nand "+
							"PostgreSQL 17 answers\n  %s\nThe answer MOVED without becoming "+
							"right, which is a change the next fix has to account "+
							"for\n  SQL: %s", arm.name, got, pinned, c.want, c.sql)
					}
					checkRoutes()
					continue
				}
				if got != c.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, c.want, c.sql)
				}
				checkRoutes()
			}
		})
	}
}
