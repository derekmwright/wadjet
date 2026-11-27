// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// #1678 line k: A DERIVED TABLE'S INNER NAMES ARE NOT THE ENCLOSING QUERY'S.
//
// PostgreSQL resolves a name written inside a derived table, a CTE body or a
// subquery in that block's own scope first, and the block shows the query
// around it ONE relation — its alias — carrying only its output columns. The
// optimizer's join-side attribution (logical.joinSidesScanInfo, read by the
// Filter-through-Join pushdown, the ON-clause split, the comma-join
// reorder and the edge membership) counted every SCAN under a derived table
// as a name of that join side: its own alias, its table name and its scan
// columns. So in
//
//	SELECT a.id AS l, b.id AS r FROM ej_t a
//	  JOIN (SELECT a.id, upper(a.s) AS k FROM ej_u a) b ON upper(a.s) = b.k
//
// the outer `a.s` looked like it belonged to the derived side (that side
// "had" an `a`), the whole ON was pushed into the derived table and compared
// ej_u's s with itself: a cross product on every arm. A bare `n` the derived
// table does not publish was attributed to it because its scan carries an
// `n`, and `… ON upper(a.s) = upper(b.bs) AND n > 1` filtered ej_u's n.
//
// The table is every cell ekCells builds (the collision kind, where the name
// is used inside the block, the outer consumer, the container, the join kind,
// and the filed statement 3's neighbours) on nine arms against
// testdata/arc_1678k_derived_scope_pg17.tsv,
// PostgreSQL 17.11's own answers (postgres:17-alpine, --locale=C
// --encoding=UTF8), rewritten by TestArc1678kMeasure (EK_PG_DSN). Rows are
// compared sorted, as text; column NAMES are not compared (a header is not
// this line's seam).

const ekAnswersPath = "testdata/arc_1678k_derived_scope_pg17.tsv"

type ekCell struct {
	name string
	sql  string
	ddl  bool
}

// ekKind is one way a name inside the derived table can meet a name outside
// it. o is the outer FROM item and q what the outer query qualifies it by;
// i is the inner FROM item, iq what the body qualifies it by; d is the
// derived table's alias.
type ekKind struct{ name, o, q, i, iq, d string }

var ekKinds = []ekKind{
	// inner ALIAS equals the outer alias
	{"aa", "ej_t a", "a", "ej_u a", "a", "b"},
	// inner alias equals the outer TABLE name
	{"at", "ej_t", "ej_t", "ej_u ej_t", "ej_t", "b"},
	// inner TABLE name (unaliased) equals the outer table name: a self-join
	// through a derived table
	{"tt", "ej_t", "ej_t", "ej_t", "ej_t", "b"},
	// inner alias equals the derived table's OWN alias
	{"ad", "ej_t a", "a", "ej_u b", "b", "b"},
	// inner table unaliased, outer aliased: no collision of names
	{"ta", "ej_t a", "a", "ej_u", "ej_u", "b"},
	// no collision (control)
	{"none", "ej_t a", "a", "ej_u c", "c", "b"},
}

func ekCells() []ekCell {
	var out []ekCell
	ddl := func(name, sql string) { out = append(out, ekCell{name: name, sql: sql, ddl: true}) }
	add := func(name, sql string) { out = append(out, ekCell{name: name, sql: sql}) }
	ddl("ddl/t", "CREATE TABLE ej_t (id BIGINT, s TEXT, n BIGINT)")
	ddl("ddl/u", "CREATE TABLE ej_u (id BIGINT, s TEXT, n BIGINT)")
	ddl("ddl/v", "CREATE TABLE ej_v (id BIGINT, s TEXT)")
	ddl("ddl/ti", "INSERT INTO ej_t VALUES (1,'a',1),(2,'b',2),(3,'c',3),(4,'A',1),(5,NULL,2)")
	ddl("ddl/ui", "INSERT INTO ej_u VALUES (1,'a',2),(2,'B',1),(3,'x',3),(4,'c',2),(5,'b',NULL),(6,NULL,1)")
	ddl("ddl/vi", "INSERT INTO ej_v VALUES (1,'a'),(2,'B'),(3,'x'),(4,'c'),(5,'b')")

	// The statements as filed (brief statements 1-3, over this fixture).
	add("filed/1", "SELECT a.id AS l, b.id AS r FROM ej_t a JOIN (SELECT a.id, upper(a.s) AS k FROM ej_u a) b ON upper(a.s) = b.k ORDER BY 1, 2")
	add("filed/2", "SELECT ej_t.id AS l, b.id AS r FROM ej_t JOIN (SELECT ej_t.id, upper(ej_t.s) AS k FROM ej_t) b ON lower(ej_t.s) = lower(b.k) ORDER BY 1, 2")
	add("filed/3", "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")

	body := func(k ekKind) string {
		return "SELECT " + k.iq + ".id, upper(" + k.iq + ".s) AS k, " + k.iq + ".n FROM " + k.i
	}
	// Axis 1: the collision kind x the outer consumer, the colliding name in
	// the derived table's SELECT list, INNER join.
	for _, k := range ekKinds {
		q, d, b := k.q, k.d, body(k)
		pre := "kind/" + k.name + "/"
		add(pre+"onEqCol", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".n = "+d+".n ORDER BY 1, 2")
		add(pre+"onExpr", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		add(pre+"onNonEq", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".n < "+d+".n ORDER BY 1, 2")
		add(pre+"onMixed", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".id = "+d+".id AND "+q+".n > 1 ORDER BY 1, 2")
		add(pre+"whereAbove", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".n = "+d+".n WHERE "+q+".id > 2 ORDER BY 1, 2")
		add(pre+"selectList", "SELECT "+q+".id AS l, "+q+".s AS ls, "+d+".id AS r, "+d+".k AS rk FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".id = "+d+".id ORDER BY 1, 3")
		add(pre+"aggAbove", "SELECT count(*) AS c, sum("+q+".id) AS sl, sum("+d+".id) AS sr FROM "+k.o+" JOIN ("+b+") "+d+" ON upper("+q+".s) = "+d+".k")
		add(pre+"bareOuter", "SELECT "+q+".id AS l, "+d+".bid AS r FROM "+k.o+" JOIN (SELECT "+k.iq+".id AS bid, upper("+k.iq+".s) AS k FROM "+k.i+") "+d+" ON upper("+q+".s) = "+d+".k AND n > 1 ORDER BY 1, 2")
	}

	// Axis 2: where the colliding name is used inside the derived table.
	for _, k := range ekKinds {
		if k.name != "aa" && k.name != "tt" && k.name != "none" {
			continue
		}
		q, d, iq := k.q, k.d, k.iq
		bodies := map[string]string{
			"where":      "SELECT " + iq + ".id, upper(" + iq + ".s) AS k, " + iq + ".n FROM " + k.i + " WHERE " + iq + ".n > 1",
			"groupBy":    "SELECT upper(" + iq + ".s) AS k, count(*) AS id, max(" + iq + ".n) AS n FROM " + k.i + " GROUP BY upper(" + iq + ".s) HAVING max(" + iq + ".n) > 0",
			"innerJoin":  "SELECT " + iq + ".id, upper(" + iq + ".s) AS k, " + iq + ".n FROM " + k.i + " JOIN ej_v z ON " + iq + ".id = z.id AND z.s <> 'x'",
			"orderLimit": "SELECT " + iq + ".id, upper(" + iq + ".s) AS k, " + iq + ".n FROM " + k.i + " ORDER BY " + iq + ".id LIMIT 4",
		}
		for _, p := range []string{"where", "groupBy", "innerJoin", "orderLimit"} {
			b := bodies[p]
			pre := "pos/" + k.name + "/" + p + "/"
			add(pre+"onExpr", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
			add(pre+"whereAbove", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN ("+b+") "+d+" ON "+q+".n = "+d+".n WHERE "+q+".id > 2 ORDER BY 1, 2")
		}
	}

	// Axis 3: the container.
	for _, k := range ekKinds {
		if k.name != "aa" && k.name != "tt" && k.name != "none" {
			continue
		}
		q, d, iq, b := k.q, k.d, k.iq, body(k)
		pre := "box/" + k.name + "/"
		add(pre+"cteOnce", "WITH "+d+" AS ("+b+") SELECT "+q+".id, "+d+".id FROM "+k.o+" JOIN "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		add(pre+"cteOnceWhere", "WITH "+d+" AS ("+b+") SELECT "+q+".id, "+d+".id FROM "+k.o+" JOIN "+d+" ON "+q+".n = "+d+".n WHERE "+q+".id > 2 ORDER BY 1, 2")
		add(pre+"cteTwice", "WITH "+d+" AS ("+b+") SELECT "+q+".id, "+d+".id, d2.id FROM "+k.o+" JOIN "+d+" ON upper("+q+".s) = "+d+".k JOIN "+d+" d2 ON d2.id = "+d+".id ORDER BY 1, 2, 3")
		add(pre+"lateral", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN LATERAL ("+b+") "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		add(pre+"lateralWhere", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN LATERAL ("+b+") "+d+" ON "+q+".n = "+d+".n WHERE "+q+".id > 2 ORDER BY 1, 2")
		add(pre+"nested2", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN (SELECT d1.id, d1.k FROM ("+b+") d1) "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		add(pre+"nested2Inner", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" JOIN (SELECT "+iq+".id, "+iq+".k FROM ("+b+") "+iq+") "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		// The subquery forms: a name inside shadows the outer one where
		// the inner block declares it, and refers OUT where it does not.
		add(pre+"scalar", "SELECT "+q+".id, (SELECT max("+iq+".id) FROM "+k.i+" WHERE upper("+iq+".s) = upper("+q+".s)) AS m FROM "+k.o+" ORDER BY 1")
		add(pre+"in", "SELECT "+q+".id FROM "+k.o+" WHERE "+q+".n IN (SELECT "+iq+".n FROM "+k.i+" WHERE "+iq+".s IS NOT NULL) ORDER BY 1")
		add(pre+"exists", "SELECT "+q+".id FROM "+k.o+" WHERE EXISTS (SELECT 1 FROM "+k.i+" WHERE upper("+iq+".s) = upper("+q+".s)) ORDER BY 1")
	}
	// Correlated controls: the inner block does NOT declare the outer name,
	// so the spelling refers to the OUTER relation.
	add("box/corr/exists", "SELECT ej_t.id FROM ej_t WHERE EXISTS (SELECT 1 FROM ej_u WHERE upper(ej_u.s) = upper(ej_t.s)) ORDER BY 1")
	add("box/corr/notExists", "SELECT a.id FROM ej_t a WHERE NOT EXISTS (SELECT 1 FROM ej_u c WHERE upper(c.s) = upper(a.s)) ORDER BY 1")
	add("box/corr/scalar", "SELECT a.id, (SELECT count(*) FROM ej_u c WHERE c.n = a.n) AS m FROM ej_t a ORDER BY 1")
	add("box/corr/in", "SELECT a.id FROM ej_t a WHERE a.id IN (SELECT c.id FROM ej_u c WHERE c.n = a.n) ORDER BY 1")
	add("box/corr/lateral", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT c.id FROM ej_u c WHERE c.n = a.n) l ON true ORDER BY 1, 2")
	add("box/corr/lateralShadow", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT a.id FROM ej_u a WHERE a.n = 2) l ON true ORDER BY 1, 2")
	// A LATERAL arm's lowered correlation is written in its body's own
	// spelling: these keep a local conjunct beside the correlated one.
	add("box/corr/lateralLocal", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT c.id FROM ej_u c WHERE c.n = a.n AND c.id > 1) l ON true ORDER BY 1, 2")
	add("box/corr/lateralLocalOuter", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT c.id FROM ej_u c WHERE c.n = a.n AND c.s IS NOT NULL AND a.id > 1) l ON true ORDER BY 1, 2")
	add("box/corr/lateralLeft", "SELECT a.id AS x, l.id AS y FROM ej_t a LEFT JOIN LATERAL (SELECT c.id FROM ej_u c WHERE c.n = a.n AND c.id > 2) l ON true ORDER BY 1, 2")
	add("box/corr/lateralAgg", "SELECT a.id AS x, l.m AS y FROM ej_t a JOIN LATERAL (SELECT count(*) AS m FROM ej_u c WHERE c.n = a.n AND c.id > 1) l ON true ORDER BY 1, 2")
	add("box/corr/lateralOnCond", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT c.id, c.s FROM ej_u c WHERE c.n = a.n) l ON l.id >= a.id ORDER BY 1, 2")
	add("box/corr/lateralShadowLocal", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT a.id FROM ej_u a WHERE a.n = 2 AND a.id > 1) l ON true ORDER BY 1, 2")
	add("box/corr/lateralDerivedInside", "SELECT a.id AS x, l.id AS y FROM ej_t a JOIN LATERAL (SELECT i.id FROM (SELECT id, n FROM ej_u) i WHERE i.n = a.n AND i.id > 1) l ON true ORDER BY 1, 2")
	add("box/corr/derivedInExists", "SELECT a.id FROM ej_t a WHERE EXISTS (SELECT 1 FROM (SELECT c.n FROM ej_u c) d WHERE d.n = a.n) ORDER BY 1")
	add("box/corr/derivedShadowInExists", "SELECT a.id FROM ej_t a WHERE EXISTS (SELECT 1 FROM (SELECT a.n FROM ej_u a) d WHERE d.n = a.n) ORDER BY 1")

	// Axis 4: the join kind.
	for _, k := range ekKinds {
		if k.name != "aa" && k.name != "tt" && k.name != "none" {
			continue
		}
		q, d, b := k.q, k.d, body(k)
		pre := "jk/" + k.name + "/"
		for _, jk := range []string{"LEFT", "RIGHT", "FULL"} {
			add(pre+strings.ToLower(jk), "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" "+jk+" JOIN ("+b+") "+d+" ON upper("+q+".s) = "+d+".k ORDER BY 1, 2")
			add(pre+strings.ToLower(jk)+"Mixed", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" "+jk+" JOIN ("+b+") "+d+" ON "+q+".id = "+d+".id AND "+q+".n > 1 ORDER BY 1, 2")
			add(pre+strings.ToLower(jk)+"Where", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" "+jk+" JOIN ("+b+") "+d+" ON "+q+".id = "+d+".id WHERE "+q+".n > 1 ORDER BY 1, 2")
		}
		add(pre+"crossWhere", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+" CROSS JOIN ("+b+") "+d+" WHERE upper("+q+".s) = "+d+".k ORDER BY 1, 2")
		add(pre+"comma", "SELECT "+q+".id AS l, "+d+".id AS r FROM "+k.o+", ("+b+") "+d+" WHERE upper("+q+".s) = "+d+".k AND "+q+".n > 1 ORDER BY 1, 2")
		add(pre+"comma3", "SELECT "+q+".id AS l, "+d+".id AS r, z.id AS z FROM "+k.o+", ("+b+") "+d+", ej_v z WHERE "+q+".id = z.id AND "+d+".id = z.id AND "+q+".n > 1 ORDER BY 1, 2, 3")
	}

	// Axis 5: statement 3 and its neighbours.
	s3 := "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs)"
	add("s3/asFiled", s3+" AND n > 1 ORDER BY 1, 2")
	add("s3/noN", s3+" ORDER BY 1, 2")
	add("s3/qualified", s3+" AND a.n > 1 ORDER BY 1, 2")
	add("s3/where", s3+" WHERE n > 1 ORDER BY 1, 2")
	add("s3/renamedN", "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs, n AS bn FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/innerNoN", "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_v) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/ambiguousN", "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs, n FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/derivedN", "SELECT a.id AS l, b.bid AS r FROM ej_v a JOIN (SELECT id AS bid, lower(s) AS bs, n FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/outerNoN", "SELECT a.id AS l, b.bid AS r FROM ej_v a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/bareId", "SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) WHERE id > 2 ORDER BY 1, 2")
	add("s3/bareIdLeft", "SELECT a.id AS l, b.bid AS r FROM ej_t a LEFT JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")
	add("s3/cte", "WITH b AS (SELECT id AS bid, lower(s) AS bs FROM ej_u) SELECT a.id AS l, b.bid AS r FROM ej_t a JOIN b ON upper(a.s) = upper(b.bs) AND n > 1 ORDER BY 1, 2")

	// PostgreSQL's refusals of a name declared twice in one FROM list.
	add("dup/derivedLikeOuter", "SELECT a.id FROM ej_t a JOIN (SELECT a.id FROM ej_u a) a ON true")
	add("dup/innerNotVisible", "SELECT a.id AS l, c.id AS r FROM ej_t a JOIN (SELECT c.id FROM ej_u c) b ON a.id = b.id")
	return out
}

// ekAnswer renders an answer as "rows=N r1 | r2 …" with the rows sorted, or
// "ERR <SQLSTATE>".
func ekAnswer(rows []string) string {
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}

// ekRows reduces an ftRender answer to ekAnswer's form.
func ekRows(s string) string {
	if strings.HasPrefix(s, "ERR") || strings.HasPrefix(s, "PANIC") {
		return s
	}
	if s == "(0 rows)" {
		return ekAnswer(nil)
	}
	_, body, ok := strings.Cut(s, " | ")
	if !ok {
		return s
	}
	rows := strings.Split(body, "; ")
	for i, r := range rows {
		f := strings.Split(r, ",")
		for j := range f {
			switch f[j] {
			case "true":
				f[j] = "t"
			case "false":
				f[j] = "f"
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return ekAnswer(rows)
}

// TestArc1678kMeasure runs every cell against PostgreSQL (EK_PG_DSN) and
// rewrites the answer file; skipped otherwise.
func TestArc1678kMeasure(t *testing.T) {
	dsn := os.Getenv("EK_PG_DSN")
	if dsn == "" {
		t.Skip("EK_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	run := func(sql string) *pgconn.Result {
		return conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	}
	for _, s := range []string{"SET statement_timeout = '30s'", "DROP TABLE IF EXISTS ej_t", "DROP TABLE IF EXISTS ej_u", "DROP TABLE IF EXISTS ej_v"} {
		if r := run(s); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	version := string(run("SELECT version()").Rows[0][0])
	loc := run("SELECT datcollate, datctype, pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()").Rows[0]
	var b strings.Builder
	b.WriteString("# #1678k: PostgreSQL answers for arc_1678k_derived_scope_arms_test.go (TestArc1678kMeasure, EK_PG_DSN).\n")
	fmt.Fprintf(&b, "# %s; datcollate/datctype/encoding %s %s %s (postgres:17-alpine, initdb --locale=C --encoding=UTF8).\n", version, loc[0], loc[1], loc[2])
	b.WriteString("# name<TAB>rows=N <sorted rows, t/f booleans> | ERR <SQLSTATE> | ok (DDL).\n")
	for _, c := range ekCells() {
		r := run(c.sql)
		var ans string
		switch {
		case r.Err != nil:
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = "ERR " + pe.Code
		case c.ddl:
			ans = "ok"
		default:
			rows := make([]string, len(r.Rows))
			for i, row := range r.Rows {
				f := make([]string, len(row))
				for j, v := range row {
					if v == nil {
						f[j] = "NULL"
					} else {
						f[j] = string(v)
					}
				}
				rows[i] = strings.Join(f, ",")
			}
			ans = ekAnswer(rows)
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(ekAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ekPinsPath pins every (cell, arm) answer that is not PostgreSQL's, one per
// line: cell<TAB>arm<TAB>answer<TAB>reason. Each reason names a mechanism
// OUTSIDE this line's seam (the attribution of a join's sides), measured
// identical at base 6ef333fb or moved only from one wrong answer to another
// on the stage DAG, whose filter respelling is a separate defect:
//
//   - dag-respell: the four DAG arms respell a conjunct that stays above the
//     join through the derived table's projection into its INNER spelling
//     (logical.ResolveFilterThroughProjects, read by dagplan's join-stage
//     filter), and that spelling binds the outer relation at the join stage
//     — `upper(a.s) = b.k` becomes `upper(a.s) = (upper(a.s))`, true on every
//     row whose s is not NULL. At base the same cells were wrong on every arm
//     because the conjunct was pushed inside the block.
//   - dag-nocollision: the DAG arms answer wrong where NO name collides (the
//     derived output respelled to an inner qualifier the join stage does not
//     carry, an outer join's ON, a CTE): identical at base.
//   - dag-0A000: the DAG arms refuse the shape loudly, `sort: key column "r"
//     does not exist in the input schema` (a derived table publishing `bid`
//     read as `b.bid AS r` under ORDER BY): identical at base, or — the
//     kind/{aa,at,tt}/bareOuter cells — this refusal where base answered a
//     wrong value (the bare n had been pushed into the block).
//   - async-nostage: the asynchronous door has no distributed lowering for
//     the shape (a correlated or scalar subquery, a grouped body): identical
//     at base.
//
// A pin FAILS the moment the arm starts agreeing or answers anything else.
const ekPinsPath = "testdata/arc_1678k_derived_scope_pins.tsv"

func ekPins(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(ekPinsPath)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			t.Fatalf("malformed pin line %q", line)
		}
		switch f[3] {
		case "dag-respell", "dag-nocollision", "dag-0A000", "async-nostage":
		default:
			t.Fatalf("pin %s@%s: unknown reason %q", f[0], f[1], f[3])
		}
		out[f[0]+"@"+f[1]] = f[2]
	}
	return out
}

// TestArc1678kDerivedScopeEveryArm asserts PostgreSQL 17.11's answer for every
// cell on every arm. EK_DUMP=<path> writes every (cell, arm, answer) for a
// base-against-tip transition table, EK_ERRDUMP=<path> every error's text.
func TestArc1678kDerivedScopeEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: nine arms over the derived-scope table")
	}
	answers := ftPGAnswersFrom(t, ekAnswersPath)
	pins := ekPins(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ekArmsFixture(t, ctx)
	var dump, errDump strings.Builder
	defer func() {
		for env, b := range map[string]*strings.Builder{"EK_DUMP": &dump, "EK_ERRDUMP": &errDump} {
			if p := os.Getenv(env); p != "" {
				if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	agree, pinned := 0, 0
	for _, c := range ekCells() {
		want, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		if c.ddl {
			if got := writer(c.sql); got != want {
				t.Fatalf("%s: %s\n  got %s want %s", c.name, c.sql, got, want)
			}
			continue
		}
		for _, arm := range arms {
			raw, errText := arm.exec(c.sql)
			got := ekRows(raw)
			dump.WriteString(c.name + "\t" + arm.name + "\t" + got + "\n")
			if errText != "" {
				errDump.WriteString(c.name + "\t" + arm.name + "\t" + errText + "\n")
			}
			if pin, ok := pins[c.name+"@"+arm.name]; ok {
				pinned++
				if got != pin {
					t.Errorf("%s on %s: the pinned answer changed\n  %s\n  got  %s\n  pin  %s\n  PostgreSQL 17.11 %s", c.name, arm.name, c.sql, got, pin, want)
				}
				continue
			}
			if got != want {
				t.Errorf("%s on %s\n  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, got, want)
				continue
			}
			agree++
		}
	}
	t.Logf("%d (cell, arm) answers agree with PostgreSQL 17.11, %d pinned", agree, pinned)
	if pinned != len(pins) {
		t.Fatalf("%d pins read of %d in %s: a pin names no (cell, arm)", pinned, len(pins), ekPinsPath)
	}
}

// ftPGAnswersFrom reads an answer file of "name<TAB>answer" lines.
func ftPGAnswersFrom(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// ekArmsFixture is ivArmsFixture's six arms plus the DAG with its skew split
// forced, the asynchronous door, and the asynchronous door with its probe
// split forced, over one shared catalog; the writer runs the DDL.
func ekArmsFixture(t *testing.T, ctx context.Context) (func(string) string, []ivArm) {
	t.Helper()
	infra := tmdInfra(t, ctx)
	open := func(budget int64) *wadjet.DB {
		t.Helper()
		cfg := wadjet.Config{MetaKV: infra.kv, Store: infra.store, Bucket: "test", MemoryBudget: budget}
		if budget > 0 {
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open over the shared catalog: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	writer := open(0)
	spilled := open(512 * 1024)
	coord := tmdCoordinator(t, ctx, infra)
	coordB := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
	coordK := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1; c.SkewSplit = true })
	coordF := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })
	coordA := tmdCoordinator(t, ctx, infra)
	coordA.BuildCacheThreshold = 1
	ot, of := skewSplitTargetBytes, skewSplitMinGroupBytes
	skewSplitTargetBytes, skewSplitMinGroupBytes = 100, 100
	t.Cleanup(func() { skewSplitTargetBytes, skewSplitMinGroupBytes = ot, of })
	spill := func(run func() (string, string)) (string, string) {
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(2)
		defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
		return run()
	}
	async := func(probeSplit bool) func(string) (string, string) {
		return func(sql string) (string, string) {
			if probeSplit {
				o := dagplan.ProbeSplitMinBytes
				dagplan.ProbeSplitMinBytes = 1
				defer func() { dagplan.ProbeSplitMinBytes = o }()
			}
			return ekRunAsync(ctx, coordA, sql)
		}
	}
	return func(sql string) string {
			_, err := writer.Query(ctx, sql)
			return ftDDLRender(err)
		}, []ivArm{
			{"single", func(sql string) (string, string) { return ivRunSingle(ctx, writer, sql) }},
			{"spilled512k", func(sql string) (string, string) {
				return spill(func() (string, string) { return ivRunSingle(ctx, spilled, sql) })
			}},
			{"dag", func(sql string) (string, string) { return ivRunDAG(ctx, coord, sql) }},
			{"dag-shuffled", func(sql string) (string, string) { return ivRunDAG(ctx, coordB, sql) }},
			{"dag-morsel4", func(sql string) (string, string) { return ivRunDAG(ctx, coordM, sql) }},
			{"dag-skew", func(sql string) (string, string) { return ivRunDAG(ctx, coordK, sql) }},
			{"fastpath", func(sql string) (string, string) { return ivRunDAG(ctx, coordF, sql) }},
			{"async", async(false)},
			{"async-probesplit", async(true)},
		}
}

// ekRunAsync is the asynchronous submit door (SubmitSQL, the tracker,
// GetQueryResults) rendered as ivRunDAG renders.
func ekRunAsync(ctx context.Context, c *Coordinator, sql string) (out, errText string) {
	defer func() {
		if r := recover(); r != nil {
			out, errText = fmt.Sprintf("PANIC %v", r), fmt.Sprintf("PANIC %v", r)
		}
	}()
	qid, _, err := c.SubmitSQL(ctx, sql)
	if err != nil {
		return ftErr(err), err.Error()
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if info := c.tracker.Get(qid); info != nil && (info.State == QueryStateCompleted || info.State == QueryStateFailed) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	res, err := c.GetQueryResults(ctx, qid)
	if err != nil {
		return ftErr(err), err.Error()
	}
	if res.Error != "" {
		return "ERR (uncoded) " + res.Error, res.Error
	}
	cols := append([]parquet.Column(nil), res.OutputSchema()...)
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return ftErr(berr), berr.Error()
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return ftErr(rerr), rerr.Error()
		}
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, col := range cols {
				row[j] = r[col.Name]
			}
			cells = append(cells, row)
		}
	}
	return ftEngineRender(cols, cells), ""
}
