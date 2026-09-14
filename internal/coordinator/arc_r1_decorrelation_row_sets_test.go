package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A CORRELATED OR SEMI-JOINED BODY ANSWERS THE ROWS POSTGRESQL ANSWERS —
// #1098, #1067, #1072, #1066, on FIVE ARMS.
//
// The four issues are four spellings of one question: for each outer row, does
// the body produce exactly the rows PostgreSQL 17.11 produces, whatever the
// body contains and wherever the outer reference sits? This is the table that
// asks it once, and it is enumerated rather than sampled because every one of
// the four was invisible to every existing gate:
//
//	{IN, NOT IN, = ANY, <> ALL, EXISTS, NOT EXISTS, scalar =}
//	× {the outer column on the OUTER relation of a join, on the INNER relation,
//	   no join at all, on a derived block, on a CTE, the join written the other
//	   way round}
//	× {body: plain, with a CTE, with a set operation, a recursive CTE, with its
//	   own join, with LIMIT, and a correlated form of each}
//	× {single, spilled, dag, dag-shuffled, dag-morsel4}
//
// plus the join-keyed-on-a-recursive-CTE family, which is a JOIN KEY and not a
// subquery at all, and the four issues' own shapes verbatim.
//
// EVERY want is live PostgreSQL 17.11 (postgres:17-alpine, --locale=C, the
// lat_ord / lat_item / c2users / c2t2 rows this package's fixtures write),
// measured BEFORE any code changed and re-measured at the tip. What the four
// fixes moved, in cells of this table:
//
//	#1098  108 cells — a decorrelated join's PROBE key was spelled by dropping
//	       the qualifier, so over a probe side that is a JOIN it bound whichever
//	       arm the reorderer emitted bare: 3 rows for PostgreSQL's 4, and 1 for
//	       its 4 through the scalar LEFT join. Silent, on all five arms.
//	#1067   24 cells wrong and 114 refused at the PARSER — a CTE declared in the
//	       body was not in scope for the body: zero rows where nothing answers
//	       to the name, the BASE TABLE's rows where something does.
//	#1072   12 cells — a set operation NESTED in a correlated body was planned
//	       uncorrelated; every outer row answered NULL. Now refused, loudly and
//	       by its own mechanism.
//	#1066    5 cells — a JOIN keyed on a materialized recursive CTE's column
//	       answered the CROSS PRODUCT, because the reference published no column
//	       list for the key to be attributed to.
//
// The three PINS below are boundaries, not answers, and each names the hunk
// that closes it. A pin that starts agreeing FAILS: the cell is asserted
// against PostgreSQL the moment the refusal stops.

// r1Pos is one PLACE the tested outer column can sit. tmpl carries one %s for
// the predicate over col.
type r1Pos struct {
	name string
	col  string
	tmpl string
}

func r1Positions() []r1Pos {
	return []r1Pos{
		{"onOuter", "o.id", "SELECT o.id AS a, i.id AS b FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE %s ORDER BY a, b"},
		{"onInner", "i.id", "SELECT o.id AS a, i.id AS b FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE %s ORDER BY a, b"},
		{"noJoin", "o.id", "SELECT o.id AS a FROM lat_ord o WHERE %s ORDER BY a"},
		{"onDerived", "d.id", "SELECT d.id AS a, i.id AS b FROM (SELECT id, customer FROM lat_ord) d JOIN lat_item i ON i.order_id = d.id WHERE %s ORDER BY a, b"},
		{"onCTE", "cc.id", "WITH cc AS (SELECT id, customer FROM lat_ord) SELECT cc.id AS a, i.id AS b FROM cc JOIN lat_item i ON i.order_id = cc.id WHERE %s ORDER BY a, b"},
		{"onOuterRev", "o.id", "SELECT o.id AS a, i.id AS b FROM lat_item i JOIN lat_ord o ON i.order_id = o.id WHERE %s ORDER BY a, b"},
	}
}

// r1Body is one BODY. corr marks the ones that read the outer row: %s is the
// outer column.
type r1Body struct {
	name string
	sql  string
	corr bool
}

// r1SetBodies are the set-valued bodies IN / NOT IN / ANY / ALL take.
func r1SetBodies() []r1Body {
	return []r1Body{
		{"plain", "SELECT id FROM lat_ord", false},
		{"cteBody", "WITH n AS (SELECT id FROM lat_ord) SELECT id FROM n", false},
		{"setop", "SELECT id FROM lat_ord UNION ALL SELECT order_id FROM lat_item", false},
		{"recCTE", "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3) SELECT v FROM r", false},
		{"joinBody", "SELECT z.id FROM lat_ord z JOIN lat_item y ON y.order_id = z.id", false},
		{"limitBody", "SELECT id FROM lat_ord ORDER BY id LIMIT 2", false},
		{"otherTable", "SELECT order_id FROM lat_item", false},
		{"corrPlain", "SELECT z.id FROM lat_ord z WHERE z.id = %s", true},
		{"corrCTE", "WITH n AS (SELECT id FROM lat_ord) SELECT n.id FROM n WHERE n.id = %s", true},
		{"corrSetop", "SELECT z.id FROM lat_ord z WHERE z.id = %s UNION ALL SELECT y.order_id FROM lat_item y WHERE y.order_id = 99", true},
		{"corrRec", "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3) SELECT r.v FROM r WHERE r.v = %s", true},
		{"corrJoin", "SELECT z.id FROM lat_ord z JOIN lat_item y ON y.order_id = z.id WHERE z.id = %s", true},
		{"corrLimit", "SELECT z.id FROM lat_ord z WHERE z.id = %s ORDER BY z.id LIMIT 2", true},
	}
}

// r1ExistsBodies are the existence bodies EXISTS / NOT EXISTS take.
func r1ExistsBodies() []r1Body {
	return []r1Body{
		{"plain", "SELECT 1 FROM lat_ord z WHERE z.id = %s", true},
		{"cteBody", "WITH n AS (SELECT id FROM lat_ord) SELECT 1 FROM n WHERE n.id = %s", true},
		{"setop", "SELECT z.id FROM lat_ord z WHERE z.id = %s UNION ALL SELECT y.order_id FROM lat_item y WHERE y.order_id = 99", true},
		{"recCTE", "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3) SELECT 1 FROM r WHERE r.v = %s", true},
		{"joinBody", "SELECT 1 FROM lat_ord z JOIN lat_item y ON y.order_id = z.id WHERE z.id = %s", true},
		{"limitBody", "SELECT 1 FROM lat_ord z WHERE z.id = %s LIMIT 1", true},
		{"uncorr", "SELECT 1 FROM lat_ord z WHERE z.id = 2", false},
	}
}

// r1ScalarBodies are the scalar bodies a `= (subquery)` takes.
func r1ScalarBodies() []r1Body {
	return []r1Body{
		{"plain", "SELECT MAX(z.id) FROM lat_ord z", false},
		{"cteBody", "WITH n AS (SELECT id FROM lat_ord) SELECT MAX(n.id) FROM n", false},
		{"corrPlain", "SELECT MAX(z.id) FROM lat_ord z WHERE z.id = %s", true},
		{"corrCTE", "WITH n AS (SELECT id FROM lat_ord) SELECT MAX(n.id) FROM n WHERE n.id = %s", true},
		{"corrSetopIn", "SELECT MAX(z.id) FROM lat_ord z WHERE z.id IN (SELECT y.order_id FROM lat_item y WHERE y.order_id = %s UNION ALL SELECT y2.order_id FROM lat_item y2 WHERE y2.order_id = 99)", true},
		{"corrRec", "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3) SELECT MAX(r.v) FROM r WHERE r.v = %s", true},
	}
}

func r1BodySQL(b r1Body, col string) string {
	if b.corr {
		return strings.ReplaceAll(b.sql, "%s", col)
	}
	return b.sql
}

type r1Case struct{ name, sql string }

// r1Cases writes the table out. The name is the KEY into the PostgreSQL row
// sets below and into the three pin lists.
func r1Cases() []r1Case {
	var out []r1Case
	for _, p := range r1Positions() {
		for _, b := range r1SetBodies() {
			body := r1BodySQL(b, p.col)
			out = append(out,
				r1Case{fmt.Sprintf("IN/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, p.col+" IN ("+body+")")},
				r1Case{fmt.Sprintf("NOTIN/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, p.col+" NOT IN ("+body+")")},
				r1Case{fmt.Sprintf("ANY/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, p.col+" = ANY ("+body+")")},
				r1Case{fmt.Sprintf("ALL/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, p.col+" <> ALL ("+body+")")},
			)
		}
		for _, b := range r1ExistsBodies() {
			body := r1BodySQL(b, p.col)
			out = append(out,
				r1Case{fmt.Sprintf("EXISTS/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, "EXISTS ("+body+")")},
				r1Case{fmt.Sprintf("NOTEXISTS/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, "NOT EXISTS ("+body+")")},
			)
		}
		for _, b := range r1ScalarBodies() {
			body := r1BodySQL(b, p.col)
			out = append(out,
				r1Case{fmt.Sprintf("SCALAREQ/%s/%s", p.name, b.name), fmt.Sprintf(p.tmpl, p.col+" = ("+body+")")})
		}
	}
	// The JOIN-KEYED-ON-A-RECURSIVE-CTE family (#1066): a join KEY, not a
	// subquery. `inSpelling` is the IN spelling of the same CTE, which was
	// right while the join was the cross product — the discriminator that said
	// the loss was in the KEY and not in the CTE's rows.
	rec := "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3)"
	out = append(out,
		r1Case{"REC/join", rec + " SELECT u.id AS a, r.v AS b FROM lat_ord u JOIN r ON r.v = u.id ORDER BY a, b"},
		r1Case{"REC/joinFiltered", rec + " SELECT u.id AS a, r.v AS b FROM lat_ord u JOIN r ON r.v = u.id WHERE u.id = 2 ORDER BY a, b"},
		r1Case{"REC/joinRev", rec + " SELECT u.id AS a, r.v AS b FROM r JOIN lat_ord u ON r.v = u.id ORDER BY a, b"},
		r1Case{"REC/leftJoin", rec + " SELECT u.id AS a, r.v AS b FROM lat_ord u LEFT JOIN r ON r.v = u.id ORDER BY a, b"},
		r1Case{"REC/nestedDerived", "SELECT u.id AS a, q.v AS b FROM lat_ord u JOIN (" + rec + " SELECT r.v FROM r) q ON q.v = u.id ORDER BY a, b"},
		r1Case{"REC/inSpelling", rec + " SELECT u.id AS a FROM lat_ord u WHERE u.id IN (SELECT v FROM r) ORDER BY a"},
		r1Case{"REC/joinTwice", rec + " SELECT u.id AS a, r.v AS b FROM lat_ord u JOIN r ON r.v = u.id JOIN lat_item i ON i.order_id = u.id ORDER BY a, b"},
		r1Case{"CTE/join", "WITH p AS (SELECT id AS v FROM lat_ord) SELECT u.id AS a, p.v AS b FROM lat_ord u JOIN p ON p.v = u.id ORDER BY a, b"},
	)
	// The four issues' own shapes, verbatim.
	out = append(out,
		r1Case{"ISSUE/1067", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (WITH n AS (SELECT 1 AS v UNION ALL SELECT 2) SELECT n.v FROM n WHERE n.v = o.id) ORDER BY a"},
		r1Case{"ISSUE/1072", "SELECT (SELECT MAX(x.visits) FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id UNION ALL SELECT k FROM c2t2 WHERE k = 99)) AS a FROM c2users u ORDER BY a"},
		r1Case{"ISSUE/1072union", "SELECT (SELECT MAX(x.visits) FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id UNION SELECT k FROM c2t2 WHERE k = 99)) AS a FROM c2users u ORDER BY a"},
		r1Case{"ISSUE/1072intersect", "SELECT (SELECT MAX(x.visits) FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id INTERSECT SELECT k FROM c2t2)) AS a FROM c2users u ORDER BY a"},
		r1Case{"ISSUE/1072except", "SELECT (SELECT MAX(x.visits) FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id EXCEPT SELECT k FROM c2t2 WHERE k = 99)) AS a FROM c2users u ORDER BY a"},
		r1Case{"ISSUE/1072exists", "SELECT u.id AS a FROM c2users u WHERE EXISTS (SELECT 1 FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id UNION ALL SELECT k FROM c2t2 WHERE k = 99)) ORDER BY a"},
		r1Case{"ISSUE/1072in", "SELECT u.id AS a FROM c2users u WHERE u.id IN (SELECT x.id FROM c2users x WHERE x.id IN (SELECT k FROM c2t2 WHERE k = u.id UNION ALL SELECT k FROM c2t2 WHERE k = 99)) ORDER BY a"},
		r1Case{"ISSUE/1098a", "SELECT COUNT(*) AS a FROM lat_ord o JOIN lat_item i ON i.order_id=o.id WHERE o.id IN (SELECT id FROM lat_ord)"},
		r1Case{"ISSUE/1098b", "SELECT COUNT(*) AS a FROM lat_ord o JOIN lat_item i ON i.order_id=o.id WHERE EXISTS (SELECT 1 FROM lat_ord z WHERE z.id=o.id)"},
		r1Case{"ISSUE/1098c", "SELECT COUNT(*) AS a FROM lat_ord o JOIN lat_item i ON i.order_id=o.id WHERE o.id IN (SELECT order_id FROM lat_item)"},
	)
	return out
}

// The three boundaries this table pins, each by the sentence its refusal says.
const (
	// A correlated subquery whose BODY is a set operation: the per-row re-run
	// renders one select per block (plansql.RebuildSQLForRerun) and has no arm
	// for info.Union, so the body cannot be written back out. Pre-existing, and
	// the refusal is ADR-0021 §1l's. Closing it takes a set-operation arm in
	// the rebuild AND the arms in plansql's outer-column CANDIDATES, which
	// widens the enclosing query's projection over a policed relation — see the
	// deliberate omission in collectOuterCandidatesBlock and
	// server.TestPolicyMaskingIsPlanTimeOnEveryDoor.
	r1SetOpBodyRefusal = "its body is a SET OPERATION, which the rebuild renders no arm for"
	// The same boundary one level down (#1072). Before the walk reached a
	// nested block's arms this shape was not refused at all: it was planned
	// UNCORRELATED and every outer row answered NULL.
	r1NestedSetOpRefusal = "holds a SET OPERATION one level down"
	// A recursive CTE JOINED to a relation, on the three DAG arms: the stage
	// for the CTE reference names a relation no catalog has, so the dispatcher
	// can resolve no files for it. Pre-existing at the base commit and
	// DAG-only — the single-process arms answer PostgreSQL's rows. Closing it
	// is #960 (recursive-CTE stage lowering); the narrower repair is a stage
	// that says it reads a CTE, so physical.refuseUnbuildableStages stops
	// exempting it and the coordinator routes the query local as it already
	// does for #806 and #812.
	r1DAGRecursiveCTERefusal = "has no dependencies and no ScanFiles"
)

var r1PostgresRowSets = map[string]string{
	"IN/onOuter/plain":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/plain":             "rows=0 ",
	"ANY/onOuter/plain":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/plain":               "rows=0 ",
	"IN/onOuter/cteBody":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/cteBody":           "rows=0 ",
	"ANY/onOuter/cteBody":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/cteBody":             "rows=0 ",
	"IN/onOuter/setop":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/setop":             "rows=0 ",
	"ANY/onOuter/setop":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/setop":               "rows=0 ",
	"IN/onOuter/recCTE":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/recCTE":            "rows=0 ",
	"ANY/onOuter/recCTE":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/recCTE":              "rows=0 ",
	"IN/onOuter/joinBody":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/joinBody":          "rows=0 ",
	"ANY/onOuter/joinBody":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/joinBody":            "rows=0 ",
	"IN/onOuter/limitBody":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/limitBody":         "rows=0 ",
	"ANY/onOuter/limitBody":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/limitBody":           "rows=0 ",
	"IN/onOuter/otherTable":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/otherTable":        "rows=0 ",
	"ANY/onOuter/otherTable":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/otherTable":          "rows=0 ",
	"IN/onOuter/corrPlain":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrPlain":         "rows=0 ",
	"ANY/onOuter/corrPlain":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrPlain":           "rows=0 ",
	"IN/onOuter/corrCTE":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrCTE":           "rows=0 ",
	"ANY/onOuter/corrCTE":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrCTE":             "rows=0 ",
	"IN/onOuter/corrSetop":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrSetop":         "rows=0 ",
	"ANY/onOuter/corrSetop":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrSetop":           "rows=0 ",
	"IN/onOuter/corrRec":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrRec":           "rows=0 ",
	"ANY/onOuter/corrRec":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrRec":             "rows=0 ",
	"IN/onOuter/corrJoin":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrJoin":          "rows=0 ",
	"ANY/onOuter/corrJoin":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrJoin":            "rows=0 ",
	"IN/onOuter/corrLimit":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuter/corrLimit":         "rows=0 ",
	"ANY/onOuter/corrLimit":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuter/corrLimit":           "rows=0 ",
	"EXISTS/onOuter/plain":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/plain":         "rows=0 ",
	"EXISTS/onOuter/cteBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/cteBody":       "rows=0 ",
	"EXISTS/onOuter/setop":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/setop":         "rows=0 ",
	"EXISTS/onOuter/recCTE":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/recCTE":        "rows=0 ",
	"EXISTS/onOuter/joinBody":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/joinBody":      "rows=0 ",
	"EXISTS/onOuter/limitBody":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/limitBody":     "rows=0 ",
	"EXISTS/onOuter/uncorr":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuter/uncorr":        "rows=0 ",
	"SCALAREQ/onOuter/plain":          "rows=0 ",
	"SCALAREQ/onOuter/cteBody":        "rows=0 ",
	"SCALAREQ/onOuter/corrPlain":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuter/corrCTE":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuter/corrSetopIn":    "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuter/corrRec":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"IN/onInner/plain":                "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/plain":             "rows=1 2,4",
	"ANY/onInner/plain":               "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/plain":               "rows=1 2,4",
	"IN/onInner/cteBody":              "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/cteBody":           "rows=1 2,4",
	"ANY/onInner/cteBody":             "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/cteBody":             "rows=1 2,4",
	"IN/onInner/setop":                "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/setop":             "rows=1 2,4",
	"ANY/onInner/setop":               "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/setop":               "rows=1 2,4",
	"IN/onInner/recCTE":               "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/recCTE":            "rows=1 2,4",
	"ANY/onInner/recCTE":              "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/recCTE":              "rows=1 2,4",
	"IN/onInner/joinBody":             "rows=2 1,1 | 1,2",
	"NOTIN/onInner/joinBody":          "rows=2 2,3 | 2,4",
	"ANY/onInner/joinBody":            "rows=2 1,1 | 1,2",
	"ALL/onInner/joinBody":            "rows=2 2,3 | 2,4",
	"IN/onInner/limitBody":            "rows=2 1,1 | 1,2",
	"NOTIN/onInner/limitBody":         "rows=2 2,3 | 2,4",
	"ANY/onInner/limitBody":           "rows=2 1,1 | 1,2",
	"ALL/onInner/limitBody":           "rows=2 2,3 | 2,4",
	"IN/onInner/otherTable":           "rows=2 1,1 | 1,2",
	"NOTIN/onInner/otherTable":        "rows=2 2,3 | 2,4",
	"ANY/onInner/otherTable":          "rows=2 1,1 | 1,2",
	"ALL/onInner/otherTable":          "rows=2 2,3 | 2,4",
	"IN/onInner/corrPlain":            "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/corrPlain":         "rows=1 2,4",
	"ANY/onInner/corrPlain":           "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/corrPlain":           "rows=1 2,4",
	"IN/onInner/corrCTE":              "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/corrCTE":           "rows=1 2,4",
	"ANY/onInner/corrCTE":             "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/corrCTE":             "rows=1 2,4",
	"IN/onInner/corrSetop":            "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/corrSetop":         "rows=1 2,4",
	"ANY/onInner/corrSetop":           "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/corrSetop":           "rows=1 2,4",
	"IN/onInner/corrRec":              "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/corrRec":           "rows=1 2,4",
	"ANY/onInner/corrRec":             "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/corrRec":             "rows=1 2,4",
	"IN/onInner/corrJoin":             "rows=2 1,1 | 1,2",
	"NOTIN/onInner/corrJoin":          "rows=2 2,3 | 2,4",
	"ANY/onInner/corrJoin":            "rows=2 1,1 | 1,2",
	"ALL/onInner/corrJoin":            "rows=2 2,3 | 2,4",
	"IN/onInner/corrLimit":            "rows=3 1,1 | 1,2 | 2,3",
	"NOTIN/onInner/corrLimit":         "rows=1 2,4",
	"ANY/onInner/corrLimit":           "rows=3 1,1 | 1,2 | 2,3",
	"ALL/onInner/corrLimit":           "rows=1 2,4",
	"EXISTS/onInner/plain":            "rows=3 1,1 | 1,2 | 2,3",
	"NOTEXISTS/onInner/plain":         "rows=1 2,4",
	"EXISTS/onInner/cteBody":          "rows=3 1,1 | 1,2 | 2,3",
	"NOTEXISTS/onInner/cteBody":       "rows=1 2,4",
	"EXISTS/onInner/setop":            "rows=3 1,1 | 1,2 | 2,3",
	"NOTEXISTS/onInner/setop":         "rows=1 2,4",
	"EXISTS/onInner/recCTE":           "rows=3 1,1 | 1,2 | 2,3",
	"NOTEXISTS/onInner/recCTE":        "rows=1 2,4",
	"EXISTS/onInner/joinBody":         "rows=2 1,1 | 1,2",
	"NOTEXISTS/onInner/joinBody":      "rows=2 2,3 | 2,4",
	"EXISTS/onInner/limitBody":        "rows=3 1,1 | 1,2 | 2,3",
	"NOTEXISTS/onInner/limitBody":     "rows=1 2,4",
	"EXISTS/onInner/uncorr":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onInner/uncorr":        "rows=0 ",
	"SCALAREQ/onInner/plain":          "rows=1 2,3",
	"SCALAREQ/onInner/cteBody":        "rows=1 2,3",
	"SCALAREQ/onInner/corrPlain":      "rows=3 1,1 | 1,2 | 2,3",
	"SCALAREQ/onInner/corrCTE":        "rows=3 1,1 | 1,2 | 2,3",
	"SCALAREQ/onInner/corrSetopIn":    "rows=2 1,1 | 1,2",
	"SCALAREQ/onInner/corrRec":        "rows=3 1,1 | 1,2 | 2,3",
	"IN/noJoin/plain":                 "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/plain":              "rows=0 ",
	"ANY/noJoin/plain":                "rows=3 1 | 2 | 3",
	"ALL/noJoin/plain":                "rows=0 ",
	"IN/noJoin/cteBody":               "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/cteBody":            "rows=0 ",
	"ANY/noJoin/cteBody":              "rows=3 1 | 2 | 3",
	"ALL/noJoin/cteBody":              "rows=0 ",
	"IN/noJoin/setop":                 "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/setop":              "rows=0 ",
	"ANY/noJoin/setop":                "rows=3 1 | 2 | 3",
	"ALL/noJoin/setop":                "rows=0 ",
	"IN/noJoin/recCTE":                "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/recCTE":             "rows=0 ",
	"ANY/noJoin/recCTE":               "rows=3 1 | 2 | 3",
	"ALL/noJoin/recCTE":               "rows=0 ",
	"IN/noJoin/joinBody":              "rows=2 1 | 2",
	"NOTIN/noJoin/joinBody":           "rows=1 3",
	"ANY/noJoin/joinBody":             "rows=2 1 | 2",
	"ALL/noJoin/joinBody":             "rows=1 3",
	"IN/noJoin/limitBody":             "rows=2 1 | 2",
	"NOTIN/noJoin/limitBody":          "rows=1 3",
	"ANY/noJoin/limitBody":            "rows=2 1 | 2",
	"ALL/noJoin/limitBody":            "rows=1 3",
	"IN/noJoin/otherTable":            "rows=2 1 | 2",
	"NOTIN/noJoin/otherTable":         "rows=1 3",
	"ANY/noJoin/otherTable":           "rows=2 1 | 2",
	"ALL/noJoin/otherTable":           "rows=1 3",
	"IN/noJoin/corrPlain":             "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/corrPlain":          "rows=0 ",
	"ANY/noJoin/corrPlain":            "rows=3 1 | 2 | 3",
	"ALL/noJoin/corrPlain":            "rows=0 ",
	"IN/noJoin/corrCTE":               "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/corrCTE":            "rows=0 ",
	"ANY/noJoin/corrCTE":              "rows=3 1 | 2 | 3",
	"ALL/noJoin/corrCTE":              "rows=0 ",
	"IN/noJoin/corrSetop":             "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/corrSetop":          "rows=0 ",
	"ANY/noJoin/corrSetop":            "rows=3 1 | 2 | 3",
	"ALL/noJoin/corrSetop":            "rows=0 ",
	"IN/noJoin/corrRec":               "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/corrRec":            "rows=0 ",
	"ANY/noJoin/corrRec":              "rows=3 1 | 2 | 3",
	"ALL/noJoin/corrRec":              "rows=0 ",
	"IN/noJoin/corrJoin":              "rows=2 1 | 2",
	"NOTIN/noJoin/corrJoin":           "rows=1 3",
	"ANY/noJoin/corrJoin":             "rows=2 1 | 2",
	"ALL/noJoin/corrJoin":             "rows=1 3",
	"IN/noJoin/corrLimit":             "rows=3 1 | 2 | 3",
	"NOTIN/noJoin/corrLimit":          "rows=0 ",
	"ANY/noJoin/corrLimit":            "rows=3 1 | 2 | 3",
	"ALL/noJoin/corrLimit":            "rows=0 ",
	"EXISTS/noJoin/plain":             "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/plain":          "rows=0 ",
	"EXISTS/noJoin/cteBody":           "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/cteBody":        "rows=0 ",
	"EXISTS/noJoin/setop":             "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/setop":          "rows=0 ",
	"EXISTS/noJoin/recCTE":            "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/recCTE":         "rows=0 ",
	"EXISTS/noJoin/joinBody":          "rows=2 1 | 2",
	"NOTEXISTS/noJoin/joinBody":       "rows=1 3",
	"EXISTS/noJoin/limitBody":         "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/limitBody":      "rows=0 ",
	"EXISTS/noJoin/uncorr":            "rows=3 1 | 2 | 3",
	"NOTEXISTS/noJoin/uncorr":         "rows=0 ",
	"SCALAREQ/noJoin/plain":           "rows=1 3",
	"SCALAREQ/noJoin/cteBody":         "rows=1 3",
	"SCALAREQ/noJoin/corrPlain":       "rows=3 1 | 2 | 3",
	"SCALAREQ/noJoin/corrCTE":         "rows=3 1 | 2 | 3",
	"SCALAREQ/noJoin/corrSetopIn":     "rows=2 1 | 2",
	"SCALAREQ/noJoin/corrRec":         "rows=3 1 | 2 | 3",
	"IN/onDerived/plain":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/plain":           "rows=0 ",
	"ANY/onDerived/plain":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/plain":             "rows=0 ",
	"IN/onDerived/cteBody":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/cteBody":         "rows=0 ",
	"ANY/onDerived/cteBody":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/cteBody":           "rows=0 ",
	"IN/onDerived/setop":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/setop":           "rows=0 ",
	"ANY/onDerived/setop":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/setop":             "rows=0 ",
	"IN/onDerived/recCTE":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/recCTE":          "rows=0 ",
	"ANY/onDerived/recCTE":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/recCTE":            "rows=0 ",
	"IN/onDerived/joinBody":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/joinBody":        "rows=0 ",
	"ANY/onDerived/joinBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/joinBody":          "rows=0 ",
	"IN/onDerived/limitBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/limitBody":       "rows=0 ",
	"ANY/onDerived/limitBody":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/limitBody":         "rows=0 ",
	"IN/onDerived/otherTable":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/otherTable":      "rows=0 ",
	"ANY/onDerived/otherTable":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/otherTable":        "rows=0 ",
	"IN/onDerived/corrPlain":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrPlain":       "rows=0 ",
	"ANY/onDerived/corrPlain":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrPlain":         "rows=0 ",
	"IN/onDerived/corrCTE":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrCTE":         "rows=0 ",
	"ANY/onDerived/corrCTE":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrCTE":           "rows=0 ",
	"IN/onDerived/corrSetop":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrSetop":       "rows=0 ",
	"ANY/onDerived/corrSetop":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrSetop":         "rows=0 ",
	"IN/onDerived/corrRec":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrRec":         "rows=0 ",
	"ANY/onDerived/corrRec":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrRec":           "rows=0 ",
	"IN/onDerived/corrJoin":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrJoin":        "rows=0 ",
	"ANY/onDerived/corrJoin":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrJoin":          "rows=0 ",
	"IN/onDerived/corrLimit":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onDerived/corrLimit":       "rows=0 ",
	"ANY/onDerived/corrLimit":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onDerived/corrLimit":         "rows=0 ",
	"EXISTS/onDerived/plain":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/plain":       "rows=0 ",
	"EXISTS/onDerived/cteBody":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/cteBody":     "rows=0 ",
	"EXISTS/onDerived/setop":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/setop":       "rows=0 ",
	"EXISTS/onDerived/recCTE":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/recCTE":      "rows=0 ",
	"EXISTS/onDerived/joinBody":       "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/joinBody":    "rows=0 ",
	"EXISTS/onDerived/limitBody":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/limitBody":   "rows=0 ",
	"EXISTS/onDerived/uncorr":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onDerived/uncorr":      "rows=0 ",
	"SCALAREQ/onDerived/plain":        "rows=0 ",
	"SCALAREQ/onDerived/cteBody":      "rows=0 ",
	"SCALAREQ/onDerived/corrPlain":    "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onDerived/corrCTE":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onDerived/corrSetopIn":  "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onDerived/corrRec":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"IN/onCTE/plain":                  "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/plain":               "rows=0 ",
	"ANY/onCTE/plain":                 "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/plain":                 "rows=0 ",
	"IN/onCTE/cteBody":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/cteBody":             "rows=0 ",
	"ANY/onCTE/cteBody":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/cteBody":               "rows=0 ",
	"IN/onCTE/setop":                  "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/setop":               "rows=0 ",
	"ANY/onCTE/setop":                 "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/setop":                 "rows=0 ",
	"IN/onCTE/recCTE":                 "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/recCTE":              "rows=0 ",
	"ANY/onCTE/recCTE":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/recCTE":                "rows=0 ",
	"IN/onCTE/joinBody":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/joinBody":            "rows=0 ",
	"ANY/onCTE/joinBody":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/joinBody":              "rows=0 ",
	"IN/onCTE/limitBody":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/limitBody":           "rows=0 ",
	"ANY/onCTE/limitBody":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/limitBody":             "rows=0 ",
	"IN/onCTE/otherTable":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/otherTable":          "rows=0 ",
	"ANY/onCTE/otherTable":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/otherTable":            "rows=0 ",
	"IN/onCTE/corrPlain":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrPlain":           "rows=0 ",
	"ANY/onCTE/corrPlain":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrPlain":             "rows=0 ",
	"IN/onCTE/corrCTE":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrCTE":             "rows=0 ",
	"ANY/onCTE/corrCTE":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrCTE":               "rows=0 ",
	"IN/onCTE/corrSetop":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrSetop":           "rows=0 ",
	"ANY/onCTE/corrSetop":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrSetop":             "rows=0 ",
	"IN/onCTE/corrRec":                "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrRec":             "rows=0 ",
	"ANY/onCTE/corrRec":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrRec":               "rows=0 ",
	"IN/onCTE/corrJoin":               "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrJoin":            "rows=0 ",
	"ANY/onCTE/corrJoin":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrJoin":              "rows=0 ",
	"IN/onCTE/corrLimit":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onCTE/corrLimit":           "rows=0 ",
	"ANY/onCTE/corrLimit":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onCTE/corrLimit":             "rows=0 ",
	"EXISTS/onCTE/plain":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/plain":           "rows=0 ",
	"EXISTS/onCTE/cteBody":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/cteBody":         "rows=0 ",
	"EXISTS/onCTE/setop":              "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/setop":           "rows=0 ",
	"EXISTS/onCTE/recCTE":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/recCTE":          "rows=0 ",
	"EXISTS/onCTE/joinBody":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/joinBody":        "rows=0 ",
	"EXISTS/onCTE/limitBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/limitBody":       "rows=0 ",
	"EXISTS/onCTE/uncorr":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onCTE/uncorr":          "rows=0 ",
	"SCALAREQ/onCTE/plain":            "rows=0 ",
	"SCALAREQ/onCTE/cteBody":          "rows=0 ",
	"SCALAREQ/onCTE/corrPlain":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onCTE/corrCTE":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onCTE/corrSetopIn":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onCTE/corrRec":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"IN/onOuterRev/plain":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/plain":          "rows=0 ",
	"ANY/onOuterRev/plain":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/plain":            "rows=0 ",
	"IN/onOuterRev/cteBody":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/cteBody":        "rows=0 ",
	"ANY/onOuterRev/cteBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/cteBody":          "rows=0 ",
	"IN/onOuterRev/setop":             "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/setop":          "rows=0 ",
	"ANY/onOuterRev/setop":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/setop":            "rows=0 ",
	"IN/onOuterRev/recCTE":            "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/recCTE":         "rows=0 ",
	"ANY/onOuterRev/recCTE":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/recCTE":           "rows=0 ",
	"IN/onOuterRev/joinBody":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/joinBody":       "rows=0 ",
	"ANY/onOuterRev/joinBody":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/joinBody":         "rows=0 ",
	"IN/onOuterRev/limitBody":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/limitBody":      "rows=0 ",
	"ANY/onOuterRev/limitBody":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/limitBody":        "rows=0 ",
	"IN/onOuterRev/otherTable":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/otherTable":     "rows=0 ",
	"ANY/onOuterRev/otherTable":       "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/otherTable":       "rows=0 ",
	"IN/onOuterRev/corrPlain":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrPlain":      "rows=0 ",
	"ANY/onOuterRev/corrPlain":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrPlain":        "rows=0 ",
	"IN/onOuterRev/corrCTE":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrCTE":        "rows=0 ",
	"ANY/onOuterRev/corrCTE":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrCTE":          "rows=0 ",
	"IN/onOuterRev/corrSetop":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrSetop":      "rows=0 ",
	"ANY/onOuterRev/corrSetop":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrSetop":        "rows=0 ",
	"IN/onOuterRev/corrRec":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrRec":        "rows=0 ",
	"ANY/onOuterRev/corrRec":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrRec":          "rows=0 ",
	"IN/onOuterRev/corrJoin":          "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrJoin":       "rows=0 ",
	"ANY/onOuterRev/corrJoin":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrJoin":         "rows=0 ",
	"IN/onOuterRev/corrLimit":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTIN/onOuterRev/corrLimit":      "rows=0 ",
	"ANY/onOuterRev/corrLimit":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"ALL/onOuterRev/corrLimit":        "rows=0 ",
	"EXISTS/onOuterRev/plain":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/plain":      "rows=0 ",
	"EXISTS/onOuterRev/cteBody":       "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/cteBody":    "rows=0 ",
	"EXISTS/onOuterRev/setop":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/setop":      "rows=0 ",
	"EXISTS/onOuterRev/recCTE":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/recCTE":     "rows=0 ",
	"EXISTS/onOuterRev/joinBody":      "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/joinBody":   "rows=0 ",
	"EXISTS/onOuterRev/limitBody":     "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/limitBody":  "rows=0 ",
	"EXISTS/onOuterRev/uncorr":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"NOTEXISTS/onOuterRev/uncorr":     "rows=0 ",
	"SCALAREQ/onOuterRev/plain":       "rows=0 ",
	"SCALAREQ/onOuterRev/cteBody":     "rows=0 ",
	"SCALAREQ/onOuterRev/corrPlain":   "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuterRev/corrCTE":     "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuterRev/corrSetopIn": "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"SCALAREQ/onOuterRev/corrRec":     "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"REC/join":                        "rows=3 1,1 | 2,2 | 3,3",
	"REC/joinFiltered":                "rows=1 2,2",
	"REC/joinRev":                     "rows=3 1,1 | 2,2 | 3,3",
	"REC/leftJoin":                    "rows=3 1,1 | 2,2 | 3,3",
	"REC/nestedDerived":               "rows=3 1,1 | 2,2 | 3,3",
	"REC/inSpelling":                  "rows=3 1 | 2 | 3",
	"REC/joinTwice":                   "rows=4 1,1 | 1,1 | 2,2 | 2,2",
	"CTE/join":                        "rows=3 1,1 | 2,2 | 3,3",
	"ISSUE/1067":                      "rows=2 1 | 2",
	"ISSUE/1072":                      "rows=3 100 | 42 | NULL",
	"ISSUE/1072union":                 "rows=3 100 | 42 | NULL",
	"ISSUE/1072intersect":             "rows=3 100 | 42 | NULL",
	"ISSUE/1072except":                "rows=3 100 | 42 | NULL",
	"ISSUE/1072exists":                "rows=2 1 | 2",
	"ISSUE/1072in":                    "rows=2 1 | 2",
	"ISSUE/1098a":                     "rows=1 4",
	"ISSUE/1098b":                     "rows=1 4",
	"ISSUE/1098c":                     "rows=1 4",
}

var r1RefusedSetOpBody = []string{
	"IN/onOuter/corrSetop",
	"NOTIN/onOuter/corrSetop",
	"ANY/onOuter/corrSetop",
	"ALL/onOuter/corrSetop",
	"EXISTS/onOuter/setop",
	"NOTEXISTS/onOuter/setop",
	"IN/onInner/corrSetop",
	"NOTIN/onInner/corrSetop",
	"ANY/onInner/corrSetop",
	"ALL/onInner/corrSetop",
	"EXISTS/onInner/setop",
	"NOTEXISTS/onInner/setop",
	"IN/noJoin/corrSetop",
	"NOTIN/noJoin/corrSetop",
	"ANY/noJoin/corrSetop",
	"ALL/noJoin/corrSetop",
	"EXISTS/noJoin/setop",
	"NOTEXISTS/noJoin/setop",
	"IN/onDerived/corrSetop",
	"NOTIN/onDerived/corrSetop",
	"ANY/onDerived/corrSetop",
	"ALL/onDerived/corrSetop",
	"EXISTS/onDerived/setop",
	"NOTEXISTS/onDerived/setop",
	"IN/onCTE/corrSetop",
	"NOTIN/onCTE/corrSetop",
	"ANY/onCTE/corrSetop",
	"ALL/onCTE/corrSetop",
	"EXISTS/onCTE/setop",
	"NOTEXISTS/onCTE/setop",
	"IN/onOuterRev/corrSetop",
	"NOTIN/onOuterRev/corrSetop",
	"ANY/onOuterRev/corrSetop",
	"ALL/onOuterRev/corrSetop",
	"EXISTS/onOuterRev/setop",
	"NOTEXISTS/onOuterRev/setop",
}

var r1RefusedNestedSetOp = []string{
	"SCALAREQ/onOuter/corrSetopIn",
	"SCALAREQ/onInner/corrSetopIn",
	"SCALAREQ/noJoin/corrSetopIn",
	"SCALAREQ/onDerived/corrSetopIn",
	"SCALAREQ/onCTE/corrSetopIn",
	"SCALAREQ/onOuterRev/corrSetopIn",
	"ISSUE/1072",
	"ISSUE/1072union",
	"ISSUE/1072intersect",
	"ISSUE/1072except",
	"ISSUE/1072exists",
	"ISSUE/1072in",
}

var r1RefusedOnTheDAG = []string{
	"REC/join",
	"REC/joinFiltered",
	"REC/joinRev",
	"REC/leftJoin",
	"REC/nestedDerived",
	"REC/joinTwice",
}

// r1Fmt renders one cell value. Every probe column is an integer or NULL by
// construction, so a formatting difference cannot be mistaken for a row-set one.
func r1Fmt(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case int32:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case int:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// r1RenderRows is the ROW SET: sorted, so a legal ordering difference between
// arms is never read as a wrong answer (ADR-0013's nondeterminism classes).
// Every case carries its own ORDER BY for the values that need one.
func r1RenderRows(rows [][]any) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, c := range r {
			cells[i] = r1Fmt(c)
		}
		out = append(out, strings.Join(cells, ","))
	}
	sort.Strings(out)
	return fmt.Sprintf("rows=%d %s", len(out), strings.Join(out, " | "))
}

func r1Single(ctx context.Context, db *wadjet.DB, sql string) string {
	out, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	var all [][]any
	for i := range out.Rows {
		all = append(all, out.Cells(i))
	}
	return r1RenderRows(all)
}

func r1DAG(ctx context.Context, c *Coordinator, sql string) string {
	out, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	if out.Error != "" {
		return "ERR " + out.Error
	}
	var all [][]any
	s := out.Stream()
	defer s.Close()
	for {
		bb, err := s.Next(ctx)
		if err != nil {
			return "ERR " + err.Error()
		}
		if bb == nil {
			break
		}
		all = append(all, bb.ToRowValues()...)
	}
	return r1RenderRows(all)
}

type r1Arm struct {
	name string
	run  func(string) string
}

// r1Arms stands the five arms up over the shared corpus.
func r1Arms(t *testing.T, ctx context.Context) []r1Arm {
	t.Helper()
	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })
	return []r1Arm{
		{"single", func(s string) string { return r1Single(ctx, single, s) }},
		{"spilled512k", func(s string) string { return r1Single(ctx, spilled, s) }},
		{"dag", func(s string) string { return r1DAG(ctx, coord, s) }},
		{"dag-shuffled", func(s string) string { return r1DAG(ctx, coordB, s) }},
		{"dag-morsel4", func(s string) string { return r1DAG(ctx, coordM, s) }},
	}
}

func r1Set(names []string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

func TestArcR1ACorrelatedBodyAnswersPostgresRowSetOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over a 450-cell table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := r1Arms(t, ctx)
	setOp := r1Set(r1RefusedSetOpBody)
	nested := r1Set(r1RefusedNestedSetOp)
	dagOnly := r1Set(r1RefusedOnTheDAG)
	seen := make(map[string]bool, len(r1PostgresRowSets))
	for _, tc := range r1Cases() {
		want, ok := r1PostgresRowSets[tc.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL row set recorded — the table and the "+
				"measurement have diverged", tc.name)
		}
		seen[tc.name] = true
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				refusal := ""
				switch {
				case setOp[tc.name]:
					refusal = r1SetOpBodyRefusal
				case nested[tc.name]:
					refusal = r1NestedSetOpRefusal
				case dagOnly[tc.name] && strings.HasPrefix(arm.name, "dag"):
					refusal = r1DAGRecursiveCTERefusal
				}
				if refusal != "" {
					// A PIN THAT STARTS AGREEING FAILS. When the boundary
					// closes, the cell is asserted against PostgreSQL — which
					// is already recorded above — and the pin is deleted.
					if !strings.Contains(got, refusal) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  the pinned refusal (%q) is gone: "+
							"assert PostgreSQL's row set %s and delete this cell's pin",
							tc.sql, arm.name, got, refusal, want)
					}
					continue
				}
				if got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						tc.sql, arm.name, got, want)
				}
			}
		})
	}
	for name := range r1PostgresRowSets {
		if !seen[name] {
			t.Errorf("%s: a PostgreSQL row set is recorded for a cell the table "+
				"no longer writes", name)
		}
	}
}
