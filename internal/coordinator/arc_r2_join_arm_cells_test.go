// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"sort"
	"strings"
)

// THE JOIN-ARM SEAM, enumerated once (arc R2).
//
// One rule, stated as a table: a join ARM THAT IS NOT A BASE SCAN — a set
// operation, a join-bodied derived block, a grouped block, a nested renamed
// block, a block carrying a sort key of its own — is keyed, shuffled, merged
// and NAMED on the three DAG arms exactly as on the single-process arms, which
// is exactly as PostgreSQL 17.11 does it.
//
// The single arm is right on all four defects this arc closes, so every cell
// has a free oracle beside the real one:
//
//   - #1102 a qualified reference into a SET-OPERATION arm bound the OTHER
//     arm's column on the three DAG arms — a wrong VALUE.
//   - #1099 DISTINCT over a join with a JOIN-BODIED arm answered rows that
//     violate the join condition on the three DAG arms — wrong ROWS.
//   - #1095 the ORDER BY above a join over a GROUPED arm ordered by the group
//     KEY where the projection reads the aggregate.
//   - #1096 a NESTED block's rename was lost where the inner block minted a
//     sort key.
//
// Every arm publishes exactly two columns — a join key and a payload — so one
// set of consumers reads all of them and a divergence is the ARM's class and
// not the statement's shape. The cross product is
//
//	{a plain derived block, UNION ALL, UNION, INTERSECT, EXCEPT, join-bodied,
//	 grouped, grouped-with-an-aggregate-aliased-like-the-key's-source,
//	 nested renamed, nested renamed over a grouped inner block,
//	 a block carrying its own sort key, the same with a LIMIT}
//	  × {names nothing else spells, names the other side spells too}
//	  × {the block on the LEFT of the join, on the RIGHT, on BOTH sides}
//	  × {an explicit list, a qualified reference alone, a star, DISTINCT,
//	     an ORDER BY above the join, a GROUP BY above the join}
//
// measured on five arms (single, spilled 512 KiB, dag, dag-shuffled,
// dag-morsel4) against live PostgreSQL 17.11 over the same rows the corpus
// holds (`lat_ord`, `lat_item`). A cell with no recorded PostgreSQL answer
// FAILS: the table is the claim, and a position nobody measured is not one.

// r2Arm is one class of non-scan join arm. Every one publishes a join key and
// a payload, and the NAMES it publishes them under are a dimension of the
// table rather than a property of the class: the defects here bind a
// qualified reference by its BARE name, so a block publishing `id` beside a
// relation that also publishes `id` is the position where a wrong relation's
// column carries a plausible value. Each class is therefore generated twice —
// once publishing `k`/`p`, which no other relation in the statement spells,
// and once publishing `id`/`customer`, which `lat_ord` spells too.
type r2Arm struct {
	key string
	// body renders the block publishing exactly two columns under the names
	// it is given.
	body func(kcol, pcol string) string
	kcol string
	pcol string
}

func (a r2Arm) k() string { return a.kcol }
func (a r2Arm) p() string { return a.pcol }

// r2ArmBody is one class of block, before its published names are chosen.
type r2ArmBody struct {
	key  string
	body func(kcol, pcol string) string
}

func r2ArmBodies() []r2ArmBody {
	return []r2ArmBody{
		// THE CONTROL: an ordinary derived block over ONE scan, which every
		// consumer here has always read correctly.
		{key: "plain", body: func(k, p string) string {
			return fmt.Sprintf("SELECT id AS %s, customer AS %s FROM lat_ord", k, p)
		}},

		// THE FOUR SET OPERATIONS. #1102's own class: the arm's columns
		// reached the join under the SCAN's qualifier, so `a.id` bound the
		// other side's column of that name.
		{key: "union-all", body: func(k, p string) string {
			return fmt.Sprintf("SELECT id AS %s, customer AS %s FROM lat_ord "+
				"UNION ALL SELECT id AS %s, customer AS %s FROM lat_ord", k, p, k, p)
		}},
		{key: "union", body: func(k, p string) string {
			return fmt.Sprintf("SELECT id AS %s, customer AS %s FROM lat_ord "+
				"UNION SELECT id AS %s, customer AS %s FROM lat_ord WHERE id < 3", k, p, k, p)
		}},
		{key: "intersect", body: func(k, p string) string {
			return fmt.Sprintf("SELECT id AS %s, customer AS %s FROM lat_ord "+
				"INTERSECT SELECT id AS %s, customer AS %s FROM lat_ord WHERE id < 3", k, p, k, p)
		}},
		{key: "except", body: func(k, p string) string {
			return fmt.Sprintf("SELECT id AS %s, customer AS %s FROM lat_ord "+
				"EXCEPT SELECT id AS %s, customer AS %s FROM lat_ord WHERE id = 3", k, p, k, p)
		}},

		// A JOIN-BODIED ARM — #1099's class. The block is itself a join, so
		// the arm the outer join keys on is a stream two stages produced.
		{key: "joinbody", body: func(k, p string) string {
			return fmt.Sprintf("SELECT o2.id AS %s, o2.customer AS %s FROM lat_item i2 "+
				"JOIN lat_ord o2 ON o2.id = i2.order_id", k, p)
		}},

		// A GROUPED ARM, and the same with the aggregate ALIASED LIKE THE
		// GROUP KEY'S SOURCE COLUMN — #1095's class, where the published name
		// and the key's own source name are the same word.
		{key: "grouped", body: func(k, p string) string {
			return fmt.Sprintf("SELECT order_id AS %s, MAX(product) AS %s FROM lat_item "+
				"GROUP BY order_id", k, p)
		}},
		{key: "grouped-alias-src", body: func(k, p string) string {
			return fmt.Sprintf("SELECT COUNT(*) AS %s, MIN(i.product) AS %s FROM lat_item i "+
				"GROUP BY i.product", k, p)
		}},

		// A NESTED RENAMED ARM: the outer block renames what the inner block
		// published, and the inner block MINTED A SORT KEY — #1096's class.
		{key: "nested-rename", body: func(k, p string) string {
			return fmt.Sprintf("SELECT z.zk AS %s, z.zp AS %s FROM "+
				"(SELECT order_id AS zk, product AS zp FROM lat_item ORDER BY amount) z", k, p)
		}},
		{key: "nested-rename-grouped", body: func(k, p string) string {
			return fmt.Sprintf("SELECT z.n AS %s, z.p AS %s FROM "+
				"(SELECT product AS p, COUNT(*) AS n FROM lat_item GROUP BY product "+
				"ORDER BY COUNT(*)) z", k, p)
		}},

		// A BLOCK CARRYING ITS OWN SORT KEY, un-nested: the control that says
		// whether a divergence above belongs to the nesting or to the key.
		{key: "sortkey", body: func(k, p string) string {
			return fmt.Sprintf("SELECT order_id AS %s, product AS %s FROM lat_item "+
				"ORDER BY amount", k, p)
		}},
		{key: "sortkey-limit", body: func(k, p string) string {
			return fmt.Sprintf("SELECT order_id AS %s, product AS %s FROM lat_item "+
				"ORDER BY amount LIMIT 3", k, p)
		}},
	}
}

// r2Arms crosses every class with the two NAME SETS: `k`/`p`, which nothing
// else in the statement spells, and `id`/`customer`, which the other side
// spells too.
func r2Arms() []r2Arm {
	var out []r2Arm
	for _, b := range r2ArmBodies() {
		out = append(out,
			r2Arm{key: b.key, body: b.body, kcol: "k", pcol: "p"},
			r2Arm{key: b.key + "-collide", body: b.body, kcol: "id", pcol: "customer"})
	}
	return out
}

// r2Side is which side of the join the block sits on. `both` makes the other
// side a second copy of the same block, which is the shape where a qualifier
// that binds the wrong relation carries a value that LOOKS plausible.
type r2Side struct {
	key string
	// from renders the FROM clause; a is the block's alias, b the other side's.
	from func(arm r2Arm) string
	// otherCols are the other side's columns, qualified, in FROM order.
	otherCols func(arm r2Arm) []string
	// otherKey is the other side's join key, qualified.
	otherKey func(arm r2Arm) string
}

func r2Sides() []r2Side {
	return []r2Side{
		{key: "left",
			from: func(arm r2Arm) string {
				return fmt.Sprintf("(%s) a JOIN lat_ord o ON o.id = a.%s",
					arm.body(arm.k(), arm.p()), arm.k())
			},
			otherCols: func(r2Arm) []string { return []string{"o.id", "o.customer"} },
			otherKey:  func(r2Arm) string { return "o.id" }},
		{key: "right",
			from: func(arm r2Arm) string {
				return fmt.Sprintf("lat_ord o JOIN (%s) a ON o.id = a.%s",
					arm.body(arm.k(), arm.p()), arm.k())
			},
			otherCols: func(r2Arm) []string { return []string{"o.id", "o.customer"} },
			otherKey:  func(r2Arm) string { return "o.id" }},
		{key: "both",
			from: func(arm r2Arm) string {
				body := arm.body(arm.k(), arm.p())
				return fmt.Sprintf("(%s) a JOIN (%s) b ON b.%s = a.%s",
					body, body, arm.k(), arm.k())
			},
			otherCols: func(arm r2Arm) []string { return []string{"b." + arm.k(), "b." + arm.p()} },
			otherKey:  func(arm r2Arm) string { return "b." + arm.k() }},
	}
}

// r2Consumer is how the statement above the join reads the arm.
type r2Consumer struct {
	key   string
	build func(arm r2Arm, side r2Side) string
	// sorted compares the rows as a MULTISET: the statement wrote no total
	// ORDER BY, so the row order is not a claim (ADR-0013).
	sorted bool
}

func r2Consumers() []r2Consumer {
	return []r2Consumer{
		// An EXPLICIT LIST over both sides — the spelling that names every
		// column and so says which relation each one came from.
		{key: "list", sorted: true, build: func(arm r2Arm, side r2Side) string {
			cols := append([]string{"a." + arm.k(), "a." + arm.p()}, side.otherCols(arm)...)
			return "SELECT " + strings.Join(cols, ", ") + " FROM " + side.from(arm)
		}},
		// A QUALIFIED REFERENCE ALONE — #1102's own spelling. Nothing else in
		// the statement can stand in for the column, so the value is the
		// whole answer.
		{key: "qual", sorted: true, build: func(arm r2Arm, side r2Side) string {
			return "SELECT a." + arm.k() + " FROM " + side.from(arm)
		}},
		{key: "star", sorted: true, build: func(arm r2Arm, side r2Side) string {
			return "SELECT * FROM " + side.from(arm)
		}},
		// DISTINCT over the pair of keys — #1099's own spelling: a row that
		// violates the join condition is one where the two keys differ.
		{key: "distinct", sorted: true, build: func(arm r2Arm, side r2Side) string {
			cols := []string{"a." + arm.k(), "a." + arm.p(), side.otherKey(arm)}
			return "SELECT DISTINCT " + strings.Join(cols, ", ") + " FROM " + side.from(arm)
		}},
		// An ORDER BY ABOVE THE JOIN whose keys ARE the projection — #1095's
		// spelling. Ties are identical rows, so PostgreSQL's sequence is
		// fully determined and comparing it pins the ORDER rather than an
		// implementation's tie-breaking.
		{key: "ord", build: func(arm r2Arm, side r2Side) string {
			cols := []string{"a." + arm.p(), "a." + arm.k(), side.otherKey(arm)}
			return "SELECT " + strings.Join(cols, ", ") + " FROM " + side.from(arm) +
				" ORDER BY " + strings.Join(cols, ", ")
		}},
		// A GROUP BY above the join, keyed on the arm's own published column.
		{key: "group", build: func(arm r2Arm, side r2Side) string {
			return "SELECT a." + arm.k() + ", COUNT(*) AS n FROM " + side.from(arm) +
				" GROUP BY a." + arm.k() + " ORDER BY a." + arm.k() + ", n"
		}},
	}
}

// r2EmptyBuildCells are the two shapes §8i item 5's rule does not reach, each
// measured on five arms and recorded as what it IS rather than as what the
// rule says (arc R2 round 2's closure review, N2 and N3).
//
// The declaration describes the stream only where this layer can derive it
// from the logical plan. Two producers put a relation into the stream that no
// walk of that plan states:
//
//   - a SET OPERATION whose arms are FILTERED, so one shuffle partition of the
//     operation's own output is empty while another is not. The stage's files
//     then disagree about a column's NAME (`names column 1 "s.id" where an
//     earlier file of the same stage input named it "k"`), about the WIDTH of
//     a star, or the GROUP BY key of the DISTINCT spelling resolves against an
//     input that no longer carries it. The 36-cell outer dimension carries a
//     set-op arm, but only with a FULL build, so the empty-partition condition
//     is never reached there — which is why these nine cells exist.
//   - a block whose body is a CO-PATHING SELF-JOIN under a FULL join, where
//     `markCoPathingSelfJoinBuilds` qualifies every build column of BOTH
//     joins and the declaration is one column narrower than the file.
//
// Every one of them is a loud REFUSAL on the arms that diverge — never a
// value — and every one is identical at base `2d819c95`. They are recorded per
// arm in `r2Refuse` with the stable part of the message, because the text
// carries a query id and a file name that differ on every run.
func r2EmptyBuildCells() []r2Cell {
	setop := "(SELECT o2.id AS k, o2.customer AS c FROM lat_ord o2 WHERE o2.id = 1 " +
		"UNION SELECT o2.id AS k, o2.customer AS c FROM lat_ord o2 WHERE o2.id = 1)"
	selfjoin := "(SELECT a.order_id AS k, b.product AS c FROM lat_item a " +
		"JOIN lat_item b ON a.id = b.id)"
	kinds := []struct{ key, from string }{
		{"left", "lat_ord o LEFT JOIN %s s ON s.k = o.id"},
		{"right", "%s s RIGHT JOIN lat_ord o ON s.k = o.id"},
		{"full", "lat_ord o FULL JOIN %s s ON s.k = o.id"},
	}
	consumers := []struct{ key, sel string }{
		{"list", "SELECT o.id, s.k, s.c FROM "},
		{"star", "SELECT * FROM "},
		{"distinct", "SELECT DISTINCT o.id, s.k FROM "},
	}
	var out []r2Cell
	for _, kind := range kinds {
		for _, c := range consumers {
			out = append(out, r2Cell{
				name:   "emptybuild/setop/" + kind.key + "/" + c.key,
				sql:    c.sel + fmt.Sprintf(kind.from, setop),
				sorted: true,
			})
		}
	}
	for _, kind := range kinds {
		out = append(out, r2Cell{
			name:   "emptybuild/selfjoin/" + kind.key + "/list",
			sql:    "SELECT o.id, s.k FROM " + fmt.Sprintf(kind.from, selfjoin),
			sorted: true,
		})
	}
	// The INNER control for each body: the same arm under a join that asks no
	// task to shape a row its data did not produce.
	out = append(out,
		r2Cell{name: "emptybuild/setop/ctl-inner/list", sorted: true,
			sql: "SELECT o.id, s.k, s.c FROM lat_ord o JOIN " + setop + " s ON s.k = o.id"},
		r2Cell{name: "emptybuild/selfjoin/ctl-inner/list", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o JOIN " + selfjoin + " s ON s.k = o.id"})
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// r2Cell is one position in the table.
type r2Cell struct {
	name   string
	sql    string
	sorted bool
}

// r2Table walks the cross product and names each cell
// `<arm>/<side>/<consumer>`.
func r2Table() []r2Cell {
	var out []r2Cell
	for _, arm := range r2Arms() {
		for _, side := range r2Sides() {
			for _, c := range r2Consumers() {
				out = append(out, r2Cell{
					name:   arm.key + "/" + side.key + "/" + c.key,
					sql:    c.build(arm, side),
					sorted: c.sorted,
				})
			}
		}
	}
	out = append(out, r2OuterCells()...)
	out = append(out, r2EmptyBuildCells()...)
	return append(out, r2IssueCells()...)
}

// r2OuterCells is the OUTER-JOIN dimension of the same seam: the arm on the
// NULL-SUPPLYING side of a LEFT, RIGHT or FULL join.
//
// The cross product above joins with a plain `JOIN` throughout, and an outer
// join asks the seam a question an inner one cannot: the task whose build
// partition is EMPTY must still emit the NULL-extended probe rows, under the
// same relation every other task emits. It does that from the side's DECLARED
// schema — so a declaration that is narrower than the stream is a file of the
// wrong WIDTH beside its siblings (ADR-0010), and a NULL-extended row whose
// column the declaration lost reads the wrong relation's value:
// `SELECT DISTINCT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT o2.id AS k FROM
// lat_item i2 JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id` read
// `3,3` on the three DAG arms where PostgreSQL 17.11 and both single-process
// arms read `3,NULL` (arc R2 round 2, B1).
//
// {LEFT, RIGHT, FULL} × {join-bodied, set-op, grouped, nested renamed} ×
// {an explicit list, DISTINCT, a star}, with the block always on the side that
// supplies NULLs.
func r2OuterCells() []r2Cell {
	bodies := map[string]func(k, p string) string{}
	for _, b := range r2ArmBodies() {
		bodies[b.key] = b.body
	}
	kinds := []struct{ key, from string }{
		{"left", "lat_ord o LEFT JOIN (%s) a ON a.k = o.id"},
		{"right", "(%s) a RIGHT JOIN lat_ord o ON a.k = o.id"},
		{"full", "lat_ord o FULL JOIN (%s) a ON a.k = o.id"},
	}
	consumers := []struct {
		key, sel string
	}{
		{"list", "SELECT o.id, a.k, a.p FROM "},
		{"distinct", "SELECT DISTINCT o.id, a.k FROM "},
		{"star", "SELECT * FROM "},
	}
	var out []r2Cell
	for _, arm := range []string{"joinbody", "union", "grouped", "nested-rename"} {
		body := bodies[arm]("k", "p")
		body = strings.TrimSpace(body)
		for _, kind := range kinds {
			from := fmt.Sprintf(kind.from, body)
			for _, c := range consumers {
				out = append(out, r2Cell{
					name:   "outer/" + kind.key + "/" + arm + "/" + c.key,
					sql:    c.sel + from,
					sorted: true,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// r2IssueCells are the four defects in the spelling each was REPORTED in, kept
// verbatim beside the cross product: the table is where a rule is enumerated,
// and these are where a report is answered.
func r2IssueCells() []r2Cell {
	return []r2Cell{
		// #1102 — `a.id` over a set-operation arm read the other side's `id`.
		{name: "issue/1102", sorted: true,
			sql: "SELECT a.id FROM (SELECT id FROM lat_ord UNION SELECT id FROM lat_ord) a " +
				"JOIN lat_item b ON b.order_id = a.id"},
		{name: "issue/1102-all", sorted: true,
			sql: "SELECT a.id FROM (SELECT id FROM lat_ord UNION ALL SELECT id FROM lat_ord) a " +
				"JOIN lat_item b ON b.order_id = a.id"},
		{name: "issue/1102-both-sides", sorted: true,
			sql: "SELECT a.id, b.id FROM (SELECT id FROM lat_ord UNION SELECT id FROM lat_ord) a " +
				"JOIN lat_item b ON b.order_id = a.id"},
		{name: "issue/1102-ctl-renamed", sorted: true,
			sql: "SELECT a.kk FROM (SELECT id AS kk FROM lat_ord UNION SELECT id AS kk FROM lat_ord) a " +
				"JOIN lat_item b ON b.order_id = a.kk"},

		// #1099 — DISTINCT over a join whose arm is itself a join.
		{name: "issue/1099", sorted: true,
			sql: "SELECT DISTINCT o.id, o.customer, s.c, s.k FROM lat_ord o JOIN " +
				"(SELECT o2.customer AS c, o2.id AS k FROM lat_item i2 JOIN lat_ord o2 " +
				"ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/1099-no-distinct", sorted: true,
			sql: "SELECT o.id, o.customer, s.c, s.k FROM lat_ord o JOIN " +
				"(SELECT o2.customer AS c, o2.id AS k FROM lat_item i2 JOIN lat_ord o2 " +
				"ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/1099-block-first", sorted: true,
			sql: "SELECT DISTINCT o.id, o.customer, s.c, s.k FROM " +
				"(SELECT o2.customer AS c, o2.id AS k FROM lat_item i2 JOIN lat_ord o2 " +
				"ON o2.id = i2.order_id) s JOIN lat_ord o ON s.k = o.id"},
		{name: "issue/1099-ctl-plain-arm", sorted: true,
			sql: "SELECT DISTINCT o.id, o.customer, s.c, s.k FROM lat_ord o JOIN " +
				"(SELECT o2.customer AS c, o2.id AS k FROM lat_ord o2) s ON s.k = o.id"},

		// #1099's BOUNDARY, and the reason the qualifier is kept only where
		// the producer publishes the name TWICE: here ONE relation inside the
		// block publishes `amount`, so the stream spells it bare, and a key
		// that carried a qualifier anyway read as "not in the build schema"
		// to `exec.HashJoin.FixKeyAssignment`'s exact-name test — which
		// swapped a correctly assigned pair and left a null-aware anti join
		// keying on the PROBE's column, losing NOT IN's NULL.
		{name: "issue/1099-boundary-not-in",
			sql: "SELECT COUNT(*) AS c FROM lat_ord a WHERE a.total NOT IN " +
				"(SELECT s.rk FROM (SELECT r.amount AS rk FROM lat_ord b JOIN lat_item r " +
				"ON r.amount = b.total AND r.amount < 60) s)"},
		{name: "issue/1099-boundary-not-in-outer",
			sql: "SELECT COUNT(*) AS c FROM lat_ord a WHERE a.total NOT IN " +
				"(SELECT s.rk FROM (SELECT r.amount AS rk FROM lat_ord b LEFT JOIN lat_item r " +
				"ON r.amount = b.total AND r.amount < 60) s)"},
		{name: "issue/1099-boundary-in-outer",
			sql: "SELECT COUNT(*) AS c FROM lat_ord a WHERE a.total IN " +
				"(SELECT s.rk FROM (SELECT r.amount AS rk FROM lat_ord b LEFT JOIN lat_item r " +
				"ON r.amount = b.total AND r.amount < 60) s)"},

		// #1095 — the ORDER half of an aggregate aliased like its group key's
		// source column, above a JOIN.
		{name: "issue/1095",
			sql: "SELECT x.product, o.id FROM (SELECT COUNT(*) AS product FROM lat_item " +
				"GROUP BY product) x JOIN lat_ord o ON true ORDER BY x.product, o.id"},
		{name: "issue/1095-desc",
			sql: "SELECT x.product, o.id FROM (SELECT COUNT(*) AS product FROM lat_item " +
				"GROUP BY product) x JOIN lat_ord o ON true ORDER BY x.product DESC, o.id"},
		{name: "issue/1095-ctl-no-collision",
			sql: "SELECT x.n, o.id FROM (SELECT COUNT(*) AS n FROM lat_item " +
				"GROUP BY product) x JOIN lat_ord o ON true ORDER BY x.n, o.id"},

		// B1's own twelve spellings (arc R2 round 2), the reviewer's probe
		// verbatim: the join-bodied arm on the NULL-supplying side, with the
		// five controls that make the divergence a property of THAT class —
		// a plain arm, a set-operation arm, a grouped arm, the same block
		// keyed on a name only ONE relation inside it publishes (so the key
		// stays bare), the block on the PRESERVED side, and the inner-join
		// control.
		{name: "issue/b1-left-joinbody", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT o2.id AS k FROM lat_item i2 " +
				"JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/b1-left-joinbody-distinct", sorted: true,
			sql: "SELECT DISTINCT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT o2.id AS k " +
				"FROM lat_item i2 JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/b1-left-joinbody-star", sorted: true,
			sql: "SELECT * FROM lat_ord o LEFT JOIN (SELECT o2.id AS k FROM lat_item i2 " +
				"JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/b1-left-joinbody-two-cols", sorted: true,
			sql: "SELECT o.id, s.k, s.c FROM lat_ord o LEFT JOIN (SELECT o2.id AS k, " +
				"o2.customer AS c FROM lat_item i2 JOIN lat_ord o2 ON o2.id = i2.order_id) s " +
				"ON s.k = o.id"},
		{name: "issue/b1-right-joinbody", sorted: true,
			sql: "SELECT o.id, s.k FROM (SELECT o2.id AS k FROM lat_item i2 JOIN lat_ord o2 " +
				"ON o2.id = i2.order_id) s RIGHT JOIN lat_ord o ON s.k = o.id"},
		{name: "issue/b1-full-joinbody", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o FULL JOIN (SELECT o2.id AS k FROM lat_item i2 " +
				"JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id"},
		{name: "issue/b1-ctl-left-joinbody-preserved", sorted: true,
			sql: "SELECT s.k, o.id FROM (SELECT o2.id AS k FROM lat_item i2 JOIN lat_ord o2 " +
				"ON o2.id = i2.order_id) s LEFT JOIN lat_ord o ON s.k = o.id"},
		{name: "issue/b1-ctl-left-joinbody-barekey", sorted: true,
			sql: "SELECT o.total, s.k FROM lat_ord o LEFT JOIN (SELECT i2.amount AS k " +
				"FROM lat_item i2 JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.total"},
		{name: "issue/b1-ctl-left-plain", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT id AS k FROM lat_ord) s " +
				"ON s.k = o.id"},
		{name: "issue/b1-ctl-left-setop", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT id AS k FROM lat_ord " +
				"UNION SELECT id AS k FROM lat_ord) s ON s.k = o.id"},
		{name: "issue/b1-ctl-left-grouped", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o LEFT JOIN (SELECT order_id AS k FROM lat_item " +
				"GROUP BY order_id) s ON s.k = o.id"},
		{name: "issue/b1-ctl-inner-joinbody", sorted: true,
			sql: "SELECT o.id, s.k FROM lat_ord o JOIN (SELECT o2.id AS k FROM lat_item i2 " +
				"JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id"},

		// ARC R1's N1, which is #1099's class with no subquery at all: the
		// block's body is a JOIN and both of its relations publish `id`, so
		// `d.a` chased to the bare name bound the wrong one and the DAG lost
		// the rows where `u.id <> d.a`. Closed by d1a89ab4; kept as the
		// measurement that says so.
		{name: "issue/r1n1-plainjoin-nosub",
			sql: "SELECT d.a, d.b, u.id FROM (SELECT o.id AS a, i.id AS b FROM lat_ord o " +
				"JOIN lat_item i ON i.order_id = o.id) d JOIN lat_ord u ON u.id = d.a " +
				"ORDER BY 1, 2, 3"},
		{name: "issue/r1n1-plainjoin-nosub-rev",
			sql: "SELECT d.a, d.b, u.id FROM lat_ord u JOIN (SELECT o.id AS a, i.id AS b " +
				"FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) d ON u.id = d.a " +
				"ORDER BY 1, 2, 3"},
		{name: "issue/r1n1-plainjoin-distinct",
			sql: "SELECT DISTINCT d.a, d.b, u.id FROM (SELECT o.id AS a, i.id AS b " +
				"FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) d " +
				"JOIN lat_ord u ON u.id = d.a ORDER BY 1, 2, 3"},
		{name: "issue/r1n1-in-a",
			sql: "SELECT d.a, d.b FROM (SELECT o.id AS a, i.id AS b FROM lat_ord o " +
				"JOIN lat_item i ON i.order_id = o.id) d WHERE d.a IN " +
				"(SELECT z.id FROM lat_ord z) ORDER BY 1, 2"},

		// #1096 — a nested block's rename, where the inner block minted a sort
		// key, read through a star over a join.
		{name: "issue/1096-depth2", sorted: true,
			sql: "SELECT * FROM (SELECT z.p, z.n FROM (SELECT product AS p, COUNT(*) AS n " +
				"FROM lat_item GROUP BY product ORDER BY COUNT(*)) z) s JOIN lat_ord o ON true"},
		{name: "issue/1096-depth3", sorted: true,
			sql: "SELECT * FROM (SELECT y.p, y.n FROM (SELECT z.p, z.n FROM " +
				"(SELECT product AS p, COUNT(*) AS n FROM lat_item GROUP BY product " +
				"ORDER BY COUNT(*)) z) y) s JOIN lat_ord o ON true"},
		{name: "issue/1096-rename", sorted: true,
			sql: "SELECT * FROM (SELECT z.p AS pp, z.n AS nn FROM (SELECT product AS p, " +
				"COUNT(*) AS n FROM lat_item GROUP BY product ORDER BY COUNT(*)) z) s " +
				"JOIN lat_ord o ON true"},
	}
}
