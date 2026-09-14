package coordinator

import (
	"fmt"
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
	return append(out, r2IssueCells()...)
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
