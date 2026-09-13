package coordinator

import (
	"fmt"
	"strings"
)

// THE DERIVED-BLOCK / SLOT-IDENTITY SEAM, enumerated once (arc O2).
//
// One rule, stated as a table rather than as a list of shapes: a derived block
// — a subquery in FROM, a CTE, a LATERAL — publishes EXACTLY its visible
// projection, by POSITION and under the name the block wrote, to every
// consumer above it (a bare star, a qualified star, an explicit list, a sort
// key). A slot the PLANNER minted for itself (`__sortkey_N`, `__key_N`) dies
// where the operator that minted it ends, and never reaches a client.
//
// The table is the cross product of
//
//	{derived table, CTE, LATERAL, grouped LATERAL, derived over a self-join}
//	  × {plain, unaliased expression item, an alias equal to a source column's
//	     name, two aliases that swap a pair of source names, an inner ORDER BY
//	     on a published key / on a hidden one, with and without LIMIT, an inner
//	     GROUP BY}
//	  × {`*`, `x.*`, an explicit list, an ORDER BY over the block's own
//	     published columns}
//
// with every cell measured on live PostgreSQL 17.11 over the SAME rows the
// corpus holds (`lat_ord`, `lat_item`) — see o2_landing_notes.md for the
// script that produced `o2Want`. A cell with no recorded PostgreSQL answer
// fails: the table is the claim, and a position nobody measured is not one.
//
// The consumers are separated because each binds by a different rule and each
// was wrong on its own: a bare star reads the block's relation, a QUALIFIED
// star resolved the block's items BY NAME (#1077 — an unaliased item has no
// name, so it read NULL under a STRING `?column?`), an explicit list is the
// spelling that was always right, and a sort key binds the published slot
// (#1023 — the second key over a derived self-join was dropped).

// o2Consumer is how a statement above the block reads it.
type o2Consumer struct {
	key string
	// build renders the whole statement from the block's pieces.
	build func(b o2Block, s o2Shape) string
	// sorted compares the rows as a MULTISET: the statement has no ORDER BY
	// of its own, so the row order is not the question and not a claim.
	sorted bool
}

// o2Block is one class of derived block.
type o2Block struct {
	key string
	// stmt renders the statement: sel is the select list, tail the clauses
	// after the block (an ORDER BY of the statement's own).
	stmt func(items, inner, sel, tail string) string
	// outer names the outer relation's key column for a total ORDER BY, or ""
	// when the block is the only relation.
	outerKey string
}

// o2Shape is one block projection: what the block writes, and what it
// therefore publishes.
type o2Shape struct {
	key   string
	items string
	inner string
	// pub is every name the block publishes, in order; spell is the subset a
	// query can write (an unaliased expression publishes `?column?`, which is
	// a rendering and not a name).
	pub   []string
	spell []string
	// ordKeys is the statement's own ORDER BY, or nil where no total order
	// over the block's published columns exists (two columns of one name).
	ordKeys []string
}

func o2Blocks() []o2Block {
	return []o2Block{
		{key: "derived", stmt: func(items, inner, sel, tail string) string {
			return fmt.Sprintf("SELECT %s FROM (SELECT %s FROM lat_item%s) x%s", sel, items, inner, tail)
		}},
		{key: "cte", stmt: func(items, inner, sel, tail string) string {
			return fmt.Sprintf("WITH x AS (SELECT %s FROM lat_item%s) SELECT %s FROM x%s", items, inner, sel, tail)
		}},
		{key: "lateral", outerKey: "o.id", stmt: func(items, inner, sel, tail string) string {
			return fmt.Sprintf("SELECT %s FROM lat_ord o JOIN LATERAL (SELECT %s FROM lat_item i "+
				"WHERE i.order_id = o.id%s) x ON true%s", sel, items, inner, tail)
		}},
		{key: "selfjoin", stmt: func(items, inner, sel, tail string) string {
			return fmt.Sprintf("SELECT %s FROM (SELECT %s FROM lat_item a JOIN lat_item b "+
				"ON b.order_id = a.order_id%s) x%s", sel, items, inner, tail)
		}},
		// A derived block that is one SIDE of a join, which is the shape a
		// minted slot reaches a client through: with the block as the only
		// relation, the statement's own output projection IS the block's and
		// the hidden tail is trimmed there (physical.hiddenSortTrimOp). Put a
		// join above it and the star reads the block's stream instead (#991).
		//
		// The block is written FIRST because `SELECT *` publishes the FROM
		// order and wadjet publishes the BUILD side first — which side builds
		// is a cost decision, and arc O1 owns the rule that it must not decide
		// a name or a position (ADR-0026 §7's note on
		// `markCoPathingSelfJoinBuilds`, #997). Writing the block first makes
		// the two agree for the shapes this table is about, so a divergence
		// here is a slot-identity one and not that.
		{key: "joined", outerKey: "o.id", stmt: func(items, inner, sel, tail string) string {
			return fmt.Sprintf("SELECT %s FROM (SELECT %s FROM lat_item%s) x JOIN lat_ord o "+
				"ON true%s", sel, items, inner, tail)
		}},
	}
}

// o2Shapes are the block projections for the four classes above, spelled
// against the source each one names.
func o2Shapes(block string) []o2Shape {
	q := ""          // the qualifier an item must carry inside this block
	qa, qb := "", "" // the self-join's two sides
	switch block {
	case "lateral":
		q = "i."
	case "selfjoin":
		qa, qb = "a.", "b."
	}
	if block == "selfjoin" {
		return []o2Shape{
			{key: "plain", items: qa + "order_id, " + qa + "amount, " + qb + "amount AS b_amount",
				pub:   []string{"order_id", "amount", "b_amount"},
				spell: []string{"order_id", "amount", "b_amount"},
				// #1023's own shape: the SECOND key is the claim, and the
				// third makes the order total over a relation with ties.
				ordKeys: []string{"x.order_id", "x.b_amount", "x.amount"}},
			{key: "two-keys", items: qa + "order_id, " + qa + "amount, " + qb + "amount AS b_amount",
				pub:     []string{"order_id", "amount", "b_amount"},
				spell:   []string{"order_id", "amount", "b_amount"},
				ordKeys: []string{"x.order_id", "x.b_amount"}},
			{key: "unaliased", items: qa + "order_id, " + qa + "amount + 1",
				pub: []string{"order_id", "?column?"}, spell: []string{"order_id"}},
			{key: "alias-src", items: qa + "order_id, " + qb + "amount AS product",
				pub: []string{"order_id", "product"}, spell: []string{"order_id", "product"},
				ordKeys: []string{"x.order_id", "x.product"}},
			{key: "inner-order-hidden", items: qa + "order_id, " + qa + "product",
				inner: " ORDER BY " + qa + "amount",
				pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"}},
			{key: "inner-order-hidden-limit", items: qa + "order_id, " + qa + "product",
				inner: " ORDER BY " + qa + "amount LIMIT 3",
				pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"}},
			{key: "distinct", items: "DISTINCT " + qa + "order_id, " + qb + "amount AS b_amount",
				pub: []string{"order_id", "b_amount"}, spell: []string{"order_id", "b_amount"},
				ordKeys: []string{"x.order_id", "x.b_amount"}},
		}
	}
	base := []o2Shape{
		{key: "plain", items: q + "order_id, " + q + "amount",
			pub: []string{"order_id", "amount"}, spell: []string{"order_id", "amount"},
			ordKeys: []string{"x.order_id", "x.amount"}},
		{key: "unaliased", items: q + "order_id, " + q + "amount + 1",
			pub: []string{"order_id", "?column?"}, spell: []string{"order_id"},
			ordKeys: []string{"x.order_id"}},
		{key: "unaliased-string", items: q + "order_id, " + q + "product || 'y'",
			pub: []string{"order_id", "?column?"}, spell: []string{"order_id"},
			ordKeys: []string{"x.order_id"}},
		{key: "alias-src", items: q + "order_id, " + q + "amount AS product",
			pub: []string{"order_id", "product"}, spell: []string{"order_id", "product"},
			ordKeys: []string{"x.order_id", "x.product"}},
		{key: "alias-swap", items: q + "amount AS order_id, " + q + "order_id AS amount",
			pub: []string{"order_id", "amount"}, spell: []string{"order_id", "amount"},
			ordKeys: []string{"x.order_id", "x.amount"}},
		{key: "inner-order-pub", items: q + "order_id, " + q + "product",
			inner: " ORDER BY " + q + "product",
			pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"},
			ordKeys: []string{"x.order_id", "x.product"}},
		{key: "inner-order-pub-limit", items: q + "order_id, " + q + "product",
			inner: " ORDER BY " + q + "product LIMIT 3",
			pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"}},
		{key: "inner-order-hidden", items: q + "order_id, " + q + "product",
			inner: " ORDER BY " + q + "amount",
			pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"},
			ordKeys: []string{"x.order_id", "x.product"}},
		{key: "inner-order-hidden-limit", items: q + "order_id, " + q + "product",
			inner: " ORDER BY " + q + "amount LIMIT 3",
			pub:   []string{"order_id", "product"}, spell: []string{"order_id", "product"}},
		{key: "group", items: q + "order_id, COUNT(*) AS n",
			inner: " GROUP BY " + q + "order_id",
			pub:   []string{"order_id", "n"}, spell: []string{"order_id", "n"},
			ordKeys: []string{"x.order_id", "x.n"}},
		{key: "group-order-pub", items: q + "product AS p, COUNT(*) AS n",
			inner: " GROUP BY " + q + "product ORDER BY p",
			pub:   []string{"p", "n"}, spell: []string{"p", "n"},
			ordKeys: []string{"x.p", "x.n"}},
		// #1020's own spelling: ONE grouped item, ordered by it. The minted
		// correlation key is the block's other column and the sort sits
		// between the projection and the join that has to drop it.
		{key: "group-order-pub-alone", items: q + "product AS p",
			inner: " GROUP BY " + q + "product ORDER BY p",
			pub:   []string{"p"}, spell: []string{"p"},
			ordKeys: []string{"x.p"}},
		{key: "group-order-hidden", items: q + "product AS p",
			inner: " GROUP BY " + q + "product ORDER BY COUNT(*)",
			pub:   []string{"p"}, spell: []string{"p"},
			ordKeys: []string{"x.p"}},
		{key: "group-alias-src", items: "COUNT(*) AS product",
			inner: " GROUP BY " + q + "product",
			pub:   []string{"product"}, spell: []string{"product"},
			ordKeys: []string{"x.product"}},
		{key: "group-unaliased", items: q + "product, COUNT(*) + 1",
			inner: " GROUP BY " + q + "product",
			pub:   []string{"product", "?column?"}, spell: []string{"product"},
			ordKeys: []string{"x.product"}},
	}
	return base
}

func o2Consumers() []o2Consumer {
	return []o2Consumer{
		{key: "star", sorted: true, build: func(b o2Block, s o2Shape) string {
			return b.stmt(s.items, s.inner, "*", "")
		}},
		{key: "qstar", sorted: true, build: func(b o2Block, s o2Shape) string {
			return b.stmt(s.items, s.inner, "x.*", "")
		}},
		{key: "list", sorted: true, build: func(b o2Block, s o2Shape) string {
			if len(s.spell) == 0 {
				return ""
			}
			cols := make([]string, len(s.spell))
			for i, c := range s.spell {
				cols[i] = "x." + c
			}
			return b.stmt(s.items, s.inner, strings.Join(cols, ", "), "")
		}},
		// A star UNDER an ORDER BY that names the block's own columns. The
		// claim here is the COLUMN SET — that the sort key binding did not
		// change what the star publishes — so the rows are compared as a
		// multiset; `ordkeys` below carries the ORDER claim, where the
		// projected tuple is the key tuple and ties are identical rows.
		{key: "ordstar", sorted: true, build: func(b o2Block, s o2Shape) string {
			if len(s.ordKeys) == 0 {
				return ""
			}
			return b.stmt(s.items, s.inner, "*", " ORDER BY "+strings.Join(o2OrdKeys(b, s), ", "))
		}},
		// The ORDER claim, and the reason it is a separate cell: a result
		// whose projection IS its sort key has no ties that are not identical
		// rows, so PostgreSQL's sequence is fully determined and comparing it
		// pins the ORDER rather than an implementation's tie-breaking
		// (ADR-0013's legal nondeterminism).
		{key: "ordkeys", build: func(b o2Block, s o2Shape) string {
			if len(s.ordKeys) == 0 {
				return ""
			}
			keys := o2OrdKeys(b, s)
			return b.stmt(s.items, s.inner, strings.Join(keys, ", "), " ORDER BY "+strings.Join(keys, ", "))
		}},
	}
}

// o2OrdKeys is the statement's own ORDER BY over a block: the block's
// published columns, then the outer relation's key where the block is joined
// to one.
func o2OrdKeys(b o2Block, s o2Shape) []string {
	keys := append([]string(nil), s.ordKeys...)
	if b.outerKey != "" {
		keys = append(keys, b.outerKey)
	}
	return keys
}

// o2Cell is one position in the table.
type o2Cell struct {
	name   string
	sql    string
	sorted bool
	// keyCols names the output POSITIONS whose sequence is this cell's ORDER
	// claim, for a statement whose own ORDER BY does not fully determine the
	// row order. The rows are then compared as a multiset and the projection
	// onto these positions as a sequence — which is the ORDER the query asked
	// for, with no tie-breaking pinned (ADR-0013).
	keyCols []int
}

// o2Table walks the cross product and names each cell
// `<block>/<shape>/<consumer>`.
func o2Table() []o2Cell {
	var out []o2Cell
	for _, b := range o2Blocks() {
		for _, s := range o2Shapes(b.key) {
			for _, c := range o2Consumers() {
				sql := c.build(b, s)
				if sql == "" {
					continue
				}
				out = append(out, o2Cell{
					name:   b.key + "/" + s.key + "/" + c.key,
					sql:    sql,
					sorted: c.sorted,
				})
			}
		}
	}
	out = append(out, o2OutputSlotCells()...)
	return append(out, o2IssueCells()...)
}

// o2IssueCells are the five issues in the spelling each was REPORTED in, kept
// verbatim beside the cross product: the table is where a rule is enumerated,
// and these are where a report is answered. Two of them (#1023, #968) no
// longer reproduce at v0.19.0 and stay as cells for that reason — an issue
// closed because it stopped reproducing is closed on a measurement, and the
// measurement has to keep running.
func o2IssueCells() []o2Cell {
	return []o2Cell{
		{name: "issue/1077-derived",
			sql: `SELECT x.* FROM (SELECT id, amount + 1 FROM lat_item) x`, sorted: true},
		{name: "issue/1077-cte",
			sql: `WITH c AS (SELECT id, amount + 1 FROM lat_item) SELECT c.* FROM c`, sorted: true},
		{name: "issue/1077-concat",
			sql: `SELECT x.* FROM (SELECT id, product || 'y' FROM lat_item) x`, sorted: true},
		{name: "issue/1077-ctl-bare-star",
			sql: `SELECT * FROM (SELECT id, amount + 1 FROM lat_item) x`, sorted: true},
		{name: "issue/1077-ctl-aliased",
			sql: `SELECT x.* FROM (SELECT id, amount + 1 AS g1 FROM lat_item) x`, sorted: true},

		{name: "issue/991",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id`, sorted: true},
		{name: "issue/991-outer-order",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id ORDER BY o.id, s.product`},
		{name: "issue/991-cte",
			sql: `WITH s AS (SELECT order_id, product FROM lat_item ORDER BY amount LIMIT 3) ` +
				`SELECT * FROM lat_ord o JOIN s ON s.order_id = o.id ORDER BY o.id, s.product`},
		{name: "issue/991-qualified-star",
			sql: `SELECT s.* FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id ORDER BY s.order_id, s.product`},

		{name: "issue/1020",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product AS p FROM lat_item i ` +
				`WHERE i.order_id = o.id GROUP BY i.product ORDER BY p) s ON true ORDER BY o.id, p`},
		{name: "issue/1020-ctl-no-inner-order",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product AS p FROM lat_item i ` +
				`WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY o.id, p`},

		// #1023's own spelling. The rows tie on (order_id, b_amount), so the
		// claim is the KEY sequence — positions 0 and 2 of `order_id, amount,
		// b_amount` — and the rows are compared as a multiset beside it.
		{name: "issue/1023", sorted: true, keyCols: []int{0, 2},
			sql: `SELECT * FROM (SELECT a.order_id, a.amount, b.amount AS b_amount ` +
				`FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id) d ` +
				`ORDER BY d.order_id, d.b_amount`},
		{name: "issue/1023-keys-only",
			sql: `SELECT d.order_id, d.b_amount FROM (SELECT a.order_id, a.amount, ` +
				`b.amount AS b_amount FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id) d ` +
				`ORDER BY d.order_id, d.b_amount`},
		{name: "issue/1023-cte",
			sql: `WITH d AS (SELECT a.order_id, a.amount, b.amount AS b_amount FROM lat_item a ` +
				`JOIN lat_item b ON b.order_id = a.order_id) SELECT d.order_id, d.b_amount ` +
				`FROM d ORDER BY d.order_id, d.b_amount`},
		{name: "issue/1023-alias-equals-a-source-name",
			sql: `SELECT d.order_id, d.amount FROM (SELECT a.order_id, a.id, b.amount AS amount ` +
				`FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id) d ` +
				`ORDER BY d.order_id, d.amount`},
		{name: "issue/1023-distinct",
			sql: `SELECT DISTINCT d.order_id, d.b_amount FROM (SELECT a.order_id, a.amount, ` +
				`b.amount AS b_amount FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id) d ` +
				`ORDER BY d.order_id, d.b_amount`},
		// The same question at SCALE and with TIES, over the 5000-row matrix
		// fixture: a second key that is dropped is invisible on four rows and
		// obvious on five thousand.
		{name: "issue/1023-at-scale",
			sql: `SELECT d.g, d.i FROM (SELECT t.g AS g, t.id AS i FROM typemx t ` +
				`WHERE t.id < 40) d ORDER BY d.g, d.i`},

		// #968's family through a LATERAL: the aggregate is aliased like the
		// group key's SOURCE column. The rows say which of the two the star
		// bound — a count is 1 here and the key is a product name.
		{name: "issue/968-lateral-alias-equals-the-key-source",
			sql: `SELECT o.id, x.product FROM lat_ord o JOIN LATERAL (SELECT COUNT(*) AS product ` +
				`FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) x ON true ` +
				`ORDER BY o.id, x.product`},
		{name: "issue/968-derived-alias-equals-the-key-source",
			sql: `SELECT x.product FROM (SELECT COUNT(*) AS product FROM lat_item i ` +
				`GROUP BY i.product) x ORDER BY x.product`},
	}
}

// o2OutputSlotCells are #968's family: an OUTPUT alias equal to a GROUP BY
// source column's name, with the aggregate aliased to that name. They stand
// apart from the cross product because the block is not derived at all — the
// collision is between two OUTPUT slots of one SELECT list.
func o2OutputSlotCells() []o2Cell {
	return []o2Cell{
		{name: "outputslot/968-alias-swap-order-limit",
			sql: "SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a LIMIT 3"},
		{name: "outputslot/968-alias-swap-order",
			sql: "SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a"},
		{name: "outputslot/968-alias-swap-no-order", sorted: true,
			sql: "SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a"},
		{name: "outputslot/968-order-by-the-other",
			sql: "SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY b LIMIT 3"},
		{name: "outputslot/968-count-spelling",
			sql: "SELECT i.order_id AS product, COUNT(*) AS order_id FROM lat_item i " +
				"GROUP BY i.order_id ORDER BY order_id, product"},
		{name: "outputslot/968-ctl-no-collision",
			sql: "SELECT x.a AS ka, SUM(x.b) AS sb FROM decpair x GROUP BY x.a ORDER BY sb LIMIT 3"},
		{name: "outputslot/968-ordinal",
			sql: "SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY 2 LIMIT 3"},
	}
}
