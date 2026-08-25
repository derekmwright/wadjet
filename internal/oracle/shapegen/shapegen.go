// Package shapegen generates random-but-valid SQL over a described schema,
// aimed at the shapes a BI client emits and the fixed benchmark corpus does
// not contain.
//
// It is a second generator alongside sqlgen, not a replacement: sqlgen targets
// the distributed-execution breakers (aggregate-free GROUP BY, dedup past a
// shuffle, scalar-subquery HAVING thresholds) with a flat clause model.
// shapegen targets NAME RESOLUTION and ORDER BY — table aliases, self-joins,
// quoted identifiers, star projections, aliases that shadow real columns,
// ordering by expressions/aliases/ordinals/hidden columns — which needs a
// query model that knows which output column carries which value. That model
// is also what lets every generated query declare how its result may be
// compared without flaking (see Query.CompareSpec).
//
// Generation is fully determined by the seed: a failing seed IS the repro.
package shapegen

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// Kind classifies a column for operator, literal, and expression selection.
type Kind int

const (
	KindInt Kind = iota
	KindFloat
	KindText
	// KindDate is a text column holding ISO-8601 dates. Ordering and
	// comparison are identical to text, but date functions apply.
	KindDate
	// KindOpaque is a scalar whose DOMAIN the generator does not model: BYTES,
	// IPv4/IPv6/CIDR/MAC, UUID, TIMESTAMP, DURATION, PORT, PROTOCOL. The
	// generator projects it, groups by it, orders by it, joins on it, counts
	// it and MIN/MAXes it — everything it does to a column without knowing
	// what the values mean — and applies no arithmetic, no string function and
	// no date function to it.
	//
	// Nineteen of the engine's twenty-two types were unreachable by this
	// generator before this kind existed, because Kind was the whole type
	// universe and it had four members. A defect that needs a BYTES or an IPv4
	// column to fire could not be generated even in principle.
	KindOpaque
	// KindDecimal is exact-decimal numeric. Numeric for the arms that need a
	// number (SUM, AVG, arithmetic), but NOT interchangeable with float: its
	// ordering, its group-key encoding and its rendering all go through
	// separate code from FLOAT64's.
	KindDecimal
)

// Column is one generatable column. Lits are rendered SQL literals drawn from
// the column's real domain, so predicates are selective without being empty.
type Column struct {
	Name string
	Kind Kind
	Lits []string
}

// Table is one generatable table. PK is a column set unique within the table —
// the generator appends it to ORDER BY to make an ordering total, which is what
// lets a LIMIT-ed or positionally-compared query be deterministic.
type Table struct {
	Name string
	PK   []string
	Cols []Column
	// SelfJoin marks a table small enough that joining it to itself stays
	// bounded. genSelfJoin picks only from these; a schema with none gets the
	// single-table FROM instead.
	SelfJoin bool
}

// Edge is a joinable equality (FK pairs in practice).
type Edge struct {
	LTable, LCol string
	RTable, RCol string
}

// Schema is the generation universe.
type Schema struct {
	Tables []Table
	Edges  []Edge
}

func (s *Schema) table(name string) *Table {
	for i := range s.Tables {
		if s.Tables[i].Name == name {
			return &s.Tables[i]
		}
	}
	return nil
}

// Item is one select-list entry. Every generated item carries an explicit
// alias so output column names are unique and identical across engines —
// without that, results keyed by column name silently collapse duplicates and
// the two engines' default names for an expression differ.
type Item struct {
	Expr string
	// Alias is the output column name. Empty only for Star items.
	Alias string
	// Star renders instead of Expr when set ("*" or "t.*").
	Star string
	// Exact marks a value both engines must produce bit-identically: a bare
	// column reference, or an integer-valued expression. Float arithmetic and
	// aggregates are not exact, so an ordering on them may legitimately break
	// ties differently.
	Exact bool
	Agg   bool
	// Opaque marks a value whose RENDERED form does not order the way the
	// value does: an IPv4 renders "10.0.0.9" and "10.0.0.10", which compare
	// lexicographically in the opposite order to the addresses. The absolute
	// order check reads rendered cells, so it cannot judge an ORDER BY over
	// one of these — see Query.OrderKeys.
	Opaque bool
}

// From is one FROM-clause entry. The first has an empty Join.
type From struct {
	Table   string // base table, or the CTE name
	Derived string // rendered subquery, for an inline derived table
	Alias   string
	Join    string // "JOIN", "LEFT JOIN", ","
	On      string
	// PK is a column set unique within this entry's rows, as reachable from
	// Alias. Empty when the entry exposes no unique key, which is what makes
	// an ordering non-total.
	PK []string
}

// Order is one ORDER BY term.
type Order struct {
	Expr string
	Desc bool
	// Key is the output column carrying this term's value, or "" when the
	// term orders by something the select list does not project.
	Key string
	// Exact marks a term both engines compute bit-identically (see Item).
	Exact bool
	// Opaque marks a term the absolute order check cannot judge (see Item).
	Opaque bool
}

// Query is a generated query in structured form, so a failure can be shrunk
// structurally instead of by text mutation.
type Query struct {
	With     string // rendered "WITH x AS (...)" prefix, or ""
	Distinct bool
	Items    []Item
	From     []From
	Where    []string
	GroupBy  []string
	Having   string
	Order    []Order
	Limit    int
	Offset   int
	// TotalOrder records that the generator appended a uniqueness tiebreaker,
	// so no two output rows tie on the full ORDER BY list.
	TotalOrder bool
	// Shape tags the generator arm that produced this query, for coverage
	// reporting.
	Shape string
	Seed  int64
	// opaqueCols are the schema's column names whose RENDERED form does not
	// order the way the value does. Set by the generator; read only by
	// OrderKeys, to decline the absolute order check on a query that orders by
	// one of them.
	opaqueCols []string
}

// SQL renders the query.
func (q *Query) SQL() string {
	var sb strings.Builder
	sb.WriteString(q.With)
	sb.WriteString("SELECT ")
	if q.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, it := range q.Items {
		if i > 0 {
			sb.WriteString(", ")
		}
		if it.Star != "" {
			sb.WriteString(it.Star)
			continue
		}
		sb.WriteString(it.Expr)
		if it.Alias != "" {
			fmt.Fprintf(&sb, " AS %s", it.Alias)
		}
	}
	sb.WriteString(" FROM ")
	for i, f := range q.From {
		if i > 0 {
			if f.Join == "," {
				sb.WriteString(", ")
			} else {
				fmt.Fprintf(&sb, " %s ", f.Join)
			}
		}
		if f.Derived != "" {
			fmt.Fprintf(&sb, "(%s) %s", f.Derived, f.Alias)
		} else {
			fmt.Fprintf(&sb, "%s %s", f.Table, f.Alias)
		}
		if i > 0 && f.On != "" {
			fmt.Fprintf(&sb, " ON %s", f.On)
		}
	}
	if len(q.Where) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(q.Where, " AND "))
	}
	if len(q.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(q.GroupBy, ", "))
	}
	if q.Having != "" {
		sb.WriteString(" HAVING ")
		sb.WriteString(q.Having)
	}
	if len(q.Order) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, o := range q.Order {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(o.Expr)
			if o.Desc {
				sb.WriteString(" DESC")
			}
		}
	}
	if q.Limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", q.Limit)
		if q.Offset > 0 {
			fmt.Fprintf(&sb, " OFFSET %d", q.Offset)
		}
	}
	return sb.String()
}

// CompareSpec derives how this query's results may be compared. This is the
// harness's trust boundary: a mismatch under the returned spec is always a
// defect, never an artifact of SQL's under-determination.
func (q *Query) CompareSpec() oracle.CompareSpec {
	spec := oracle.CompareSpec{Limit: q.Limit}
	if len(q.Order) == 0 {
		// No ORDER BY: the row sequence carries no meaning. A LIMIT on top of
		// that picks arbitrary rows, so only the count is defined.
		if q.Limit > 0 {
			spec.Mode = oracle.CmpCount
			return spec
		}
		spec.Mode = oracle.CmpUnordered
		return spec
	}
	exact := q.TotalOrder
	for _, o := range q.Order {
		if !o.Exact {
			exact = false
		}
	}
	if exact {
		// Total order over exactly-computed keys: the sequence is fully
		// determined, so compare rows positionally — LIMIT included.
		spec.Mode = oracle.CmpOrdered
		return spec
	}
	// Ties or inexact keys are possible. Compare the row multiset plus the
	// sequence of projected key values; a LIMIT on top of that cuts at a
	// boundary SQL does not disambiguate, so drop to counts.
	spec.Mode = oracle.CmpUnordered
	if q.Limit > 0 {
		spec.Mode = oracle.CmpCount
		return spec
	}
	spec.OrderKeys = q.OrderKeys()
	return spec
}

// OrderKeys returns the ORDER BY terms whose values the result projects, in
// ORDER BY sequence. It stops at the first term the select list does not
// carry, since an unprojected key makes every later term unverifiable.
func (q *Query) OrderKeys() []oracle.OrderKey {
	var out []oracle.OrderKey
	for _, o := range q.Order {
		if o.Key == "" {
			break
		}
		if o.Opaque || q.touchesOpaque(o) {
			// The absolute check compares RENDERED cells, and for the opaque
			// types the rendering does not order like the value: IPv4
			// "10.0.0.10" sorts before "10.0.0.9" as text and after it as an
			// address; IPv6 and UUID have the same property, and DECIMAL's
			// scale-preserving text orders "10.001" before "2.0002". Judging
			// those would accuse a correct engine, so the whole query declines
			// the absolute check rather than judging a prefix of its keys.
			return nil
		}
		out = append(out, oracle.OrderKey{Alias: o.Key, Desc: o.Desc})
	}
	return out
}

// touchesOpaque reports whether an ORDER BY term reads a column whose rendered
// form does not order like its value. It matches on the SQL text of the term
// and of the select item it names, which is deliberately over-eager: declining
// the absolute check costs a check, judging an opaque ordering costs a false
// accusation against a correct engine.
func (q *Query) touchesOpaque(o Order) bool {
	if len(q.opaqueCols) == 0 {
		return false
	}
	texts := []string{o.Expr}
	for _, it := range q.Items {
		if it.Alias != "" && it.Alias == o.Key {
			texts = append(texts, it.Expr)
		}
	}
	for _, t := range texts {
		for _, c := range q.opaqueCols {
			if strings.Contains(t, c) {
				return true
			}
		}
	}
	return false
}

// opaqueColumns lists the schema's columns whose rendered form does not order
// like the value.
func (s *Schema) opaqueColumns() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range s.Tables {
		for _, c := range t.Cols {
			if opaqueOrder(c.Kind) && !seen[c.Name] {
				seen[c.Name] = true
				out = append(out, c.Name)
			}
		}
	}
	return out
}

// Gen is a seeded generator. Identical (seed, schema) always yields the
// identical query.
type Gen struct {
	r  *rand.Rand
	s  *Schema
	sc *scope
	n  int // alias counter for generated output names
}

// New creates a generator.
func New(seed int64, s *Schema) *Gen {
	return &Gen{r: rand.New(rand.NewSource(seed)), s: s}
}

func (g *Gen) pick(n int) int        { return g.r.Intn(n) }
func (g *Gen) chance(p float64) bool { return g.r.Float64() < p }

func (g *Gen) alias() string {
	g.n++
	return fmt.Sprintf("c%d", g.n)
}

// ref is one column reachable in the current FROM scope, bound to the table
// alias that carries it.
type ref struct {
	tblAlias string
	table    string
	col      Column
}

type scope struct {
	refs []ref
	// dupTable is true once one table appears under two aliases, which makes
	// bare column references ambiguous — every reference must qualify.
	dupTable bool
}

func (sc *scope) ofKind(k Kind) []ref {
	var out []ref
	for _, r := range sc.refs {
		if r.col.Kind == k {
			out = append(out, r)
		}
	}
	return out
}

func (sc *scope) numeric() []ref {
	var out []ref
	for _, r := range sc.refs {
		switch r.col.Kind {
		case KindInt, KindFloat, KindDecimal:
			out = append(out, r)
		}
	}
	return out
}

func (sc *scope) withLits() []ref {
	var out []ref
	for _, r := range sc.refs {
		if len(r.col.Lits) > 0 {
			out = append(out, r)
		}
	}
	return out
}

// name renders a reference to r: qualified or bare, plain or double-quoted.
// Bare references are only produced when no table appears twice, since a
// self-join makes them ambiguous.
func (g *Gen) name(r ref) string {
	col := r.col.Name
	if g.chance(0.12) {
		col = `"` + col + `"`
	}
	if g.sc != nil && !g.sc.dupTable && g.chance(0.35) {
		return col
	}
	return r.tblAlias + "." + col
}

// shapes are the generator arms, weighted. Each targets a cluster where the
// engine has historically been thin.
var shapes = []struct {
	name   string
	weight int
}{
	{"projection", 12},
	{"alias-shadow", 8},
	{"selfjoin", 10},
	{"joinchain", 12},
	{"outerjoin", 10},
	{"groupagg", 16},
	{"distinct", 8},
	{"subquery", 10},
	{"dates", 8},
	{"star", 6},
	{"window", 10},
}

// Query generates one query.
func (g *Gen) Query() *Query {
	total := 0
	for _, s := range shapes {
		total += s.weight
	}
	roll := g.pick(total)
	shape := shapes[len(shapes)-1].name
	for _, s := range shapes {
		if roll < s.weight {
			shape = s.name
			break
		}
		roll -= s.weight
	}

	q := &Query{Shape: shape, opaqueCols: g.s.opaqueColumns()}
	switch shape {
	case "selfjoin":
		g.genSelfJoin(q)
		g.rebuildScope(q)
	case "outerjoin":
		g.genFrom(q, 1+g.pick(2), true)
		g.rebuildScope(q)
	case "joinchain":
		g.genFrom(q, 2+g.pick(4), g.chance(0.3))
		g.rebuildScope(q)
	case "subquery":
		g.genDerivedFrom(q) // sets the scope itself: derived columns are a subset
	case "window":
		// One base table. A window's answer is only determined when its
		// ORDER BY is unique, and a single entry is where a single-column PK
		// is reachable to make it so — see genWindow.
		g.genFrom(q, 0, false)
		g.rebuildScope(q)
	default:
		g.genFrom(q, g.pick(3), g.chance(0.25))
		g.rebuildScope(q)
	}

	g.genWhere(q)

	switch shape {
	case "groupagg":
		g.genGroupAgg(q)
	case "distinct":
		g.genDistinct(q)
	case "star":
		g.genStar(q)
	case "alias-shadow":
		g.genShadow(q)
	case "dates":
		g.genDates(q)
	case "window":
		g.genWindow(q)
	default:
		g.genProjection(q)
		if g.chance(0.25) {
			g.genGroupAgg(q)
		}
	}
	if len(q.Items) == 0 {
		g.genProjection(q)
	}
	g.genOrderLimit(q)
	return q
}

// rebuildScope refreshes the column universe from q.From.
func (g *Gen) rebuildScope(q *Query) {
	sc := &scope{}
	seen := map[string]bool{}
	for _, f := range q.From {
		if f.Derived != "" {
			continue
		}
		if seen[f.Table] {
			sc.dupTable = true
		}
		seen[f.Table] = true
		t := g.s.table(f.Table)
		if t == nil {
			continue
		}
		for _, c := range t.Cols {
			sc.refs = append(sc.refs, ref{tblAlias: f.Alias, table: f.Table, col: c})
		}
	}
	g.sc = sc
}

// genFrom builds a base table plus joins along FK edges.
func (g *Gen) genFrom(q *Query, joins int, outer bool) {
	base := g.s.Tables[g.pick(len(g.s.Tables))]
	q.From = []From{{Table: base.Name, Alias: "t0", PK: base.PK}}
	tables := []string{base.Name}

	for j := 0; j < joins; j++ {
		var cands []Edge
		for _, e := range g.s.Edges {
			l, r := contains(tables, e.LTable), contains(tables, e.RTable)
			if l != r {
				cands = append(cands, e)
			}
		}
		if len(cands) == 0 {
			break
		}
		e := cands[g.pick(len(cands))]
		newTable, newCol, oldCol := e.RTable, e.RCol, e.LCol
		if contains(tables, e.RTable) {
			newTable, newCol, oldCol = e.LTable, e.LCol, e.RCol
		}
		oldAlias := ""
		for i, f := range q.From {
			if (f.Table == e.LTable || f.Table == e.RTable) && f.Table != newTable {
				oldAlias = q.From[i].Alias
			}
		}
		if oldAlias == "" {
			break
		}
		alias := fmt.Sprintf("t%d", len(q.From))
		join := "JOIN"
		switch {
		case outer && j == joins-1:
			join = "LEFT JOIN"
		case g.chance(0.12):
			join = ","
		}
		on := fmt.Sprintf("%s.%s = %s.%s", oldAlias, oldCol, alias, newCol)
		nt := g.s.table(newTable)
		f := From{Table: newTable, Alias: alias, Join: join, On: on, PK: nt.PK}
		if join == "," {
			// A comma join carries no ON clause; the equality becomes a WHERE
			// conjunct, which is the shape a BI client emits.
			f.On = ""
			q.Where = append(q.Where, on)
		}
		q.From = append(q.From, f)
		tables = append(tables, newTable)
	}
}

// genSelfJoin joins one table to itself under two aliases — the shape where a
// column reference must resolve to the right side and nothing else.
func (g *Gen) genSelfJoin(q *Query) {
	// Restrict to tables whose self-join stays bounded. Which those are is a
	// property of the SCHEMA, declared by Table.SelfJoin — it used to be a
	// hardcoded list of TPC-H table names here, which made this function panic
	// on any other schema (an empty candidate list indexed by pick).
	var cands []Table
	for _, t := range g.s.Tables {
		if t.SelfJoin {
			cands = append(cands, t)
		}
	}
	if len(cands) == 0 {
		// No table in this schema can be self-joined within a sane row count.
		// Emit the single-table FROM instead of an unbounded product.
		t := g.s.Tables[g.pick(len(g.s.Tables))]
		q.From = []From{{Table: t.Name, Alias: "t0", PK: t.PK}}
		return
	}
	t := cands[g.pick(len(cands))]
	q.From = []From{{Table: t.Name, Alias: "t0", PK: t.PK}}
	pk := t.PK[0]

	on := fmt.Sprintf("t0.%s = t1.%s", pk, pk) // identity self-join
	if g.chance(0.5) {
		// Attribute self-join, bounded by a PK inequality so the row count
		// stays sane while both aliases really carry different rows.
		var attr string
		for _, c := range t.Cols {
			if c.Kind == KindInt && c.Name != pk {
				attr = c.Name
				break
			}
		}
		if attr != "" {
			on = fmt.Sprintf("t0.%s = t1.%s AND t0.%s < t1.%s", attr, attr, pk, pk)
		}
	}
	join := "JOIN"
	if g.chance(0.25) {
		join = "LEFT JOIN"
	}
	q.From = append(q.From, From{Table: t.Name, Alias: "t1", Join: join, On: on, PK: t.PK})
	if g.chance(0.3) {
		q.From = append(q.From, From{Table: t.Name, Alias: "t2", Join: "JOIN", PK: t.PK,
			On: fmt.Sprintf("t0.%s = t2.%s", pk, pk)})
	}
}

// genDerivedFrom builds a derived table or CTE over a base table, so the outer
// query resolves names through a subquery boundary.
func (g *Gen) genDerivedFrom(q *Query) {
	t := g.s.Tables[g.pick(len(g.s.Tables))]
	// The FULL composite primary key, every column, projected into the
	// subquery — not just t.PK[0]. A derived table that exposes only PART of
	// a composite key is NOT unique on what it exposes: lineitem's
	// (l_orderkey, l_linenumber) projected as l_orderkey alone has 448 tied
	// groups, and declaring that column the derived PK made appendTiebreaker
	// mark the query TotalOrder and compare a non-unique order POSITIONALLY.
	// That is a harness false positive — the query's SQL does not determine
	// the order the comparison then demands of it (seed 21) — so the key the
	// derived table advertises must be one it is actually unique on, which is
	// the whole composite or nothing.
	cols := append([]string(nil), t.PK...)
	for _, c := range t.Cols {
		if !contains(cols, c.Name) && len(cols) < 4+len(t.PK) && g.chance(0.5) {
			cols = append(cols, c.Name)
		}
	}
	inner := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), t.Name)
	if lits := litCols(t); len(lits) > 0 && g.chance(0.6) {
		c := lits[g.pick(len(lits))]
		inner += fmt.Sprintf(" WHERE %s %s %s", c.Name, cmpOp(g), c.Lits[g.pick(len(c.Lits))])
	}

	// The derived table exposes a subset of columns; the scope must reflect
	// that, so register a synthetic table under the alias. Its PK is the full
	// composite one, which the projection above guarantees is present.
	derivedPK := append([]string(nil), t.PK...)
	sub := Table{Name: t.Name, PK: derivedPK}
	for _, c := range t.Cols {
		if contains(cols, c.Name) {
			sub.Cols = append(sub.Cols, c)
		}
	}
	if g.chance(0.5) {
		q.With = fmt.Sprintf("WITH src AS (%s) ", inner)
		q.From = []From{{Table: "src", Alias: "t0", PK: derivedPK}}
	} else {
		q.From = []From{{Derived: inner, Alias: "t0", PK: derivedPK}}
	}
	// Register the subquery's columns under t0 without consulting the real
	// schema — the derived table exposes only the projected subset.
	g.sc = &scope{}
	for _, c := range sub.Cols {
		g.sc.refs = append(g.sc.refs, ref{tblAlias: "t0", table: sub.Name, col: c})
	}
}

func litCols(t Table) []Column {
	var out []Column
	for _, c := range t.Cols {
		if len(c.Lits) > 0 {
			out = append(out, c)
		}
	}
	return out
}

func cmpOp(g *Gen) string {
	ops := []string{"=", "<", ">", "<=", ">=", "<>"}
	return ops[g.pick(len(ops))]
}

// orderLits puts two rendered literals in ascending order so a BETWEEN range
// is non-empty. Numeric literals must compare numerically — lexicographic
// order would render "BETWEEN 10 AND 3", which matches nothing and wastes the
// query on an empty result.
func orderLits(a, b string) (lo, hi string) {
	af, aOK := strconv.ParseFloat(a, 64)
	bf, bOK := strconv.ParseFloat(b, 64)
	if aOK == nil && bOK == nil {
		if af <= bf {
			return a, b
		}
		return b, a
	}
	if a <= b {
		return a, b
	}
	return b, a
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (g *Gen) genWhere(q *Query) {
	preds := g.sc.withLits()
	n := g.pick(4)
	for i := 0; i < n && len(preds) > 0; i++ {
		r := preds[g.pick(len(preds))]
		lit := r.col.Lits[g.pick(len(r.col.Lits))]
		nm := g.name(r)
		var p string
		switch {
		case r.col.Kind == KindText && g.chance(0.25):
			s := strings.Trim(lit, "'")
			if len(s) > 3 {
				s = s[:3]
			}
			p = fmt.Sprintf("%s LIKE '%s%%'", nm, s)
		case g.chance(0.18):
			lits := map[string]bool{lit: true}
			for len(lits) < 2+g.pick(3) && len(lits) < len(r.col.Lits) {
				lits[r.col.Lits[g.pick(len(r.col.Lits))]] = true
			}
			ordered := make([]string, 0, len(lits))
			for l := range lits {
				ordered = append(ordered, l)
			}
			sort.Strings(ordered) // map iteration must not leak into SQL text
			p = fmt.Sprintf("%s IN (%s)", nm, strings.Join(ordered, ", "))
		case g.chance(0.15):
			lo, hi := orderLits(lit, r.col.Lits[g.pick(len(r.col.Lits))])
			p = fmt.Sprintf("%s BETWEEN %s AND %s", nm, lo, hi)
		default:
			p = fmt.Sprintf("%s %s %s", nm, cmpOp(g), lit)
		}
		q.Where = append(q.Where, p)
	}

	// A cross-table predicate that is NOT a join key: the class where a
	// predicate went missing once a later stage became fusable.
	if len(q.From) > 1 && g.chance(0.35) {
		nums := g.sc.numeric()
		if len(nums) > 1 {
			a := nums[g.pick(len(nums))]
			b := nums[g.pick(len(nums))]
			if a.tblAlias != b.tblAlias {
				q.Where = append(q.Where, fmt.Sprintf("%s %s %s", g.name(a), cmpOp(g), g.name(b)))
			}
		}
	}
	// IS NULL / IS NOT NULL over an outer-joined column.
	if g.chance(0.12) && len(g.sc.refs) > 0 {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		neg := ""
		if g.chance(0.5) {
			neg = "NOT "
		}
		q.Where = append(q.Where, fmt.Sprintf("%s IS %sNULL", g.name(r), neg))
	}
	// Subquery predicates.
	if g.chance(0.15) {
		g.genSubqueryPred(q)
	}
}

func (g *Gen) genSubqueryPred(q *Query) {
	nums := g.sc.numeric()
	if len(nums) == 0 {
		return
	}
	r := nums[g.pick(len(nums))]
	t := g.s.Tables[g.pick(len(g.s.Tables))]
	switch g.pick(3) {
	case 0:
		// Scalar threshold from the same column's own table, in both spellings.
		// The unqualified form is the one that found issue #334: an unqualified
		// inner name that also exists in the outer scope used to bind to the
		// OUTER query, making an uncorrelated subquery correlated — re-planned
		// per row from the parallel pipeline, which killed the process with
		// "concurrent map writes". It is generated again now that an inner name
		// resolves against the inner FROM first.
		if g.chance(0.5) {
			q.Where = append(q.Where, fmt.Sprintf("%s > (SELECT AVG(%s) FROM %s)",
				g.name(r), r.col.Name, r.table))
		} else {
			q.Where = append(q.Where, fmt.Sprintf("%s > (SELECT AVG(sq.%s) FROM %s sq)",
				g.name(r), r.col.Name, r.table))
		}
	case 1:
		// IN (SELECT pk FROM t WHERE ...)
		pk := t.PK[0]
		inner := fmt.Sprintf("SELECT %s FROM %s", pk, t.Name)
		if lits := litCols(t); len(lits) > 0 {
			c := lits[g.pick(len(lits))]
			inner += fmt.Sprintf(" WHERE %s %s %s", c.Name, cmpOp(g), c.Lits[g.pick(len(c.Lits))])
		}
		ints := g.sc.ofKind(KindInt)
		if len(ints) == 0 {
			return
		}
		k := ints[g.pick(len(ints))]
		q.Where = append(q.Where, fmt.Sprintf("%s IN (%s)", g.name(k), inner))
	default:
		// Correlated EXISTS. Half the time on MORE THAN ONE key, which is the
		// shape this generator could not produce at all until #562: the FK
		// arm below emits exactly one equality, so a defect that needs two
		// correlated columns to fire was unreachable in principle. It fired
		// in practice — a two-column correlated EXISTS answered zero rows and
		// its NOT EXISTS twin answered every row.
		if g.chance(0.5) {
			// Half of THOSE across two relations, where the two sides carry
			// different bare names. That is not a cosmetic difference: a
			// self-correlation reads `sub.x = t0.x`, which resolves on both
			// sides, so dedupSemiAntiBuildSide cannot attribute either and
			// DECLINES — the narrowing never runs. Only a distinct-name
			// correlation makes it fire, which is the code path #562 lived
			// on.
			if g.chance(0.5) && g.genMultiKeyEdgeExists(q, r) {
				return
			}
			if g.genMultiKeyExists(q, r) {
				return
			}
		}
		// Correlated EXISTS over an FK edge.
		for _, e := range g.s.Edges {
			if e.LTable == r.table {
				neg := ""
				if g.chance(0.4) {
					neg = "NOT "
				}
				q.Where = append(q.Where, fmt.Sprintf("%sEXISTS (SELECT 1 FROM %s sub WHERE sub.%s = %s.%s)",
					neg, e.RTable, e.RCol, r.tblAlias, e.LCol))
				return
			}
		}
	}
}

// genMultiKeyExists emits a correlated EXISTS keyed on two or three columns.
//
// It correlates the outer relation with its OWN table, for a reason: an FK
// edge gives one column pair and nothing says a second pair of columns from
// two different tables holds any value in common, so a cross-table multi-key
// EXISTS would be empty for every row and prove nothing. A self-correlation
// always has a match — each row matches itself — unless a key is NULL, which
// is the other rule worth generating: a NULL key matches nothing, itself
// included, so those rows drop out and the answer is neither "all" nor "none".
//
// An inner-only predicate goes in most of the time, on either side of the
// correlations, because where it sits decides which conjunct the rewrite
// classifies first.
//
// ok=false when the table has too few columns to key on, and the caller falls
// through to the FK arm.
func (g *Gen) genMultiKeyExists(q *Query, r ref) bool {
	t := g.s.table(r.table)
	if t == nil {
		return false
	}
	// Key columns come from the SCOPE, not from the table: the outer entry may
	// be a derived table exposing only part of it, and a correlation naming a
	// column that entry does not carry is a query neither engine can answer.
	// Every scope ref still belongs to r.table, so `sub.<col>` resolves too.
	var cols []Column
	for _, e := range g.sc.refs {
		if e.tblAlias == r.tblAlias {
			cols = append(cols, e.col)
		}
	}
	if len(cols) < 2 {
		return false
	}
	// Deterministic, distinct, and in the scope's own column order so the
	// rendered SQL is a function of the seed alone.
	want := 2
	if len(cols) > 2 && g.chance(0.35) {
		want = 3
	}
	chosen := map[int]bool{}
	for len(chosen) < want {
		chosen[g.pick(len(cols))] = true
	}
	conds := make([]string, 0, want+1)
	for i, c := range cols {
		if chosen[i] {
			conds = append(conds, fmt.Sprintf("sub.%s = %s.%s", c.Name, r.tblAlias, c.Name))
		}
	}
	if lits := litCols(*t); len(lits) > 0 && g.chance(0.7) {
		c := lits[g.pick(len(lits))]
		pred := fmt.Sprintf("sub.%s %s %s", c.Name, cmpOp(g), c.Lits[g.pick(len(c.Lits))])
		// Before, between or after the correlations.
		switch g.pick(3) {
		case 0:
			conds = append([]string{pred}, conds...)
		case 1:
			conds = append(conds[:1], append([]string{pred}, conds[1:]...)...)
		default:
			conds = append(conds, pred)
		}
	}
	neg := ""
	if g.chance(0.4) {
		neg = "NOT "
	}
	q.Where = append(q.Where, fmt.Sprintf("%sEXISTS (SELECT 1 FROM %s sub WHERE %s)",
		neg, r.table, strings.Join(conds, " AND ")))
	return true
}

// genMultiKeyEdgeExists emits a correlated EXISTS across an FK edge with a
// SECOND correlated equality, so the outer and inner key names differ.
//
// The second pair is any two same-Kind columns, one from each relation, that
// are not the edge's own. It is not a meaningful relationship and does not
// need to be: what it exercises is the key LIST, and both engines answer the
// same question whatever the columns mean. The edge equality keeps the answer
// off zero.
//
// ok=false when no second pair exists, and the caller falls back to the
// self-correlated form.
func (g *Gen) genMultiKeyEdgeExists(q *Query, r ref) bool {
	for _, e := range g.s.Edges {
		if e.LTable != r.table {
			continue
		}
		lt, rt := g.s.table(e.LTable), g.s.table(e.RTable)
		if lt == nil || rt == nil {
			continue
		}
		// Deterministic: first same-Kind pair in declared column order.
		for _, oc := range lt.Cols {
			if oc.Name == e.LCol {
				continue
			}
			for _, ic := range rt.Cols {
				if ic.Name == e.RCol || ic.Kind != oc.Kind {
					continue
				}
				conds := []string{
					fmt.Sprintf("sub.%s = %s.%s", e.RCol, r.tblAlias, e.LCol),
					fmt.Sprintf("sub.%s = %s.%s", ic.Name, r.tblAlias, oc.Name),
				}
				if len(ic.Lits) > 0 && g.chance(0.5) {
					conds = append(conds, fmt.Sprintf("sub.%s %s %s",
						ic.Name, cmpOp(g), ic.Lits[g.pick(len(ic.Lits))]))
				}
				neg := ""
				if g.chance(0.4) {
					neg = "NOT "
				}
				q.Where = append(q.Where, fmt.Sprintf("%sEXISTS (SELECT 1 FROM %s sub WHERE %s)",
					neg, e.RTable, strings.Join(conds, " AND ")))
				return true
			}
		}
	}
	return false
}

func (g *Gen) genProjection(q *Query) {
	n := 1 + g.pick(3)
	for i := 0; i < n; i++ {
		if len(g.sc.refs) == 0 {
			return
		}
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		q.Items = append(q.Items, g.exprItem(r))
	}
}

// exprItem renders one select item over r: the bare column, or an expression.
func (g *Gen) exprItem(r ref) Item {
	nm := g.name(r)
	a := g.alias()
	switch {
	case r.col.Kind == KindInt && g.chance(0.25):
		op := []string{"+", "-", "*"}[g.pick(3)]
		return Item{Expr: fmt.Sprintf("%s %s %d", nm, op, 1+g.pick(9)), Alias: a, Exact: true}
	case r.col.Kind == KindInt && g.chance(0.15):
		return Item{Expr: fmt.Sprintf("%s %% %d", nm, 2+g.pick(9)), Alias: a, Exact: true}
	case r.col.Kind == KindFloat && g.chance(0.3):
		op := []string{"+", "-", "*"}[g.pick(3)]
		return Item{Expr: fmt.Sprintf("%s %s %d", nm, op, 1+g.pick(9)), Alias: a}
	case r.col.Kind == KindFloat && g.chance(0.2):
		return Item{Expr: fmt.Sprintf("ROUND(%s, %d)", nm, g.pick(3)), Alias: a}
	case r.col.Kind == KindText && g.chance(0.3):
		return Item{Expr: fmt.Sprintf("SUBSTR(%s, 1, %d)", nm, 2+g.pick(5)), Alias: a, Exact: true}
	case r.col.Kind == KindText && g.chance(0.2):
		fn := []string{"UPPER", "LOWER", "TRIM"}[g.pick(3)]
		return Item{Expr: fmt.Sprintf("%s(%s)", fn, nm), Alias: a, Exact: true}
	case r.col.Kind == KindText && g.chance(0.15):
		return Item{Expr: fmt.Sprintf("LENGTH(%s)", nm), Alias: a, Exact: true}
	case len(r.col.Lits) > 0 && g.chance(0.2):
		lit := r.col.Lits[g.pick(len(r.col.Lits))]
		return Item{Expr: fmt.Sprintf("CASE WHEN %s = %s THEN 1 ELSE 0 END", nm, lit), Alias: a, Exact: true}
	case r.col.Kind != KindOpaque && g.chance(0.1):
		return Item{Expr: fmt.Sprintf("COALESCE(%s, %s)", nm, zeroLit(r.col.Kind)), Alias: a, Exact: true}
	default:
		return Item{Expr: nm, Alias: a, Exact: true, Opaque: opaqueOrder(r.col.Kind)}
	}
}

// opaqueOrder reports whether a kind's RENDERED form orders differently from
// its value, which is what makes the absolute order check unable to judge it.
func opaqueOrder(k Kind) bool { return k == KindOpaque || k == KindDecimal }

// zeroLit is the COALESCE fallback for a kind. KindOpaque has none — there is
// no literal that is simultaneously a valid IPv4, UUID and BYTES — which is
// why the COALESCE arm above declines that kind rather than guessing.
func zeroLit(k Kind) string {
	switch k {
	case KindText, KindDate:
		return "'?'"
	case KindDecimal:
		return "0.0"
	}
	return "0"
}

// genWindow emits window functions, including the ones whose answer is CHOSEN
// by a comparison rather than computed: MIN and MAX.
//
// This arm did not exist. The package comment claimed window functions among
// the shapes it generates and no arm produced one, so the fuzzer could not
// have found #569 — windowed MIN/MAX failing the query outright for twelve of
// the twenty-two types — nor any other defect that needs a window to fire.
//
// Three rules keep the generated query's answer DETERMINED, which is what a
// differential arm needs before it can call a difference a defect:
//
//  1. The window's ORDER BY is the entry's single-column PK, so no two rows
//     tie and a running or sliding frame has exactly one answer. Without a
//     unique key the arm emits only whole-partition and OVER () shapes, whose
//     answers do not depend on the order within the partition.
//  2. SUM and AVG over a float are generated but marked NOT exact, because
//     accumulation order is a legal difference (ADR-0013 §4).
//  3. MIN, MAX and the value functions are marked exact: they return one of
//     their input's values untouched, so any difference is a real one — and
//     Opaque follows the column's kind, since an IPv4's rendered form does
//     not order the way the address does.
//
// The PARTITION BY key is written BARE and is always a plain column. A
// QUALIFIED reference (`PARTITION BY t0.g`) or an EXPRESSION (`PARTITION BY
// id % 3`) is silently dropped by the engine today and the window degrades to
// a single partition (#585). Generating those shapes here would bury every
// other window divergence under that one; widen this once it is fixed.
//
// # Adding an arm moves every seed
//
// The shape roll is weighted over this table, so a new entry changes which
// shape each seed draws and therefore the QUERY each seed generates — in
// every shape, not just this one. That is not a side effect to be avoided; it
// is how a generator covers new ground. It does mean the arm lands with a
// batch of unrelated findings: this one surfaced #593 (a comma-join whose
// equi-predicate is in WHERE runs as a real cross product and is OOM-killed,
// seed 51) and #594 (the same FROM shape answering ZERO ROWS, seed 24). Both
// are pre-existing — the same suite with this arm's weight at 0 passes — and
// both are now filed rather than hidden.
func (g *Gen) genWindow(q *Query) {
	if len(g.sc.refs) == 0 || len(q.From) != 1 {
		g.genProjection(q)
		return
	}
	// A unique in-partition order, when the entry offers one.
	ordCol := ""
	if pk := q.From[0].PK; len(pk) == 1 {
		ordCol = pk[0]
	}

	// A low-cardinality PARTITION BY, so partitions hold several rows. Any
	// column will do — the point is to partition, not to be clever — but an
	// integer one keeps the partitions few.
	part := ""
	if g.chance(0.75) {
		var cands []ref
		for _, r := range g.sc.refs {
			if r.col.Kind == KindInt && r.col.Name != ordCol {
				cands = append(cands, r)
			}
		}
		if len(cands) == 0 {
			cands = g.sc.refs
		}
		part = cands[g.pick(len(cands))].col.Name
	}

	// Project the order key so the outer ORDER BY has a total one to reach
	// for, and so a divergence names the row it is on.
	if ordCol != "" {
		q.Items = append(q.Items, Item{Expr: ordCol, Alias: g.alias(), Exact: true})
	}

	n := 1 + g.pick(3)
	for i := 0; i < n; i++ {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		q.Items = append(q.Items, g.windowItem(r, part, ordCol))
	}
}

// windowItem renders one window select item over r.
func (g *Gen) windowItem(r ref, part, ordCol string) Item {
	col := r.col.Name
	a := g.alias()
	over := g.windowOver(part, ordCol, true)

	// The numeric-only functions, when the column can carry them.
	if r.col.Kind == KindInt || r.col.Kind == KindFloat {
		switch {
		case g.chance(0.10):
			// Item.Agg means "an aggregate that COLLAPSES rows and therefore
			// needs a GROUP BY" — wellFormed rejects a select list mixing one
			// with a bare column. A windowed aggregate collapses nothing, so
			// it is not one, whatever its spelling.
			return Item{Expr: fmt.Sprintf("SUM(%s) %s", col, over), Alias: a}
		case g.chance(0.10):
			return Item{Expr: fmt.Sprintf("AVG(%s) %s", col, over), Alias: a}
		}
	}
	switch {
	case g.chance(0.10) && ordCol != "":
		// The rank family reads the partition and its ORDER BY, never the
		// frame, so it is emitted only where the order is unique.
		fn := []string{"ROW_NUMBER()", "RANK()", "DENSE_RANK()"}[g.pick(3)]
		return Item{Expr: fmt.Sprintf("%s %s", fn, g.windowOver(part, ordCol, false)), Alias: a, Exact: true}
	case g.chance(0.08):
		return Item{Expr: fmt.Sprintf("COUNT(%s) %s", col, over), Alias: a, Exact: true}
	case g.chance(0.20) && ordCol != "":
		fn := []string{"FIRST_VALUE", "LAST_VALUE"}[g.pick(2)]
		return Item{Expr: fmt.Sprintf("%s(%s) %s", fn, col, over), Alias: a,
			Exact: true, Opaque: opaqueOrder(r.col.Kind)}
	case g.chance(0.15) && ordCol != "":
		fn := []string{"LAG", "LEAD"}[g.pick(2)]
		return Item{Expr: fmt.Sprintf("%s(%s) %s", fn, col, g.windowOver(part, ordCol, false)), Alias: a,
			Exact: true, Opaque: opaqueOrder(r.col.Kind)}
	}
	// The default, and the arm this exists for: MIN/MAX over ANY type.
	fn := []string{"MIN", "MAX"}[g.pick(2)]
	return Item{Expr: fmt.Sprintf("%s(%s) %s", fn, col, over), Alias: a,
		Exact: true, Opaque: opaqueOrder(r.col.Kind)}
}

// windowOver renders the OVER clause. framed=false suppresses the explicit
// frame for the functions SQL defines against the partition rather than the
// frame (the rank family, LAG/LEAD), where a frame would be noise.
//
// An explicit frame is emitted only alongside a unique ORDER BY: a ROWS frame
// over a TIED order advances one row at a time through a sequence SQL does not
// determine, so two correct engines may legitimately disagree (the reason
// WindowDefaultFrameIsRange in the PostgreSQL corpus orders by a unique key
// for its ROWS control).
func (g *Gen) windowOver(part, ordCol string, framed bool) string {
	var sb strings.Builder
	sb.WriteString("OVER (")
	sep := ""
	if part != "" {
		fmt.Fprintf(&sb, "PARTITION BY %s", part)
		sep = " "
	}
	if ordCol != "" {
		fmt.Fprintf(&sb, "%sORDER BY %s", sep, ordCol)
		if framed {
			switch g.pick(4) {
			case 0:
				sb.WriteString(" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
			case 1:
				sb.WriteString(" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
			case 2:
				fmt.Fprintf(&sb, " ROWS BETWEEN %d PRECEDING AND CURRENT ROW", 1+g.pick(4))
			}
		}
	}
	sb.WriteString(")")
	return sb.String()
}

// genShadow aliases a column to the NAME OF A DIFFERENT REAL COLUMN, so the
// output name collides with something the resolver also knows.
func (g *Gen) genShadow(q *Query) {
	if len(g.sc.refs) < 2 {
		g.genProjection(q)
		return
	}
	a := g.sc.refs[g.pick(len(g.sc.refs))]
	var b ref
	for i := 0; i < 20; i++ {
		b = g.sc.refs[g.pick(len(g.sc.refs))]
		if b.col.Name != a.col.Name {
			break
		}
	}
	// SELECT a AS <name of b>: the alias shadows a real, in-scope column.
	q.Items = append(q.Items, Item{Expr: g.name(a), Alias: b.col.Name, Exact: true})
	if g.chance(0.7) {
		// ...and select the shadowed column too, under its own generated
		// alias, so both values are visible and a swap is detectable.
		q.Items = append(q.Items, Item{Expr: g.name(b), Alias: g.alias(), Exact: true})
	}
	if g.chance(0.4) {
		// An alias that shadows a TABLE alias in scope.
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		q.Items = append(q.Items, Item{Expr: g.name(r), Alias: r.tblAlias, Exact: true})
	}
	if g.chance(0.3) {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		q.Items = append(q.Items, Item{Expr: g.name(r), Alias: `"odd.name"`, Exact: true})
	}
}

func (g *Gen) genStar(q *Query) {
	// Star over a single table only: two starred tables would produce
	// duplicate output column names, which a name-keyed row cannot represent.
	q.From = q.From[:1]
	g.rebuildScope(q)
	star := "*"
	if g.chance(0.5) {
		star = q.From[0].Alias + ".*"
	}
	q.Items = []Item{{Star: star}}
	if g.chance(0.4) && len(g.sc.refs) > 0 {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		q.Items = append(q.Items, Item{Expr: fmt.Sprintf("LENGTH(%s)", nameOf(r)), Alias: g.alias(), Exact: true})
	}
}

func nameOf(r ref) string { return r.tblAlias + "." + r.col.Name }

func (g *Gen) genDistinct(q *Query) {
	q.Distinct = true
	n := 1 + g.pick(3)
	seen := map[string]bool{}
	for i := 0; i < n && len(g.sc.refs) > 0; i++ {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		it := g.exprItem(r)
		if seen[it.Expr] {
			continue
		}
		seen[it.Expr] = true
		q.Items = append(q.Items, it)
	}
}

func (g *Gen) genGroupAgg(q *Query) {
	q.Items = nil
	q.GroupBy = nil
	ng := 1 + g.pick(2)
	seen := map[string]bool{}
	for i := 0; i < ng && len(g.sc.refs) > 0; i++ {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		it := g.groupItem(r)
		if seen[it.Expr] {
			continue
		}
		seen[it.Expr] = true
		q.Items = append(q.Items, it)
		// GROUP BY by expression, by ordinal, or by output alias — three
		// different resolution paths for the same query.
		switch g.pick(3) {
		case 0:
			q.GroupBy = append(q.GroupBy, fmt.Sprint(len(q.Items)))
		case 1:
			q.GroupBy = append(q.GroupBy, it.Alias)
		default:
			q.GroupBy = append(q.GroupBy, it.Expr)
		}
	}
	if len(q.Items) == 0 {
		return
	}

	na := 1 + g.pick(3)
	nums := g.sc.numeric()
	var havingAgg string
	for i := 0; i < na; i++ {
		var agg string
		switch g.pick(8) {
		case 0:
			agg = "COUNT(*)"
		case 1:
			r := g.sc.refs[g.pick(len(g.sc.refs))]
			agg = fmt.Sprintf("COUNT(%s)", g.name(r))
		case 2:
			r := g.sc.refs[g.pick(len(g.sc.refs))]
			agg = fmt.Sprintf("COUNT(DISTINCT %s)", g.name(r))
		case 3:
			if len(nums) == 0 {
				continue
			}
			r := nums[g.pick(len(nums))]
			agg = fmt.Sprintf("SUM(%s)", g.name(r))
		case 4:
			if len(nums) == 0 {
				continue
			}
			r := nums[g.pick(len(nums))]
			agg = fmt.Sprintf("AVG(%s)", g.name(r))
		case 5:
			r := g.sc.refs[g.pick(len(g.sc.refs))]
			fn := "MIN"
			if g.chance(0.5) {
				fn = "MAX"
			}
			agg = fmt.Sprintf("%s(%s)", fn, g.name(r))
		case 6:
			// Aggregate over an expression.
			if len(nums) < 2 {
				continue
			}
			a, b := nums[g.pick(len(nums))], nums[g.pick(len(nums))]
			agg = fmt.Sprintf("SUM(%s * (1 - %s))", g.name(a), g.name(b))
		default:
			// COUNT(CASE WHEN ...) / SUM(CASE WHEN ...)
			lits := g.sc.withLits()
			if len(lits) == 0 {
				continue
			}
			r := lits[g.pick(len(lits))]
			lit := r.col.Lits[g.pick(len(r.col.Lits))]
			if g.chance(0.5) {
				agg = fmt.Sprintf("COUNT(CASE WHEN %s = %s THEN 1 END)", g.name(r), lit)
			} else {
				agg = fmt.Sprintf("SUM(CASE WHEN %s = %s THEN 1 ELSE 0 END)", g.name(r), lit)
			}
		}
		exact := strings.HasPrefix(agg, "COUNT")
		q.Items = append(q.Items, Item{Expr: agg, Alias: g.alias(), Agg: true, Exact: exact})
		if havingAgg == "" {
			havingAgg = agg
		}
	}

	if havingAgg != "" && g.chance(0.35) {
		switch g.pick(3) {
		case 0:
			q.Having = fmt.Sprintf("%s > %d", havingAgg, 1+g.pick(20))
		case 1:
			// HAVING over the output alias.
			for _, it := range q.Items {
				if it.Expr == havingAgg {
					q.Having = fmt.Sprintf("%s > %d", it.Alias, 1+g.pick(20))
					break
				}
			}
		default:
			// Scalar-subquery threshold, data-derived on both sides.
			nums := g.sc.numeric()
			if len(nums) > 0 {
				r := nums[g.pick(len(nums))]
				q.Having = fmt.Sprintf("%s > (SELECT AVG(sq.%s) * 0.%d FROM %s sq)",
					havingAgg, r.col.Name, 1+g.pick(9), r.table)
			}
		}
	}
}

// groupItem renders one GROUP BY term: the bare column or an expression.
func (g *Gen) groupItem(r ref) Item {
	nm := g.name(r)
	a := g.alias()
	switch {
	case r.col.Kind == KindText && g.chance(0.3):
		return Item{Expr: fmt.Sprintf("SUBSTR(%s, 1, %d)", nm, 1+g.pick(4)), Alias: a, Exact: true}
	case r.col.Kind == KindInt && g.chance(0.25):
		return Item{Expr: fmt.Sprintf("%s %% %d", nm, 2+g.pick(9)), Alias: a, Exact: true}
	case r.col.Kind == KindDate && g.chance(0.4):
		part := []string{"YEAR", "MONTH"}[g.pick(2)]
		return Item{Expr: fmt.Sprintf("EXTRACT(%s FROM CAST(%s AS DATE))", part, nm), Alias: a, Exact: true}
	default:
		return Item{Expr: nm, Alias: a, Exact: true}
	}
}

// genDates projects and filters date columns through comparisons, BETWEEN and
// date-part extraction.
func (g *Gen) genDates(q *Query) {
	dates := g.sc.ofKind(KindDate)
	if len(dates) == 0 {
		g.genProjection(q)
		return
	}
	r := dates[g.pick(len(dates))]
	nm := g.name(r)
	q.Items = append(q.Items, Item{Expr: nm, Alias: g.alias(), Exact: true})
	switch g.pick(3) {
	case 0:
		part := []string{"YEAR", "MONTH", "DAY"}[g.pick(3)]
		q.Items = append(q.Items, Item{Expr: fmt.Sprintf("EXTRACT(%s FROM CAST(%s AS DATE))", part, nm),
			Alias: g.alias(), Exact: true})
	case 1:
		q.Items = append(q.Items, Item{Expr: fmt.Sprintf("SUBSTR(%s, 1, 7)", nm), Alias: g.alias(), Exact: true})
	}
	if len(r.col.Lits) > 1 {
		lo, hi := r.col.Lits[0], r.col.Lits[len(r.col.Lits)-1]
		if g.chance(0.5) {
			q.Where = append(q.Where, fmt.Sprintf("%s BETWEEN %s AND %s", nm, lo, hi))
		} else {
			q.Where = append(q.Where, fmt.Sprintf("CAST(%s AS DATE) >= DATE %s", nm, lo))
		}
	}
	// A comparison between two date columns of the same table.
	for _, o := range dates {
		if o.col.Name != r.col.Name && o.tblAlias == r.tblAlias {
			if g.chance(0.4) {
				q.Where = append(q.Where, fmt.Sprintf("%s < %s", nm, g.name(o)))
			}
			break
		}
	}
	if g.chance(0.4) {
		g.genGroupAgg(q)
	}
}

// genOrderLimit attaches ORDER BY and LIMIT, appending a uniqueness
// tiebreaker whenever one is available so the row sequence — and therefore a
// LIMIT — is fully determined.
func (g *Gen) genOrderLimit(q *Query) {
	if g.chance(0.25) {
		// No ORDER BY. A bare LIMIT here is compared by count only, which is
		// what caught a limit that failed to bind at all.
		if g.chance(0.3) {
			q.Limit = 1 + g.pick(40)
		}
		return
	}

	// Leading keys: alias, ordinal, expression, or a column the select list
	// does not project.
	nKeys := 1 + g.pick(2)
	used := map[string]bool{}
	for i := 0; i < nKeys; i++ {
		if len(q.Items) == 0 {
			break
		}
		idx := g.pick(len(q.Items))
		it := q.Items[idx]
		if it.Star != "" || used[it.Alias] {
			continue
		}
		used[it.Alias] = true
		desc := g.chance(0.45)
		switch {
		case g.chance(0.2) && !q.Distinct && it.Star == "":
			// By ordinal. Not with SELECT * (rejected by design) and not with
			// DISTINCT (the term must be a select item, which it is, but the
			// ordinal form is the interesting path here).
			q.Order = append(q.Order, Order{Expr: fmt.Sprint(idx + 1), Desc: desc, Key: it.Alias, Exact: it.Exact, Opaque: it.Opaque})
		case g.chance(0.25) && !it.Agg && len(q.GroupBy) == 0:
			// By the expression itself rather than its alias. PostgreSQL
			// resolves a BARE name in ORDER BY against OUTPUT column names
			// BEFORE input columns, so when this item's expression text is
			// also some other item's alias, the term orders by THAT item —
			// `SELECT c_f32 AS id, id AS c1 ... ORDER BY id` orders by c_f32,
			// not by the table's id. Recording this item's key regardless made
			// the absolute order check compare the wrong column and accuse a
			// correct engine. The SQL is unchanged; only the key this term is
			// judged by moves.
			tgt := it
			if shadow := itemByAlias(q, it.Expr); shadow != nil {
				tgt = *shadow
			}
			q.Order = append(q.Order, Order{Expr: it.Expr, Desc: desc, Key: tgt.Alias, Exact: tgt.Exact, Opaque: tgt.Opaque})
		default:
			q.Order = append(q.Order, Order{Expr: it.Alias, Desc: desc, Key: it.Alias, Exact: it.Exact, Opaque: it.Opaque})
		}
	}

	// ORDER BY a column absent from the SELECT list. Only for ungrouped,
	// non-DISTINCT queries, where SQL allows it.
	if len(q.GroupBy) == 0 && !q.Distinct && !q.hasStar() && g.chance(0.3) && len(g.sc.refs) > 0 {
		r := g.sc.refs[g.pick(len(g.sc.refs))]
		if !projects(q, r) {
			q.Order = append(q.Order, Order{Expr: nameOf(r), Desc: g.chance(0.4),
				Exact: r.col.Kind != KindFloat})
		}
	}

	g.appendTiebreaker(q)

	if q.TotalOrder && g.chance(0.45) {
		q.Limit = 1 + g.pick(40)
		if g.chance(0.25) {
			q.Offset = g.pick(10)
		}
	} else if g.chance(0.2) {
		q.Limit = 1 + g.pick(40)
	}
}

func (q *Query) hasStar() bool {
	for _, it := range q.Items {
		if it.Star != "" {
			return true
		}
	}
	return false
}

// itemByAlias returns the select item whose OUTPUT name is expr, when expr is
// a bare identifier — the only form PostgreSQL resolves against output names.
// A qualified name (t0.id), an ordinal, or anything with an operator or a call
// in it is resolved against the input, so those return nil.
func itemByAlias(q *Query, expr string) *Item {
	if !isBareName(expr) {
		return nil
	}
	for i := range q.Items {
		if q.Items[i].Star == "" && q.Items[i].Alias == expr {
			return &q.Items[i]
		}
	}
	return nil
}

// isBareName reports whether s is a single unqualified identifier.
func isBareName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func projects(q *Query, r ref) bool {
	n := nameOf(r)
	for _, it := range q.Items {
		if it.Expr == n || strings.Contains(it.Expr, r.col.Name) {
			return true
		}
	}
	return false
}

// appendTiebreaker extends ORDER BY until no two output rows can tie, and
// records that on the query. Without it, a LIMIT cuts through a tie group and
// two correct engines legitimately return different rows.
func (g *Gen) appendTiebreaker(q *Query) {
	seen := map[string]bool{}
	for _, o := range q.Order {
		seen[o.Expr] = true
		if o.Key != "" {
			seen[o.Key] = true
		}
	}
	add := func(expr string, key string, exact bool) {
		if seen[expr] || (key != "" && seen[key]) {
			return
		}
		seen[expr] = true
		if key != "" {
			seen[key] = true
		}
		q.Order = append(q.Order, Order{Expr: expr, Key: key, Exact: exact})
	}

	switch {
	case len(q.GroupBy) > 0:
		// The group keys are unique across output rows.
		for _, it := range q.Items {
			if it.Agg || it.Star != "" {
				continue
			}
			add(it.Alias, it.Alias, it.Exact)
		}
		q.TotalOrder = true
	case q.Distinct:
		// Every output row is distinct on the full select list.
		for _, it := range q.Items {
			if it.Star != "" {
				return
			}
			add(it.Alias, it.Alias, it.Exact)
		}
		q.TotalOrder = true
	default:
		// One row per FROM-tuple: the primary keys together are unique. An
		// outer-joined table contributes NULLs on unmatched rows, which keeps
		// the tuple unique.
		//
		// The key columns are PROJECTED rather than left hidden. Two reasons:
		// the harness can then verify the ordering it was promised (an
		// unprojected key is unverifiable), and a hidden sort key is itself a
		// shape the generator produces deliberately elsewhere — having every
		// tiebreaker be one would make that variable constant.
		total := true
		for _, f := range q.From {
			if len(f.PK) == 0 {
				total = false
				continue
			}
			for _, pk := range f.PK {
				expr := f.Alias + "." + pk
				if seen[expr] {
					continue
				}
				alias := g.projected(q, expr)
				add(expr, alias, true)
			}
		}
		q.TotalOrder = total
	}
}

// projected returns the output alias carrying expr, adding a select item for
// it when the query does not already project it.
func (g *Gen) projected(q *Query, expr string) string {
	for _, it := range q.Items {
		if it.Expr == expr && it.Alias != "" {
			return it.Alias
		}
	}
	alias := g.alias()
	q.Items = append(q.Items, Item{Expr: expr, Alias: alias, Exact: true})
	return alias
}
