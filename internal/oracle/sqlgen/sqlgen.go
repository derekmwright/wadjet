// Package sqlgen generates random-but-valid SQL queries over a described
// schema, for differential testing (#288). Generation is fully determined
// by the seed — a failing seed IS the repro — and is weighted toward the
// shapes that have historically produced distributed-mode divergence:
// aggregate-free GROUP BY (#163/#166), DISTINCT (#163), expressions in
// the SELECT list past a shuffle (#169/#273), and scalar-subquery
// thresholds in HAVING (#272).
//
// The generator builds a structured Query first and renders SQL from it,
// so a failure can be shrunk structurally (see Shrink) instead of by
// text mutation.
package sqlgen

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// Kind classifies a column for operator/literal selection.
type Kind int

const (
	KindInt Kind = iota
	KindFloat
	KindString
)

// Column is one generatable column. Lits are rendered SQL literals drawn
// from the column's actual domain; columns with no Lits appear in select
// lists and GROUP BY but never in predicates.
type Column struct {
	Name string
	Kind Kind
	Lits []string
}

// Table is one generatable table.
type Table struct {
	Name string
	Cols []Column
}

// Edge is a joinable equality between two tables (FK pairs in practice —
// arbitrary column pairs explode row counts without exercising anything
// new).
type Edge struct {
	LeftTable, LeftCol   string
	RightTable, RightCol string
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

// Query is a structured query: rendered clause fragments plus enough
// structure for Shrink to remove parts and keep the query valid.
type Query struct {
	Distinct bool
	Select   []string // rendered select items (aggregates already aliased)
	Tables   []string // FROM chain: Tables[0] base, rest joined via On
	On       []string // join conditions; On[i] joins Tables[i+1]
	Where    []string // conjuncts
	GroupBy  []string
	Having   string
	OrderBy  []string
	Limit    int
}

// SQL renders the query.
func (q *Query) SQL() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if q.Distinct {
		sb.WriteString("DISTINCT ")
	}
	sb.WriteString(strings.Join(q.Select, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(q.Tables[0])
	for i := 1; i < len(q.Tables); i++ {
		sb.WriteString(" JOIN ")
		sb.WriteString(q.Tables[i])
		sb.WriteString(" ON ")
		sb.WriteString(q.On[i-1])
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
	if len(q.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(q.OrderBy, ", "))
	}
	if q.Limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", q.Limit)
	}
	return sb.String()
}

// Gen is a seeded query generator.
type Gen struct {
	r *rand.Rand
	s *Schema
}

// New creates a generator. Identical (seed, schema) always yields the
// identical query sequence.
func New(seed int64, s *Schema) *Gen {
	return &Gen{r: rand.New(rand.NewSource(seed)), s: s}
}

func (g *Gen) pick(n int) int        { return g.r.Intn(n) }
func (g *Gen) chance(p float64) bool { return g.r.Float64() < p }

// scope is the working set during generation: joined tables and their
// columns.
type scope struct {
	tables []string
	cols   []Column
}

// Query generates one query.
func (g *Gen) Query() *Query {
	q := &Query{}
	sc := g.genFrom(q)

	g.genWhere(q, sc)

	// Shape weights lean toward the historical breakers.
	switch roll := g.r.Float64(); {
	case roll < 0.30:
		g.genAggFreeGroupBy(q, sc)
	case roll < 0.55:
		g.genAggregation(q, sc)
	case roll < 0.70:
		g.genDistinct(q, sc)
	default:
		g.genProjection(q, sc)
	}

	g.genOrderLimit(q)
	return q
}

// genFrom picks a base table and joins 0-2 edges reachable from the
// current scope.
func (g *Gen) genFrom(q *Query) *scope {
	base := g.s.Tables[g.pick(len(g.s.Tables))]
	sc := &scope{tables: []string{base.Name}, cols: append([]Column(nil), base.Cols...)}
	q.Tables = []string{base.Name}

	joins := 0
	if g.chance(0.6) {
		joins++
		if g.chance(0.35) {
			joins++
		}
	}
	for j := 0; j < joins; j++ {
		var candidates []Edge
		for _, e := range g.s.Edges {
			leftIn, rightIn := sc.has(e.LeftTable), sc.has(e.RightTable)
			if leftIn != rightIn { // exactly one side already joined
				candidates = append(candidates, e)
			}
		}
		if len(candidates) == 0 {
			break
		}
		e := candidates[g.pick(len(candidates))]
		newTable := e.RightTable
		if sc.has(e.RightTable) {
			newTable = e.LeftTable
		}
		q.Tables = append(q.Tables, newTable)
		q.On = append(q.On, fmt.Sprintf("%s = %s", e.LeftCol, e.RightCol))
		nt := g.s.table(newTable)
		sc.tables = append(sc.tables, newTable)
		sc.cols = append(sc.cols, nt.Cols...)
	}
	return sc
}

func (sc *scope) has(t string) bool {
	for _, x := range sc.tables {
		if x == t {
			return true
		}
	}
	return false
}

func (sc *scope) colsWithLits() []Column {
	var out []Column
	for _, c := range sc.cols {
		if len(c.Lits) > 0 {
			out = append(out, c)
		}
	}
	return out
}

func (sc *scope) colsOfKind(k Kind) []Column {
	var out []Column
	for _, c := range sc.cols {
		if c.Kind == k {
			out = append(out, c)
		}
	}
	return out
}

func (g *Gen) genWhere(q *Query, sc *scope) {
	preds := sc.colsWithLits()
	if len(preds) == 0 {
		return
	}
	n := g.pick(4) // 0-3 conjuncts
	for i := 0; i < n; i++ {
		c := preds[g.pick(len(preds))]
		lit := c.Lits[g.pick(len(c.Lits))]
		var p string
		switch {
		case c.Kind == KindString && g.chance(0.3):
			// LIKE with a prefix of the literal (strip quotes, take a
			// short prefix, re-quote).
			s := strings.Trim(lit, "'")
			if len(s) > 3 {
				s = s[:3]
			}
			p = fmt.Sprintf("%s LIKE '%s%%'", c.Name, s)
		case g.chance(0.2):
			lits := map[string]bool{lit: true}
			for len(lits) < 2+g.pick(3) && len(lits) < len(c.Lits) {
				lits[c.Lits[g.pick(len(c.Lits))]] = true
			}
			var ordered []string
			for l := range lits {
				ordered = append(ordered, l)
			}
			sort.Strings(ordered) // map iteration must not leak into SQL text
			p = fmt.Sprintf("%s IN (%s)", c.Name, strings.Join(ordered, ", "))
		case c.Kind != KindString && g.chance(0.25):
			lo, hi := lit, c.Lits[g.pick(len(c.Lits))]
			p = fmt.Sprintf("%s BETWEEN %s AND %s", c.Name, minLit(lo, hi), maxLit(lo, hi))
		default:
			ops := []string{"=", "<", ">", "<=", ">=", "<>"}
			p = fmt.Sprintf("%s %s %s", c.Name, ops[g.pick(len(ops))], lit)
		}
		q.Where = append(q.Where, p)
	}
}

// minLit/maxLit order two rendered literals so BETWEEN bounds are sane.
// Lexicographic order is correct for quoted date strings and for the
// numeric literal pools (rendered fixed-width where it matters).
func minLit(a, b string) string {
	if a <= b {
		return a
	}
	return b
}

func maxLit(a, b string) string {
	if a <= b {
		return b
	}
	return a
}

// groupTerm returns a GROUP BY term over column c — usually the bare
// column, sometimes an expression (the #273 shape: SUBSTR over a grouped
// column past a shuffle).
func (g *Gen) groupTerm(c Column) string {
	if c.Kind == KindString && g.chance(0.3) {
		return fmt.Sprintf("SUBSTR(%s, 1, %d)", c.Name, 2+g.pick(4))
	}
	if c.Kind == KindInt && g.chance(0.2) {
		return fmt.Sprintf("%s %% %d", c.Name, 2+g.pick(9))
	}
	return c.Name
}

// genAggFreeGroupBy: SELECT g1, g2 FROM ... GROUP BY g1, g2 — the
// dedup-past-shuffle shape (#163/#166).
func (g *Gen) genAggFreeGroupBy(q *Query, sc *scope) {
	n := 1 + g.pick(3)
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		term := g.groupTerm(sc.cols[g.pick(len(sc.cols))])
		if seen[term] {
			continue
		}
		seen[term] = true
		q.Select = append(q.Select, term)
		q.GroupBy = append(q.GroupBy, term)
	}
}

func (g *Gen) genAggregation(q *Query, sc *scope) {
	ng := 1 + g.pick(2)
	seen := map[string]bool{}
	for i := 0; i < ng; i++ {
		term := g.groupTerm(sc.cols[g.pick(len(sc.cols))])
		if seen[term] {
			continue
		}
		seen[term] = true
		q.Select = append(q.Select, term)
		q.GroupBy = append(q.GroupBy, term)
	}

	nums := append(sc.colsOfKind(KindInt), sc.colsOfKind(KindFloat)...)
	na := 1 + g.pick(2)
	var havingAgg string
	for i := 0; i < na; i++ {
		alias := fmt.Sprintf("a%d", i)
		var agg string
		switch g.pick(6) {
		case 0:
			agg = "COUNT(*)"
		case 1:
			c := sc.cols[g.pick(len(sc.cols))]
			agg = fmt.Sprintf("COUNT(DISTINCT %s)", c.Name)
		case 2, 3:
			c := nums[g.pick(len(nums))]
			agg = fmt.Sprintf("SUM(%s)", c.Name)
		case 4:
			c := nums[g.pick(len(nums))]
			agg = fmt.Sprintf("MIN(%s)", c.Name)
		default:
			c := nums[g.pick(len(nums))]
			agg = fmt.Sprintf("MAX(%s)", c.Name)
		}
		q.Select = append(q.Select, fmt.Sprintf("%s AS %s", agg, alias))
		if havingAgg == "" && (strings.HasPrefix(agg, "SUM") || agg == "COUNT(*)") {
			havingAgg = agg
		}
	}

	if havingAgg != "" && g.chance(0.4) {
		if g.chance(0.5) {
			// Scalar-subquery threshold — the #272/Q11 shape. The subquery
			// re-runs the same FROM/WHERE so the threshold is data-derived
			// on both sides of the differential.
			sub := &Query{
				Select: []string{fmt.Sprintf("%s * 0.%d", havingAgg, 1+g.pick(9))},
				Tables: q.Tables,
				On:     q.On,
				Where:  q.Where,
			}
			q.Having = fmt.Sprintf("%s > (%s)", havingAgg, sub.SQL())
		} else {
			q.Having = fmt.Sprintf("%s > %d", havingAgg, 1+g.pick(20))
		}
	}
}

func (g *Gen) genDistinct(q *Query, sc *scope) {
	q.Distinct = true
	n := 1 + g.pick(3)
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		c := sc.cols[g.pick(len(sc.cols))]
		if seen[c.Name] {
			continue
		}
		seen[c.Name] = true
		q.Select = append(q.Select, c.Name)
	}
}

// genProjection: plain SELECT with expressions — the compute-past-shuffle
// shape (#169).
func (g *Gen) genProjection(q *Query, sc *scope) {
	n := 1 + g.pick(3)
	for i := 0; i < n; i++ {
		c := sc.cols[g.pick(len(sc.cols))]
		switch {
		case c.Kind != KindString && g.chance(0.4):
			ops := []string{"+", "-", "*"}
			q.Select = append(q.Select, fmt.Sprintf("%s %s %d AS e%d", c.Name, ops[g.pick(len(ops))], 1+g.pick(9), i))
		case c.Kind == KindString && g.chance(0.4):
			q.Select = append(q.Select, fmt.Sprintf("SUBSTR(%s, 1, %d) AS e%d", c.Name, 2+g.pick(5), i))
		case len(c.Lits) > 0 && g.chance(0.3):
			lit := c.Lits[g.pick(len(c.Lits))]
			q.Select = append(q.Select, fmt.Sprintf("CASE WHEN %s = %s THEN 1 ELSE 0 END AS e%d", c.Name, lit, i))
		default:
			q.Select = append(q.Select, c.Name)
		}
	}
}

func (g *Gen) genOrderLimit(q *Query) {
	if !g.chance(0.4) {
		return
	}
	// Order by output positions of plain (un-aliased) select items, or by
	// aliases for computed ones.
	item := q.Select[g.pick(len(q.Select))]
	if i := strings.LastIndex(item, " AS "); i >= 0 {
		item = item[i+4:]
	}
	q.OrderBy = []string{item}
	if g.chance(0.6) {
		q.OrderBy[0] += " DESC"
	}
	if g.chance(0.5) {
		q.Limit = 1 + g.pick(50)
	}
}
