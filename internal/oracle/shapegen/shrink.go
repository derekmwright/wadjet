package shapegen

import "strings"

// Shrink reduces q to the smallest query for which fails still reports true
// ("still reproduces the divergence"). It applies structural removals — LIMIT,
// OFFSET, ORDER BY terms, HAVING, DISTINCT, WHERE conjuncts, select items,
// unreferenced trailing joins — accepting any removal that keeps the failure,
// until a full pass removes nothing.
//
// Reporting the unshrunk query is close to useless: "some 8-table query
// differs" is not a defect report. The shrunk query is.
func Shrink(s *Schema, q *Query, fails func(*Query) bool) *Query {
	cur := q.Clone()
	// A pathological shrink walk on a slow arm would dominate the run, so the
	// number of accepted reductions is bounded; each accepted step strictly
	// shrinks the query, so the bound is generous.
	for step := 0; step < 200; step++ {
		reduced := false
		for _, cand := range candidates(s, cur) {
			if fails(cand) {
				cur = cand
				reduced = true
				break
			}
		}
		if !reduced {
			break
		}
	}
	return cur
}

// Clone returns a deep copy of q.
func (q *Query) Clone() *Query {
	c := *q
	c.Items = append([]Item(nil), q.Items...)
	c.From = append([]From(nil), q.From...)
	c.Where = append([]string(nil), q.Where...)
	c.GroupBy = append([]string(nil), q.GroupBy...)
	c.Order = append([]Order(nil), q.Order...)
	return &c
}

// candidates returns every single-step reduction of q, cheapest first. Only
// well-formed reductions are offered: an invalid query is not a smaller repro,
// it is a different failure, and reporting one wastes the reader's time.
func candidates(s *Schema, q *Query) []*Query {
	var out []*Query
	add := func(c *Query) {
		if wellFormed(c) {
			out = append(out, c)
		}
	}

	if q.Offset > 0 {
		c := q.Clone()
		c.Offset = 0
		add(c)
	}
	if q.LimitZero {
		c := q.Clone()
		c.LimitZero = false
		add(c)
	}
	if q.Limit > 0 {
		c := q.Clone()
		c.Limit = 0
		c.LimitZero = false
		add(c)
	}
	if len(q.Order) > 0 {
		// Whole ORDER BY first: a repro that survives without it is smaller
		// and says something different about the defect.
		c := q.Clone()
		c.Order = nil
		c.Limit = 0 // a LIMIT without ORDER BY is never part of a minimal repro
		c.LimitZero = false
		c.TotalOrder = false
		add(c)
		for i := range q.Order {
			c := q.Clone()
			c.Order = append(c.Order[:i:i], c.Order[i+1:]...)
			// Dropping a term can drop the uniqueness tiebreaker, and a
			// no-longer-total order compared positionally would report tie
			// order as a divergence. Shrinking must never make the comparison
			// STRICTER than the generator's own contract.
			c.TotalOrder = false
			if len(c.Order) == 0 {
				c.Limit = 0
				c.LimitZero = false
			}
			add(c)
		}
	}
	if q.Having != "" {
		c := q.Clone()
		c.Having = ""
		add(c)
	}
	if q.Distinct {
		c := q.Clone()
		c.Distinct = false
		add(c)
	}
	for i := range q.Where {
		c := q.Clone()
		c.Where = append(c.Where[:i:i], c.Where[i+1:]...)
		add(c)
	}
	// Select items: keep at least one, and keep the query well-formed — an
	// item that is also a GROUP BY term leaves both lists, and an ORDER BY
	// term that referenced it goes too.
	if len(q.Items) > 1 {
		for i := range q.Items {
			c := q.Clone()
			it := c.Items[i]
			c.Items = append(c.Items[:i:i], c.Items[i+1:]...)
			c.GroupBy = dropGroupTerm(c.GroupBy, it, i)
			if before := len(c.Order); before > 0 {
				c.Order = dropOrderTerms(c.Order, it)
				if len(c.Order) != before {
					c.TotalOrder = false // see the ORDER BY branch above
				}
			}
			if len(c.Order) == 0 {
				c.Limit = 0
				c.LimitZero = false
				c.TotalOrder = false
			}
			if len(c.GroupBy) == 0 && hasAgg(c.Items) && anyNonAgg(c.Items) {
				continue // would leave a bare column beside an aggregate
			}
			add(c)
		}
	}
	// Trailing join whose table nothing else references.
	if n := len(q.From); n > 1 {
		last := q.From[n-1]
		if !referencesAlias(q, last.Alias, n-1) {
			c := q.Clone()
			c.From = c.From[:n-1]
			add(c)
		}
	}
	return out
}

// wellFormed reports whether q is still a valid query after a reduction.
// Shrinking removes clauses, and a removal can leave a GROUP BY ordinal
// pointing at an aggregate, an ORDER BY alias with no select item behind it,
// or a bare column beside an aggregate with no GROUP BY at all.
func wellFormed(q *Query) bool {
	if len(q.Items) == 0 {
		return false
	}
	aliases := make(map[string]Item, len(q.Items))
	for _, it := range q.Items {
		if it.Alias != "" {
			aliases[it.Alias] = it
		}
	}
	agg, nonAgg := false, false
	for _, it := range q.Items {
		if it.Star != "" {
			nonAgg = true
			continue
		}
		if it.Agg {
			agg = true
		} else {
			nonAgg = true
		}
	}
	if agg && nonAgg && len(q.GroupBy) == 0 {
		return false
	}
	for _, g := range q.GroupBy {
		if n, ok := atoi(g); ok {
			if n < 1 || n > len(q.Items) || q.Items[n-1].Agg || q.Items[n-1].Star != "" {
				return false
			}
			continue
		}
		if it, ok := aliases[g]; ok && it.Agg {
			return false
		}
	}
	for _, o := range q.Order {
		if n, ok := atoi(o.Expr); ok {
			if n < 1 || n > len(q.Items) {
				return false
			}
			continue
		}
		// A term naming an output alias must still have that item behind it.
		if o.Key != "" {
			if _, ok := aliases[o.Key]; !ok {
				return false
			}
		}
	}
	return true
}

func dropGroupTerm(group []string, it Item, idx int) []string {
	ordinal := itoa(idx + 1)
	for j, g := range group {
		if g == it.Expr || g == it.Alias || g == ordinal {
			return append(group[:j:j], group[j+1:]...)
		}
	}
	// Ordinals after the removed item now point one position too far.
	out := make([]string, 0, len(group))
	for _, g := range group {
		if n, ok := atoi(g); ok && n > idx+1 {
			out = append(out, itoa(n-1))
			continue
		}
		out = append(out, g)
	}
	return out
}

func dropOrderTerms(order []Order, it Item) []Order {
	out := make([]Order, 0, len(order))
	for _, o := range order {
		if o.Key == it.Alias || o.Expr == it.Alias || o.Expr == it.Expr {
			continue
		}
		if _, ok := atoi(o.Expr); ok {
			continue // an ordinal now points at a different column
		}
		out = append(out, o)
	}
	return out
}

func hasAgg(items []Item) bool {
	for _, it := range items {
		if it.Agg {
			return true
		}
	}
	return false
}

func anyNonAgg(items []Item) bool {
	for _, it := range items {
		if !it.Agg {
			return true
		}
	}
	return false
}

// referencesAlias reports whether any clause other than the dropped entry's
// own ON condition mentions the table alias.
func referencesAlias(q *Query, alias string, fromIdx int) bool {
	needle := alias + "."
	var clauses []string
	for _, it := range q.Items {
		clauses = append(clauses, it.Expr, it.Star)
	}
	clauses = append(clauses, q.Where...)
	clauses = append(clauses, q.GroupBy...)
	clauses = append(clauses, q.Having)
	for _, o := range q.Order {
		clauses = append(clauses, o.Expr)
	}
	for i, f := range q.From {
		if i != fromIdx {
			clauses = append(clauses, f.On)
		}
	}
	for _, c := range clauses {
		if strings.Contains(c, needle) {
			return true
		}
	}
	return false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
