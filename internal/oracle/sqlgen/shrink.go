package sqlgen

import "strings"

// Shrink reduces q to a smaller query for which fails still returns true
// (fails = "still reproduces the divergence"). It repeatedly tries
// structural removals — LIMIT, ORDER BY, HAVING, DISTINCT, individual
// WHERE conjuncts, individual select/GROUP BY items, and unreferenced
// trailing joins — accepting any removal that keeps the failure, until a
// full pass removes nothing. The result is the minimal repro to report.
func Shrink(s *Schema, q *Query, fails func(*Query) bool) *Query {
	cur := q.clone()
	for {
		reduced := false
		for _, cand := range candidates(s, cur) {
			if fails(cand) {
				cur = cand
				reduced = true
				break
			}
		}
		if !reduced {
			return cur
		}
	}
}

// Clone returns a deep copy of q.
func (q *Query) Clone() *Query { return q.clone() }

func (q *Query) clone() *Query {
	c := *q
	c.Select = append([]string(nil), q.Select...)
	c.Tables = append([]string(nil), q.Tables...)
	c.On = append([]string(nil), q.On...)
	c.Where = append([]string(nil), q.Where...)
	c.GroupBy = append([]string(nil), q.GroupBy...)
	c.OrderBy = append([]string(nil), q.OrderBy...)
	return &c
}

// candidates returns every single-step reduction of q, simplest first.
func candidates(s *Schema, q *Query) []*Query {
	var out []*Query
	add := func(c *Query) { out = append(out, c) }

	if q.LimitZero {
		c := q.clone()
		c.LimitZero = false
		add(c)
	}
	if q.Offset > 0 {
		c := q.clone()
		c.Offset = 0
		add(c)
	}
	if q.Limit > 0 {
		c := q.clone()
		c.Limit = 0
		c.LimitZero = false
		add(c)
	}
	if len(q.OrderBy) > 0 {
		c := q.clone()
		c.OrderBy = nil
		c.Limit = 0 // LIMIT without ORDER BY is never part of a minimal repro
		c.LimitZero = false
		add(c)
	}
	if q.Having != "" {
		c := q.clone()
		c.Having = ""
		add(c)
	}
	if q.Distinct {
		c := q.clone()
		c.Distinct = false
		add(c)
	}
	for i := range q.Where {
		c := q.clone()
		c.Where = append(c.Where[:i], c.Where[i+1:]...)
		add(c)
	}
	// Select items: keep at least one. A grouped query's select item that
	// is also a GROUP BY term drops from both lists (aggregate-free
	// GROUP BY stays aggregate-free and grouped queries stay valid).
	if len(q.Select) > 1 {
		for i := range q.Select {
			c := q.clone()
			item := c.Select[i]
			c.Select = append(c.Select[:i], c.Select[i+1:]...)
			for j, gTerm := range c.GroupBy {
				if gTerm == item {
					c.GroupBy = append(c.GroupBy[:j], c.GroupBy[j+1:]...)
					break
				}
			}
			if orderByReferencesRemoved(c, item) {
				c.OrderBy = nil
				c.Limit = 0
				c.LimitZero = false
			}
			add(c)
		}
	}
	// Trailing join whose table no remaining clause references.
	if n := len(q.Tables); n > 1 {
		last := q.Tables[n-1]
		if !referencesTable(s, q, last) {
			c := q.clone()
			c.Tables = c.Tables[:n-1]
			c.On = c.On[:n-2]
			add(c)
		}
	}
	return out
}

func orderByReferencesRemoved(q *Query, removed string) bool {
	alias := removed
	if i := strings.LastIndex(removed, " AS "); i >= 0 {
		alias = removed[i+4:]
	}
	for _, o := range q.OrderBy {
		if strings.Contains(o, alias) {
			return true
		}
	}
	return false
}

// referencesTable reports whether any clause other than the trailing join
// itself mentions a column of table name. TPC-H column names are globally
// unique, so substring containment is a sound reference check.
func referencesTable(s *Schema, q *Query, name string) bool {
	tbl := s.table(name)
	if tbl == nil {
		return true // unknown table: never drop
	}
	var clauses []string
	clauses = append(clauses, q.Select...)
	clauses = append(clauses, q.Where...)
	clauses = append(clauses, q.GroupBy...)
	clauses = append(clauses, q.OrderBy...)
	clauses = append(clauses, q.Having)
	clauses = append(clauses, q.On[:len(q.On)-1]...) // all joins but the trailing one
	for _, c := range tbl.Cols {
		for _, cl := range clauses {
			if strings.Contains(cl, c.Name) {
				return true
			}
		}
	}
	return false
}
