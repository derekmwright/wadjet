// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A QUALIFIED REFERENCE NAMES ONE RELATION IN SCOPE AT THE POINT IT IS
// WRITTEN, and the point is not the whole query block.
//
// The binder builds ONE scope per block from every FROM item and every JOIN,
// which is the right scope for WHERE, the SELECT list, GROUP BY, HAVING and
// ORDER BY — every relation of the block is visible in all of those. It is the
// wrong scope for a JOIN's ON clause: SQL scopes an ON to the two sides of the
// join it belongs to, so `FROM a JOIN b ON c.x = a.x JOIN c ON …` names `c`
// where `c` is not yet in scope, and `FROM a, b JOIN c ON a.x = c.x` names `a`
// where `a` is a DIFFERENT FROM item. PostgreSQL 17.11 refuses both; this
// binder answered both, because `c` and `a` were in the one flat scope (#1220).
//
// This file is the block's relation CENSUS — every relation the FROM declares,
// in the order the FROM writes them — and the two things it decides:
//
//   - which relations an ON clause may name (relationCensus.visibleAtJoin);
//   - which of PostgreSQL's two 42P01 sentences an unmatched qualifier gets.
//
// The sentences are PostgreSQL's own, measured on 17.11 over this package's
// lat_ord / lat_item fixture:
//
//	FROM lat_ord a JOIN lat_item b ON j.id = a.id JOIN lat_item j ON …
//	  ERROR:  42P01: missing FROM-clause entry for table "j"
//	FROM lat_ord a, lat_item b JOIN lat_item c ON a.id = c.order_id
//	  ERROR:  42P01: invalid reference to FROM-clause entry for table "a"
//	  DETAIL: There is an entry for table "a", but it cannot be referenced
//	          from this part of the query.
//	FROM lat_ord a … lat_ord.id
//	  ERROR:  42P01: invalid reference to FROM-clause entry for table "lat_ord"
//	  HINT:   Perhaps you meant to reference the table alias "a".
//
// The split is POSITIONAL and that is not an accident of PostgreSQL's
// implementation, it is what the two sentences mean: "invalid reference" says
// the statement HAS such an entry and this position cannot see it, and the
// parser can only say that about an entry it has already read. A relation
// introduced by a LATER join has not been read yet, so the same reference is
// "missing" — measured above, and it is why a forward ON reference gets the
// missing sentence rather than the invalid one the issue expected.

// relationSite is one relation the block's FROM declares: the name it answers
// to, the comma-separated FROM item it belongs to, and the JOIN that
// introduced it (-1 for the item's own table).
type relationSite struct {
	qual string
	item int
	join int
}

// relationCensus is every relationSite of one block, in PARSE ORDER — FROM
// item 0, then the joins that extend it, then FROM item 1, and so on. That is
// the order the FROM list is written and the order a reference's position is
// measured against.
type relationCensus struct {
	sites []relationSite
	// hidden maps a BASE TABLE's own name to the alias that hides it. Only
	// resolveSource can fill it: whether a FROM source is a base table at all
	// is a catalog question, and PostgreSQL's alias sentence is reserved for
	// a real relation (it looks the name up in the catalog before deciding).
	hidden map[string]string
}

// relationQualName is the name a FROM source answers to: its alias, or its own
// name when it has none. Folded, because every scope map here is.
func relationQualName(tr *plansql.TableRef) string {
	if tr == nil {
		return ""
	}
	if tr.Alias != "" {
		return strings.ToLower(tr.Alias)
	}
	return strings.ToLower(tr.Name)
}

// newRelationCensus reads the block's FROM positionally. It declines (nil)
// when a join names a FROM item that does not exist, which is the conservative
// side: without the item association the ON scope cannot be computed and the
// binder keeps the flat scope it has always used.
func newRelationCensus(info *plansql.SelectInfo) *relationCensus {
	if info == nil || len(info.Tables) == 0 {
		return nil
	}
	for i := range info.Joins {
		if info.Joins[i].FromItem < 0 || info.Joins[i].FromItem >= len(info.Tables) {
			return nil
		}
	}
	c := &relationCensus{hidden: map[string]string{}}
	for k := range info.Tables {
		c.sites = append(c.sites, relationSite{qual: relationQualName(&info.Tables[k]), item: k, join: -1})
		for j := range info.Joins {
			if info.Joins[j].FromItem != k {
				continue
			}
			c.sites = append(c.sites, relationSite{
				qual: relationQualName(joinRightRef(&info.Joins[j])), item: k, join: j})
		}
	}
	return c
}

// noteAliasedTable records that a BASE TABLE of this block is reachable only
// through an alias, so a reference to its own name gets PostgreSQL's alias
// sentence rather than "missing". A name already answering to something in
// this block is left alone: `FROM (SELECT …) lat_item JOIN lat_item b` is a
// statement PostgreSQL ANSWERS, because the derived table owns the name.
func (c *relationCensus) noteAliasedTable(name, alias string) {
	if c == nil || name == "" || alias == "" {
		return
	}
	n := strings.ToLower(name)
	if _, taken := c.hidden[n]; taken {
		return
	}
	c.hidden[n] = alias
}

// declaredAt returns the parse position of the FIRST site answering to qual,
// and whether the block declares it at all.
func (c *relationCensus) declaredAt(qual string) (int, bool) {
	if c == nil {
		return 0, false
	}
	for i, s := range c.sites {
		if s.qual == qual {
			return i, true
		}
	}
	return 0, false
}

// visibleAtJoin is the set of relations join j's ON clause may name and the
// number of declarations parsed at that point. ok=false when the census
// cannot place the join, which leaves the caller with the flat scope.
//
// The rule is SQL's own: an ON belongs to one join, and the relations it can
// name are the ones already joined INSIDE its own FROM item — the item's own
// table and every join of that item up to and including this one. A relation
// of another comma-separated item is a different FROM item and is not one of
// them; a relation joined LATER has not been written yet.
func (c *relationCensus) visibleAtJoin(j int) (map[string]bool, int, bool) {
	if c == nil {
		return nil, 0, false
	}
	at := -1
	for i, s := range c.sites {
		if s.join == j {
			at = i
			break
		}
	}
	if at < 0 {
		return nil, 0, false
	}
	item := c.sites[at].item
	visible := map[string]bool{}
	for i := 0; i <= at; i++ {
		if c.sites[i].item == item {
			visible[c.sites[i].qual] = true
		}
	}
	return visible, at + 1, true
}

// scopeAtJoin is this scope with every relation of its OWN block that the
// point cannot see removed. The caller merges any outer scope back in
// afterwards, so a correlated reference — which SQL does allow in an ON — is
// unaffected: an outer level re-supplies the qualifier and the reference
// resolves there, which is what PostgreSQL does with it.
func (s *colScope) scopeAtJoin(visible map[string]bool, through int) *colScope {
	c := s.clone()
	c.parsedThrough = through
	if s.relations == nil {
		return c
	}
	for _, site := range s.relations.sites {
		if site.qual == "" || visible[site.qual] {
			continue
		}
		delete(c.quals, site.qual)
		delete(c.qualColTypes, site.qual)
		delete(c.dupQualified, site.qual)
	}
	return c
}

// refuseUnmatchedQualifier is PostgreSQL's verdict for a qualifier no relation
// in scope answers to. Three cases, in the order PostgreSQL decides them:
//
//  1. the name is a BASE TABLE this block reads under an alias — the alias is
//     the only name in scope, and saying "missing" sends the reader looking
//     for a FROM entry that is right there;
//  2. the name IS declared by this block, EARLIER in the FROM than the point
//     that names it, but this position cannot reach it;
//  3. otherwise nothing at this level declares it — including a relation a
//     LATER join introduces, which the parser has not read yet.
func (s *colScope) refuseUnmatchedQualifier(ref *plansql.ColRef) error {
	q := strings.ToLower(ref.Table)
	if err := s.refuseSiblingReference(ref); err != nil {
		return err
	}
	if s.relations != nil {
		if alias, hidden := s.relations.hidden[q]; hidden {
			if at, declared := s.relations.declaredAt(strings.ToLower(alias)); !declared || at < s.parsedThrough {
				return sqlerr.New("42P01",
					"invalid reference to FROM-clause entry for table %q: the FROM clause reads "+
						"that table under the alias %q, and an alias is the only name it answers to",
					ref.Table, alias)
			}
		}
		if at, declared := s.relations.declaredAt(q); declared && at < s.parsedThrough {
			return sqlerr.New("42P01",
				"invalid reference to FROM-clause entry for table %q: there is an entry for table %q, "+
					"but it cannot be referenced from this part of the query",
				ref.Table, ref.Table)
		}
	}
	return sqlerr.New("42P01", "missing FROM-clause entry for table %q", ref.Table)
}

// fromCensusSites is the census's site list, nil-safe, so a block whose FROM
// could not be read positionally leaves parsedThrough at zero and every
// unmatched qualifier keeps the "missing" sentence it had.
func fromCensusSites(c *relationCensus) []relationSite {
	if c == nil {
		return nil
	}
	return c.sites
}

// refuseSiblingReference is PostgreSQL's sentence for a plain derived table
// that names a SIBLING FROM item of the query it sits in:
//
//	SELECT s.m FROM lat_ord o, (SELECT o.id AS m) s
//	ERROR:  42P01: invalid reference to FROM-clause entry for table "o"
//	HINT:   To reference that table, you must mark this subquery with LATERAL.
//
// Both engines refuse it and both say 42P01; what this adds is WHICH refusal,
// because "missing FROM-clause entry" sends the reader looking for a FROM
// entry that is written three words away. Positional, like every other case-2
// verdict here: the sibling scope is the one built SO FAR, so a derived table
// naming an item written AFTER it keeps "missing", which is what PostgreSQL
// answers for that spelling (measured).
func (s *colScope) refuseSiblingReference(ref *plansql.ColRef) error {
	if s == nil || s.siblingDiag == nil || ref == nil {
		return nil
	}
	if s.siblingDiag.quals[strings.ToLower(ref.Table)] == nil {
		return nil
	}
	return sqlerr.New("42P01",
		"invalid reference to FROM-clause entry for table %q: there is an entry for table %q "+
			"in the enclosing FROM clause, but a derived table cannot reference it — "+
			"mark the subquery LATERAL",
		ref.Table, ref.Table)
}
