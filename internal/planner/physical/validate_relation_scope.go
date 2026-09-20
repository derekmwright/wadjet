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
// lat_ord / lat_item fixture. So is the explanatory clause after the colon,
// verbatim from PostgreSQL's DETAIL or HINT: `sqlerr.Error` carries one
// message and no detail field, and a client that matches on PostgreSQL's
// wording — SQLancer's `PostgresCommon.getCommonFetchErrors` lists the string
// "but it cannot be referenced from this part of the query" — then matches
// this engine's refusal too, which is what lets a generated corpus get PAST
// the shape instead of stopping on it.
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
//
// `qual` is FOLDED, because every scope map is and an unquoted reference
// arrives folded from the lexer. `spelled` is the name AS DECLARED, and the
// duplicate-name verdict is the one thing that must read it: `FROM qa t, qb
// "T"` declares TWO relations in PostgreSQL — a delimited identifier keeps its
// bytes — and a verdict taken on the folded key would refuse a statement
// PostgreSQL answers. The scope has never distinguished two sources under one
// folded key; a verdict need not inherit that (#731, and arc SR's round-2
// finding that a COUNT taken on the folded key refused `SELECT t.c1 FROM clt1
// t, clt2 "T"`).
type relationSite struct {
	qual    string
	spelled string
	item    int
	join    int
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

// relationDeclaredName is the name a FROM source answers to, AS DECLARED: its
// alias, or its own name when it has none.
func relationDeclaredName(tr *plansql.TableRef) string {
	if tr == nil {
		return ""
	}
	if tr.Alias != "" {
		return tr.Alias
	}
	return tr.Name
}

// relationSiteFor builds one site from a FROM source.
func relationSiteFor(tr *plansql.TableRef, item, join int) relationSite {
	name := relationDeclaredName(tr)
	return relationSite{qual: strings.ToLower(name), spelled: name, item: item, join: join}
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
		c.sites = append(c.sites, relationSiteFor(&info.Tables[k], k, -1))
		for j := range info.Joins {
			if info.Joins[j].FromItem != k {
				continue
			}
			c.sites = append(c.sites, relationSiteFor(joinRightRef(&info.Joins[j]), k, j))
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
	// An alias EQUAL to the table's own name hides nothing, and the parser
	// records one for several spellings that wrote none. Without this test the
	// out-of-scope `t3` of `FROM t0, t3, t1 JOIN t2 ON t3.c4` earned the ALIAS
	// sentence naming `t3` as its own alias — PostgreSQL's is the ordinary
	// case-2 one, and SQLancer's expected-error list carries that one and not
	// this (measured: 178 of 200 generated databases stopped on it).
	if strings.EqualFold(name, alias) {
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
					"invalid reference to FROM-clause entry for table %q: perhaps you meant to "+
						"reference the table alias %q",
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

// resolveStarQualifier holds a QUALIFIED STAR to the same rule as any other
// qualified reference: `q.*` names one relation in scope or it names nothing.
//
// A star was the one SELECT-list item the binder skipped entirely, so
// `SELECT lat_ord.* FROM lat_ord o` ANSWERED the aliased relation's rows —
// PostgreSQL refuses it, because an alias is the only name the relation has —
// and `SELECT zz.*` reached the star expander, which said the relation's
// column list was not known rather than that no such relation exists
// (measured on 17.11: 42P01 `missing FROM-clause entry for table "zz"`).
//
// A qualifier that is itself a COLUMN is left alone: `(rw).*` over a ROW
// column is a different construct and this rule has nothing to say about it.
func (s *colScope) resolveStarQualifier(table string) error {
	if s == nil || s.open || table == "" {
		return nil
	}
	return s.refuseUnknownRelationQualifier(table)
}

// refuseUnknownRelationQualifier is the QUALIFIER half of the reference rule
// on its own: the name before the dot either answers to something this scope
// knows, or it earns one of PostgreSQL's 42P01 sentences. It is separate from
// resolveRef because two callers need exactly this and not the column half —
// a qualified STAR, which has no column to check, and a reference in a scope
// opened by a STAR OUTPUT, where the FROM's relations are still known even
// though the output names are not.
func (s *colScope) refuseUnknownRelationQualifier(table string) error {
	if s == nil || table == "" {
		return nil
	}
	q := strings.ToLower(table)
	if s.quals[q] != nil || s.cols[q] || strings.Contains(q, ".") {
		return nil
	}
	ref := &plansql.ColRef{Table: table, Column: "*"}
	if err := s.refuseOuterLevelReference(ref); err != nil {
		return err
	}
	return s.refuseUnmatchedQualifier(ref)
}

// refuseDuplicateRelationName is PostgreSQL's 42712 for a FROM list that names
// one relation twice under one name — `FROM t, t`, `FROM t JOIN t ON …`, or two
// derived tables sharing an alias. The name would answer to two relations, so
// every reference through it is ambiguous before any column is looked at, and
// PostgreSQL refuses the FROM clause itself:
//
//	SELECT 1 FROM lat_item JOIN lat_item ON true
//	ERROR:  42712: table name "lat_item" specified more than once
//
// This engine answered the cross product and refused only the REFERENCES into
// it, as 42702 on the column — a different question with a different answer.
// The comparison is on the name AS DECLARED, not on the folded key: a
// DELIMITED alias keeps its bytes, so `FROM qa t, qb "T"` declares two
// relations and PostgreSQL answers it. An empty qualifier (a derived table
// written without an alias, which this parser can produce) is skipped: it is
// not a name anything can be written through.
func (c *relationCensus) refuseDuplicateRelationName() error {
	if c == nil {
		return nil
	}
	seen := map[string]bool{}
	for _, s := range c.sites {
		if s.spelled == "" || strings.HasPrefix(s.spelled, "(") {
			continue
		}
		if seen[s.spelled] {
			return sqlerr.New("42712", "table name %q specified more than once", s.spelled)
		}
		seen[s.spelled] = true
	}
	return nil
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
		"invalid reference to FROM-clause entry for table %q: there is an entry for table %q, "+
			"but it cannot be referenced from this part of the query — to reference that table, "+
			"you must mark this subquery with LATERAL",
		ref.Table, ref.Table)
}
