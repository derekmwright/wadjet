// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The relation census resolves qualifiers by scope and FROM position.
// An ON clause sees relations declared so far, including an earlier comma
// sibling, but not a later join or comma item. Alias-hidden, duplicate and
// unknown qualifiers keep their named refusals. The earlier-comma extension
// is recorded in ADR-0012 §5 (#617); #1220 retains the later-reference rule.
// The 42P01 DETAIL/HINT text is verbatim because clients, including
// SQLancer's getCommonFetchErrors, match it.

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
// sentence rather than "missing".
//
// What it records is only ever a DIAGNOSIS: refuseUnmatchedQualifier is
// reached only where no relation in scope answers to the name at all, so
// `FROM (SELECT …) lat_item JOIN lat_item b` — where a derived table owns the
// name and PostgreSQL ANSWERS the statement — never gets here.
func (c *relationCensus) noteAliasedTable(name, alias string) {
	if c == nil || name == "" || alias == "" {
		return
	}
	// An alias EQUAL to the table's own name hides nothing, and the parser
	// records one for several spellings that wrote none. Without this test,
	// an out-of-scope unaliased table earned the ALIAS sentence naming itself
	// as its own alias — PostgreSQL's is the ordinary case-2 one, and
	// SQLancer's expected-error list carries that one and not this (measured:
	// 178 of 200 generated databases stopped on it). The motivating example,
	// `FROM t0, t3, t1 JOIN t2 ON t3.c4`, is itself an EARLIER comma sibling
	// of `t2`'s join now (ADR-0012 §5 #617, the BX hotfix) and so answers —
	// this guard's remaining reach is WHERE/SELECT/etc. positions and a
	// relation a later join or comma item introduces, where an unaliased name
	// stays genuinely out of scope.
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
// The rule is POSITIONAL, not per-item: an ON may name anything the FROM
// clause has already declared by the point it is written — the item's own
// table, every join of that item up to and including this one, AND an
// earlier comma-separated FROM item. That last part is a deliberate
// DuckDB-matching superset PostgreSQL does not share (ADR-0012 §5 #617):
// `FROM a, b JOIN c ON a.k = c.k` answers here. Only what is written LATER —
// a relation a later join or a later comma item introduces — is out of
// scope, because the parser has not read it yet.
//
// A prior version of this rule (arc RS, #1220) restricted visibility to the
// join's own FROM item, which correctly refused a relation joined later but
// also refused the earlier-comma-sibling case #617 had already settled as an
// answered superset — conflating "not yet written" with "written elsewhere."
// Restored by the BX hotfix.
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
	visible := map[string]bool{}
	for i := 0; i <= at; i++ {
		visible[c.sites[i].qual] = true
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
	removed := map[string]bool{}
	for i, site := range s.relations.sites {
		if site.qual == "" || visible[site.qual] || removed[site.qual] {
			continue
		}
		removed[site.qual] = true
		// Its BARE columns leave with it when it is written LATER than this
		// ON. A relation a later join introduces publishes no name to an
		// earlier ON — PostgreSQL's `SELECT 1 FROM o JOIN ev ON a = o.id
		// JOIN nn ON true`, where only nn has `a`, is 42703 there — and a
		// name the scope kept anyway both resolved that reference (the
		// statement answered) and read as a ROW container for `nn.x`'s
		// qualifier. An EARLIER comma item keeps its bare columns: `FROM
		// customer, orders JOIN nation ON c_nationkey = n_nationkey` is
		// answered by folding the item into the join (#F1, a superset of
		// PostgreSQL kept for DuckDB parity); only its qualifier leaves.
		if i < through {
			delete(c.quals, site.qual)
			delete(c.qualColTypes, site.qual)
			delete(c.dupQualified, site.qual)
			continue
		}
		for col := range s.quals[site.qual] {
			if c.srcCount[col] > 0 {
				c.srcCount[col]--
			}
			if c.srcCount[col] == 0 {
				delete(c.cols, col)
				delete(c.colTypes, col)
				delete(c.rowFields, col)
				delete(c.elemTypes, col)
			}
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
	// The SIBLING diagnosis is asked LAST, which is PostgreSQL's own order.
	// It was asked first, and then a derived table whose OWN FROM reads the
	// named table under an alias, sitting beside an outer FROM item of that
	// same name, earned the LATERAL hint where PostgreSQL gives the alias one:
	//
	//	SELECT s.m FROM lat_ord, (SELECT lat_ord.id AS m FROM lat_ord q) s
	//	HINT:  Perhaps you meant to reference the table alias "q".
	//
	// and the same statement with the OUTER item aliased — which puts nothing
	// of that name in siblingDiag — already earned the alias hint. One
	// reference, two hints, decided by whether the ENCLOSING block happened to
	// alias its own copy: the arc's own defect one level up (measured by the
	// round-1 review, P1). This block's own FROM is asked about first now, and
	// the sibling only where it says nothing.
	if err := s.refuseSiblingReference(ref); err != nil {
		return err
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
	// A DELIMITED qualifier is byte-exact here too. resolveRef's CLOSED path
	// already refuses `"O".id` over `FROM lat_ord o` (#731), and this helper
	// is the STAR-opened scope's way in — folding it back left ONE reference
	// with two dispositions decided by the enclosing SELECT list, which is
	// the very split docs/design/window-key-ownership.md §(e) item 8 records
	// as closed (measured by the round-1 review, P3). It fires only where the
	// FOLD would have resolved and no FROM source declared those bytes, so a
	// scope that lost a spelling cannot manufacture a refusal.
	if plansql.FoldIdent(table) != table && !s.exactQuals[table] && s.quals[q] != nil {
		return sqlerr.New("42P01", "missing FROM-clause entry for table %q", table)
	}
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
