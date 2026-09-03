package physical

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// tableColumnSource resolves a table name to its catalog metadata. It is the
// only catalog capability the column binder needs; *catalog.Catalog satisfies
// it, and tests can substitute a fake.
type tableColumnSource interface {
	GetTable(ctx context.Context, name string) (*catalog.TableMeta, error)
}

// ValidateColumns checks that every column reference in a SELECT statement
// resolves to a column available in its scope, returning an error naming the
// first unresolvable reference. This is plan-time name binding: it runs over the
// SQL AST (where query-block boundaries — FROM sources, derived tables, CTEs,
// subqueries — are still explicit) rather than the inlined logical tree.
//
// The binder is deliberately conservative: it errors only when a reference
// provably resolves to no column in any reachable source. Whenever a scope is
// uncertain — an open-schema source is present (table function, recursive CTE,
// SELECT *, a table absent from the catalog), an expression fails to parse, or a
// qualifier can't be matched — it skips rather than risk rejecting a valid
// query. A false positive breaks a working query; a false negative merely lets a
// typo through to the existing runtime check.
func (p *Planner) ValidateColumns(ctx context.Context, info *plansql.SelectInfo) error {
	if p == nil || p.catalog == nil || info == nil {
		return nil
	}
	return validateColumns(ctx, p.catalog, info)
}

func validateColumns(ctx context.Context, src tableColumnSource, info *plansql.SelectInfo) error {
	b := &binder{src: src, ctes: map[string]cteEntry{}}
	return b.validateBlock(ctx, info, nil)
}

// colScope is the set of columns visible at a point in a query. `cols` holds
// every bare (unqualified) column name; `quals` maps a qualifier (table name or
// alias) to its column set for qualified resolution; `srcCount` counts how many
// of this block's OWN FROM sources provide each bare name (outer scopes never
// contribute — an inner name shadows an outer one silently, per PostgreSQL).
// `open` means an unenumerable source is present, so no reference can be proven
// absent.
type colScope struct {
	open     bool
	cols     map[string]bool
	quals    map[string]map[string]bool
	srcCount map[string]int
	// colTypes / qualColTypes record the declared parquet.TypeID of the
	// columns a BASE TABLE provides, for the plan-time literal refusal
	// (validate_literal.go). A bare name two sources declare with DIFFERENT
	// types is recorded typeAmbiguous — the refusal has to be certain — and a
	// name from a derived table or a CTE, whose column types this binder does
	// not carry, appears in neither map and is therefore never refused.
	colTypes     map[string]parquet.TypeID
	qualColTypes map[string]map[string]parquet.TypeID
	// rowFields records the declared FIELDS of the ROW columns a BASE TABLE
	// provides, keyed by bare column name, so a dotted field path (rw.field)
	// can be checked at plan time the way an unknown column already is (#604).
	// A bare name two sources declare with DIFFERENT ROW shapes is removed —
	// the refusal has to be certain — and a name from a derived table or CTE,
	// whose field shape this binder does not carry, is never in the map and so
	// is never refused. Only ROW columns with a non-empty field list appear.
	rowFields map[string][]parquet.Column
	// exact records the SPELLING each source published, so a DELIMITED
	// reference can be held to the bytes PostgreSQL holds it to. Every other
	// map here is keyed on the folded name, which is right for an unquoted
	// reference — the lexer already folded it (#731) — and wrong for a
	// quoted one: `SELECT "G"` over a column `g` is 42703 in PostgreSQL, not
	// a read of `g`.
	exact map[string]bool
}

// typeAmbiguous marks a bare column name that two FROM sources declare with
// different types: it is present in colTypes (so it was seen) but names no one
// type the refusal can prove. TypeBool is a real TypeID (iota 0), so the
// sentinel has to sit outside the enum's range.
const typeAmbiguous = parquet.TypeID(-1)

// providesBareColumn reports, with CERTAINTY, that one of this scope's own
// sources carries the bare column name. An open scope — a table function, a
// SELECT *, a table the catalog does not have — answers false for everything,
// because nothing there is certain and the caller's fallback is the behaviour
// that existed before it asked (#739).
func (s *colScope) providesBareColumn(name string) bool {
	if s == nil || s.open {
		return false
	}
	return s.cols[strings.ToLower(name)]
}

func newColScope() *colScope {
	return &colScope{cols: map[string]bool{}, quals: map[string]map[string]bool{}, srcCount: map[string]int{},
		colTypes: map[string]parquet.TypeID{}, qualColTypes: map[string]map[string]parquet.TypeID{},
		rowFields: map[string][]parquet.Column{}, exact: map[string]bool{}}
}

func (s *colScope) addColumn(col string) {
	s.cols[strings.ToLower(col)] = true
	s.exact[col] = true
}

// addOutputColumn adds a SELECT output alias. An output name resolves before
// input columns in ORDER BY / GROUP BY / HAVING, so it also clears any input
// ambiguity the same name carries.
func (s *colScope) addOutputColumn(col string) {
	c := strings.ToLower(col)
	s.cols[c] = true
	s.exact[col] = true
	if s.srcCount[c] > 1 {
		s.srcCount[c] = 1
	}
}

// addQualified registers one column of one FROM source. Each call site invokes
// it once per column per source, so it doubles as the ambiguity census: a bare
// name two sources provide reaches srcCount 2.
func (s *colScope) addQualified(qual, col string) {
	c := strings.ToLower(col)
	s.cols[c] = true
	s.exact[col] = true
	s.srcCount[c]++
	if qual == "" {
		return
	}
	q := strings.ToLower(qual)
	if s.quals[q] == nil {
		s.quals[q] = map[string]bool{}
	}
	s.quals[q][c] = true
}

// addQualifiedTyped is addQualified for a source whose column TYPES are known
// — a base table, the only source this binder reads a catalog schema for.
// Everything else keeps calling addQualified and contributes no type, which is
// what keeps the literal refusal from firing on a column it cannot prove.
func (s *colScope) addQualifiedTyped(qual, col string, typ parquet.TypeID) {
	s.addQualified(qual, col)
	c := strings.ToLower(col)
	if prev, seen := s.colTypes[c]; !seen {
		s.colTypes[c] = typ
	} else if prev != typ {
		// Two sources, two types: the bare name is not provably any one type.
		// (resolveRef refuses it as ambiguous first, so this is belt and
		// braces — but the refusal must never be the thing that decides.)
		s.colTypes[c] = typeAmbiguous
	}
	if qual == "" {
		return
	}
	q := strings.ToLower(qual)
	if s.qualColTypes[q] == nil {
		s.qualColTypes[q] = map[string]parquet.TypeID{}
	}
	s.qualColTypes[q][c] = typ
}

// provableColType reports the declared TypeID ref PROVABLY names, and whether
// it could prove one. Every uncertain case answers (_, false): an open scope,
// a name no base table registered, an ambiguous bare name, or a qualifier this
// scope does not know.
func (s *colScope) provableColType(ref *plansql.ColRef) (parquet.TypeID, bool) {
	if s == nil || s.open {
		return 0, false
	}
	col := strings.ToLower(ref.Column)
	if ref.Table != "" {
		cols, ok := s.qualColTypes[strings.ToLower(ref.Table)]
		if !ok {
			return 0, false
		}
		t, ok := cols[col]
		return t, ok
	}
	if s.srcCount[col] > 1 {
		return 0, false
	}
	t, ok := s.colTypes[col]
	if !ok || t == typeAmbiguous {
		return 0, false
	}
	return t, true
}

// merge folds an OUTER (or sibling) scope into s for resolution only: names
// and qualifiers become visible, but srcCount is untouched — outer columns
// never make an inner reference ambiguous.
func (s *colScope) merge(o *colScope) {
	if o == nil {
		return
	}
	if o.open {
		s.open = true
	}
	for c := range o.cols {
		s.cols[c] = true
	}
	for c := range o.exact {
		s.exact[c] = true
	}
	for q, cs := range o.quals {
		if s.quals[q] == nil {
			s.quals[q] = map[string]bool{}
		}
		for c := range cs {
			s.quals[q][c] = true
		}
	}
	for c, typ := range o.colTypes {
		if prev, seen := s.colTypes[c]; !seen {
			s.colTypes[c] = typ
		} else if prev != typ {
			s.colTypes[c] = typeAmbiguous
		}
	}
	for q, cs := range o.qualColTypes {
		if s.qualColTypes[q] == nil {
			s.qualColTypes[q] = map[string]parquet.TypeID{}
		}
		for c, typ := range cs {
			s.qualColTypes[q][c] = typ
		}
	}
	for c, fields := range o.rowFields {
		if prev, seen := s.rowFields[c]; seen && !sameRowFields(prev, fields) {
			// Two sources declare the same ROW column with different shapes:
			// the field refusal cannot be certain, so drop it.
			delete(s.rowFields, c)
			continue
		}
		s.rowFields[c] = fields
	}
}

func (s *colScope) clone() *colScope {
	c := newColScope()
	c.merge(s)
	for col, n := range s.srcCount {
		c.srcCount[col] = n
	}
	return c
}

// resolveRef resolves ref against this scope, returning nil when it resolves
// (or when the scope is too uncertain to judge) and a SQLSTATE-carrying error
// for the three refusals PostgreSQL makes at analysis time: an unknown column
// (42703), a qualifier no FROM entry provides (42P01, #380), and a bare name
// two sources both provide (42702, #367). Every uncertain case resolves —
// a false positive breaks a working query; a false negative merely lets a
// typo through to the existing runtime check.
// refuseDelimitedMiss is the byte-exactness a DELIMITED identifier is owed.
//
// Every other map on this scope is keyed on the FOLDED name, which is the
// right key for an unquoted reference because the lexer folded it already
// (#731). A reference that still carries an ASCII upper-case letter can only
// have been written between double quotes, and PostgreSQL matches such a name
// byte for byte: over a column `g`, `SELECT "G"` is 42703 there, and answering
// it with `g`'s values — or, as this engine did, with a column of NULLs — is a
// silent wrong answer either way.
//
// It fires ONLY where the scope KNOWS the spelling, which is a BASE TABLE's
// columns — the ones whose declared type this binder carries, so colTypes is
// the test. Every other name here has passed through a planner pass that may
// have lowercased it before registering (agg_output_projection emits
// `strings.ToLower(alias)`), and refusing on a spelling the scope no longer
// has would break `SELECT id AS "Kk" … ORDER BY "Kk"`, which works.
// `SELECT "WatchID"` over a base table publishing `WatchID` finds those bytes
// in exact and passes.
func (s *colScope) refuseDelimitedMiss(ref *plansql.ColRef) error {
	if plansql.FoldIdent(ref.Column) == ref.Column || s.exact[ref.Column] {
		return nil
	}
	if _, fromBaseTable := s.colTypes[strings.ToLower(ref.Column)]; !fromBaseTable {
		return nil
	}
	return sqlerr.New("42703", "column %q does not exist", ref.Column)
}

func (s *colScope) resolveRef(ref *plansql.ColRef) error {
	if s == nil || s.open {
		return nil
	}
	col := strings.ToLower(ref.Column)
	if ref.Table != "" {
		q := strings.ToLower(ref.Table)
		if cols, ok := s.quals[q]; ok {
			// Qualifier is a known table/alias: a miss iff the column is absent.
			if cols[col] {
				return s.refuseDelimitedMiss(ref)
			}
			return s.unknownColumn(ref)
		}
		if s.cols[q] {
			// Qualifier is itself a column → dotted ROW/struct field access
			// (e.g. attrs.score). When the qualifier is a ROW column whose
			// fields are known (a base table), the FIELD must exist too:
			// a path naming no field resolved to NULL at runtime, silently
			// wrong the way an unknown column would have been before #147
			// (#604). A qualifier whose fields are not known — a non-ROW
			// column, or one from a derived table / CTE — keeps the runtime
			// fieldIdx == -1 check as its defence.
			if fields, known := s.rowFields[q]; known {
				parent := parquet.Column{Type: parquet.TypeRow, Fields: fields}
				if _, found := parent.Field(ref.Column); !found {
					return s.unknownField(ref, fields)
				}
			}
			return nil
		}
		if strings.Contains(q, ".") {
			// Multi-part qualifier (catalog.schema.table) — not modeled here.
			return nil
		}
		// Every FROM source either registers its qualifier or opens the scope,
		// and outer aliases arrive via merge — so an unmatched qualifier in a
		// closed scope provably names no FROM entry. Answering such a query
		// with rows was #380's silent-empty-result defect.
		return sqlerr.New("42P01", "missing FROM-clause entry for table %q", ref.Table)
	}
	if !s.cols[col] {
		return s.unknownColumn(ref)
	}
	if err := s.refuseDelimitedMiss(ref); err != nil {
		return err
	}
	if s.srcCount[col] > 1 {
		// Two of this block's sources both provide the name. Resolving it
		// silently would pick a side the client never learns about.
		return sqlerr.New("42702", "column reference %q is ambiguous", ref.Column)
	}
	return nil
}

// unknownColumn builds the 42703 undefined_column error for ref.
func (s *colScope) unknownColumn(ref *plansql.ColRef) error {
	if avail := s.available(); len(avail) > 0 {
		return sqlerr.New("42703", "unknown column %q (available: %s)", ref.String(), strings.Join(avail, ", "))
	}
	return sqlerr.New("42703", "unknown column %q", ref.String())
}

// unknownField builds the 42703 undefined_column error for a dotted field
// path (rw.field) whose FIELD names no field of the ROW column rw. The
// SQLSTATE and message mirror what PostgreSQL raises for `(rw).field`:
// "could not identify column ... in record data type", 42703 (#604).
func (s *colScope) unknownField(ref *plansql.ColRef, fields []parquet.Column) error {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name)
	}
	sort.Strings(names)
	if len(names) > 8 {
		names = names[:8]
	}
	if len(names) > 0 {
		return sqlerr.New("42703", "could not identify column %q in record data type %q (fields: %s)",
			ref.Column, ref.Table, strings.Join(names, ", "))
	}
	return sqlerr.New("42703", "could not identify column %q in record data type %q", ref.Column, ref.Table)
}

// available returns up to a handful of sorted column names for an error message.
func (s *colScope) available() []string {
	names := make([]string, 0, len(s.cols))
	for c := range s.cols {
		names = append(names, c)
	}
	sort.Strings(names)
	if len(names) > 8 {
		names = names[:8]
	}
	return names
}

type cteEntry struct {
	cols []string
	open bool
}

type binder struct {
	src  tableColumnSource
	ctes map[string]cteEntry
}

func (b *binder) validateBlock(ctx context.Context, info *plansql.SelectInfo, outer *colScope) error {
	if info == nil {
		return nil
	}

	// Register this block's CTEs first so FROM sources and later CTEs can
	// reference them. CTEs accumulate on the binder (additive scoping is only
	// ever more lenient, never a source of false positives).
	for i := range info.CTEs {
		if err := b.registerCTE(ctx, info.CTEs[i]); err != nil {
			return err
		}
	}

	// The names this block MINTS: an explicit `AS <alias>`, and nothing else.
	//
	// A bare `SELECT __win_0 FROM oldtab` is not minting anything — it is
	// READING a stored column, and refusing it makes a table that
	// `SELECT *` returns unreachable by name, which is the trap the
	// stored-column refusal was. `SELECT x AS __win_0` IS a mint: the user is
	// creating a name in the planner's namespace, and the projection that
	// resolves it cannot tell it from the planner's own slot
	// (reserved_slots.go).
	for i := range info.Columns {
		if a := info.Columns[i].Alias; a != "" {
			if err := refuseReservedSlotName(a, "output column alias"); err != nil {
				return err
			}
		}
	}

	// UNION / INTERSECT / EXCEPT: each branch is its own block.
	if info.Union != nil {
		if err := b.validateBlock(ctx, info.Union.Left, outer); err != nil {
			return err
		}
		return b.validateBlock(ctx, info.Union.Right, outer)
	}

	// Build the FROM scope from base tables, joins, derived tables and CTE refs.
	from := newColScope()
	for i := range info.Tables {
		if err := b.resolveSource(ctx, info.Tables[i], nil, from); err != nil {
			return err
		}
	}
	for i := range info.Joins {
		ref := joinRightRef(info.Joins[i])
		var lateralOuter *colScope
		if info.Joins[i].Lateral {
			// A LATERAL right side may reference columns from sources to its
			// left (and any outer scope).
			lateralOuter = from.clone()
			lateralOuter.merge(outer)
		}
		if err := b.resolveSource(ctx, ref, lateralOuter, from); err != nil {
			return err
		}
	}

	// Resolution scope for WHERE and SELECT: FROM sources plus any outer scope
	// (correlated subqueries). Output aliases are NOT visible here — a SELECT
	// item cannot reference its own output, and WHERE cannot see aliases.
	resolve := from.clone()
	resolve.merge(outer)

	// GROUP BY / HAVING / ORDER BY / QUALIFY may additionally reference SELECT
	// output aliases.
	outNames, starOut := blockOutputs(info)
	withOut := resolve.clone()
	if starOut {
		withOut.open = true
	}
	for _, n := range outNames {
		withOut.addOutputColumn(n)
	}

	// WHERE
	if err := b.checkExpr(info.WhereExpr, resolve); err != nil {
		return err
	}
	// JOIN conditions reference columns from the joined sources (all already in
	// `resolve`). USING/NATURAL/CROSS joins have no CondExpr → skipped.
	for i := range info.Joins {
		if err := b.checkExpr(info.Joins[i].CondExpr, resolve); err != nil {
			return err
		}
	}
	// SELECT expressions (skip stars and window functions — window outputs and
	// star expansion add no enumerable refs the binder can reason about safely).
	for i := range info.Columns {
		col := info.Columns[i]
		if col.Star || col.IsWindow {
			continue
		}
		if err := b.checkExpr(col.ASTExpr, resolve); err != nil {
			return err
		}
		if err := b.checkExpr(col.AggArgExpr, resolve); err != nil {
			return err
		}
	}
	// PostgreSQL's precedence for a bare GROUP BY name: an INPUT COLUMN wins
	// over a SELECT alias. The parser substituted the alias's expression
	// unconditionally — it has no schema — and this is the layer that does,
	// so the substitution is undone for every name one of this block's own
	// sources really provides (#739).
	//
	// BEFORE checkUngrouped, deliberately: the grouping rule is asked of the
	// terms the query actually groups by, and reverting afterwards would
	// check one set of keys and execute another. `SELECT c_i32 AS g,
	// COUNT(*) FROM t GROUP BY g` becomes GROUP BY the INPUT g here, which
	// leaves c_i32 ungrouped, which is the 42803 PostgreSQL raises for it.
	//
	// `from`, not `resolve`: an OUTER scope's columns are not this block's
	// input, and a correlated name that happens to match an alias must not
	// pull the group key out from under it. An OPEN scope answers false for
	// every name — nothing is certain there — so the substitution stands and
	// the answer is the pre-#739 one, which is the binder's own stance that
	// a false positive breaks a working query.
	plansql.RevertGroupByAliasesShadowedByInput(info, from.providesBareColumn)
	// A bare column beside an aggregate with no GROUP BY has no defined
	// answer — which n_name should the single aggregate row carry?
	if err := checkUngrouped(info, from); err != nil {
		return err
	}
	// GROUP BY expressions
	for _, gb := range info.GroupByExprs {
		if err := b.checkExpr(gb, withOut); err != nil {
			return err
		}
	}
	// HAVING / QUALIFY
	if err := b.checkExpr(info.HavingExpr, withOut); err != nil {
		return err
	}
	if err := b.checkExpr(info.QualifyExpr, withOut); err != nil {
		return err
	}
	// ORDER BY — items are raw expression strings; parse and check what parses.
	for _, ob := range info.OrderBy {
		expr, err := plansql.ParseExpression(ob.Column)
		if err != nil {
			continue
		}
		if err := b.checkExpr(expr, withOut); err != nil {
			return err
		}
	}

	// Recurse into subqueries embedded in this block's expressions. They run in
	// their own scope but can see this block's columns (correlation), so pass
	// `resolve` as their outer scope.
	for _, sql := range b.blockSubqueries(info) {
		sub := parseSelect(sql)
		if sub == nil {
			continue
		}
		if err := b.validateBlock(ctx, sub, resolve); err != nil {
			return err
		}
	}

	return nil
}

// checkExpr errors on the first column reference the scope refuses.
func (b *binder) checkExpr(expr plansql.Node, scope *colScope) error {
	if expr == nil || scope == nil || scope.open {
		return nil
	}
	var refs []*plansql.ColRef
	walkExpr(expr, &refs, nil)
	for _, r := range refs {
		if pgSystemColumns[strings.ToLower(r.Column)] {
			continue
		}
		if err := scope.resolveRef(r); err != nil {
			return err
		}
	}
	// Names first, then the literals they are compared against: a reference
	// that resolves to nothing has no declared type to refuse a literal
	// against, and reporting the name is the more useful of the two errors.
	return checkLiteralTypes(expr, scope)
}

// resolveSource resolves one FROM source into `into`. lateralOuter is the scope a
// LATERAL derived table may reference (nil otherwise). It returns an error only
// when a confirmed miss is found while validating a derived table's internals.
func (b *binder) resolveSource(ctx context.Context, tr plansql.TableRef, lateralOuter *colScope, into *colScope) error {
	qual := tr.Alias
	if qual == "" {
		qual = tr.Name
	}

	// Table function (read_json, read_csv, unnest, ...) → open schema.
	if tr.IsFunction {
		into.open = true
		return nil
	}

	// Derived table: FROM (SELECT ...) alias.
	if strings.HasPrefix(tr.Name, "(") {
		inner := parseSelect(strings.TrimSuffix(strings.TrimPrefix(tr.Name, "("), ")"))
		if inner == nil {
			into.open = true
			return nil
		}
		// Validate the derived block's internals. Non-LATERAL derived tables
		// cannot see the enclosing query; LATERAL ones can.
		if err := b.validateBlock(ctx, inner, lateralOuter); err != nil {
			return err
		}
		names, star := blockOutputs(inner)
		if star {
			into.open = true
			return nil
		}
		for _, n := range names {
			into.addQualified(qual, n)
		}
		return nil
	}

	// CTE reference.
	if e, ok := b.ctes[strings.ToLower(tr.Name)]; ok {
		if e.open {
			into.open = true
			return nil
		}
		for _, n := range e.cols {
			into.addQualified(qual, n)
		}
		return nil
	}

	// Base table. Use the original-case name (matching AnnotateScanColumns).
	// A confirmed miss — the catalog was reachable and says the table does not
	// exist — is 42P01: nothing downstream can answer this query, and before
	// this check nothing refused it either, so a typo'd table read as "no
	// matching rows" (#367). A transport failure keeps the old open-scope
	// stance: the table's existence is unknown, and a false rejection breaks
	// a working query.
	meta, err := b.src.GetTable(ctx, tr.Name)
	if err != nil {
		if errors.Is(err, catalog.ErrTableNotFound) {
			return sqlerr.New("42P01", "relation %q does not exist", tr.Name)
		}
		into.open = true
		return nil
	}
	if meta == nil {
		into.open = true
		return nil
	}
	for _, c := range meta.Schema.Columns {
		// A STORED column is NEVER refused here. Reading a table is not
		// minting a name: the column already exists, some binary wrote it, and
		// refusing it makes the table unreadable by every query including the
		// `SELECT *` that would show the user what is in it. The reservation
		// is enforced at the DDL and ingest DOORS instead (wadjet.CreateTable,
		// CREATE TABLE, NewIngester), where the name is being CREATED, and the
		// planner renumbers its own slot past a stored collision
		// (renameCollidingSlots) so the two can coexist in one query.
		into.addQualifiedTyped(qual, c.Name, c.Type)
		into.addRowColumn(c)
	}
	return nil
}

// addRowColumn records a ROW column's declared FIELDS so a field path against
// it can be checked at plan time (#604). Only a ROW with a declared field list
// is recorded; anything else leaves field existence to the runtime resolver.
// A second source declaring the same bare name with a DIFFERENT shape removes
// the entry, so the refusal is never made on an uncertain shape.
func (s *colScope) addRowColumn(c parquet.Column) {
	if c.Type != parquet.TypeRow || len(c.Fields) == 0 {
		return
	}
	name := strings.ToLower(c.Name)
	if prev, seen := s.rowFields[name]; seen && !sameRowFields(prev, c.Fields) {
		delete(s.rowFields, name)
		return
	}
	s.rowFields[name] = c.Fields
}

// registerCTE parses, validates and records a CTE's output columns so later
// references resolve. Any uncertainty registers it as open.
func (b *binder) registerCTE(ctx context.Context, cte plansql.CTEDef) error {
	name := strings.ToLower(cte.Name)
	if _, exists := b.ctes[name]; exists {
		return nil
	}
	// Recursive CTEs reference themselves; register open and skip body
	// validation to avoid a false miss on the self-reference.
	if cte.Recursive {
		b.ctes[name] = cteEntry{open: true}
		return nil
	}
	body := parseSelect(cte.SQL)
	if body == nil {
		b.ctes[name] = cteEntry{open: true}
		return nil
	}
	// Register before validating the body so a body parse/validation issue can't
	// leave the name unregistered.
	if len(cte.Columns) > 0 {
		if err := refuseReservedSlotNames(cte.Columns, "CTE column"); err != nil {
			return err
		}
		b.ctes[name] = cteEntry{cols: cte.Columns}
	} else if names, star := blockOutputs(body); star {
		b.ctes[name] = cteEntry{open: true}
	} else {
		b.ctes[name] = cteEntry{cols: names}
	}
	return b.validateBlock(ctx, body, nil)
}

// checkUngrouped enforces PostgreSQL's grouping rule: in a GROUPED query,
// every SELECT / HAVING / ORDER BY expression must be built from the grouped
// expressions, aggregate calls and constants — a bare reference to anything
// else has no defined value for the group and is 42803 grouping_error.
//
// A query is GROUPED when it has a GROUP BY, a GROUPING SETS, an aggregate in
// the SELECT list, or a HAVING clause (the last two collapse the table into
// one group, which is #367's `SELECT n_name, COUNT(*) FROM nation`).
//
// PostgreSQL relaxes the rule for a column functionally dependent on a grouped
// PRIMARY KEY. Wadjet has no primary keys, so the relaxation has nothing to
// apply to and every such reference is refused.
//
// The check keeps the binder's stance that a false positive breaks a working
// query while a false negative merely lets one through: it is skipped whenever
// a source is unenumerable, and a reference that does not certainly resolve to
// one of this block's OWN sources is skipped too — it may be a correlated
// outer reference (constant per group) or a niladic the parser reads as a
// column.
func checkUngrouped(info *plansql.SelectInfo, from *colScope) error {
	if from == nil || from.open {
		return nil
	}
	hasAgg := false
	for i := range info.Columns {
		if info.Columns[i].IsAgg && !info.Columns[i].IsWindow {
			hasAgg = true
			break
		}
	}
	// An aggregate in ORDER BY makes the query grouped too, which is what
	// PostgreSQL's parseCheckAggregates does — it reads the sort clause
	// alongside the select list and the HAVING. Without this,
	// `SELECT id FROM t ORDER BY MAX(id)` was not judged at all and returned
	// every row, where PostgreSQL raises the 42803 below (#811).
	for _, ob := range info.OrderBy {
		if ob.Expr != nil && len(plansql.FindAllAggregates(ob.Expr)) > 0 {
			hasAgg = true
			break
		}
	}
	grouped := len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 || hasAgg || info.HavingExpr != nil
	if !grouped {
		return nil
	}

	g := &groupCheck{from: from, keys: map[string]bool{}, idents: map[string]bool{}, bare: map[string]bool{},
		// One source in the FROM: a qualifier then names that source and
		// nothing else, so it is spelling (#738).
		unqualify: len(from.quals) == 1}
	g.addGroupTerms(info)

	// SELECT list. An output alias is NOT visible here — a select item cannot
	// reference another item's alias — so this arm runs against the group
	// terms alone.
	for i := range info.Columns {
		col := info.Columns[i]
		if col.Star || col.IsWindow || col.ASTExpr == nil {
			continue
		}
		if err := g.check(col.ASTExpr); err != nil {
			return err
		}
	}

	// HAVING and ORDER BY additionally see the SELECT list's output names.
	// Whatever each of those names stands for was just checked on its own, so
	// admitting the name here cannot admit an expression the SELECT arm would
	// have refused.
	for i := range info.Columns {
		if a := strings.ToLower(strings.TrimSpace(info.Columns[i].Alias)); a != "" {
			g.bare[a] = true
			g.keys[a] = true
		}
	}
	if err := g.check(info.HavingExpr); err != nil {
		return err
	}
	for _, ob := range info.OrderBy {
		expr, err := plansql.ParseExpression(ob.Column)
		if err != nil {
			continue
		}
		if err := g.check(expr); err != nil {
			return err
		}
	}
	return nil
}

// groupCheck holds the grouped expressions of one query block.
//
// keys holds the rendered text of every grouped expression, so a SELECT item
// that repeats a grouping expression verbatim (`GROUP BY substr(c_phone,1,2)`)
// matches as a whole and its columns are never examined.
//
// idents holds the RESOLVED identity — base source plus column — of every
// grouped term that is a plain column reference, so `GROUP BY t3.c1` licenses
// exactly t3.c1 (however it is spelled, qualified or bare) and NOT some other
// table's column that merely shares the name `c1`. Matching on the bare name
// alone let a JOINed table's ungrouped `t1.c1` pass whenever any grouped term
// happened to be named `c1` — silently wrong SELECT values, and a HAVING that
// excluded every group (#620).
//
// bare is the lenient fallback: the bare names of grouped terms whose base
// source could NOT be resolved uniquely (an unknown or ambiguous qualifier),
// plus SELECT output aliases, which have no base source. A reference the check
// cannot resolve to a source is never judged, so the fallback only ever admits
// more — it cannot turn a working query into a false rejection.
type groupCheck struct {
	from   *colScope
	keys   map[string]bool
	idents map[string]bool
	bare   map[string]bool
	// unqualify erases TABLE QUALIFIERS from every expression identity this
	// check renders, so `SELECT typemx.g + 1 ... GROUP BY g + 1` matches
	// (#738). It is set only when the block's FROM provides exactly ONE
	// source, because that is the scope in which `t.x` and `x` are the same
	// expression; over a join they are not, and `GROUP BY zzj.d92` licensing
	// `SELECT zzp.d92` would be a wrong answer, not a missed match.
	//
	// PostgreSQL erases the qualifier at every arity, because its comparison
	// is over RESOLVED targetlist entries rather than over text. Matching that
	// needs the resolution, not a wider text rule — which is why the bound is
	// here and why the join case keeps its 42803, gated below.
	unqualify bool
}

// identKey renders a resolved (source, column) identity. Both halves are
// already lower-cased by the callers; the NUL separator cannot occur in either.
func identKey(table, col string) string {
	return table + "\x00" + col
}

// resolveOwnTable resolves ref to the single base source (qualifier/alias) of
// this block that provides it. ok is false whenever that is not certain — a
// system column, an unknown qualifier, a bare name no source or more than one
// source provides — and an uncertain reference is never judged by the grouping
// check, matching the binder's stance that a false positive breaks a working
// query while a false negative merely lets one through.
func (g *groupCheck) resolveOwnTable(ref *plansql.ColRef) (string, bool) {
	c := strings.ToLower(ref.Column)
	if pgSystemColumns[c] {
		return "", false
	}
	if ref.Table != "" {
		q := strings.ToLower(ref.Table)
		if cols, ok := g.from.quals[q]; ok && cols[c] {
			return q, true
		}
		return "", false
	}
	// A bare name must be provided by exactly one of this block's sources.
	if g.from.srcCount[c] != 1 {
		return "", false
	}
	for q, cols := range g.from.quals {
		if cols[c] {
			return q, true
		}
	}
	return "", false
}

// addGroupTerms records this block's grouped expressions. A term written as a
// positional reference (`GROUP BY 1`) or as a SELECT output alias resolves to
// the select item it names, and that item's own expression is grouped too.
func (g *groupCheck) addGroupTerms(info *plansql.SelectInfo) {
	// An empty key would match every expression whose String() is empty and
	// turn the check off silently, so nothing empty is ever recorded.
	add := func(n plansql.Node) {
		if n == nil {
			return
		}
		if k := groupTermKey(n); k != "" {
			g.keys[k] = true
		}
		if ref, ok := unparen(n).(*plansql.ColRef); ok {
			if c := strings.ToLower(ref.Column); c != "" {
				if tbl, resolved := g.resolveOwnTable(ref); resolved {
					g.idents[identKey(tbl, c)] = true
				} else {
					g.bare[c] = true
				}
			}
		}
	}
	// The grouped terms are read from their PARSED forms and from nothing
	// else. A loop that also recorded each term's recorded TEXT, re-parsed as
	// an expression, used to stand here, and it is what stopped
	// `GROUP BY "g + 1"` from refusing `SELECT g + 1`: since #725 that text
	// is the key's published NAME with the delimiters stripped, so re-parsing
	// it read a COLUMN as ARITHMETIC and marked `g` grouped. PostgreSQL
	// answers 42803; we answered 60 NULL-keyed rows on one path and 3 rows on
	// the other. `"g plus 1"` did the same with no operator in it — it parsed
	// to just `g` and marked THAT grouped — which is why the repair is to
	// stop reading text as structure, not to special-case operators.
	//
	// The non-parallel case (an ordinal or an alias, where GroupByExprs is
	// not populated) is answered by the fallback loop below, which parses
	// terms that have no AST of their own and is guarded on the lengths
	// differing.
	for i := range info.GroupByExprs {
		gbExpr := info.GroupByExprs[i]
		if gbExpr == nil {
			continue
		}
		add(gbExpr)
		// A bare term the INPUT provides binds to the input column, not to
		// the SELECT alias that happens to share its name — PostgreSQL's
		// precedence, applied to the group terms just above by
		// plansql.RevertGroupByAliasesShadowedByInput. Admitting the
		// same-named select item's expression here would undo it for the
		// grouping check alone: `SELECT c_i32 AS g … GROUP BY g` would group
		// by `g` and still count `c_i32` as grouped, which is the 42803
		// PostgreSQL raises going missing (#739).
		if groupTermBindsToInput(gbExpr, g.from) {
			continue
		}
		if item := selectItemForGroupTerm(info, gbExpr); item != nil {
			add(item.ASTExpr)
			if a := strings.ToLower(strings.TrimSpace(item.Alias)); a != "" {
				g.keys[a] = true
				g.bare[a] = true
			}
		}
	}
	// GroupByExprs is documented as parallel to GroupBy but is not always
	// populated; fall back to parsing the raw terms so an ordinal or alias
	// still resolves.
	if len(info.GroupByExprs) != len(info.GroupBy) {
		for _, gb := range info.GroupBy {
			parsed, err := plansql.ParseExpression(gb)
			if err != nil {
				continue
			}
			add(parsed)
			if item := selectItemForGroupTerm(info, parsed); item != nil {
				add(item.ASTExpr)
				if a := strings.ToLower(strings.TrimSpace(item.Alias)); a != "" {
					g.keys[a] = true
					g.bare[a] = true
				}
			}
		}
	}
}

// groupTermBindsToInput reports whether a GROUP BY term is a bare column name
// one of this block's own sources certainly provides — the case where
// PostgreSQL binds it to the INPUT COLUMN rather than to a SELECT alias of the
// same name (#739). A qualified term always names a source column and needs no
// question asked.
func groupTermBindsToInput(term plansql.Node, from *colScope) bool {
	ref, ok := unparen(term).(*plansql.ColRef)
	if !ok {
		return false
	}
	if ref.Table != "" {
		return true
	}
	return from.providesBareColumn(ref.Column)
}

// selectItemForGroupTerm resolves a GROUP BY term that names a select item
// rather than an input expression: a 1-based ordinal, or an output alias.
func selectItemForGroupTerm(info *plansql.SelectInfo, term plansql.Node) *plansql.SelectColumn {
	switch n := unparen(term).(type) {
	case *plansql.Lit:
		idx, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(n.Value)))
		if err != nil || idx < 1 || idx > len(info.Columns) {
			return nil
		}
		return &info.Columns[idx-1]
	case *plansql.ColRef:
		if n.Table != "" {
			return nil
		}
		name := strings.ToLower(n.Column)
		for i := range info.Columns {
			if strings.ToLower(strings.TrimSpace(info.Columns[i].Alias)) == name {
				return &info.Columns[i]
			}
		}
	}
	return nil
}

// check walks one expression and returns the 42803 for the first reference
// that is neither grouped nor inside an aggregate.
func (g *groupCheck) check(node plansql.Node) error {
	if node == nil {
		return nil
	}
	if k := groupTermKey(node); k != "" && g.keys[k] {
		return nil
	}
	// The same term with its table QUALIFIER erased, in a single-relation
	// block: `SELECT typemx.g + 1 ... GROUP BY g + 1` is one expression
	// written twice, and PostgreSQL answers it (#738).
	//
	// The erasure is on the TERM alone and deliberately NOT on the KEY. That
	// asymmetry is what keeps the MIRROR spelling — a qualified KEY and a bare
	// term — refusing: PostgreSQL answers that one too, and answering it here
	// needs the aggregate to evaluate `typemx.g + 1` over a batch whose column
	// is `g`, which it cannot; the projection above then read a column that
	// does not exist and every group's key came back NULL. A loud 42803 is the
	// right disposition for a shape this engine cannot compute, and turning it
	// into a plausible NULL would be the regression protocol method 8 names.
	if g.unqualify {
		if k := plansql.ExprIdentityUnqualified(node); k != "" && g.keys[k] {
			return nil
		}
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		tbl, ok := g.resolveOwnTable(n)
		if !ok {
			// Not certainly one of this block's own columns (an outer
			// correlated reference, a niladic the parser reads as a column, or
			// an ambiguous bare name another arm refuses as 42702) — not
			// judged here.
			return nil
		}
		c := strings.ToLower(n.Column)
		if g.idents[identKey(tbl, c)] || g.bare[c] {
			return nil
		}
		return sqlerr.New("42803", "column %q must appear in the GROUP BY clause or be used in an aggregate function", n.String())
	case *plansql.FuncCallNode:
		// An aggregate is the licence to read ungrouped columns; its
		// arguments are consumed, not published.
		if plansql.IsAggregate(n.Name) {
			return nil
		}
		// A name nothing implements is settled BEFORE grouping coverage is.
		// PostgreSQL resolves the function in parse analysis
		// (transformFuncCall → 42883) and runs parseCheckAggregates only
		// afterwards, so `SELECT no_such_agg(price) FROM t GROUP BY name`
		// is `function no_such_agg(double precision) does not exist` there,
		// not a complaint about `price`. Reporting 42803 on the ARGUMENT of
		// a call that will not resolve names the wrong problem, and it
		// masked the sharper wording for an unimplemented aggregate
		// (`array_agg` under a GROUP BY) that this engine already had.
		//
		// The verdict is expr's, not a second name list, so the two cannot
		// drift and WADJET_STRICT_FUNCTIONS still disables both. Returning
		// it rather than skipping the node matters: a bare skip leaves
		// `… GROUP BY k ORDER BY no_such_agg(x)` to the logical builder's
		// own refusal, which carries no SQLSTATE.
		if err := expr.ResolveFuncName(n.Name); err != nil {
			return err
		}
	case *plansql.SubqueryNode, *plansql.ExistsNode, *plansql.WindowFuncNode:
		// Their own scope, validated on their own terms.
		return nil
	}
	for _, child := range exprOperands(node) {
		if err := g.check(child); err != nil {
			return err
		}
	}
	return nil
}

// groupTermKey renders an expression for comparison against the grouped
// terms. It is plansql.ExprIdentity: parentheses and identifier case are not
// part of an expression, so `(a+1)`, `((a) + 1)` and `A+1` are one key, while
// `a - 1 - 2` and `a - (1 - 2)` stay two.
//
// Stripping only the OUTER parentheses — which is what this did — left
// `SELECT ((g) + 1) … GROUP BY g + 1` rendering as `(g) + 1`, matching no
// grouped term, so the walk descended into it and reported 42803 on `g` for a
// query PostgreSQL answers (#723).
func groupTermKey(node plansql.Node) string {
	if node == nil {
		return ""
	}
	return plansql.ExprIdentity(node)
}

func unparen(node plansql.Node) plansql.Node {
	for {
		p, ok := node.(*plansql.ParenNode)
		if !ok || p.Inner == nil {
			return node
		}
		node = p.Inner
	}
}

// exprOperands returns a node's direct sub-expressions. It mirrors walkExpr's
// case list, and like walkExpr it does NOT descend into a subquery. A node type
// it does not know contributes no children, which makes the grouping check more
// permissive, never less.
func exprOperands(node plansql.Node) []plansql.Node {
	switch n := node.(type) {
	case *plansql.BinaryOp:
		return []plansql.Node{n.Left, n.Right}
	case *plansql.UnaryOp:
		return []plansql.Node{n.Inner}
	case *plansql.CmpExpr:
		return []plansql.Node{n.Left, n.Right}
	case *plansql.AndNode:
		return []plansql.Node{n.Left, n.Right}
	case *plansql.OrNode:
		return []plansql.Node{n.Left, n.Right}
	case *plansql.NotNode:
		return []plansql.Node{n.Inner}
	case *plansql.ParenNode:
		return []plansql.Node{n.Inner}
	case *plansql.FuncCallNode:
		return n.Args
	case *plansql.CastNode:
		return []plansql.Node{n.Inner}
	case *plansql.InExpr:
		return append([]plansql.Node{n.Left}, n.Values...)
	case *plansql.BetweenExpr:
		return []plansql.Node{n.Left, n.Low, n.High}
	case *plansql.LikeExpr:
		return []plansql.Node{n.Left, n.Pattern}
	case *plansql.IsExpr:
		return []plansql.Node{n.Left}
	case *plansql.CaseNode:
		out := []plansql.Node{n.Subject}
		for _, w := range n.Whens {
			out = append(out, w.Cond, w.Result)
		}
		return append(out, n.Else)
	case *plansql.ArrayLitNode:
		return n.Elements
	case *plansql.TupleNode:
		return n.Elements
	case *plansql.AnyAllExpr:
		return append([]plansql.Node{n.Left}, n.Values...)
	}
	return nil
}

// blockSubqueries collects the raw SQL of every subquery embedded in a block's
// expressions (WHERE, SELECT, HAVING, QUALIFY) for recursive validation.
func (b *binder) blockSubqueries(info *plansql.SelectInfo) []string {
	var subs []string
	walkExpr(info.WhereExpr, nil, &subs)
	walkExpr(info.HavingExpr, nil, &subs)
	walkExpr(info.QualifyExpr, nil, &subs)
	for i := range info.Columns {
		walkExpr(info.Columns[i].ASTExpr, nil, &subs)
		walkExpr(info.Columns[i].AggArgExpr, nil, &subs)
	}
	for i := range info.Joins {
		walkExpr(info.Joins[i].CondExpr, nil, &subs)
	}
	return subs
}

// blockOutputs returns a block's output column names and whether it has a star
// (which makes its output unenumerable → treat as open).
func blockOutputs(info *plansql.SelectInfo) ([]string, bool) {
	if info == nil {
		return nil, true
	}
	if info.Union != nil {
		return blockOutputs(info.Union.Left)
	}
	var names []string
	for i := range info.Columns {
		c := info.Columns[i]
		if c.Star {
			return nil, true
		}
		name := c.Alias
		if name == "" {
			name = c.ColumnRef
		}
		if name == "" {
			name = strings.TrimSpace(c.Expr)
		}
		if c.IsWindow {
			// The same choice the logical builder's projection makes, so the
			// namespace this enumerates is the one the query really produces.
			name = plansql.WindowOutputName(c)
		}
		if name != "" {
			names = append(names, strings.ToLower(name))
		}
	}
	return names, false
}

// joinRightRef extracts the right-hand table reference of a join.
func joinRightRef(j plansql.JoinInfo) plansql.TableRef {
	if j.RightTableRef != nil {
		return *j.RightTableRef
	}
	return plansql.TableRef{Name: j.RightTable, Alias: j.RightAlias}
}

// parseSelect parses a SQL string into a SelectInfo, returning nil on any error
// (the caller treats nil as "can't reason about this → open/skip").
func parseSelect(sql string) *plansql.SelectInfo {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil
	}
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil
	}
	return info
}

// walkExpr collects column references and/or embedded subquery SQL from an
// expression AST. It does NOT descend into subqueries (they form their own
// scope) — it records their SQL for separate validation. Any node type it does
// not recognize contributes nothing (safe: an un-walked ref is a false negative,
// never a false positive).
func walkExpr(node plansql.Node, refs *[]*plansql.ColRef, subs *[]string) {
	switch n := node.(type) {
	case nil:
		return
	case *plansql.ColRef:
		if refs != nil {
			*refs = append(*refs, n)
		}
	case *plansql.SubqueryNode:
		if subs != nil {
			*subs = append(*subs, n.SQL)
		}
	case *plansql.ExistsNode:
		if subs != nil {
			*subs = append(*subs, n.SQL)
		}
	case *plansql.BinaryOp:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.UnaryOp:
		walkExpr(n.Inner, refs, subs)
	case *plansql.CmpExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.AndNode:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.OrNode:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.NotNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.ParenNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			walkExpr(a, refs, subs)
		}
	case *plansql.CastNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.InExpr:
		walkExpr(n.Left, refs, subs)
		for _, v := range n.Values {
			walkExpr(v, refs, subs)
		}
	case *plansql.BetweenExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Low, refs, subs)
		walkExpr(n.High, refs, subs)
	case *plansql.LikeExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Pattern, refs, subs)
	case *plansql.IsExpr:
		walkExpr(n.Left, refs, subs)
	case *plansql.CaseNode:
		walkExpr(n.Subject, refs, subs)
		for _, w := range n.Whens {
			walkExpr(w.Cond, refs, subs)
			walkExpr(w.Result, refs, subs)
		}
		walkExpr(n.Else, refs, subs)
	case *plansql.ArrayLitNode:
		for _, e := range n.Elements {
			walkExpr(e, refs, subs)
		}
	case *plansql.TupleNode:
		for _, e := range n.Elements {
			walkExpr(e, refs, subs)
		}
	case *plansql.AnyAllExpr:
		walkExpr(n.Left, refs, subs)
		for _, v := range n.Values {
			walkExpr(v, refs, subs)
		}
	}
}

// pgSystemColumns are PostgreSQL's per-row system columns. Clients request
// them unconditionally — DataGrip opens a table with `SELECT t.*, CTID FROM
// public.customer t`, and rejecting the reference made double-clicking a table
// fail outright.
//
// They resolve to NULL rather than to a value this server invents. ctid is a
// physical row address, and this engine has none: rows live in immutable
// Parquet files that compaction rewrites, so any identifier synthesized here
// would be stable only until the next rewrite. Since UPDATE and DELETE are
// supported, a fabricated ctid is not merely useless but dangerous — a client
// re-issuing `DELETE ... WHERE ctid = '(0,5)'` could address a row other than
// the one the user saw. NULL keeps the read working and makes the write match
// nothing, which is the honest outcome for a row identity that does not exist.
var pgSystemColumns = map[string]bool{
	"ctid":     true,
	"xmin":     true,
	"xmax":     true,
	"cmin":     true,
	"cmax":     true,
	"tableoid": true,
}

// IsPGSystemColumn reports whether a name is one of those, for the DML doors.
// They do not go through this package's validation at all, and their own
// name-resolution step (#678) has to make the same allowance the query path
// makes — otherwise `DELETE ... WHERE ctid = '(0,1)'`, which PostgreSQL
// accepts and this engine deliberately answers by matching nothing, would
// become a 42703.
func IsPGSystemColumn(name string) bool {
	return pgSystemColumns[strings.ToLower(name)]
}
