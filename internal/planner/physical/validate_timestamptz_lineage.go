// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The timestamp family's declared kind, by lineage (#1680 line d).
//
// This engine has no timestamptz TYPE: a timestamptz value is held in the
// TIMESTAMP carrier as its UTC instant (every session runs at UTC; a SET
// TimeZone naming another zone is refused). PostgreSQL types an untyped
// quoted literal from the operand beside it, and the two timestamp types
// read it differently: beside a `timestamp` an offset in the text is
// DISCARDED, beside a `timestamptz` it is APPLIED (the instant wall −
// offset). So at a comparison — or any construct that resolves a quoted
// literal to the type of its neighbours — the planner has to know which of
// the two the neighbour is, and the carrier cannot say.
//
// The kind is carried the way interval lineage is (validate_interval_
// lineage.go, ADR-0052 §8): a block's output slot that a timestamptz
// producer fills (a TIMESTAMPTZ literal or cast, now() and its family,
// to_timestamp, date_trunc over one, a timestamp AT TIME ZONE) is declared
// timestamptz (binder.tzOut), one a plain-timestamp producer fills (a
// TIMESTAMP literal or cast, localtimestamp, make_timestamp) is declared
// timestamp (binder.tsOut), and both are forwarded to every scope that reads
// the block — a derived table, a CTE reference, a scalar subquery, a set
// operation, a join's output — by the same forwarding rule
// (colScope.mergeLineageSet). A TIMESTAMP slot in neither is a STORED column
// (or an expression over one): the catalog, the Parquet reader and the
// table-creation doors record no timestamptz declaration (no door accepts
// one: TIMESTAMPTZ is 42704 in every DDL), so its kind is unknown.
//
// The rule, at every consumer that resolves a quoted literal to a timestamp
// neighbour (zonedLiteralRule):
//  1. beside a timestamptz by lineage the literal is read as a timestamptz:
//     its text is rewritten in place to the UTC wall clock of the instant it
//     names, which the TIMESTAMP reading then reads as that instant;
//  2. beside a plain timestamp by lineage nothing changes (the offset is
//     discarded, PostgreSQL's timestamp input);
//  3. beside a timestamp of unknown declaration a literal that spells a
//     nonzero offset is refused (timestamp-zone-ambiguous): which reading
//     PostgreSQL applies depends on a declaration this engine does not have.
//
// A literal without an offset, or with a zero one, names the same value
// under both readings at UTC and is never touched.

// tsKind is a timestamp-family operand's declared kind.
type tsKind int

const (
	tsKindNone    tsKind = iota // not a timestamp, or not typed
	tsKindTz                    // timestamptz by lineage
	tsKindPlain                 // timestamp (without time zone) by lineage
	tsKindUnknown               // a TIMESTAMP of unknown declaration (stored)
)

// addTimestampLineage declares qual.name (and name) by the i-th position of
// a source block's output kinds.
func (s *colScope) addTimestampLineage(qual, name string, tz, ts []bool, i int) {
	add := func(m *map[string]bool) {
		if *m == nil {
			*m = map[string]bool{}
		}
		(*m)[strings.ToLower(name)] = true
		if qual != "" {
			(*m)[strings.ToLower(qual+"."+name)] = true
		}
	}
	switch {
	case i < len(tz) && tz[i]:
		add(&s.tzLineage)
	case i < len(ts) && ts[i]:
		add(&s.tsLineage)
	}
}

// mergeTimestampLineage adds o's timestamp-kind lineage for the names s does
// not resolve itself (colScope.mergeLineageSet).
func (s *colScope) mergeTimestampLineage(o *colScope) {
	if o == nil {
		return
	}
	if len(o.tzLineage) > 0 {
		if s.tzLineage == nil {
			s.tzLineage = map[string]bool{}
		}
		s.mergeLineageSet(s.tzLineage, o.tzLineage)
	}
	if len(o.tsLineage) > 0 {
		if s.tsLineage == nil {
			s.tsLineage = map[string]bool{}
		}
		s.mergeLineageSet(s.tsLineage, o.tsLineage)
	}
}

// lineageOf is a column reference's declared kind by lineage.
func (d ColDecls) timestampLineageOf(c *plansql.ColRef) tsKind {
	key := strings.ToLower(c.Column)
	if c.Table != "" {
		key = strings.ToLower(c.Table + "." + c.Column)
	}
	switch {
	case d.tzLineage[key]:
		return tsKindTz
	case d.tsLineage[key]:
		return tsKindPlain
	}
	return tsKindNone
}

// tsKinder decides an operand's declared kind in one scope.
type tsKinder struct {
	decls ColDecls
	// typeOf is the scope's structural typing: a TIMESTAMP operand no
	// producer or lineage declares is of unknown declaration.
	typeOf func(plansql.Node) (parquet.TypeID, bool)
	// sub is a scalar subquery body's kind, tsKindNone where unknown.
	sub func(sql string) tsKind
	// rewrites counts the literals rewritten (binder.tzRewrites), so a
	// clause the planners also read as TEXT is re-rendered (syncClauseText).
	rewrites *int
}

// newTsKinder is the binder's kinder over scope.
func (b *binder) newTsKinder(scope *colScope, typeOf func(plansql.Node) (parquet.TypeID, bool)) tsKinder {
	return tsKinder{decls: rowFieldScopeDecls(scope), typeOf: typeOf, rewrites: &b.tzRewrites, sub: func(sql string) tsKind {
		body := b.validatedBody(sql, scope)
		if body == nil {
			return tsKindNone
		}
		if tz := b.tzOut[body]; len(tz) == 1 && tz[0] {
			return tsKindTz
		}
		if ts := b.tsOut[body]; len(ts) == 1 && ts[0] {
			return tsKindPlain
		}
		return tsKindNone
	}}
}

// timestamptzProducers answer a timestamptz whatever their arguments
// (PostgreSQL's declared result).
var timestamptzProducers = map[string]bool{
	"now": true, "current_timestamp": true, "transaction_timestamp": true,
	"statement_timestamp": true, "clock_timestamp": true, "to_timestamp": true,
}

// timestampProducers answer a plain timestamp whatever their arguments.
var timestampProducers = map[string]bool{"localtimestamp": true, "make_timestamp": true}

// tsKindValueArg are the aggregate and window functions that answer
// their first argument's type.
var tsKindValueArg = map[string]bool{
	"min": true, "max": true, "first_value": true, "last_value": true,
	"lag": true, "lead": true, "nth_value": true, "any_value": true, "arbitrary": true,
}

// isTimestamp reports an operand the scope types TIMESTAMP.
func (k tsKinder) isTimestamp(n plansql.Node) bool {
	if k.typeOf == nil {
		return false
	}
	t, ok := k.typeOf(n)
	return ok && t == parquet.TypeTimestamp
}

// of is n's declared kind.
func (k tsKinder) of(n plansql.Node) tsKind {
	switch v := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		return tsKindNone
	case *plansql.CastNode:
		switch castTypeBase(v.TypeName) {
		case "timestamptz", "timestamp with time zone":
			return tsKindTz
		case "timestamp", "timestamp without time zone":
			return tsKindPlain
		}
		return tsKindNone
	case *plansql.ColRef:
		if kind := k.decls.timestampLineageOf(v); kind != tsKindNone {
			return kind
		}
	case *plansql.SubqueryNode:
		if !v.Array {
			if kind := k.sub(v.SQL); kind != tsKindNone {
				return kind
			}
		}
	case *plansql.BinaryOp:
		if v.Op == "+" || v.Op == "-" {
			if l := k.of(v.Left); l != tsKindNone {
				return l
			}
			if v.Op == "+" {
				if r := k.of(v.Right); r != tsKindNone {
					return r
				}
			}
		}
	case *plansql.CaseNode:
		arms := make([]plansql.Node, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		if kind := k.common(arms); kind != tsKindNone {
			return kind
		}
	case *plansql.FuncCallNode:
		name := strings.ToLower(strings.TrimSpace(v.Name))
		switch {
		case timestamptzProducers[name]:
			return tsKindTz
		case timestampProducers[name]:
			return tsKindPlain
		case name == "date_trunc" && len(v.Args) == 3:
			return tsKindTz
		case name == "date_trunc" && len(v.Args) == 2:
			// date_trunc over a DATE resolves to the timestamptz form.
			if kind := k.of(v.Args[1]); kind != tsKindNone {
				return kind
			}
			if k.typeOf == nil {
				break
			}
			if t, ok := k.typeOf(v.Args[1]); ok && t == parquet.TypeDate {
				return tsKindTz
			}
		case name == "timezone" && len(v.Args) == 2:
			// AT TIME ZONE turns a timestamp into a timestamptz and back.
			switch k.of(v.Args[1]) {
			case tsKindTz:
				return tsKindPlain
			case tsKindPlain:
				return tsKindTz
			}
		case tsKindValueArg[name] && len(v.Args) > 0:
			if kind := k.of(v.Args[0]); kind != tsKindNone {
				return kind
			}
		case name == "coalesce" || name == "ifnull" || name == "nullif" || name == "greatest" || name == "least":
			if kind := k.common(v.Args); kind != tsKindNone {
				return kind
			}
		}
	}
	if k.isTimestamp(n) {
		return tsKindUnknown
	}
	return tsKindNone
}

// common is the kind PostgreSQL's common-type resolution gives a set of
// arms: a timestamptz beside a timestamp is a timestamptz; an arm of
// unknown declaration leaves the set unknown; untyped literals and NULL take
// the others' type.
func (k tsKinder) common(arms []plansql.Node) tsKind {
	out := tsKindNone
	for _, a := range arms {
		if l, ok := plansql.Unparen(a).(*plansql.Lit); ok && (l.Kind == plansql.LitString || l.Kind == plansql.LitNull) {
			continue
		}
		switch k.of(a) {
		case tsKindTz:
			return tsKindTz
		case tsKindUnknown:
			out = tsKindUnknown
		case tsKindPlain:
			if out == tsKindNone {
				out = tsKindPlain
			}
		}
	}
	return out
}

// timestampKindBlock records a block's output kinds (binder.tzOut, tsOut).
func (b *binder) timestampKindBlock(info *plansql.SelectInfo, from, resolve *colScope) {
	k := b.newTsKinder(resolve, structuralTypeOf(rowFieldScopeDecls(resolve)))
	var tz, ts []bool
	star := false
	for _, c := range info.Columns {
		if c.Star {
			star = true
			continue
		}
		kind := tsKindNone
		if c.ASTExpr != nil {
			kind = k.of(c.ASTExpr)
		}
		tz = append(tz, kind == tsKindTz)
		ts = append(ts, kind == tsKindPlain)
	}
	if star {
		// A star publishes its sources' columns, their kinds with them.
		tz, ts = nil, nil
		names, known := b.blockColumns(b.ctx, info)
		if known {
			for _, n := range names {
				l := strings.ToLower(n)
				tz = append(tz, from.tzLineage[l])
				ts = append(ts, from.tsLineage[l])
			}
		}
	}
	b.setTimestampKindOut(info, tz, ts)
}

func (b *binder) setTimestampKindOut(info *plansql.SelectInfo, tz, ts []bool) {
	if b.tzOut == nil {
		b.tzOut = map[*plansql.SelectInfo][]bool{}
		b.tsOut = map[*plansql.SelectInfo][]bool{}
	}
	b.tzOut[info] = tz
	b.tsOut[info] = ts
}

// reconcileTimestampOut is a set operation's output kinds, position by
// position, as PostgreSQL's select_common_type resolves them: timestamptz
// where either arm is (a NULL or a quoted arm takes the other's type),
// timestamp where both are timestamp or one is and the other a NULL or a
// quoted literal, unknown otherwise.
func (b *binder) reconcileTimestampOut(info *plansql.SelectInfo) {
	l, r := info.Union.Left, info.Union.Right
	lz, rz, lt, rt := b.tzOut[l], b.tzOut[r], b.tsOut[l], b.tsOut[r]
	n := len(lz)
	if len(rz) > n {
		n = len(rz)
	}
	at := func(v []bool, i int) bool { return i < len(v) && v[i] }
	tz, ts := make([]bool, n), make([]bool, n)
	for i := 0; i < n; i++ {
		tz[i] = at(lz, i) || at(rz, i)
		lOpen, rOpen := outputIsNullOrQuoted(l, i), outputIsNullOrQuoted(r, i)
		ts[i] = !tz[i] && (at(lt, i) || lOpen) && (at(rt, i) || rOpen) && (at(lt, i) || at(rt, i))
	}
	b.setTimestampKindOut(info, tz, ts)
}

// outputIsNullOrQuoted reports a plain block whose i-th output is the NULL
// literal or a quoted literal (it takes the other arm's type).
func outputIsNullOrQuoted(info *plansql.SelectInfo, i int) bool {
	if info == nil || info.Union != nil || i >= len(info.Columns) {
		return false
	}
	l, ok := plansql.Unparen(info.Columns[i].ASTExpr).(*plansql.Lit)
	return ok && (l.Kind == plansql.LitNull || l.Kind == plansql.LitString)
}

// zonedLiteral parses a quoted literal that spells a nonzero zone offset:
// its wall clock and the offset in seconds east of UTC.
func zonedLiteral(n plansql.Node) (*plansql.Lit, time.Time, int, bool) {
	lit, ok := plansql.Unparen(n).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return nil, time.Time{}, 0, false
	}
	wall, off, hasZone, ok := parquet.ParseTimestampZone(lit.Value)
	if !ok || !hasZone || off == 0 {
		return nil, time.Time{}, 0, false
	}
	return lit, wall, off, true
}

// timestampZoneAmbiguous is the refusal of rule 3.
func timestampZoneAmbiguous() error {
	return sqlerr.Refuse("timestamp-zone-ambiguous", "a quoted literal with a time zone offset beside a "+
		"stored timestamp column is not supported: this engine does not record whether the column was "+
		"declared timestamp or timestamptz, and PostgreSQL reads the literal differently for each; "+
		"write TIMESTAMPTZ '…' to mean the instant the offset names, or the literal without its "+
		"offset to mean that wall-clock value")
}

// zonedLiteralRule applies the rule to a quoted literal lit beside an
// operand of kind kind: rewrites it (timestamptz), refuses it (unknown) or
// leaves it.
func zonedLiteralRule(n plansql.Node, kind tsKind, rewrites *int) error {
	if kind != tsKindTz && kind != tsKindUnknown {
		return nil
	}
	lit, wall, off, ok := zonedLiteral(n)
	if !ok {
		return nil
	}
	if kind == tsKindUnknown {
		return timestampZoneAmbiguous()
	}
	instant := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), wall.Second(),
		wall.Nanosecond(), time.UTC).Add(-time.Duration(off) * time.Second)
	if err := parquet.TimestampMillisInRange(instant.UnixMilli()); err != nil || instant.Year() < 1 || instant.Year() > 9999 {
		return sqlerr.New("22008", "timestamp out of range: %s", sqlerr.Quote(lit.Value))
	}
	lit.Value = instant.Format("2006-01-02 15:04:05.999999999")
	if rewrites != nil {
		*rewrites++
	}
	return nil
}

// zonedLiteralWalk applies zonedLiteralRule at every consumer of n that
// resolves a quoted literal to the type of a timestamp neighbour: the
// comparisons (IS [NOT] DISTINCT FROM among them), BETWEEN, IN and = ANY /
// ALL over a list or an ARRAY constructor, a simple CASE's WHEN values, a
// CASE's results, and COALESCE / IFNULL / NULLIF / GREATEST / LEAST. A
// subquery's body is its own block and is walked when it is bound.
func zonedLiteralWalk(n plansql.Node, k tsKinder) error {
	if n == nil {
		return nil
	}
	pair := func(other plansql.Node, lits ...plansql.Node) error {
		var kind tsKind
		computed := false
		for _, l := range lits {
			if _, _, _, ok := zonedLiteral(l); !ok {
				continue
			}
			if !computed {
				kind, computed = k.of(other), true
			}
			if err := zonedLiteralRule(l, kind, k.rewrites); err != nil {
				return err
			}
		}
		return nil
	}
	arms := func(ns []plansql.Node) error {
		var kind tsKind
		computed := false
		for _, l := range ns {
			if _, _, _, ok := zonedLiteral(l); !ok {
				continue
			}
			if !computed {
				kind, computed = k.common(ns), true
			}
			if err := zonedLiteralRule(l, kind, k.rewrites); err != nil {
				return err
			}
		}
		return nil
	}
	flat := func(vs []plansql.Node) []plansql.Node {
		var out []plansql.Node
		for _, v := range vs {
			if a, ok := plansql.Unparen(v).(*plansql.ArrayLitNode); ok {
				out = append(out, a.Elements...)
				continue
			}
			out = append(out, v)
		}
		return out
	}
	var err error
	switch t := n.(type) {
	case *plansql.CmpExpr:
		if err = pair(t.Left, t.Right); err == nil {
			err = pair(t.Right, t.Left)
		}
		if err == nil {
			err = k.timestamptzTextOperator(t.Left, t.Op, t.Right)
		}
	case *plansql.BetweenExpr:
		err = pair(t.Left, t.Low, t.High)
		if err == nil {
			// A literal operand bounded by typed ends takes their type.
			err = pair(t.Low, t.Left)
		}
	case *plansql.InExpr:
		err = pair(t.Left, t.Values...)
	case *plansql.AnyAllExpr:
		// An ARRAY constructor of quoted literals is text[] in PostgreSQL,
		// and a timestamptz has no operator against text: its 42883
		// (measured, lin/anyArray).
		if len(t.Values) == 1 && k.of(t.Left) == tsKindTz {
			if a, ok := plansql.Unparen(t.Values[0]).(*plansql.ArrayLitNode); ok && len(a.Elements) > 0 && allQuoted(a.Elements) {
				return sqlerr.New("42883", "operator does not exist: timestamp with time zone %s text", t.Op)
			}
		}
		err = pair(t.Left, flat(t.Values)...)
	case *plansql.CaseNode:
		if t.Subject != nil {
			conds := make([]plansql.Node, 0, len(t.Whens))
			for _, w := range t.Whens {
				conds = append(conds, w.Cond)
			}
			err = arms(append([]plansql.Node{t.Subject}, conds...))
		}
		if err == nil {
			results := make([]plansql.Node, 0, len(t.Whens)+1)
			for _, w := range t.Whens {
				results = append(results, w.Result)
			}
			if t.Else != nil {
				results = append(results, t.Else)
			}
			err = arms(results)
		}
	case *plansql.FuncCallNode:
		switch strings.ToLower(strings.TrimSpace(t.Name)) {
		case "coalesce", "ifnull", "nullif", "greatest", "least":
			err = arms(t.Args)
		}
	}
	if err != nil {
		return err
	}
	for _, c := range exprOperands(n) {
		if err := zonedLiteralWalk(c, k); err != nil {
			return err
		}
	}
	return nil
}

// timestamptzTextOperator raises PostgreSQL's 42883 for a timestamptz by
// lineage compared with a TEXT operand that is not a quoted literal (a text
// column, a text expression): PostgreSQL has no operator between the two.
// The engine compared the text read by the TIMESTAMP rule, discarding an
// offset it carried.
func (k tsKinder) timestamptzTextOperator(l plansql.Node, op string, r plansql.Node) error {
	text := func(n plansql.Node) bool {
		if _, lit := plansql.Unparen(n).(*plansql.Lit); lit || k.typeOf == nil {
			return false
		}
		t, ok := k.typeOf(n)
		return ok && t == parquet.TypeString
	}
	switch {
	case k.of(l) == tsKindTz && text(r):
		return sqlerr.New("42883", "operator does not exist: timestamp with time zone %s text", op)
	case text(l) && k.of(r) == tsKindTz:
		return sqlerr.New("42883", "operator does not exist: text %s timestamp with time zone", op)
	}
	return nil
}

// refuseTimezoneUnit raises PostgreSQL's own refusal of a time zone field
// extracted from an operand known not to be a timestamptz: a plain
// timestamp by lineage, or a DATE (extract over a date names the date;
// date_part reads it as a timestamp). Over a timestamptz under UTC the
// field is 0, which is what this engine answers; over a stored column of
// unknown declaration it answers 0 too (an open line: it cannot tell).
func refuseTimezoneUnit(n plansql.Node, k tsKinder) error {
	var err error
	walkNodes(n, func(x plansql.Node) bool {
		if err != nil {
			return false
		}
		fc, ok := x.(*plansql.FuncCallNode)
		if !ok {
			return true
		}
		name := strings.ToLower(strings.TrimSpace(fc.Name))
		var unit string
		var operand plansql.Node
		extract := false
		switch {
		case (name == "timezone_hour" || name == "timezone_minute") && fc.OutputLabel == "extract" && len(fc.Args) == 1:
			unit, operand, extract = name, fc.Args[0], true
		case name == "date_part" && len(fc.Args) == 2:
			l, ok := plansql.Unparen(fc.Args[0]).(*plansql.Lit)
			if !ok || l.Kind != plansql.LitString {
				return true
			}
			switch u := strings.ToLower(strings.TrimSpace(l.Value)); u {
			case "timezone", "timezone_hour", "timezone_minute":
				unit, operand = u, fc.Args[1]
			default:
				return true
			}
		default:
			return true
		}
		typeName := ""
		if k.of(operand) == tsKindPlain {
			typeName = "timestamp without time zone"
		} else if k.typeOf != nil && isDateTyped(k.typeOf, operand) {
			typeName = "timestamp without time zone"
			if extract {
				typeName = "date"
			}
		}
		if typeName != "" {
			err = sqlerr.New("0A000", "unit %s not supported for type %s", sqlerr.Quote(unit), typeName)
		}
		return true
	})
	return err
}

func isDateTyped(typeOf func(plansql.Node) (parquet.TypeID, bool), n plansql.Node) bool {
	t, ok := typeOf(n)
	return ok && t == parquet.TypeDate
}

// walkNodes visits n and its operands (not subquery bodies) in pre-order
// while visit answers true.
func walkNodes(n plansql.Node, visit func(plansql.Node) bool) {
	if n == nil || !visit(n) {
		return
	}
	for _, c := range exprOperands(n) {
		walkNodes(c, visit)
	}
}

// ZonedLiteralRuleOverStored applies the rule to a DML expression (a DELETE
// or UPDATE predicate, a SET or VALUES expression, a MERGE clause) over the
// target's stored columns: every column there is of unknown declaration, so
// a quoted literal with a nonzero offset beside one is refused, and one
// beside a timestamptz producer (a TIMESTAMPTZ literal, now()) is read as a
// timestamptz.
func ZonedLiteralRuleOverStored(node plansql.Node, alias string, schema []parquet.Column) error {
	if node == nil {
		return nil
	}
	scope := newColScope()
	for _, c := range schema {
		scope.addQualifiedTyped(alias, c.Name, c.Type)
	}
	decls := rowFieldScopeDecls(scope)
	k := tsKinder{decls: decls, typeOf: structuralTypeOf(decls), sub: func(string) tsKind { return tsKindNone }}
	return zonedLiteralWalk(node, k)
}

// RefuseZonedLiteralAssignment refuses a quoted literal with a nonzero
// offset ASSIGNED to a stored TIMESTAMP column (INSERT … VALUES, INSERT …
// SELECT, UPDATE … SET, MERGE): PostgreSQL stores the wall clock into a
// timestamp column and the instant into a timestamptz one, and the column's
// declaration is not recorded.
func RefuseZonedLiteralAssignment(node plansql.Node, target parquet.TypeID) error {
	if target != parquet.TypeTimestamp {
		return nil
	}
	return zonedLiteralRule(node, tsKindUnknown, nil)
}

// allQuoted reports a list of quoted literals only.
func allQuoted(ns []plansql.Node) bool {
	for _, n := range ns {
		if l, ok := plansql.Unparen(n).(*plansql.Lit); !ok || l.Kind != plansql.LitString {
			return false
		}
	}
	return true
}
