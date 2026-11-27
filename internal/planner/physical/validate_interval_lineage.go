// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Typed lineage, stage A (ADR-0052 §8; the definition record,
// docs/design/interval-execution-type.md §9.1; #1680c).
//
// The interim decided an interval by the syntax of the expression that
// computes it and refused any block that would publish one, so no column
// was ever an interval. Stage A keeps every one of those refusals and adds
// the DECLARATION: a block's output slot that an interval producer fills is
// declared interval (binder.intervalOut), and that declaration is forwarded
// to every scope that reads the block — a derived table, a CTE reference
// (used or not), a scalar or correlated subquery, a set operation
// (reconciled positionally: an interval arm beside an interval, NULL or
// quoted arm), a join's output — so a consumer in an enclosing block sees
// the interval it reads and is judged by the same rule as an inline one.
//
// NOTHING IS ADMITTED by it. The block that publishes the interval still
// records its refusal (an internal column, refused until stage D admits
// one: the interim's sentence, unchanged). What the declaration adds is
// that an UNSUPPORTED consumer of the forwarded slot — a function with an
// item of its own, a container, an ordered or DISTINCT aggregate, a TIME or
// timestamptz operand, an implicit text conversion — is refused under its
// own name, and that a refusal the program holds permanently is named in
// preference to one a later stage lifts (intervalFinding, chooseInterval).
//
// A slot whose lineage the binder cannot resolve — a star over a body whose
// columns it cannot enumerate, below which an interval was published — is
// refused loudly (interval-lineage-unresolved), never passed.

// intervalFinding is one refusal of the interval rule, with whether the
// program holds it permanently (a capability this program never admits:
// ADR-0052 §3, §5, §8 and the definition record's items) or only until a
// later stage admits the use (an interval key, an aggregate, an internal
// column).
type intervalFinding struct {
	err       error
	permanent bool
}

// permanentIntervalReasons are the refusal identities this program holds at
// every stage (ADR-0052; the stage manifest's named refusals). The bare
// result is one only at the statement's own result (a published column of
// an inner block is an internal column, stage D's).
var permanentIntervalReasons = map[string]bool{
	"time-type":                     true,
	"timestamptz":                   true,
	"typmod":                        true,
	"intervalstyle":                 true,
	"container":                     true,
	"ordered-aggregate":             true,
	"distinct-aggregate":            true,
	"function-date_trunc":           true,
	"function-to_char":              true,
	"function-generate_series":      true,
	"function-overlaps":             true,
	"function-width_bucket":         true,
	"postgresql-internal-function":  true,
	"numeric-infinity":              true,
	"char-display":                  true,
	"implicit-text-conversion":      true,
	"timestamp-millisecond-carrier": true,
	"interval-write":                true,
	"interval-lineage-unresolved":   true,
}

// newIntervalFinding classifies err; topResult says err refuses a column of
// the statement's own result.
func newIntervalFinding(err error, topResult bool) intervalFinding {
	r := sqlerr.ReasonOf(err)
	return intervalFinding{err: err, permanent: permanentIntervalReasons[r] || (topResult && r == "bare-interval-result")}
}

// chooseInterval is the statement's refusal among its findings, in the order
// they were found (innermost first, blocks in binding order): the first, as
// the interim chose, unless the first is a 0A000 a later stage lifts and a
// permanent refusal was found too, which is then the one named. A refusal
// PostgreSQL itself raises (any other SQLSTATE) found first stays first.
func chooseInterval(fs []intervalFinding) error {
	if len(fs) == 0 {
		return nil
	}
	first := fs[0]
	if first.permanent || sqlerr.StateOf(first.err) != "0A000" {
		return first.err
	}
	for _, f := range fs[1:] {
		if f.permanent {
			if sqlerr.ReasonOf(f.err) == sqlerr.ReasonOf(first.err) {
				// The same refusal, found first deeper (a published
				// column read by the statement's own star): its sentence
				// stands.
				return first.err
			}
			return f.err
		}
	}
	return first.err
}

// lineageUnresolved is the refusal of a slot whose interval lineage the
// binder cannot resolve.
func lineageUnresolved(what string) error {
	return sqlerr.Refuse("interval-lineage-unresolved", "the type of %s cannot be resolved by the planner, "+
		"and an interval was published below it: refused rather than read without its type "+
		"(name the columns instead of *)", what)
}

// addIntervalLineage declares qual.name (and name) interval-typed by lineage.
func (s *colScope) addIntervalLineage(qual, name string) {
	if s.ivLineage == nil {
		s.ivLineage = map[string]bool{}
	}
	s.ivLineage[strings.ToLower(name)] = true
	if qual != "" {
		s.ivLineage[strings.ToLower(qual+"."+name)] = true
	}
}

// mergeIntervalLineage adds o's lineage for the names s does not resolve
// itself: an inner scope's own column shadows an outer one of that name.
func (s *colScope) mergeIntervalLineage(o *colScope) {
	if o == nil || len(o.ivLineage) == 0 {
		return
	}
	if s.ivLineage == nil {
		s.ivLineage = map[string]bool{}
	}
	s.mergeLineageSet(s.ivLineage, o.ivLineage)
	if o.ivUnresolved {
		s.ivUnresolved = true
	}
}

// mergeLineageSet adds src's names to dst for the names s does not resolve
// itself — an inner scope's own column shadows an outer one of that name —
// and that dst does not hold already. It is the one forwarding rule of every
// lineage property a scope carries: interval (ivLineage) and the timestamp
// family's declared kind (tzLineage, tsLineage; #1680 line d).
func (s *colScope) mergeLineageSet(dst, src map[string]bool) {
	for k := range src {
		if _, own := dst[k]; own {
			continue
		}
		if q, n, dotted := strings.Cut(k, "."); dotted {
			if s.quals[q] != nil && s.quals[q][n] {
				continue
			}
		} else if s.cols[k] {
			continue
		}
		dst[k] = true
	}
}

// isLineageInterval reports a column reference whose slot is declared
// interval by lineage.
func (d ColDecls) isLineageInterval(n plansql.Node) bool {
	c, ok := plansql.Unparen(n).(*plansql.ColRef)
	if !ok || len(d.ivLineage) == 0 {
		return false
	}
	if c.Table != "" {
		return d.ivLineage[strings.ToLower(c.Table+"."+c.Column)]
	}
	return d.ivLineage[strings.ToLower(c.Column)]
}

// intervalValueArg is, for an aggregate or window function that answers its
// argument's own type, that argument's position: over an interval it
// answers an interval (PostgreSQL's min / max / sum / avg over interval, the
// window value functions).
var intervalValueArg = map[string]int{
	"min": 0, "max": 0, "sum": 0, "avg": 0,
	"lag": 0, "lead": 0, "first_value": 0, "last_value": 0, "nth_value": 0,
}

// intervalReturningFunctions are the PostgreSQL functions whose result is an
// interval whatever their arguments: a producer by declaration, whether this
// engine implements the function or not (an unimplemented one is refused
// where it is consumed, never left to an unknown-function error that hides
// the interval).
//
// age is NOT one although PostgreSQL declares it interval: this engine has
// no age, and every call of it is the named capability refusal function-age,
// raised where the unknown-function error was (expr.checkKnown; the
// architect's ruling on 1680c r4: rule 4, without declaring age an interval
// producer).
var intervalReturningFunctions = map[string]bool{
	"make_interval": true,
	"justify_days":  true, "justify_hours": true, "justify_interval": true,
}

// --- The wrong-today intersections (ADR-0052 §5; the record §11) ---------
//
// Where an interval operation produces or consumes a value through one of
// the engine's pre-existing wrong TIME or timestamptz paths, or through a
// declaration it cannot implement exactly, the statement is refused under
// the path's name. Each rule is decided on the operation's operands as
// written: a TIME or TIMESTAMPTZ literal is a CAST the parser builds (`TIME
// '...'`), and this engine has neither type as a column. Measured on main
// first (1680c_r3.md): every statement these rules refuse answered wrong
// there or was already refused.

// castTypeBase is a CAST target's base name, lower case, without its
// parenthesised modifier.
func castTypeBase(typeName string) string {
	base := strings.ToLower(strings.TrimSpace(typeName))
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	return base
}

// isTimeOfDayType reports PostgreSQL's TIME / TIMETZ spellings.
func isTimeOfDayType(base string) bool {
	switch base {
	case "time", "timetz", "time without time zone", "time with time zone":
		return true
	}
	return false
}

// isTimeOperand reports an operand typed TIME by its spelling (a TIME
// literal or a CAST to TIME).
func isTimeOperand(n plansql.Node) bool {
	c, ok := plansql.Unparen(n).(*plansql.CastNode)
	return ok && isTimeOfDayType(castTypeBase(c.TypeName))
}

// isTimestamptzOperand reports an operand typed timestamptz by its spelling.
func isTimestamptzOperand(n plansql.Node) bool {
	c, ok := plansql.Unparen(n).(*plansql.CastNode)
	if !ok {
		return false
	}
	switch castTypeBase(c.TypeName) {
	case "timestamptz", "timestamp with time zone":
		return true
	}
	return false
}

// isDateOperand reports an operand this rule reads as a DATE.
func (t intervalTyper) isDateOperand(n plansql.Node) bool {
	if c, ok := plansql.Unparen(n).(*plansql.CastNode); ok && castTypeBase(c.TypeName) == "date" {
		return true
	}
	id, ok := t.declaredID(n)
	return ok && id == parquet.TypeDate
}

// isTimestampLike reports an operand of a timestamp difference: a
// TIMESTAMP, a timestamptz literal, a date.
func (t intervalTyper) isTimestampLike(n plansql.Node) bool {
	if isTimestamptzOperand(n) || t.isTimestampOperand(n) {
		return true
	}
	if c, ok := plansql.Unparen(n).(*plansql.CastNode); ok && castTypeBase(c.TypeName) == "timestamp" {
		return true
	}
	return false
}

// subMilliTimestampLiteral reports a TIMESTAMP literal with more than three
// fractional digits: this engine's TIMESTAMP is a millisecond carrier
// (ADR-0012 temporal, E36), so such a literal is not the value written.
func subMilliTimestampLiteral(n plansql.Node) bool {
	c, ok := plansql.Unparen(n).(*plansql.CastNode)
	if !ok {
		return false
	}
	switch castTypeBase(c.TypeName) {
	case "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone":
	default:
		return false
	}
	s, ok := quotedLiteral(c.Inner)
	if !ok {
		return false
	}
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		digits := 0
		for _, r := range s[i+1:] {
			if r < '0' || r > '9' {
				break
			}
			digits++
		}
		return digits > 3
	}
	return false
}

// timeTypeRefusal, timestamptzRefusal, typmodRefusal, charDisplayRefusal and
// msCarrierRefusal are the named refusals of the intersections.
func timeTypeRefusal(what string) error {
	return sqlerr.Refuse("time-type", "%s is not supported: this engine has no TIME type, and a TIME "+
		"operand is not read as a time of day (ADR-0012 temporal r23)", what)
}

func timestamptzRefusal(what string) error {
	return sqlerr.Refuse("timestamptz", "%s is not supported: this engine has no timestamptz arithmetic "+
		"(the time zone and daylight saving time are not applied); use TIMESTAMP operands", what)
}

func typmodRefusal(typeName string) error {
	return sqlerr.Refuse("typmod", "an interval precision or field qualifier (%s) is not supported", strings.ToLower(strings.TrimSpace(typeName)))
}

func charDisplayRefusal(typeName string) error {
	return sqlerr.Refuse("char-display", "an interval cast to %s is not supported: only TEXT and VARCHAR "+
		"display an interval (PostgreSQL blank-pads a character(n) value this engine does not)", strings.ToLower(strings.TrimSpace(typeName)))
}

func msCarrierRefusal(what string) error {
	return sqlerr.Refuse("timestamp-millisecond-carrier", "%s is not supported: a TIMESTAMP here holds "+
		"milliseconds, and the literal has finer digits", what)
}

// intervalArithOp reports an interval arithmetic or shift operator over
// node's operands: one an interval, or the difference of two timestamps.
func (t intervalTyper) intervalArithOp(b *plansql.BinaryOp) bool {
	if b.Op != "+" && b.Op != "-" {
		return false
	}
	if t.isInterval(b.Left) || t.isInterval(b.Right) {
		return true
	}
	return b.Op == "-" && t.isTimestampLike(b.Left) && t.isTimestampLike(b.Right)
}

// permanentRule is the intersections' judgment of one node, before the
// interim's own: nil where none applies.
func (t intervalTyper) permanentRule(node plansql.Node) error {
	switch n := node.(type) {
	case *plansql.BinaryOp:
		if n.Op != "+" && n.Op != "-" {
			return nil
		}
		lt, rt := isTimeOperand(n.Left), isTimeOperand(n.Right)
		if lt || rt {
			// TIME ± interval, interval + TIME, TIME - TIME and DATE ± TIME:
			// measured on main, each answered NULL, a number of hours or a
			// day count where PostgreSQL answers a time, an interval or a
			// timestamp (the record §11; the architect's ruling on 1680c).
			other := n.Right
			if rt {
				other = n.Left
			}
			if (lt && rt) || t.isInterval(other) || t.isDateOperand(other) {
				return timeTypeRefusal("TIME arithmetic (" + n.Op + ")")
			}
			return nil
		}
		if !t.intervalArithOp(n) {
			return nil
		}
		if isTimestamptzOperand(n.Left) || isTimestamptzOperand(n.Right) {
			return timestamptzRefusal("timestamptz " + n.Op + " interval or a timestamptz difference")
		}
		if subMilliTimestampLiteral(n.Left) || subMilliTimestampLiteral(n.Right) {
			return msCarrierRefusal("an interval operation over a sub-millisecond TIMESTAMP literal")
		}
	case *plansql.CastNode:
		base := castTypeBase(n.TypeName)
		switch {
		case base == "interval" && strings.Contains(n.TypeName, "("):
			return typmodRefusal(n.TypeName)
		case base == "interval" && isTimeOperand(n.Inner):
			return timeTypeRefusal("a cast of TIME to interval")
		case isTimeOfDayType(base) && t.isInterval(n.Inner):
			return timeTypeRefusal("a cast of an interval to TIME")
		case (base == "char" || base == "character" || base == "bpchar") && t.isInterval(n.Inner):
			return charDisplayRefusal(n.TypeName)
		}
	case *plansql.FuncCallNode:
		name := strings.ToLower(strings.TrimSpace(n.Name))
		switch name {
		case "age":
			// age(DATE, DATE) resolves to the timestamptz overload
			// (measured: 10_definitions:age/dateGapHavana against
			// age/dateGapUTC); age over timestamptz is time-zone dependent.
			for _, a := range n.Args {
				if isTimestamptzOperand(a) || t.isDateOperand(a) {
					return timestamptzRefusal("age over a date or a timestamptz")
				}
			}
			for _, a := range n.Args {
				if subMilliTimestampLiteral(a) {
					return msCarrierRefusal("age over a sub-millisecond TIMESTAMP literal")
				}
			}
		case "date_add", "date_subtract", "date_sub":
			hasIv := false
			for _, a := range n.Args {
				hasIv = hasIv || t.isInterval(a)
			}
			if !hasIv {
				return nil
			}
			if len(n.Args) >= 3 || (len(n.Args) > 0 && isTimestamptzOperand(n.Args[0])) {
				return timestamptzRefusal(name + " with a time zone or a timestamptz")
			}
		}
	}
	return nil
}

// timestamptzCallRefusal is the binder's refusal of a call the signature
// check cannot resolve because it is PostgreSQL's timestamptz form:
// date_add / date_subtract over a timestamptz first argument and an interval
// literal second (an optional zone third). Nil for any other call, whose
// signature error stands.
func timestamptzCallRefusal(fc *plansql.FuncCallNode) error {
	name := strings.ToLower(strings.TrimSpace(fc.Name))
	if (name != "date_add" && name != "date_subtract") || len(fc.Args) < 2 || len(fc.Args) > 3 {
		return nil
	}
	if !isTimestamptzOperand(fc.Args[0]) {
		return nil
	}
	switch a := plansql.Unparen(fc.Args[1]).(type) {
	case *plansql.IntervalLit:
	case *plansql.CastNode:
		if castTypeBase(a.TypeName) != "interval" {
			return nil
		}
	default:
		return nil
	}
	return timestamptzRefusal(name + " with a time zone or a timestamptz")
}

// tableFuncTimestamptz is the intersection's judgment of a table function's
// arguments: generate_series over timestamptz bounds with an interval step.
func (t intervalTyper) tableFuncTimestamptz(tr *plansql.TableRef) error {
	if tr == nil || !tr.IsFunction {
		return nil
	}
	tz, iv := false, false
	for _, a := range tr.FuncArgExprs {
		tz = tz || isTimestamptzOperand(a)
		iv = iv || t.isInterval(a)
	}
	if tz && iv {
		return timestamptzRefusal(strings.ToLower(tr.Name) + " over timestamptz bounds with an interval step")
	}
	return nil
}

// literalsArePGInput reports that every interval literal in n is input
// PostgreSQL reads: a unit it does not know (`1 fortnight`) is its own 22007
// at parse, which this engine answers with its literal reader's refusal, and
// that refusal keeps its precedence.
func literalsArePGInput(n plansql.Node) bool {
	if l, ok := n.(*plansql.IntervalLit); ok {
		return expr.IsIntervalInputText(fmt.Sprintf("%d %s", l.Value, l.Unit))
	}
	for _, k := range exprOperands(n) {
		if !literalsArePGInput(k) {
			return false
		}
	}
	return true
}

// isTop reports a block whose result columns are the statement's result.
func (b *binder) isTop(info *plansql.SelectInfo) bool {
	return info != nil && (info == b.root || b.tops[info])
}

// markTop marks a set operation arm of the statement's own result.
func (b *binder) markTop(info *plansql.SelectInfo) {
	if b.tops == nil {
		b.tops = map[*plansql.SelectInfo]bool{}
	}
	b.tops[info] = true
}

// setIntervalOut records a block's output lineage.
func (b *binder) setIntervalOut(info *plansql.SelectInfo, out []bool, below bool) {
	if b.intervalOut == nil {
		b.intervalOut = map[*plansql.SelectInfo][]bool{}
		b.intervalBelow = map[*plansql.SelectInfo]bool{}
	}
	b.intervalOut[info] = out
	for _, v := range out {
		below = below || v
	}
	b.intervalBelow[info] = b.intervalBelow[info] || below
}

// reconcileIntervalOut is a set operation's output lineage, position by
// position: an interval in either arm declares the position interval
// (PostgreSQL's select_common_type answers interval beside an interval, a
// NULL or a quoted arm, and refuses any other type, 42804: either way no
// consumer may read the position as anything but an interval).
func (b *binder) reconcileIntervalOut(info *plansql.SelectInfo) {
	l, r := b.intervalOut[info.Union.Left], b.intervalOut[info.Union.Right]
	n := len(l)
	if len(r) > n {
		n = len(r)
	}
	out := make([]bool, n)
	for i := range out {
		out[i] = !expr.IntervalLineageDisabledAt(expr.LineageSetOp) && ((i < len(l) && l[i]) || (i < len(r) && r[i]))
	}
	b.setIntervalOut(info, out, b.intervalBelow[info.Union.Left] || b.intervalBelow[info.Union.Right])
}

// intervalBlock applies the interval rule to one block's own expressions and
// records its output lineage (binder.intervalOut).
func (b *binder) intervalBlock(info *plansql.SelectInfo, from, resolve *colScope) {
	decls := rowFieldScopeDecls(resolve)
	subquery := func(sql string) (parquet.TypeID, bool) {
		if types := b.subqueryDeclaredTypes(sql, resolve); len(types) == 1 && types[0] != typeAmbiguous {
			return types[0], true
		}
		return 0, false
	}
	tsSubquery := func(sql string) bool {
		sub := b.validatedBody(sql, resolve)
		ts := b.timestampOut[sub]
		return sub != nil && len(ts) == 1 && ts[0]
	}
	ivSubquery := func(n *plansql.SubqueryNode) bool {
		if expr.IntervalLineageDisabledAt(expr.LineageSubquery) {
			return false
		}
		sub := b.validatedBody(n.SQL, resolve)
		out := b.intervalOut[sub]
		return sub != nil && len(out) == 1 && out[0]
	}
	top := b.isTop(info)
	b.intervalFindings = append(b.intervalFindings, intervalBlockFindings(info, decls, subquery, tsSubquery, ivSubquery, top)...)

	t := newIntervalTyper(decls, subquery, tsSubquery)
	t.ivSubquery = ivSubquery
	var out []bool
	star := false
	explicit := map[string]bool{}
	for _, c := range info.Columns {
		if c.Star {
			star = true
			continue
		}
		iv := c.ASTExpr != nil && t.isInterval(c.ASTExpr)
		out = append(out, iv)
		if iv && c.Alias != "" {
			explicit[strings.ToLower(c.Alias)] = true
		}
	}
	below := len(from.ivLineage) > 0 || from.ivUnresolved
	if star {
		// A star publishes its sources' columns, their lineage with them.
		out = nil
		names, known := b.blockColumns(b.ctx, info)
		if !known || from.ivUnresolved {
			if top && from.ivUnresolved {
				b.intervalFindings = append(b.intervalFindings, newIntervalFinding(lineageUnresolved("the columns of *"), true))
			}
			b.setIntervalOut(info, nil, below)
			return
		}
		for _, n := range names {
			iv := explicit[strings.ToLower(n)] || (!expr.IntervalLineageDisabledAt(expr.LineageStar) && from.ivLineage[strings.ToLower(n)])
			out = append(out, iv)
			if iv && top {
				b.intervalFindings = append(b.intervalFindings, newIntervalFinding(intervalRefusal("a query result column"), true))
			}
		}
	}
	b.setIntervalOut(info, out, below)
}
