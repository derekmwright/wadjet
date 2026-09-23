// SPDX-License-Identifier: MIT

// Set-operation TYPE reconciliation: the output name of a UNION/INTERSECT/
// EXCEPT column, the type categories the arms are widened through, the
// carrier gaps and the literal-arm rules. It was called set_op_stages.go; the
// set-op STAGE emission is in internal/coordinator/dagplan under that name,
// and none of it is here (ADR-0037).
package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func setOpName(node *logical.Node) string {
	if node.UnionAll {
		return setOpBaseName(node) + " ALL"
	}
	return setOpBaseName(node)
}

// setOpBaseName is the operation without ALL, which is how PostgreSQL names it
// in the 42804 message: `UNION ALL` of two incompatible arms is reported as
// "UNION types … cannot be matched" (measured live on 17.11).
func setOpBaseName(node *logical.Node) string {
	switch node.Type {
	case logical.NodeIntersect:
		return "INTERSECT"
	case logical.NodeExcept:
		return "EXCEPT"
	}
	return "UNION"
}

// setOpTypeMismatch is the refusal a set operation whose arms have NO COMMON
// TYPE takes, on BOTH execution paths and at PLAN time, with PostgreSQL's own
// SQLSTATE and wording (42804, measured live on 17.11 for every pair in
// ADR-0012 item 12's family — numeric ∪ text, bigint ∪ text, double precision
// ∪ text, boolean ∪ bigint, uuid ∪ text, timestamp ∪ text, in either arm
// order and for UNION, INTERSECT and EXCEPT alike).
//
// Nothing outside the numeric family widens (ADR-0012 item 12), and before
// this the two paths disagreed about what that meant. The DAG refused with a
// message of its own carrying no SQLSTATE; the single-process path let the
// arms meet at runtime and answered whatever the first arm's box happened to
// allow — `SELECT s FROM t UNION ALL SELECT d FROM t` came back as a STRING
// column holding rendered decimals, and the same pair the other way round
// failed mid-execution with 22P02 on the first row of text that is not a
// number (#648).
//
// The COLUMN is named after PostgreSQL's sentence rather than inside it: the
// set operation's arms correspond by position and the position is the
// localization a reader needs, which PostgreSQL's message does not carry.
func setOpTypeMismatch(op, column string, a, b parquet.TypeID) error {
	return sqlerr.New("42804", "%s types %s and %s cannot be matched: result column %q",
		op, pgTypeName(a), pgTypeName(b), column)
}

// setOpCategory maps declared types to their PostgreSQL WIRE categories.
// Different categories have no common type; within one, require implicit conversion.
// Numeric: INT32/INT64/FLOAT32/FLOAT64/DECIMAL and PORT/PROTOCOL/DURATION
// (int4/int4/int8 on wire, #834). String: STRING; boolean: BOOL.
// Datetime: DATE/TIMESTAMP → TIMESTAMP. Network: IPV4/IPV6/CIDR → inet.
// Other: BYTES/UUID/MAC/ARRAY/ROW/MAP/VECTOR each match only themselves;
// sharing a category does not imply mutual implicit casts.
// See docs/internals/set-operation-type-categories.md for the design.
type setOpCategory int

const (
	setOpCatOther setOpCategory = iota
	setOpCatNumeric
	setOpCatString
	setOpCatBoolean
	setOpCatDateTime
	setOpCatNetwork
)

func setOpTypeCategory(t parquet.TypeID) setOpCategory {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64,
		parquet.TypeDecimal, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return setOpCatNumeric
	case parquet.TypeString:
		return setOpCatString
	case parquet.TypeBool:
		return setOpCatBoolean
	case parquet.TypeDate, parquet.TypeTimestamp:
		return setOpCatDateTime
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR:
		return setOpCatNetwork
	}
	return setOpCatOther
}

// setOpNoCommonType is PostgreSQL's question, not "SetOpWiden declines them".
//
// It used to be a hand-written exemption list — the ladder, plus "the two map
// to the same pgTypeName", plus date/timestamp — and that list refused pairs
// PostgreSQL MATCHES. Measured at that commit against 2d4220c9 and live
// 17.11: thirteen ordered column pairs (`c_i64 ∪ c_port`, `c_i32 ∪ c_dur`,
// `c_f64 ∪ c_proto` and their mirrors) and six literal idioms
// (`c_ipv4 ∪ '10.0.0.9'`, `c_mac ∪ 'aa:bb:…'`, `c_date ∪ '2010-01-01'`,
// `c_uuid ∪ '000…'`, `c_dec ∪ '0'`, `'1.5' ∪ numeric`) answered PostgreSQL's
// exact rows on the single-process path and became a plan-time 42804 — a
// right → loud move, and the single-process path is the coordinator's local
// fast path, so it is the default for a small query.
//
// The rule is the documented algorithm instead: different CATEGORIES have no
// common type; within a category, one exists. An UNKNOWN-typed literal has no
// type of its own and takes the others' — that is the literal half, and it is
// handled by the caller, which does not offer such an arm's type to this
// function at all.
func setOpNoCommonType(a, b parquet.TypeID) bool {
	if a == b {
		return false
	}
	ca, cb := setOpTypeCategory(a), setOpTypeCategory(b)
	if ca != cb {
		return true
	}
	switch ca {
	case setOpCatNumeric, setOpCatDateTime, setOpCatNetwork:
		// The three categories with more than one member and an implicit
		// conversion between them: the numeric ladder, date → timestamp, and
		// the inet family.
		return false
	}
	// One category, no implicit conversion — PostgreSQL's step 6 failure, and
	// the same SQLSTATE.
	return true
}

// setOpNoCarrier reports a pair PostgreSQL RESOLVES and this engine cannot
// carry: DATE beside TIMESTAMP, and two members of the inet family. The
// numeric category is fully carried by the ladder, so this is exactly the
// datetime and network cross-kind pairs.
//
// They are refused LOUDLY on both paths rather than answered, because the
// single-process path's answer for all three is CORRUPT — measured at
// febf0435: `c_date ∪ c_ts` renders the timestamps as `-2207656-04-19`, and
// `c_ipv4 ∪ c_ipv6` and `c_ipv4 ∪ c_cidr` render every row of the second arm
// as `0.0.0.0`. The refusal is NOT PostgreSQL's 42804, because PostgreSQL
// matches these; it says what is true, which is that this engine has no common
// carrier for the two arms' files. Closing it means a real DATE → TIMESTAMP
// promotion and an inet-family carrier, which is a typing feature.
func setOpNoCarrier(a, b parquet.TypeID) bool {
	if a == b {
		return false
	}
	// A WIRE-DECLARED integer (PORT, PROTOCOL, DURATION) meeting DECIMAL. The
	// ladder resolves the pair — PostgreSQL answers numeric — and this engine
	// has no coercion that moves those carriers there: the DECIMAL rung's
	// DecimalCoercion reads an INT32/INT64 unscaled carrier and knows nothing
	// of a PORT vector, and SetOpDecimalTarget has no digit count for one.
	//
	// REAL is NOT here, and was, wrongly: `SELECT c_f32 … UNION ALL SELECT
	// c_port …` and its PROTOCOL and DURATION siblings ANSWERED PostgreSQL's
	// `real` rows at 2d4220c9, and refusing them was a right → loud move. The
	// float rungs carry these boxes — setOpMoveValue converts an int32 or int64
	// to the float, and the arm's declared spec is the reconciled FLOAT32 — so
	// the pair resolves and answers on both paths.
	if setOpWireIntegerCarrier(a) && b == parquet.TypeDecimal {
		return true
	}
	if setOpWireIntegerCarrier(b) && a == parquet.TypeDecimal {
		return true
	}
	if _, ok := setOpWiden(a, b); ok {
		return false
	}
	if setOpTypeCategory(a) != setOpTypeCategory(b) {
		return false // no common type at all: that is the 42804, not this
	}
	// Only the categories with an implicit conversion BETWEEN their members.
	// The `other` category's members (BYTES, UUID, MAC, the containers) match
	// only themselves in PostgreSQL too — `uuid ∪ bytea` is refused there — so
	// a pair drawn from it has no common type and takes PostgreSQL's 42804
	// rather than a claim that wadjet is the one missing a carrier.
	switch setOpTypeCategory(a) {
	case setOpCatDateTime, setOpCatNetwork:
		return true
	}
	return false
}

// setOpWireIntegerCarrier names the three types whose WIRE declaration is an
// integer (#834) but whose storage is a domain vector of its own.
func setOpWireIntegerCarrier(t parquet.TypeID) bool {
	return t == parquet.TypePort || t == parquet.TypeProtocol || t == parquet.TypeDuration
}

// setOpCarrierGap is the refusal for a pair PostgreSQL resolves and this
// engine has no carrier for. It names wadjet's own CARRIERS, not PostgreSQL's
// types: IPV4 and IPV6 are both `inet` there, and it is the carriers that
// cannot be concatenated.
func setOpCarrierGap(column string, a, b parquet.TypeID) error {
	// 0A000, feature_not_supported: PostgreSQL ANSWERS this query, so the
	// refusal is this engine saying what it does not do yet — the class the
	// order-by-unselected-aggregate refusal and ADR-0021 §1e's container
	// refusals already use. Left unclassified, `sqlerr.StateOf` answered "" and
	// the pgwire door fell back to XX000, which tells a driver the server
	// BROKE on a query PostgreSQL runs.
	return sqlerr.New("0A000",
		"set operation not supported: result column %q is %s in one arm and %s in another, and "+
			"this engine has no common carrier for that pair — PostgreSQL resolves it, wadjet "+
			"does not yet; CAST both arms to one type", column, a, b)
}

// setOpQuotedLiteralGap refuses UNKNOWN quoted text at plan time with 0A000
// when its resolved type cannot be built from text. VectorAcceptsText excludes
// BOOL, four machine numeric types, TIMESTAMP, PORT, PROTOCOL and DURATION;
// letting their STRING boxes reach vectors triggers #361 instead of a typed error.
// Bare NULL and unquoted typed literals are unaffected. Closing the gap requires
// plan-time target-box parsing locally and target-literal projection rewriting
// on the DAG, including 22P02 for invalid text (ADR-0012 item 12).
// See docs/internals/set-operation-quoted-literal-gap.md for the design.
func setOpQuotedLiteralGap(column string, arm int, t parquet.TypeID) error {
	return sqlerr.New("0A000",
		"set operation not supported: result column %q resolves to %s and arm %d selects a "+
			"QUOTED literal — PostgreSQL types an unknown literal from the other arms and parses "+
			"it with that type's input function, and wadjet cannot yet build a %s value from a "+
			"literal's text here; write the literal unquoted, or CAST it to the column's type",
		column, t, arm+1, t)
}

// setOpQuotedLiteralArms marks, per OUTPUT POSITION, the select items of one
// arm that are QUOTED string literals. It is SetOpUnknownLiteralArms minus the
// bare NULLs: both are UNKNOWN-typed to PostgreSQL and take the other arms'
// type, but only a quoted one carries TEXT that has to reach a typed vector.
func setOpQuotedLiteralArms(arm *logical.Node, cols int) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) != cols {
		return nil
	}
	out := make([]bool, cols)
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		lit, ok := plansql.Unparen(pr.ASTExpr).(*plansql.Lit)
		if !ok {
			continue
		}
		if lit.Kind == plansql.LitString {
			out[i], any = true, true
		}
	}
	if !any {
		return nil
	}
	return out
}

// setOpCarrierGapPairs is every ordered pair this engine refuses with
// setOpCarrierGap: PostgreSQL resolves it and there is no carrier here. It is
// COMPUTED from setOpNoCarrier over the whole type list rather than written
// down, because the hand-written version of this list is what ADR-0012 and
// docs/sql-reference.md described when the code refused twenty pairs and the
// docs named two.
func setOpCarrierGapPairs() [][2]parquet.TypeID {
	all := []parquet.TypeID{
		parquet.TypeBool, parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32,
		parquet.TypeFloat64, parquet.TypeString, parquet.TypeBytes, parquet.TypeTimestamp,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration, parquet.TypeUUID,
		parquet.TypeDate, parquet.TypeDecimal, parquet.TypeArray, parquet.TypeRow,
		parquet.TypeMap, parquet.TypeVector,
	}
	var out [][2]parquet.TypeID
	for _, a := range all {
		for _, b := range all {
			if setOpNoCarrier(a, b) {
				out = append(out, [2]parquet.TypeID{a, b})
			}
		}
	}
	return out
}

// setOpOversizeLiteralArms marks, per OUTPUT POSITION, the select items of one
// arm that are numeric LITERALS whose spelling no DECIMAL this engine declares
// can hold — more than 38 digits, or a scale past the carrier's.
//
// litDeclType declines those, which left the arm on the float8 rung and
// answered `1.2345678901234568e+38` for
// `SELECT a FROM t UNION ALL SELECT 123456789012345678901234567890123456789.5`
// where PostgreSQL answers the exact numeric — a silently rounded number under
// an exact type, on both paths, while ADR-0024 item 4 and ADR-0012 item 12 both
// say a value with no exact carrier is 22003 and never the nearest storable
// one. The caller raises that where the other arms make the union's type
// EXACT; beside a float arm PostgreSQL resolves double precision and the
// float8 the literal folds to is that type's own answer.
func setOpOversizeLiteralArms(arm *logical.Node, cols int) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) != cols {
		return nil
	}
	out := make([]bool, cols)
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		if _, ok := setOpLitArm(pr.ASTExpr); ok {
			continue // a literal this engine CAN hold exactly
		}
		if !setOpIsNumericLiteral(pr.ASTExpr) {
			continue
		}
		out[i], any = true, true
	}
	if !any {
		return nil
	}
	return out
}

// setOpIsNumericLiteral reports a select item that is a numeric literal with a
// decimal point or an exponent — PostgreSQL's `numeric` constant — parentheses
// and a leading sign included, the same shapes setOpLitArm reads.
func setOpIsNumericLiteral(e plansql.Node) bool {
	for {
		switch n := e.(type) {
		case *plansql.Lit:
			return n.Kind == plansql.LitNumber && strings.ContainsAny(n.Value, ".eE")
		case *plansql.ParenNode:
			e = n.Inner
		case *plansql.UnaryOp:
			if n.Op != "-" && n.Op != "+" {
				return false
			}
			e = n.Inner
		default:
			return false
		}
	}
}

// setOpExactNumeric names the types whose values this engine holds EXACTLY, so
// a union of one with a numeric literal is `numeric` in PostgreSQL and must be
// exact here too.
func setOpExactNumeric(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeDecimal,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return true
	}
	return false
}

// setOpUnknownLiteralArms marks, per OUTPUT POSITION, the select items of one
// arm that are UNKNOWN-typed literals — a quoted string or NULL, which
// PostgreSQL gives no type of its own and resolves to the other arms' type
// (algorithm steps 3 and 5).
//
// Without this, `SELECT c_ipv4 … UNION ALL SELECT '10.0.0.9'` read the literal
// arm as TEXT and refused inet ∪ text, which PostgreSQL answers as inet; the
// same for a mac, a date, a uuid and a numeric column beside a quoted literal,
// and for a quoted literal in the FIRST arm.
func setOpUnknownLiteralArms(arm *logical.Node, cols int) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) != cols {
		return nil
	}
	out := make([]bool, cols)
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		lit, ok := plansql.Unparen(pr.ASTExpr).(*plansql.Lit)
		if !ok {
			continue
		}
		if lit.Kind == plansql.LitString || lit.Kind == plansql.LitNull {
			out[i], any = true, true
		}
	}
	if !any {
		return nil
	}
	return out
}

// setOpArmTypeConflict is the no-common-type refusal, computed WITHOUT
// emitting any stage, so the single-process path takes the same plan-time
// answer the stage DAG does. It walks the same arm projections
// reconcileSetOpArmTypes walks, and reports ONLY the type conflict: an arm the
// walk cannot type is not a conflict, and every other refusal
// reconcileSetOpArmTypes makes is about the DAG's own materialization rather
// than about the query's meaning, so neither is raised here.
func setOpArmTypeConflict(node *logical.Node) error {
	if !isSetOpNode(node) || len(node.Children) < 2 {
		return nil
	}
	for _, child := range node.Children {
		if inner := setOpUnwrap(child); isSetOpNode(inner) {
			if err := setOpArmTypeConflict(inner); err != nil {
				return err
			}
		}
	}
	outNames := setOpOutputNames(node.Children[0])
	if len(outNames) == 0 {
		return nil
	}
	plans := make([]SetOpArmPlan, 0, len(node.Children))
	unknown := make([][]bool, 0, len(node.Children))
	quoted := make([][]bool, 0, len(node.Children))
	oversize := make([][]bool, 0, len(node.Children))
	for _, child := range node.Children {
		plan, err := setOpArmProjection(child, outNames)
		if err != nil {
			return nil // a shape this walk cannot read is not a conflict
		}
		plans = append(plans, plan)
		unknown = append(unknown, setOpUnknownLiteralArms(child, len(outNames)))
		quoted = append(quoted, setOpQuotedLiteralArms(child, len(outNames)))
		oversize = append(oversize, setOpOversizeLiteralArms(child, len(outNames)))
	}
	op := setOpBaseName(node)
	// PostgreSQL's refusal wins over wadjet's. A column with NO COMMON TYPE is
	// a fact about the QUERY and is 42804 wherever it sits; a column whose
	// common type this engine cannot carry is a fact about this engine. So
	// every column is resolved first and the carrier gap is reported only when
	// no column has a real type conflict — otherwise `SELECT c_date, c_dec …
	// UNION ALL SELECT c_ts, c_str …` answered wadjet's carrier message where
	// PostgreSQL says "UNION types numeric and text cannot be matched", and
	// the same query with its columns swapped said something else again.
	var carrierGap error
	for col := range outNames {
		var want SetOpColType
		for i, plan := range plans {
			if unknown[i] != nil && unknown[i][col] {
				// An UNKNOWN-typed literal has no type of its own and takes
				// the other arms' (PostgreSQL's algorithm, steps 3 and 5).
				continue
			}
			if setOpArmIsUnknownLit(oversize, i, col) {
				// A numeric literal wider than any DECIMAL this engine
				// declares. PostgreSQL types it `numeric`, not float8, so it
				// must not drag the union onto the float rung — the arm walk
				// gave it FLOAT64 only because litDeclType declined it. Its
				// own disposition is decided below, once the OTHER arms have
				// said whether the result is exact.
				continue
			}
			ct := plan.Types[col]
			if !ct.Known {
				continue
			}
			if !want.Known {
				want = ct
				continue
			}
			// TWO ROW SHAPES are two types. The TypeID comparison below sees
			// ROW on both arms and resolves nothing, and the single-process
			// path then read arm 2's rows under arm 1's field list while the
			// DAG answered differently (#1065); PostgreSQL 17.11 refuses the
			// pair at parse analysis with 42846 (`UNION could not convert
			// type rownt to rowt`, measured). The shapes are spelled where
			// the server names its composite types.
			if want.Typ == parquet.TypeRow && ct.Typ == parquet.TypeRow {
				a := parquet.Column{Type: parquet.TypeRow, Fields: want.Fields}
				b := parquet.Column{Type: parquet.TypeRow, Fields: ct.Fields}
				if !foldCompatible(a, b) {
					return sqlerr.New("42846", "%s could not convert type %s to %s: result column %q",
						op, foldTypeName(b), foldTypeName(a), outNames[col])
				}
			}
			if setOpNoCarrier(want.Typ, ct.Typ) {
				if carrierGap == nil {
					carrierGap = setOpCarrierGap(outNames[col], want.Typ, ct.Typ)
				}
				break
			}
			widened, ok := setOpWiden(want.Typ, ct.Typ)
			if !ok {
				if setOpNoCommonType(want.Typ, ct.Typ) {
					return setOpTypeMismatch(op, outNames[col], want.Typ, ct.Typ)
				}
				if setOpNoCarrier(want.Typ, ct.Typ) {
					// PostgreSQL resolves this pair and wadjet has no carrier
					// for it. Loud on both paths rather than the corrupt values
					// the single-process path used to answer — a timestamp
					// rendered `-2207656-04-19`, an IPv6 or CIDR row rendered
					// `0.0.0.0` — but only once no column has a real type
					// conflict to report first.
					if carrierGap == nil {
						carrierGap = setOpCarrierGap(outNames[col], want.Typ, ct.Typ)
					}
					break
				}
				// Not this refusal's business FOR THIS COLUMN — and it says
				// nothing at all about the columns beside it. Abandoning the
				// whole walk here (which is what this used to do) left #648's
				// own filed symptom reachable one column to the left: over
				// `SELECT c_date, c_dec … UNION ALL SELECT c_ts, c_str …` the
				// date/timestamp pair in column 1 stopped the check and the
				// numeric/text pair in column 2 was never seen, so the
				// single-process path failed mid-execution with 22P02 on the
				// first row of text that is not a number — the exact failure
				// this check exists to replace — while the same query with its
				// two columns SWAPPED refused at plan time. A disposition that
				// depends on column ORDER is not a rule.
				break
			}
			want = SetOpColType{Typ: widened, Known: true, Fields: want.Fields}
		}
		// A QUOTED literal whose resolved type cannot be built from text.
		// Deferred like the carrier gap and for the same reason: it is a fact
		// about this engine, and PostgreSQL's own 42804 for some other column
		// outranks it.
		if want.Known && !batch.VectorAcceptsText(want.Typ) {
			for i := range plans {
				if !setOpArmIsUnknownLit(quoted, i, col) {
					continue
				}
				if carrierGap == nil {
					carrierGap = setOpQuotedLiteralGap(outNames[col], i, want.Typ)
				}
				break
			}
		}
		// A numeric literal no DECIMAL this engine declares can hold, beside an
		// arm whose values are EXACT. PostgreSQL's numeric is unbounded and
		// answers it; wadjet's carrier is 38 digits, and the honest answer is
		// the overflow error, never the float8 the literal silently folded to
		// (ADR-0024 items 1 and 4).
		if want.Known && setOpExactNumeric(want.Typ) {
			for i := range plans {
				if !setOpArmIsUnknownLit(oversize, i, col) {
					continue
				}
				return sqlerr.New("22003",
					"numeric field overflow: the literal in arm %d of this %s has more digits than "+
						"a DECIMAL can hold, and result column %q is exact — wadjet's numeric "+
						"carrier is 38 digits where PostgreSQL's is unbounded",
					i+1, op, outNames[col])
			}
		}
	}
	return carrierGap
}

func isSetOpNode(n *logical.Node) bool {
	return n != nil &&
		(n.Type == logical.NodeUnion || n.Type == logical.NodeIntersect || n.Type == logical.NodeExcept)
}

// setOpUnwrap descends the wrappers FindOutputProjectionNode descends
// (ORDER BY / LIMIT / WHERE / DISTINCT above an arm) and returns the first
// node that produces rows. Used to recognise an arm that is ITSELF a set
// operation — `a UNION ALL b UNION ALL c` parses left-deep, so the outer
// union's first arm is another union node with no projection of its own.
func setOpUnwrap(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeSort, logical.NodeLimit, logical.NodeFilter, logical.NodeDistinct:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return n
	}
	return nil
}

// setOpOutputNames takes the first arm's names, descending nested set operations
// to the whole chain's leftmost arm. Use declaredProjectionName: alias, then
// column's own unqualified name, then rendered expression (#743).
// Do not lowercase again: lexer folding already handled unquoted identifiers;
// delimited aliases and expression rendering must survive verbatim (#731).
// SELECT * keeps catalog spelling, matching the arm stream and local output.
func setOpOutputNames(arm *logical.Node) []string {
	inner := setOpUnwrap(arm)
	if isSetOpNode(inner) && len(inner.Children) > 0 {
		return setOpOutputNames(inner.Children[0])
	}
	// `SELECT * FROM t` builds no Project at all — the arm IS the scan, and
	// its output columns are the table's, in catalog order.
	if inner != nil && inner.Type == logical.NodeScan && len(inner.ScanColumns) > 0 {
		names := make([]string, len(inner.ScanColumns))
		copy(names, inner.ScanColumns)
		return names
	}
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) == 0 {
		return nil
	}
	names := make([]string, 0, len(proj.Projections))
	for _, pr := range proj.Projections {
		// Delimiters are not part of the name: a rendered reference to a
		// delimited identifier re-quotes, so a set operation over
		// `SELECT "g + 1"` published a result column literally called
		// `"g + 1"`, quotes included (#725). CleanExpr strips them inside
		// declaredProjectionName; NormalizeIdentRef is the same strip for the
		// alias and column arms.
		name := plansql.NormalizeIdentRef(strings.TrimSpace(declaredProjectionName(pr)))
		if name == "" {
			return nil
		}
		names = append(names, name)
	}
	return names
}

// SetOpArmPlan is one arm's contribution to the union stage: the projection
// that renames/computes its columns, plus the plan-time output type of each,
// which reconcileSetOpArmTypes needs to make the arms concatenable.
type SetOpArmPlan struct {
	Specs []ProjectExprSpec
	Types []SetOpColType
	// Coerce names the columns whose VALUES this arm must move before they
	// enter the union stream — a DECIMAL carrier at the wrong scale, or an
	// integer that has to become one (#533). A CAST cannot do this job: the
	// cast evaluator produces a float64 for a DECIMAL destination, which is
	// exactly the precision loss the exact carrier exists to avoid.
	Coerce []DecimalCoercion
}

// SetOpColType is a plan-time output type, or the absence of one. There is no
// spare TypeID to mean "unknown" — TypeBool is the zero value — so the flag
// carries it.
type SetOpColType struct {
	Fields []parquet.Column
	Typ    parquet.TypeID
	Known  bool
	// Dec is a DECIMAL column's declared precision and scale: the two facts
	// a bare TypeID cannot express, and the ones two DECIMAL arms can
	// DISAGREE on while looking identical to a TypeID comparison. That is
	// #533 — reconcileSetOpArmTypes saw one TypeID on both arms, reconciled
	// nothing, and the wider arm's unscaled Int128 was then read at the
	// narrower arm's scale, 100x too large.
	//
	// decKnown is false for a DECIMAL whose (p,s) the arm walk could not
	// resolve — a computed expression carries none, the same case
	// declaredProjectionDecimal declines (#458).
	Dec      logical.DecimalMeta
	DecKnown bool
}

// setOpArmProjection builds the OpProject spec list that puts one arm's
// output under the set operation's result column names, plus each column's
// plan-time type.
//
// The projection runs in the union stage's own fragment, over the arm's
// materialized output, so it works the same whether the arm ended in a scan,
// a filter-scan, a join or a sort. Aggregate outputs are the exception: they
// exist under names the aggregate machinery chose, not under the SELECT
// list's expression text, so those arms are refused rather than guessed at.
func setOpArmProjection(arm *logical.Node, outNames []string) (SetOpArmPlan, error) {
	inner := setOpUnwrap(arm)
	// A nested set operation already projected ITS arms onto ITS OWN result
	// names; read the arm through those, not through a projection it does
	// not have.
	if isSetOpNode(inner) {
		innerNames := setOpOutputNames(inner)
		if len(innerNames) != len(outNames) {
			return SetOpArmPlan{}, fmt.Errorf("nested %s emits %d columns, the enclosing set operation has %d",
				setOpName(inner), len(innerNames), len(outNames))
		}
		plan := SetOpArmPlan{
			Specs: make([]ProjectExprSpec, len(outNames)),
			Types: make([]SetOpColType, len(outNames)),
		}
		for i, n := range innerNames {
			plan.Specs[i] = ProjectExprSpec{
				Expr: n, Name: outNames[i],
				// The SLOT, because a nested operation's result columns may
				// repeat a NAME and the enclosing arm reads them from its
				// stream: `A UNION B UNION C` parses left-deep, so arm 1 of
				// the outer operation IS a set operation, and where its result
				// list carries `amount` twice both specs read the first of
				// them. The file then declared column two FLOAT64 and carried
				// the INT64 column one, which the consumer refuses loudly
				// (ADR-0010) — the same "a name is not a handle" as #1022,
				// one nesting level up. The positions are the inner
				// operation's own, by the construction this function relies
				// on everywhere else: every arm is projected onto the result
				// column list, in order.
				SourceSlot: i, SourceSlotSet: true,
			}
		}
		// The nested operation's OWN reconciliation decides what this arm
		// actually emits, so ask for it rather than reporting "unknown".
		// Reporting unknown is what made `a UNION ALL b UNION ALL c` — which
		// parses left-deep, so arm 1 of the outer union IS a union — skip
		// reconciliation entirely: the enclosing operation saw one typed arm
		// and one untyped one, declined to cast either, and the three files
		// then disagreed about the column. For a DECIMAL that is #533 again
		// one level up; for the INT32/INT64/FLOAT64 ladder it dropped a whole
		// arm's rows on the floor.
		if inferred := setOpNodeResultTypes(inner); len(inferred) == len(outNames) {
			plan.Types = inferred
		}
		return plan, nil
	}
	// A bare scan arm (`SELECT * FROM t`): the columns correspond by catalog
	// order, which is the order the star expands in.
	if inner != nil && inner.Type == logical.NodeScan && len(inner.ScanColumns) > 0 {
		if len(inner.ScanColumns) != len(outNames) {
			return SetOpArmPlan{}, fmt.Errorf("selects %d columns, the first arm selects %d",
				len(inner.ScanColumns), len(outNames))
		}
		plan := SetOpArmPlan{
			Specs: make([]ProjectExprSpec, len(outNames)),
			Types: make([]SetOpColType, len(outNames)),
		}
		for i, c := range inner.ScanColumns {
			lc := strings.ToLower(c)
			plan.Specs[i] = ProjectExprSpec{Expr: lc, Name: outNames[i]}
			if t, ok := inner.ScanColTypes[lc]; ok {
				plan.Types[i] = SetOpColType{Typ: t, Known: true, Fields: inner.ScanColFields[lc]}
				if t == parquet.TypeDecimal {
					plan.Types[i].Dec, plan.Types[i].DecKnown = setOpColDecimalMeta(inner.ScanColDecimal, lc)
				}
			}
		}
		return plan, nil
	}

	projNode := findOutputProjectionNode(arm)
	if projNode == nil {
		return SetOpArmPlan{}, fmt.Errorf("no resolvable SELECT list to project onto the result columns %v", outNames)
	}
	if len(projNode.Projections) != len(outNames) {
		return SetOpArmPlan{}, fmt.Errorf("selects %d columns, the first arm selects %d",
			len(projNode.Projections), len(outNames))
	}
	// Types for computed outputs have to be decided here: the output column
	// does not exist in the arm's schema, so the worker cannot resolve it
	// (same reason attachScanSelectProjections carries Type — #333).
	//
	// setOpArmDecls rather than InputColDecls: this is the set operation's own
	// view of the arm, with a JOIN's two sides kept apart under their
	// qualified names (#551) and a derived table's Project descended into
	// (#554). Its DECIMAL (p,s) rides in the same ColDecls as the TypeID, so
	// the type and the scale are read out of ONE resolved key and cannot come
	// to describe different columns.
	var colTypes ColDecls
	var strictInt map[string]bool
	var below *logical.Node
	if len(projNode.Children) == 1 {
		below = projNode.Children[0]
		colTypes = setOpArmDecls(below)
		// Same integer-preserving-arithmetic hint as
		// attachScanSelectProjections (#297, #445).
		strictInt = strictIntArithCols(below)
	}
	plan := SetOpArmPlan{
		Specs: make([]ProjectExprSpec, 0, len(outNames)),
		Types: make([]SetOpColType, 0, len(outNames)),
	}
	for i, pr := range projNode.Projections {
		if pr.IsAgg {
			return SetOpArmPlan{}, fmt.Errorf(
				"selects the aggregate %q, whose output the arm's aggregate stage names for itself — "+
					"the union stage cannot project the SELECT list over it", pr.Expr)
		}
		e := pr.Expr
		if e == "" {
			e = pr.Column
		}
		if e == "" {
			return SetOpArmPlan{}, fmt.Errorf("select item %d has neither an expression nor a column", i+1)
		}
		// The arm's SELECT list is written against the arm's OUTPUT schema,
		// and the arm's stream carries SOURCE names — a Project inside the
		// arm emits no stage, the convention every consumer compensates for.
		// The union stage is a consumer like the rest: without this,
		// `SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x UNION ALL
		// …` projected a column named `k` over a stream that carries
		// s_suppkey and the task failed loud with `column "k" does not exist
		// in the input schema` (#490).
		ast := pr.ASTExpr
		// forwardedComputed marks a bare reference that turned OUT to name a
		// derived table's COMPUTED column: the spec now carries an
		// expression, so the worker builds the output vector from the
		// declared type instead of copying a column (#554).
		forwardedComputed := false
		if below != nil {
			if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
				if sub, ok := substituteNestedRenameRefs(pr.ASTExpr, below); ok && sub != nil {
					ast = sub
					e = sub.String()
				}
			} else {
				// A reference that forwards a derived table's COMPUTED column
				// names nothing the arm's stream carries, so it has to become
				// the expression that builds it — but ONLY when the arm walk
				// can also TYPE that column. The union stage EVALUATES the
				// rewritten expression and builds the output vector from the
				// declared type, and a wrong declaration there is a silently
				// wrong column, where the un-rewritten name is a loud task
				// failure (#554).
				sub, rewritable := setOpArmComputedSource(e, below)
				_, typed := setOpRefDecl(colTypes, e, pr)
				if rewritable && typed && sub != nil {
					ast = sub
					e = sub.String()
					forwardedComputed = true
				} else if src := resolveOutputRenameSource(strings.ToLower(e), below); src != "" {
					e = src
				}
			}
		}
		spec := ProjectExprSpec{Expr: e, Name: outNames[i]}
		ct := SetOpColType{}
		if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
			if referencesSyntheticAgg(pr.ASTExpr) {
				return SetOpArmPlan{}, fmt.Errorf("select item %d references an aggregate the gather evaluates", i+1)
			}
			// A computed column's declared type IS its runtime type: the
			// worker builds the output vector from it.
			decl := inferProjectionDeclType(ast, parquet.TypeString, strictInt, colTypes)
			// A numeric LITERAL arm carries the (p,s) of its SPELLING, which
			// PostgreSQL reads as numeric and this walk otherwise read as
			// float8 — so `SELECT d FROM t UNION ALL SELECT 1.23456`
			// resolved double precision where PostgreSQL resolves numeric
			// (#665). setOpLitArm is scoped to this site on purpose; see its
			// own comment.
			//
			// The expression is REWRITTEN to the literal's plain text as a
			// quoted string, because the evaluator folds a numeric literal
			// into a float64 and `1234567890123456.78` is not one: declaring
			// DECIMAL over that box would put an exact type on a number that
			// is already rounded. SetValueChecked parses the text at the
			// column's scale with no float in between.
			if d, ok := setOpLitArm(ast); ok {
				decl = d.decl
				e = "'" + d.text + "'"
				spec.Expr = e
			}
			materialized := declTypeParts(decl)
			spec.Type, spec.Precision, spec.Scale, spec.Fields = materialized.Type, materialized.Precision, materialized.Scale, materialized.Fields
			spec.TypeKnown = true
			ct = SetOpColType{Typ: spec.Type, Known: true, Fields: materialized.Fields}
			if decl.ID == parquet.TypeDecimal && decl.DecKnown {
				// A computed DECIMAL arm now knows its own (p,s), so the
				// arms reconcile through the ordinary rule instead of
				// leaving every arm as written — the #551 silent channel
				// (ADR-0024 item 2).
				ct.Dec = logical.DecimalMeta{Precision: decl.Precision, Scale: decl.Scale}
				ct.DecKnown = true
			}
		} else if c, ok := setOpRefDecl(colTypes, e, pr); ok {
			// A bare reference copies its source column, so the source's
			// type is what the arm emits. spec.Type stays unset: the worker
			// resolves a plain ColRef by DirectCopy and ignores it — unless
			// the reference was rewritten into the derived table's computed
			// EXPRESSION above, which the worker has to evaluate and
			// therefore has to be told the type of.
			ct = c
			if forwardedComputed {
				spec.Type = ct.Typ
				spec.Fields = ct.Fields
				spec.TypeKnown = true
				if ct.DecKnown {
					spec.Precision, spec.Scale = ct.Dec.Precision, ct.Dec.Scale
				}
			}
		} else if cr, isRef := bareColRefOf(pr.ASTExpr); isRef && colTypes.isFieldPath(cr) {
			// A ROW FIELD PATH is not the bare reference it looks like: `rd.d`
			// names no column of anything, so the lookups above all miss and
			// the arm came back untyped — which for a DECIMAL field beside a
			// DECIMAL column is #551's channel with the disagreement one level
			// in. The FIELD's declaration answers, on exactly the terms
			// ColDecls.colDecl resolves it (ADR-0022), and the spec carries the
			// type because nothing downstream resolves a field path by name:
			// it is MATERIALIZED the way a computed expression is.
			if fc, ok := colTypes.field(cr); ok {
				ct = SetOpColType{Typ: fc.Type, Known: true, Fields: fc.Fields}
				spec.Type = fc.Type
				spec.Fields = fc.Fields
				spec.TypeKnown = true
				if fc.Type == parquet.TypeDecimal && fc.Precision > 0 {
					ct.Dec = logical.DecimalMeta{Precision: fc.Precision, Scale: fc.Scale}
					ct.DecKnown = true
					spec.Precision, spec.Scale = fc.Precision, fc.Scale
				}
			}
		}
		plan.Specs = append(plan.Specs, spec)
		plan.Types = append(plan.Types, ct)
	}
	return plan, nil
}

// setOpRefDecl tries the SELECT list's own spelling before the resolved SOURCE:
// derived arms declare EMITTED names, and swapped aliases must not capture the
// wrong source (#554). Try qualifier-aware/stripping lookup for each candidate
// (#533); qualified keys distinguish join sides (#551). Read TypeID and DECIMAL
// (p,s) from the SAME resolved key (ADR-0024). If neither spelling resolves,
// leave the column untyped.
func setOpRefDecl(decls ColDecls, resolved string, pr logical.Projection) (SetOpColType, bool) {
	for _, cand := range []string{pr.Expr, pr.Column, resolved, pr.Alias} {
		if cand == "" {
			continue
		}
		key, ok := lookupColKey(decls.Types, cand)
		if !ok {
			continue
		}
		d := declFromKey(decls, key)
		ct := SetOpColType{Typ: d.ID, Known: true, Fields: declTypeParts(d).Fields}
		if d.ID == parquet.TypeDecimal && d.DecKnown && d.Precision > 0 {
			ct.Dec = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
			ct.DecKnown = true
		}
		return ct, true
	}
	return SetOpColType{}, false
}

// setOpAnyDecimalArm reports whether any arm of one result column resolved to
// DECIMAL. It is what makes an UNTYPED sibling arm a refusal rather than a
// shrug: two arms nothing typed are the pre-existing "leave it alone" case and
// carry no scale to disagree about, while a typed DECIMAL beside an untyped
// arm is the reinterpretation #551 is about.
// setOpArmIsUnknownLit reports whether arm i's select item at this column is an
// UNKNOWN-typed literal.
func setOpArmIsUnknownLit(unknown [][]bool, i, col int) bool {
	return unknown != nil && i < len(unknown) && unknown[i] != nil &&
		col < len(unknown[i]) && unknown[i][col]
}

// setOpTargetType folds one result column's arms into the type they must all
// emit. allKnown is false when some arm carries no type at all, which is the
// caller's signal to leave the column alone.
func setOpTargetType(plans []SetOpArmPlan, col int, name, op string, unknown [][]bool) (SetOpColType, bool, error) {
	var want SetOpColType
	allKnown := true
	for i, plan := range plans {
		if setOpArmIsUnknownLit(unknown, i, col) {
			continue // an unknown literal takes the other arms' type
		}
		ct := plan.Types[col]
		if !ct.Known {
			allKnown = false
			continue
		}
		if !want.Known {
			want = ct
			continue
		}
		widened, ok := setOpWiden(want.Typ, ct.Typ)
		if !ok {
			if setOpNoCommonType(want.Typ, ct.Typ) {
				// PostgreSQL's own 42804. SetOpArmTypeConflict raises the same
				// refusal before any stage is emitted and the single-process
				// path calls it too, so one query takes one answer; this is
				// the backstop for a shape that reaches here without it.
				return SetOpColType{}, false, setOpTypeMismatch(op, name, want.Typ, ct.Typ)
			}
			// A pair PostgreSQL DOES match, on a ladder that does not reach it
			// — PORT/PROTOCOL beside an integer, DURATION beside a bigint, two
			// members of the inet family, DATE beside a TIMESTAMP. This engine
			// cannot concatenate the two .wshf files, so the DAG still refuses;
			// the message says so rather than claiming PostgreSQL would.
			// The same carrier refusal SetOpArmTypeConflict raises, which runs
			// ahead of this walk on both paths; this is the backstop for a
			// shape that reaches here without it.
			return SetOpColType{}, false, setOpCarrierGap(name, want.Typ, ct.Typ)
		}
		want = SetOpColType{Typ: widened, Known: true, Fields: want.Fields}
	}
	if want.Known && want.Typ == parquet.TypeDecimal && allKnown {
		arms := make([]SetOpColType, 0, len(plans))
		for i, plan := range plans {
			if setOpArmIsUnknownLit(unknown, i, col) {
				// It contributes no type, so it contributes no (p,s) either;
				// counting its STRING here made the target unresolvable and
				// refused a union PostgreSQL answers as numeric.
				continue
			}
			arms = append(arms, plan.Types[col])
		}
		want.Dec, want.DecKnown = setOpDecimalTarget(arms)
	}
	return want, allKnown, nil
}

// setOpNodeResultTypes is the per-column type a nested set-operation node
// emits: exactly what reconcileSetOpArmTypes will make ITS arms agree on,
// computed without emitting anything. nil when the shape is one this walk
// cannot type, which the caller reads as "unknown", the answer it had before.
func setOpNodeResultTypes(n *logical.Node) []SetOpColType {
	names := setOpOutputNames(n)
	if len(names) == 0 || len(n.Children) < 2 {
		return nil
	}
	plans := make([]SetOpArmPlan, 0, len(n.Children))
	unknown := make([][]bool, 0, len(n.Children))
	for _, child := range n.Children {
		plan, err := setOpArmProjection(child, names)
		if err != nil {
			return nil
		}
		plans = append(plans, plan)
		// The UNKNOWN-literal mask travels into the nested node too. Without
		// it this walk counted a quoted literal's STRING as a type of its own
		// and returned "unknown" for the whole nested result — so
		// `SELECT '1.5' … UNION ALL SELECT a … UNION ALL SELECT a …`, whose
		// arms nest as (literal ∪ a) ∪ a, reached reconcileSetOpArmTypes with
		// an untyped arm beside a DECIMAL one and was REFUSED, while the
		// single-process path answered it. PostgreSQL answers it numeric.
		unknown = append(unknown, setOpUnknownLiteralArms(child, len(names)))
	}
	out := make([]SetOpColType, len(names))
	for col := range names {
		want, allKnown, err := setOpTargetType(plans, col, names[col], setOpBaseName(n), unknown)
		if err != nil || !allKnown {
			continue
		}
		out[col] = want
	}
	return out
}

// setOpWiden resolves INT32 → INT64 → DECIMAL → FLOAT32 → FLOAT64,
// independent of arm order. Both float types beat exact numeric types; only
// FLOAT64 beats FLOAT32. Keep REAL's separate rung: widening its stored value
// to double changes its rendering as well as its OID. FLOAT in DDL is FLOAT32.
// See docs/internals/set-operation-numeric-widening.md for the design.
func setOpWiden(a, b parquet.TypeID) (parquet.TypeID, bool) {
	if a == b {
		return a, true
	}
	rank := func(t parquet.TypeID) int {
		switch t {
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
			// PORT and PROTOCOL declare int4 on the wire (#834), so
			// PostgreSQL sees `c_port ∪ c_i32` as an int4 union and
			// `c_port ∪ c_i64` as bigint. Measured live on 17.11.
			return 1
		case parquet.TypeInt64, parquet.TypeDuration:
			// DURATION declares int8.
			return 2
		case parquet.TypeDecimal:
			return 3
		case parquet.TypeFloat32:
			return 4
		case parquet.TypeFloat64:
			return 5
		}
		return 0
	}
	ra, rb := rank(a), rank(b)
	if ra == 0 || rb == 0 {
		return 0, false
	}
	switch {
	case ra == 5 || rb == 5:
		return parquet.TypeFloat64, true
	case ra == 4 || rb == 4:
		return parquet.TypeFloat32, true
	case ra == 3 || rb == 3:
		return parquet.TypeDecimal, true
	}
	// Two members of the int4 family that are not the same type — a PORT
	// beside an INT32, a PORT beside a PROTOCOL — resolve to INT64 here where
	// PostgreSQL resolves int4. The VALUE is the same in either (no integer
	// this engine stores in an int4 carrier is outside int8); what differs is
	// the declared width, and this engine has no CAST spelling that produces
	// an INT32 carrier, so declaring int4 would put the type on a box that is
	// not one. Recorded in ADR-0012 item 12 as a width divergence.
	return parquet.TypeInt64, true
}
