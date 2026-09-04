package expr

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// DeclType is a resolved declared type: the vector type a value can be stored
// in, plus — for a DECIMAL — the (precision, scale) a bare TypeID cannot
// express.
//
// It is ONE shape, deliberately, and it is the shape the whole declared-type
// inference layer speaks: expr.Ret.Resolve here, and physical's
// nodeDeclaredType / colRefDeclaredType / funcReturnType / caseDeclaredType /
// windowSpecOutputType / declaredProjectionDecl on the planner side. Before
// ADR-0024 that layer was (batch.TypeID, Confidence) with no room for (p,s),
// so colRefDeclaredType answered Undecided for every DECIMAL column and
// everything downstream fell to its non-DECIMAL default — which is #529
// (GREATEST/LEAST over DECIMAL), #555 (COALESCE), #586/#587 (window) and
// #542 (set operations), one defect wearing five hats.
//
// DecKnown distinguishes a resolved (p,s) from the zero value, which a
// COMPUTED decimal legitimately has none of (#458) — the same shape
// ProjectExprSpec.TypeKnown and AggSpec.OutputTypeKnown carry, and for the
// same reason: precision 0 is a sentinel a caller must not take at face
// value.
type DeclType struct {
	ID        batch.TypeID
	Precision int
	Scale     int
	DecKnown  bool
	// Untyped marks SQL's `unknown`: an operand that names no type AND
	// produces no value of its own — a bare NULL literal, and nothing else.
	// It rides alongside Undecided because the two are not the same fact and
	// CommonDeclType has to tell them apart: an operand that decided nothing
	// but WILL produce a value at runtime (a scalar subquery, element_at over
	// a container) makes a DECIMAL fold unsafe, because that value arrives at
	// ITS OWN scale and the fold would declare a different one; a NULL never
	// arrives at all, so COALESCE(d, NULL) is a DECIMAL expression exactly as
	// PostgreSQL says it is.
	Untyped bool

	// Exact is the fixed-point (p,s) a NON-DECIMAL numeric operand
	// contributes to a DECIMAL fold, and ExactSet says it has one. It is
	// carried apart from Precision/Scale on purpose: those two ARE the
	// declaration when ID is DECIMAL, and declTypeParts writes them into
	// projection, sort-key and window-key specs, where a precision on an
	// INT64 column would be read as a DECIMAL's.
	//
	// The one operand that needs it is a numeric LITERAL, whose own
	// declaration is INT64 or FLOAT64 (`SELECT 1.5` is a double — ADR-0024's
	// recorded deferral) while its fixed-point contribution is its SPELLING:
	// `0` is DECIMAL(1,0) and `0.5` is DECIMAL(1,1). That is the whole
	// difference between `CASE … THEN d ELSE 0.5 END`, which PostgreSQL types
	// numeric, and `CASE … THEN d ELSE f END` over a FLOAT COLUMN, which it
	// types double precision: both branches declare FLOAT64 here, and only
	// this says which of them is an exact number the user wrote.
	//
	// An INTEGER COLUMN needs no field — its contribution is its whole range
	// at scale 0, a function of the TypeID alone (batch.DecimalTypeOf).
	Exact    batch.DecimalType
	ExactSet bool

	// Lit marks a declaration that came from a CONSTANT rather than from a
	// column, a cast or a computed expression, and FoldID is the type
	// PostgreSQL resolves that constant to inside select_common_type — which
	// is NOT the type it declares on its own here (a bare numeric literal
	// declares INT64 or FLOAT64, ADR-0024's recorded deferral, while
	// PostgreSQL calls `0` an integer and `1.5` a numeric).
	//
	// The two are separate because only the FOLD needs PostgreSQL's rung.
	// `CASE … THEN i32_col ELSE 0 END` is `integer` there, and reading the
	// literal at its own INT64 declaration would widen the call to bigint —
	// a divergence in the OID for a shape TPC-H is full of. FoldID is zero
	// when this layer cannot name the constant's rung, and the ID then
	// stands: that is the wide-literal deferral #555 records.
	Lit    bool
	FoldID batch.TypeID
	// Quoted marks SQL's `unknown`: a QUOTED string literal.
	//
	// PostgreSQL types one `unknown` and resolves it FROM the other operands,
	// so it contributes NO rung to a polymorphic call's fold and is coerced
	// to whatever that fold resolves. Typing it a DECIDED string put a
	// non-numeric decider in every call that held one, CommonDeclType could
	// not fold, and the call fell back to its FIRST argument — a declaration
	// NARROWER than the value the call produces, which the output vector then
	// wrapped rather than narrowed: `GREATEST(bigint, real, double, '1e39')`
	// is double precision in PostgreSQL and was int64's MINIMUM here (#724).
	//
	// The ID stays TypeString, because that IS the answer when nothing else
	// decides: PostgreSQL resolves a composite whose every argument is a
	// quoted literal to `text`, and `SELECT 'x'` is a text column.
	Quoted bool
}

// DeclNumericLit builds the declaration of a numeric LITERAL: the type it
// declares on its own (INT64 for integer digits, FLOAT64 otherwise — ADR-0024's
// recorded deferral) plus the exact fixed-point (p,s) of its spelling, which is
// what a DECIMAL fold over it resolves against.
func DeclNumericLit(id batch.TypeID, text string) DeclType {
	d := DeclType{ID: id, Lit: true}
	if t, ok := LiteralChoiceDecimalType(text); ok {
		d.Exact, d.ExactSet = t, true
		// FoldID rides on the SAME qualification as Exact, deliberately: it
		// can put the fold on the DECIMAL rung, and a spelling whose box
		// already lost digits must not do that. `GREATEST(numeric(18,4),
		// 493827160549382.7160549350)` would then declare DECIMAL(18,4) and
		// store the ROUNDED double as if it were exact — a plausible wrong
		// number in place of a recorded one, which the deferral pinned by
		// TestWideNumericLiteralInAChoiceStaysFloat exists to keep visible.
		// Without a rung the literal folds at its own FLOAT64 declaration,
		// which is where that shape was.
		if t, ok := NumericConstTypeOfText(text); ok {
			d.FoldID = t
		}
	}
	return d
}

// DeclQuotedLit is a QUOTED string literal's declaration: SQL's `unknown`.
//
// It names TypeString — which is what the literal is when nothing else in the
// expression names a type, and what `SELECT 'x'` must allocate — and marks
// itself Quoted so a polymorphic fold resolves it FROM its neighbours the way
// PostgreSQL does, instead of letting it decide the whole call's type (#724).
//
// It carries the spelling's fixed-point (p,s) for the same reason a numeric
// literal does: a fold that lands on DECIMAL has to declare a width that holds
// the literal too, or the value the call produces on the rows the literal wins
// does not survive the store.
func DeclQuotedLit(text string) DeclType {
	d := DeclType{ID: batch.TypeString, Lit: true, Quoted: true}
	if t, ok := QuotedLitDecimalType(text); ok {
		d.Exact, d.ExactSet = t, true
	}
	return d
}

// Decl builds a declaration for a type that needs no parameters.
func Decl(t batch.TypeID) DeclType { return DeclType{ID: t} }

// DeclUntyped is SQL's `unknown`: a NULL literal, which contributes no type
// and produces no value. Answered with Undecided confidence, like anything
// else that names no type.
func DeclUntyped() DeclType { return DeclType{Untyped: true} }

// DeclDecimal builds a DECIMAL declaration with its (precision, scale).
func DeclDecimal(prec, scale int) DeclType {
	return DeclType{ID: batch.TypeDecimal, Precision: prec, Scale: scale, DecKnown: true}
}

// Dec returns the (precision, scale) as the rules in batch take them.
func (d DeclType) Dec() batch.DecimalType {
	return batch.DecimalType{Precision: d.Precision, Scale: d.Scale}
}

// String renders the declaration the way a type is written in SQL: the type
// name, with a DECIMAL's (p,s) when it has one.
func (d DeclType) String() string {
	if d.ID == batch.TypeDecimal && d.DecKnown {
		return fmt.Sprintf("%s(%d,%d)", d.ID, d.Precision, d.Scale)
	}
	return d.ID.String()
}

// Ret is a scalar function's declared return type: the vector type its results
// can be stored in. It is declared where the function is registered, and the
// planner types a projection from the same declaration the kernel writes
// through.
//
// Before this existed the two halves lived apart — a function was registered in
// this package while its return type was asserted by a hand-maintained name
// list in the physical planner (isNumericFunc). A function missing from that
// list was typed String, so the projection allocated a Bytes output vector and
// the function's vec kernel wrote Float64Data/BoolData off the end of a
// zero-length slice, killing the server process for every connection. That
// happened four times (temporal extractors, vector distances, the length
// family, and starts_with/contains/ends_with) before the list was replaced by
// this declaration (#310).
//
// The zero value is *undeclared* and the registry refuses it: Register's
// signature makes a missing declaration a compile error, and the zero value
// makes a field-named literal that skips it a panic at init.
type Ret struct {
	kind retKind
	typ  batch.TypeID
	// args lists the candidate argument positions a polymorphic return
	// mirrors, in preference order. Empty means every argument.
	args []int
	// ctrl lists argument positions that STEER the choice without ever
	// becoming its value — IF's condition, and nothing else today. They are
	// evaluated, so they are not absent, but they cannot arrive in the
	// output vector and therefore cannot make a DECIMAL fold unsafe. Every
	// other non-candidate argument can: NULLIF's argument 1 is compared
	// rather than returned and still has to be seen, because the pair's
	// COMPARISON is only correct when both declarations are known.
	ctrl []int
	// typeAll widens the TYPE fold to every argument while args keeps the
	// TYPMOD fold narrow. NULLIF is the only declaration that needs it, and
	// it needs it because PostgreSQL answers the two questions over different
	// lists: select_common_type over both arguments (they must be comparable,
	// so `NULLIF(0, numeric)` is numeric), select_common_typmod over the one
	// the value comes from (so the pair keeps argument 0's numeric(9,2)).
	typeAll bool
	// opResolved marks a declaration whose type comes from the comparison
	// OPERATOR its two arguments select rather than from select_common_type
	// over them. NULLIF is the only one; see operatorResolvedType, where the
	// twelve measured rows that separate the two rules are written down.
	opResolved bool
}

// TypeOverAllArgs lets the TYPE fold reach an argument the TYPMOD fold does
// not, when that argument is a DECIMAL the candidate list's answer cannot
// hold. See Ret.typeAll and widenToDecimalBeyondCandidates.
func (r Ret) TypeOverAllArgs() Ret {
	r.typeAll = true
	return r
}

// typeFromNonCandidates is the other half of the NULLIF correction, and it is
// as narrow as its sibling below: it fires ONLY when every candidate is a
// quoted literal, so the candidate list named `unknown` and nothing else.
//
// `NULLIF('0xC.C', real_col)` is real in PostgreSQL — the type comes from the
// operator the two arguments select, and an unknown-typed literal selects
// nothing — and it answers 12.75, the hex-float grammar's reading of that
// spelling. Wadjet folded over the candidate list alone, so argument 0's
// `unknown` answered for the call, the projection allocated a text vector and
// the literal's own characters went out unread. GREATEST and LEAST never had
// the gap: their candidate list is every argument, so the column is in it.
//
// It is not a general "fold every argument" rule, for the same two reasons the
// DECIMAL widening below is not: that would widen `NULLIF(numeric(9,2),
// numeric(18,4))` to (18,4) for a result that is always argument 0's value,
// and it would re-open the Guessed/Decided contract of #331/#333.
func (r Ret) typeFromNonCandidates(d DeclType, seen []DeclType, conf []Confidence, nargs int) (DeclType, bool) {
	if !r.typeAll || !d.Quoted {
		return DeclType{}, false
	}
	// The quoted literal rides along. It still names no rung — CommonDeclType
	// skips it for that — but a fold that lands on DECIMAL has to declare a
	// (p,s) wide enough for the literal's own spelling, or the value the call
	// returns is the one digit-shortened on the way into the vector:
	// `NULLIF('12.750000000000000001', numeric(15,2))` answers the literal on
	// every row and PostgreSQL keeps all twenty digits.
	others := []DeclType{d}
	for i := 0; i < nargs && i < len(seen); i++ {
		if r.isCandidate(i, nargs) || r.isControl(i) || conf[i] != Decided {
			continue
		}
		others = append(others, seen[i])
	}
	if len(others) == 1 {
		return DeclType{}, false
	}
	out, ok := CommonDeclType(others, false)
	if !ok || out.Quoted {
		return DeclType{}, false
	}
	return out, true
}

// operatorResolvedType is NULLIF's own rule, and it is not select_common_type
// (#757).
//
// PostgreSQL types NULLIF from the `=` OPERATOR its two arguments select, and
// the answer is that operator's LEFT input type. Measured live on 17, every
// row of it:
//
//	NULLIF(int4,  int8)     integer            <- argument 0, not the common type
//	NULLIF(int8,  int4)     bigint
//	NULLIF(int2,  int8)     smallint
//	NULLIF(float4,float8)   real               <- argument 0 again
//	NULLIF(float4,int4)     real
//	NULLIF(float4,numeric)  real
//	NULLIF(numeric,int4)    numeric
//	NULLIF(int4,  numeric)  numeric
//	NULLIF(int8,  numeric)  numeric
//	NULLIF(int4,  float4)   double precision   <- NOT real, and NOT argument 0
//	NULLIF(int4,  float8)   double precision
//	NULLIF(numeric,float4)  double precision   <- NOT real
//
// The last three are what makes this a separate rule rather than a fold:
// GREATEST and COALESCE over `(numeric, float4)` are BOTH `real` on the same
// server, because they run select_common_type and float4 wins that ladder.
// NULLIF has to find an operator, there is no `int4 = float4` or
// `numeric = float4`, so both sides coerce to the preferred type in the
// category — float8 — and the operator's left input is float8.
//
// So: within the integer family and within the float family, and whenever
// argument 0 is itself a float, the cross-type operator exists and argument 0's
// own width is the answer. An integer or a numeric compared against a FLOAT
// resolves to float8. Everything else is the ordinary ladder.
//
// Wadjet answered argument 0's type for ALL of these, because NULLIF's
// candidate list is [0]. The values agree on the census fixture, so this is an
// OID and typmod divergence today — and a wrong answer waiting, since a value
// only representable at the wider type would be narrowed into the output
// vector on the way out.
//
// It fires only for a declaration that names an operator-resolved pair
// (Ret.opResolved, set by NULLIF's registration alone) with exactly two
// arguments, both Decided, both numeric.
func (r Ret) operatorResolvedType(d DeclType, seen []DeclType, conf []Confidence, nargs int) (DeclType, bool) {
	if !r.opResolved || nargs != 2 || len(seen) < 2 {
		return DeclType{}, false
	}
	if conf[0] != Decided || conf[1] != Decided || seen[0].Quoted || seen[1].Quoted {
		return DeclType{}, false
	}
	a0, a1 := seen[0], seen[1]
	if !isNumericDecl(a0) || !isNumericDecl(a1) {
		return DeclType{}, false
	}
	if a0.ExactSet || a1.ExactSet {
		// A numeric LITERAL, whose own declaration is FLOAT64 here while its
		// fixed-point contribution is its SPELLING (ADR-0024's recorded
		// deferral, DeclType.Exact). PostgreSQL types `12.75` numeric, so
		// `NULLIF(numeric(15,2), 12.75)` is numeric there — and reading that
		// FLOAT64 as "argument 1 is a float" made it double precision, which
		// widened a real COLUMN in the COALESCE above it and printed 0.1 as
		// 0.10000000149011612. The ordinary ladder below already answers this
		// pair correctly, so decline and let it.
		return DeclType{}, false
	}
	switch {
	case isIntDecl(a0) && isIntDecl(a1):
		return a0, true
	case isFloatDecl(a0):
		return a0, true
	case isFloatDecl(a1):
		// argument 0 is an integer or a DECIMAL: no direct operator, both
		// coerce to the category's preferred type.
		return Decl(batch.TypeFloat64), true
	}
	// Both integer-or-DECIMAL with at least one DECIMAL: the ordinary ladder,
	// which is what CommonDeclType already answered over the candidates plus
	// the widening below. Returning d here would drop the DECIMAL, so decline
	// and let widenToDecimalBeyondCandidates answer.
	_ = d
	return DeclType{}, false
}

func isNumericDecl(d DeclType) bool {
	return isIntDecl(d) || isFloatDecl(d) || d.ID == batch.TypeDecimal
}

func isIntDecl(d DeclType) bool {
	return d.ID == batch.TypeInt32 || d.ID == batch.TypeInt64
}

func isFloatDecl(d DeclType) bool {
	return d.ID == batch.TypeFloat32 || d.ID == batch.TypeFloat64
}

// OperatorResolved marks a declaration whose type comes from the OPERATOR its
// arguments select rather than from select_common_type over them. NULLIF is
// the only such construct; see operatorResolvedType.
func (r Ret) OperatorResolved() Ret {
	r.opResolved = true
	return r
}

// widenToDecimalBeyondCandidates is the NULLIF correction, and it is
// deliberately the narrowest form of it.
//
// PostgreSQL resolves NULLIF's TYPE with select_common_type over BOTH
// arguments — they have to be comparable — while the RESULT is argument 0's
// value and the TYPMOD is argument 0's. Wadjet folded the type over the
// candidate list alone, so `NULLIF(0, numeric(9,2))` declared INT64 where
// PostgreSQL says numeric, and the integer 0 went out as an integer column.
//
// Folding EVERY argument into the type instead would be the general rule and
// it is not taken here, for two reasons that both cost answers. It would widen
// `NULLIF(numeric(9,2), numeric(18,4))` to (18,4), where the result is
// argument 0's value and (9,2) holds it exactly — a rendering of 12.7500 for a
// column that holds 12.75, which the corpus pins the other way. And it would
// re-open the Guessed/Decided contract of #331/#333, where a non-candidate
// argument deciding a type is exactly what must NOT displace the candidate's
// answer.
//
// So the widening fires only when the candidates produced a NON-DECIMAL type
// and some other evaluated argument DECIDED a DECIMAL: that is the one case
// where the candidate answer cannot represent the value the pair is compared
// at, and it is the case PostgreSQL's numeric ladder is about.
func (r Ret) widenToDecimalBeyondCandidates(d DeclType, seen []DeclType, conf []Confidence, nargs int) (DeclType, bool) {
	if !r.typeAll || d.ID == batch.TypeDecimal {
		return DeclType{}, false
	}
	widened := []DeclType{d}
	sawDecimal := false
	for i := 0; i < nargs && i < len(seen); i++ {
		if r.isCandidate(i, nargs) || r.isControl(i) || conf[i] != Decided {
			continue
		}
		if seen[i].ID != batch.TypeDecimal || !seen[i].DecKnown {
			continue
		}
		widened = append(widened, seen[i])
		sawDecimal = true
	}
	if !sawDecimal {
		return DeclType{}, false
	}
	return CommonDeclType(widened, false)
}

// isCandidate reports whether argument i is on the declaration's candidate
// list — every argument when the list is empty.
func (r Ret) isCandidate(i, nargs int) bool {
	if len(r.args) == 0 {
		return i >= 0 && i < nargs
	}
	for _, a := range r.args {
		if a == i {
			return true
		}
	}
	return false
}

// Control marks argument positions that steer a polymorphic choice without
// supplying its value. See Ret.ctrl.
func (r Ret) Control(args ...int) Ret {
	r.ctrl = args
	return r
}

type retKind uint8

const (
	retUndeclared retKind = iota
	retFixed
	retSameAsArg
	retDynamic
)

// The fixed declarations. These name the type the function's Go results are
// stored as, not the type SQL calls them: the date/time functions below return
// formatted strings, so they declare RetString.
var (
	RetBool      = Ret{kind: retFixed, typ: batch.TypeBool}
	RetInt32     = Ret{kind: retFixed, typ: batch.TypeInt32}
	RetInt64     = Ret{kind: retFixed, typ: batch.TypeInt64}
	RetFloat64   = Ret{kind: retFixed, typ: batch.TypeFloat64}
	RetString    = Ret{kind: retFixed, typ: batch.TypeString}
	RetBytes     = Ret{kind: retFixed, typ: batch.TypeBytes}
	RetArray     = Ret{kind: retFixed, typ: batch.TypeArray}
	RetMap       = Ret{kind: retFixed, typ: batch.TypeMap}
	RetTimestamp = Ret{kind: retFixed, typ: batch.TypeTimestamp}

	// RetVector is embed()'s declaration. The output *dimension* is a
	// separate, deliberately dynamic answer the registry already carried
	// before this type existed — see RegisterVecReturn / VecReturnDim.
	RetVector = Ret{kind: retFixed, typ: batch.TypeVector}

	// RetDynamic declares that only the value knows: element_at returns the
	// element type of its argument, json_extract whatever the document held.
	// The planner keeps its own fallback for these. It is an explicit
	// declaration, not an omission — a function whose vec kernel writes a
	// typed slice must never carry it.
	RetDynamic = Ret{kind: retDynamic}
)

// RetSameAsArg declares a polymorphic return: the type of the first listed
// argument the caller can decide, or fallback when it can decide none of them —
// and Resolve marks that fallback as a guess, so a CALLER with candidates of
// its own keeps looking rather than inheriting it (see Confidence).
// With no indices every argument is a candidate, which is what coalesce,
// greatest and least want; nullif mirrors argument 0 only.
func RetSameAsArg(fallback batch.TypeID, args ...int) Ret {
	return Ret{kind: retSameAsArg, typ: fallback, args: args}
}

// SameAsArgs reports the argument positions a polymorphic declaration mirrors
// for the TYPMOD fold, for a call with nargs arguments, and whether the
// declaration is polymorphic at all.
//
// It exists so PostgreSQL's select_common_typmod runs over the arguments the
// RESULT is resolved from: NULLIF's result is always argument 0's value, so
// NULLIF(numeric(9,2), numeric(18,4)) keeps numeric(9,2) while GREATEST over
// the same pair drops to unconstrained (ADR-0024 item 5).
//
// It is NOT the TYPE fold's candidate list, and conflating the two was a
// defect: PostgreSQL runs select_common_TYPE over BOTH of NULLIF's arguments —
// they have to be comparable — so `NULLIF(0, numeric(9,2))` is numeric there
// and was INT64 here. typeArgs is that list; see Ret.typeAll.
func (r Ret) SameAsArgs(nargs int) ([]int, bool) {
	if r.kind != retSameAsArg {
		return nil, false
	}
	if len(r.args) > 0 {
		return r.args, true
	}
	all := make([]int, nargs)
	for i := range all {
		all[i] = i
	}
	return all, true
}

// isControl reports whether argument i steers the choice without supplying
// its value. See Ret.ctrl.
func (r Ret) isControl(i int) bool {
	for _, c := range r.ctrl {
		if c == i {
			return true
		}
	}
	return false
}

// RetTypeOf builds a fixed declaration for a type without a named constant
// above. Kept for callers registering functions over the network-native types.
func RetTypeOf(t batch.TypeID) Ret { return Ret{kind: retFixed, typ: t} }

// Confidence says how a resolved type was arrived at: whether the declaration
// DECIDED it or only GUESSED it. A same-as-argument declaration has to answer
// even when none of its candidate arguments decided anything, and that answer
// — its fallback — is a guess. Reporting a guess as fact is what typed
//
//	SELECT COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') FROM nation
//
// Float64, so every row came back as the integer 0: nullif's argument 0 is a
// bare column, which decides nothing by design (its type comes from the input
// schema at runtime), so nullif fell back to its numeric default — and coalesce
// took that for a decision, stopped, and never consulted the string literal in
// argument 1 that would have decided it correctly (#331).
//
// The fallback itself is right where there is nothing better: NULLIF(int_col, 1)
// as a projection is numeric and stays numeric. What Confidence adds is that a
// caller holding another candidate can tell the two apart.
type Confidence uint8

const (
	// Undecided: nothing here names a type, and the caller keeps its own
	// fallback. RetDynamic answers this way, as does an unregistered name.
	Undecided Confidence = iota
	// Guessed: a polymorphic declaration reached its fallback because no
	// candidate argument decided. Still an answer — it is THE answer at top
	// level — but a caller with a candidate of its own left to ask must
	// prefer that candidate's decision over this.
	Guessed
	// Decided: the declaration names this type outright, or a candidate
	// argument decided it.
	Decided
)

func (c Confidence) String() string {
	switch c {
	case Guessed:
		return "GUESSED"
	case Decided:
		return "DECIDED"
	}
	return "UNDECIDED"
}

// Declared reports whether this is a real declaration rather than the zero
// value. Registration rejects an undeclared Ret.
func (r Ret) Declared() bool { return r.kind != retUndeclared }

// Resolve returns the output type for a call with nargs arguments, and how
// confidently. argType reports the type of argument i and how confidently the
// caller decided it; it is consulted only by polymorphic declarations and may
// be nil.
//
// Undecided means the caller should keep its own fallback: the function is
// RetDynamic, or the name is not registered at all.
//
// A polymorphic declaration takes the first candidate argument that DECIDED a
// type. A candidate that only guessed does not end the search — it is
// remembered, in preference order, and answered only if no later candidate
// decides. A guess stays a guess all the way up, so an argument that guessed at
// any depth never displaces an argument that knows.
func (r Ret) Resolve(nargs int, argType func(i int) (DeclType, Confidence)) (DeclType, Confidence) {
	switch r.kind {
	case retFixed:
		return DeclType{ID: r.typ}, Decided
	case retSameAsArg:
		if argType != nil {
			// ONE pass, and one argType call per argument. argType is a
			// recursive walk of the argument's whole expression tree on the
			// planner side, so asking twice — once for the candidate fold
			// and once for the safety check — cost 2^depth over nested
			// polymorphic calls: a linear-text COALESCE nested 22 deep took
			// 2.7 seconds to PLAN, and 30 deep would not have finished
			// (#555's review, round 4).
			seen := make([]DeclType, nargs)
			conf := make([]Confidence, nargs)
			sawUnknown := false
			for i := 0; i < nargs; i++ {
				t, c := argType(i)
				seen[i], conf[i] = t, c
				// sawUnknown over EVERY argument that can supply or be
				// COMPARED against the value — not over the candidate list.
				// The two are the same for coalesce/greatest/least and
				// different for nullif, whose candidate list is [0] while
				// argument 1 is the one it compares against: an operand this
				// layer cannot type produces a DECIMAL at its own scale
				// whether or not the result is taken from it, so
				// `NULLIF(a, (SELECT b …))` declared numeric(9,2) from
				// argument 0 alone and the runtime then compared "12.75"
				// against "12.7500" by bytes. A CONTROL argument is exempt:
				// it steers and never arrives.
				if c == Undecided && !t.Untyped && !r.isControl(i) {
					sawUnknown = true
				}
			}
			// The candidate fold runs in the declaration's PREFERENCE order,
			// which is not always positional: RetSameAsArg names the
			// positions it mirrors, first one wins, and a guess is kept in
			// that same order (#331).
			candidates := r.args
			if len(candidates) == 0 {
				candidates = make([]int, nargs)
				for i := range candidates {
					candidates[i] = i
				}
			}
			var decided []DeclType
			guess, guessed := DeclType{}, false
			for _, i := range candidates {
				if i < 0 || i >= nargs {
					continue
				}
				switch conf[i] {
				case Decided:
					decided = append(decided, seen[i])
				case Guessed:
					if !guessed {
						guess, guessed = seen[i], true
					}
				}
			}
			if d, ok := CommonDeclType(decided, sawUnknown); ok {
				if other, ok := r.typeFromNonCandidates(d, seen, conf, nargs); ok {
					return other, Decided
				}
				if op, ok := r.operatorResolvedType(d, seen, conf, nargs); ok {
					return op, Decided
				}
				if wider, ok := r.widenToDecimalBeyondCandidates(d, seen, conf, nargs); ok {
					return wider, Decided
				}
				return d, Decided
			}
			if guessed {
				return guess, Guessed
			}
		}
		return DeclType{ID: r.typ}, Guessed
	}
	return DeclType{ID: batch.TypeString}, Undecided
}

// CommonDeclType answers a polymorphic declaration from the argument types
// that DECIDED one. It is the shared rule for every construct that CHOOSES
// BETWEEN operands — COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST here, and
// CASE's branches in the physical planner, which calls this so the two can
// never disagree.
//
// ok=false means DECLINE: the caller must answer as if nothing had decided,
// which is what it did before a DECIMAL operand could decide anything.
//
// The NUMERIC deciders fold through PostgreSQL's select_common_type ladder —
// INT32 → INT64 → DECIMAL → FLOAT32 → FLOAT64 — and not through "the first
// decider wins", which is what this did until #724. The difference is a VALUE,
// not an OID: `GREATEST(bigint, real, double)` is double precision in
// PostgreSQL, and declaring it bigint from argument 0 does not narrow the
// double the call produces, it WRAPS it — 1e39 stored into an int64 vector is
// int64's MINIMUM, #462's failure mode. The ladder is verified live on
// postgres:17-alpine for every ordered pair of the six numeric widths and is
// the same one setOpWiden pins for set operations and joinFoldKinds runs over
// the compiled tree.
//
// A DECIMAL is not a type on its own: COALESCE over DECIMAL(9,2) and
// DECIMAL(18,4) has to answer a type that holds BOTH, or the narrower
// declaration truncates the wider argument's digits on the way into the output
// vector. So when the ladder lands on DECIMAL, every decider's fixed-point
// contribution is folded through batch.DecimalCommon — the same rule a set
// operation reconciles its arms with (ADR-0024 item 2).
//
// A QUOTED literal contributes NO rung. PostgreSQL types one `unknown` and
// resolves it from the other operands, which is exactly what DeclType.Quoted
// says here; a composite whose every argument is quoted is `text` there and
// answers TypeString here.
//
// sawUnknown is the safety clause and it is not optional. A branch that
// decided nothing still PRODUCES a value at runtime — a scalar subquery, a
// container element, anything this layer cannot type — and a DECIMAL one
// arrives as text at ITS OWN scale, not at the fold's. Folding only the
// branches that spoke declared DECIMAL(9,2) for
// `COALESCE(a, (SELECT MAX(b) FROM t))`, which then TRUNCATED the subquery's
// 12.7501 to 12.75 and, at the comparison sites, left the operand
// unclassifiable so the extremum was picked by BYTE order. A declined fold
// answers exactly what it answered before ADR-0024 — a loud mismatch or the
// STRING fallback — which is the only honest answer while the operand has no
// declaration to fold in.
//
// A DECIMAL beside an INTEGER — a column, or a numeric literal — resolves to
// numeric in PostgreSQL, and does here (#695, verified live on 17.11:
// `pg_typeof(CASE WHEN true THEN 1.5::numeric(15,2) ELSE 0 END)` is numeric,
// and so are COALESCE/GREATEST/LEAST/NULLIF over the same pair). The integer
// contributes its fixed-point form to the fold — its whole range at scale 0
// for a COLUMN, its own spelling for a LITERAL (DeclType.Exact) — and the
// value materializes through the exact-TEXT box every DECIMAL producer here
// answers with, never as the already-scaled carrier an integer box means to
// SetValue (ADR-0018 §4). That was the deferral this function carried until
// #695: `GREATEST(dec_col, 100)` declared INT64, answered 100 on every row the
// integer won, and failed at the #361 store guard on the first row the decimal
// won — data-dependent, which is why it could not stand.
//
// A DECIMAL beside a FLOAT is the float, which is PostgreSQL's rule (both
// float types are preferred in the numeric category, and only float8 beats
// float4) — in EITHER argument order now. `COALESCE(numeric, real)` answered
// real before #724 and `COALESCE(real, numeric)` answered real too, but
// `GREATEST(numeric(15,2), c_i64, real)` answered bigint, because the first
// non-DECIMAL decider was the bigint. The rows the DECIMAL arm wins hand over
// that branch's TEXT, which the float vector then has to read: choice_decimal.go
// does that at the box, so the declaration and the value agree (#555's float
// half).
func CommonDeclType(decided []DeclType, sawUnknown bool) (DeclType, bool) {
	if len(decided) == 0 {
		return DeclType{}, false
	}
	// A QUOTED literal is SQL's `unknown` and names no rung: PostgreSQL
	// resolves it FROM the others and coerces it to what they agree on. With
	// every argument quoted there is nothing to resolve from and the answer is
	// `text`, which is what decided[0] already is (DeclQuotedLit).
	typed := make([]DeclType, 0, len(decided))
	for _, d := range decided {
		if !d.Quoted {
			typed = append(typed, d)
		}
	}
	if len(typed) == 0 {
		return decided[0], true
	}
	if allLiterals(typed) {
		// Nothing but CONSTANTS. A constant's OWN declaration is ADR-0024's
		// recorded deferral — `SELECT 1` is a bigint here and an integer in
		// PostgreSQL — and with no typed operand there is nothing for the
		// fold to resolve it FROM, so the deferral stands exactly where it
		// was: `GREATEST(0.5, 1.5)` keeps the FLOAT64 a bare numeric literal
		// declares, along with the spelling an OUTER fold reads off it.
		return typed[0], true
	}
	rung, numeric := numericRungFold(typed)
	if !numeric {
		// Not a fold this rule can make: a string, a date, a bool, a mixture
		// of categories PostgreSQL would refuse outright. The first
		// non-DECIMAL decider answers, exactly as it did before a DECIMAL
		// column reference could decide anything at all.
		for _, alt := range typed {
			if alt.ID != batch.TypeDecimal {
				return alt, true
			}
		}
		return typed[0], true
	}
	if rung != batch.TypeDecimal {
		// INT32/INT64/FLOAT32/FLOAT64: the rung IS the declaration, and it
		// holds every operand's values by construction.
		return Decl(rung), true
	}
	metas := make([]batch.DecimalType, 0, len(decided))
	// The same list without the CONSTANTS. A literal's SCALE belongs in the
	// fold — ADR-0012 item 12's "scale = max over the arms" is the only choice
	// that moves no value, and `CASE … THEN numeric(9,2) ELSE 0.125 END` needs
	// scale 3 or the 0.125 has nowhere to go — but a literal that pushes the
	// fold PAST the carrier must not cost the query the rows it never
	// supplies. When the full fold does not fit, the constants drop out and
	// the DECLARED operands decide alone (expr.foldDecimalMetas, which the
	// runtime's decimalArmFold shares).
	declared := make([]batch.DecimalType, 0, len(decided))
	sawDecimal := false
	for _, d := range decided {
		m, isDec, ok := declFixedPoint(d)
		if !ok {
			continue
		}
		sawDecimal = sawDecimal || isDec
		metas = append(metas, m)
		if !d.Lit {
			declared = append(declared, m)
		}
	}
	if !sawDecimal && !fractionalLitTriggersFold(decided) {
		// The rung is DECIMAL because a numeric LITERAL put it there, no
		// operand is a genuine DECIMAL, and the literal is a WHOLE number —
		// `COALESCE(i32, 2)`. A constant CONTRIBUTES to the fold and does not
		// TRIGGER it (ADR-0024), so the integer declaration stands and the
		// runtime's choice fold declines in step.
		//
		// A FRACTIONAL literal beside a non-constant arm is the exception, and
		// fractionalLitTriggersFold is where it is decided for both sides.
		return typed[0], true
	}
	if sawUnknown {
		return DeclType{}, false
	}
	m, ok := foldDecimalMetas(metas, declared)
	if !ok {
		return typed[0], true
	}
	return DeclDecimal(m.Precision, m.Scale), true
}

// fractionalLitTriggersFold reports whether a numeric literal with a
// FRACTIONAL spelling, beside at least one arm that is not a constant, must
// put a choice construct on the DECIMAL rung.
//
// It is the one exception to "a constant contributes to the fold and never
// triggers it" (ADR-0024), and it is here because the alternative is a wrong
// VALUE rather than a wrong type. `LEAST(c_i64, 1.5)` is `numeric` on
// PostgreSQL 17.11 and answers 1.5; declaring it INT64 — which the integer
// rung's `typed[0]` did — builds an int64 vector, and the 1.5 the evaluator
// produces is TRUNCATED into it. Arithmetic over it then made the truncation
// worse: `LEAST(c_i64, 1.5) * 3` was 4 for the server's 4.5, and
// `(CASE … ELSE 1.5 END) * <int8 max>` was MinInt64 for an exact numeric
// (round-1 review, B3).
//
// Two conditions, and the second is what keeps the deferral this narrows:
//
//   - the literal's SPELLING has a non-zero scale. `COALESCE(i32, 2)` is
//     integer in PostgreSQL too and is untouched.
//   - at least one arm is NOT a constant. With every arm constant there is
//     nothing to resolve the literal against, and `GREATEST(-2.5, -7.5)` keeps
//     the FLOAT64 a bare numeric literal declares — ADR-0024's literal
//     deferral, unchanged, and the case CommonDeclType's allLiterals clause
//     returns before reaching here anyway.
//
// expr.decimalArmFold makes the identical call over the COMPILED arms, so the
// vector the plan builds and the box the runtime hands it stay one decision.
func fractionalLitTriggersFold(decided []DeclType) bool {
	frac, nonConst := false, false
	for _, d := range decided {
		if d.Lit {
			if d.ExactSet && d.Exact.Scale > 0 {
				frac = true
			}
			continue
		}
		nonConst = true
	}
	return frac && nonConst
}

// numericRungFold folds the deciders through PostgreSQL's select_common_type
// ladder over the numeric category — INT32 → INT64 → DECIMAL → FLOAT32 →
// FLOAT64 — and reports numeric=false when any decider is outside it.
//
// It is the DECLARED-type twin of expr.joinFoldKinds, which resolves the same
// composite over the COMPILED tree, and of physical.foldArgTypes, which
// resolves it over the AST for the literal refusal. All three run
// widerNumericType and must give one answer per composite: the comparison
// decides which argument wins, the refusal decides whether a literal beside
// them can be read at all, and the declaration decides the vector the winner is
// stored into. A declaration NARROWER than the fold does not narrow the value,
// it WRAPS it (#724).
//
// A CONSTANT is folded at PostgreSQL's rung for its spelling (DeclType.FoldID),
// not at the type it declares alone: `CASE … THEN i32 ELSE 0 END` is `integer`
// there, and reading the 0 at the INT64 a bare numeric literal declares here
// would widen the call to bigint. With no typed operand beside it a constant
// keeps its own declaration — `SELECT CASE WHEN … THEN 1 ELSE 2 END` is a
// bigint here and an integer in PostgreSQL, which is ADR-0024's literal
// deferral and not this function's business to reopen.
func numericRungFold(typed []DeclType) (batch.TypeID, bool) {
	out, have := batch.TypeBool, false
	for _, d := range typed {
		t, ok := declFoldRung(d)
		if !ok {
			return batch.TypeBool, false
		}
		if !have {
			out, have = t, true
			continue
		}
		out = widerNumericType(out, t)
	}
	return out, have
}

// declFoldRung is one decider's rung on that ladder.
func declFoldRung(d DeclType) (batch.TypeID, bool) {
	if d.ID == batch.TypeDecimal {
		// #458's unconstrained sentinel: a DECIMAL nobody could put a (p,s)
		// on is not a declaration to fold against.
		if !d.DecKnown {
			return batch.TypeBool, false
		}
		return batch.TypeDecimal, true
	}
	if d.Lit && d.FoldID != 0 {
		return d.FoldID, true
	}
	switch d.ID {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64:
		return d.ID, true
	}
	return batch.TypeBool, false
}

// allLiterals reports whether every decider came from a constant.
func allLiterals(typed []DeclType) bool {
	for _, d := range typed {
		if !d.Lit {
			return false
		}
	}
	return true
}

// declFixedPoint is one decider's contribution to a DECIMAL fold: the (p,s) it
// brings, whether it is a genuine DECIMAL rather than an integer wearing a
// fixed-point type, and whether it participates at all.
//
// The second return is what keeps `COALESCE(i, 0)` an integer expression: both
// operands answer a (p,s) — an integer IS DECIMAL(19,0) for a result-type
// computation (ADR-0024 item 2) — and only this tells the two shapes apart.
// physical.decimalArithOperand and expr.decimalChoiceArm draw the same line
// over the AST and over the compiled tree.
func declFixedPoint(d DeclType) (batch.DecimalType, bool, bool) {
	if d.ID == batch.TypeDecimal {
		if !d.DecKnown {
			// #458's unconstrained sentinel: a DECIMAL nobody could put a
			// (p,s) on is not a declaration to fold against.
			return batch.DecimalType{}, false, false
		}
		return d.Dec(), true, true
	}
	if d.ExactSet {
		// A LITERAL, quoted or not, at its own spelling.
		return d.Exact, false, true
	}
	m, ok := batch.DecimalTypeOf(d.ID, batch.DecimalType{})
	return m, false, ok
}

// Numeric reports whether the function always returns a number. It is the
// registry-backed replacement for the compiler's own hand-maintained numeric
// name list: a numeric call can be wrapped so it satisfies Float64Expr/
// Int64Expr and binary operators over it take the typed path.
func (r Ret) Numeric() bool {
	if r.kind != retFixed {
		return false
	}
	switch r.typ {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64:
		return true
	}
	return false
}

func (r Ret) String() string {
	switch r.kind {
	case retFixed:
		return r.typ.String()
	case retSameAsArg:
		if len(r.args) == 0 {
			return fmt.Sprintf("SAME AS ANY ARG (else %s)", r.typ)
		}
		return fmt.Sprintf("SAME AS ARG %v (else %s)", r.args, r.typ)
	case retDynamic:
		return "DYNAMIC"
	}
	return "UNDECLARED"
}

// Integer reports whether the function always returns an INTEGER — declared
// RetInt32 or RetInt64 — which is what makes arithmetic over its result
// integer arithmetic.
//
// PostgreSQL's `length(s) / 2` is integer division, so it is 2 for a
// five-character string and not 2.5 (#636). compileBinOp could not see that:
// it chose the arithmetic node from the operands' COMPILE-TIME shape and a
// function call had none, so `length(s) / 2` compiled to BinOpFloat64. The
// declaration is the shape it was missing — and reading it from the registry
// rather than from a name list is what keeps a function added later from
// silently answering a fraction.
//
// Only a FIXED declaration answers. A polymorphic one (RetSameAsArg) mirrors
// an argument whose type is not known until a batch arrives, so claiming
// integer for it at compile time would be a guess — and a WRONG int claim
// truncates every value it touches.
func (r Ret) Integer() bool {
	return r.kind == retFixed && (r.typ == batch.TypeInt32 || r.typ == batch.TypeInt64)
}

// Boolean reports whether a function always returns a BOOLEAN. Only a FIXED
// declaration answers, for Integer's reason: the operand-classification layer
// reads it to apply PostgreSQL's boolean input grammar to whatever the result
// is compared against (#628), and a wrong claim would apply that grammar to a
// value that is not a boolean.
func (r Ret) Boolean() bool {
	return r.kind == retFixed && r.typ == batch.TypeBool
}

// FuncReturnsInteger reports whether a registered function always returns an
// integer, for the planner's declared-type layer. It is the AST-side twin of
// isIntNative's registry lookup, so the DECLARED type of `length(s) / 2` is
// the INT64 the runtime actually produces (#636).
func FuncReturnsInteger(name string) bool {
	return DefaultRegistry.ReturnType(name).Integer()
}
