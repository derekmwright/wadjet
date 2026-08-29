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
}

// DeclNumericLit builds the declaration of a numeric LITERAL: the type it
// declares on its own (INT64 for integer digits, FLOAT64 otherwise — ADR-0024's
// recorded deferral) plus the exact fixed-point (p,s) of its spelling, which is
// what a DECIMAL fold over it resolves against.
func DeclNumericLit(id batch.TypeID, text string) DeclType {
	d := DeclType{ID: id}
	if t, ok := LiteralChoiceDecimalType(text); ok {
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

// SameAsArgs reports the argument positions a polymorphic declaration mirrors,
// for a call with nargs arguments, and whether the declaration is polymorphic
// at all.
//
// It exists so PostgreSQL's select_common_typmod runs over exactly the
// arguments select_common_type ran over: NULLIF mirrors argument 0 alone, so
// NULLIF(numeric(9,2), numeric(18,4)) keeps numeric(9,2) while
// GREATEST over the same pair drops to unconstrained. Reading the candidate
// list off the declaration is what keeps those two answers from drifting
// apart (ADR-0024 item 5).
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
// The first decider still wins, which is what it always did — with one
// exception, and it is the whole reason this function exists rather than a
// `return decided[0]`. A DECIMAL is not a type on its own: COALESCE over
// DECIMAL(9,2) and DECIMAL(18,4) has to answer a type that holds BOTH, or the
// narrower declaration truncates the wider argument's digits on the way into
// the output vector. So when every decider is a DECIMAL with a known (p,s),
// they are folded through batch.DecimalCommon — the same rule a set operation
// reconciles its arms with (ADR-0024 item 2).
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
// A DECIMAL beside a FLOAT stays float8, which is PostgreSQL's rule (float8 is
// the preferred type of the numeric category) and is what the first non-DECIMAL
// decider already answered.
func CommonDeclType(decided []DeclType, sawUnknown bool) (DeclType, bool) {
	if len(decided) == 0 {
		return DeclType{}, false
	}
	metas := make([]batch.DecimalType, 0, len(decided))
	sawDecimal := false
	for _, d := range decided {
		m, isDec, ok := declFixedPoint(d)
		if !ok {
			// Not a fold this rule can make: a float, a string, a date. The
			// first non-DECIMAL decider answers, exactly as it did before a
			// DECIMAL column reference could decide anything at all.
			for _, alt := range decided {
				if alt.ID != batch.TypeDecimal {
					return alt, true
				}
			}
			return decided[0], true
		}
		sawDecimal = sawDecimal || isDec
		metas = append(metas, m)
	}
	if !sawDecimal {
		// Every decider is an integer or a numeric literal and none is a
		// DECIMAL: `COALESCE(i, 0)` is integer in PostgreSQL and stays INT64
		// here, and `COALESCE(0.5, 1.5)` keeps the FLOAT64 a bare numeric
		// literal declares (ADR-0024's deferral — a literal's OWN declaration
		// is not this change's business).
		return decided[0], true
	}
	if sawUnknown {
		return DeclType{}, false
	}
	m, ok := batch.DecimalCommon(metas)
	if !ok {
		return decided[0], true
	}
	return DeclDecimal(m.Precision, m.Scale), true
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
		// A numeric LITERAL at its own spelling.
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

// FuncReturnsInteger reports whether a registered function always returns an
// integer, for the planner's declared-type layer. It is the AST-side twin of
// isIntNative's registry lookup, so the DECLARED type of `length(s) / 2` is
// the INT64 the runtime actually produces (#636).
func FuncReturnsInteger(name string) bool {
	return DefaultRegistry.ReturnType(name).Integer()
}
