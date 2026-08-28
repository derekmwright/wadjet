package expr

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

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
func (r Ret) Resolve(nargs int, argType func(i int) (batch.TypeID, Confidence)) (batch.TypeID, Confidence) {
	switch r.kind {
	case retFixed:
		return r.typ, Decided
	case retSameAsArg:
		if argType != nil {
			candidates := r.args
			if len(candidates) == 0 {
				candidates = make([]int, nargs)
				for i := range candidates {
					candidates[i] = i
				}
			}
			guess, guessed := batch.TypeID(0), false
			for _, i := range candidates {
				if i < 0 || i >= nargs {
					continue
				}
				switch t, c := argType(i); c {
				case Decided:
					return t, Decided
				case Guessed:
					if !guessed {
						guess, guessed = t, true
					}
				}
			}
			if guessed {
				return guess, Guessed
			}
		}
		return r.typ, Guessed
	}
	return batch.TypeString, Undecided
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
