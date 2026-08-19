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
// argument the caller can decide, or fallback when it can decide none of them.
// With no indices every argument is a candidate, which is what coalesce,
// greatest and least want; nullif mirrors argument 0 only.
func RetSameAsArg(fallback batch.TypeID, args ...int) Ret {
	return Ret{kind: retSameAsArg, typ: fallback, args: args}
}

// RetTypeOf builds a fixed declaration for a type without a named constant
// above. Kept for callers registering functions over the network-native types.
func RetTypeOf(t batch.TypeID) Ret { return Ret{kind: retFixed, typ: t} }

// Declared reports whether this is a real declaration rather than the zero
// value. Registration rejects an undeclared Ret.
func (r Ret) Declared() bool { return r.kind != retUndeclared }

// Resolve returns the output type for a call with nargs arguments. argType
// reports the type of argument i and whether the caller could decide it; it is
// consulted only by polymorphic declarations and may be nil.
//
// ok=false means the caller should keep its own fallback: the function is
// RetDynamic, or the name is not registered at all.
func (r Ret) Resolve(nargs int, argType func(i int) (batch.TypeID, bool)) (batch.TypeID, bool) {
	switch r.kind {
	case retFixed:
		return r.typ, true
	case retSameAsArg:
		if argType != nil {
			candidates := r.args
			if len(candidates) == 0 {
				candidates = make([]int, nargs)
				for i := range candidates {
					candidates[i] = i
				}
			}
			for _, i := range candidates {
				if i < 0 || i >= nargs {
					continue
				}
				if t, ok := argType(i); ok {
					return t, true
				}
			}
		}
		return r.typ, true
	}
	return batch.TypeString, false
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
