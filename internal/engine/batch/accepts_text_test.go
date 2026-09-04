package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// VectorAcceptsText is a statement ABOUT SetValue, so it is checked against
// SetValue rather than maintained beside it: every one of the 22 types gets a
// string written into a vector of that type, and the answer — stored, or the
// #361 guard — is compared with what the function claims.
//
// A hand-kept list of types is exactly the shape that cost this arc a review
// round: the first draft of the set operation's common-type rule exempted a
// pair by a list somebody wrote down, and it refused 21 shapes PostgreSQL
// answers. A list nobody writes down cannot drift.
func TestVectorAcceptsTextIsWhatSetValueDoes(t *testing.T) {
	for _, typ := range []TypeID{
		TypeBool, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64, TypeString,
		TypeBytes, TypeTimestamp, TypeIPv4, TypeIPv6, TypeCIDR, TypeMAC,
		TypePort, TypeProtocol, TypeDuration, TypeUUID, TypeDate, TypeDecimal,
		TypeArray, TypeRow, TypeMap, TypeVector,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			got := setValueTakesText(t, typ)
			if want := VectorAcceptsText(typ); got != want {
				t.Errorf("VectorAcceptsText(%s) = %v, but SetValueChecked %s a string — "+
					"the function has drifted from the writer it describes",
					typ, want, map[bool]string{true: "STORES", false: "refuses"}[got])
			}
		})
	}
}

// setValueTakesText writes a string into a one-row vector of typ and reports
// whether it was stored. The #361 guard is a PANIC carrying a typed error, so
// it is recovered here — any other panic is re-raised, since only the guard is
// an answer to the question being asked.
func setValueTakesText(t *testing.T, typ TypeID) (ok bool) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, isGuard := r.(*TypeMismatchError); isGuard {
			ok = false
			return
		}
		panic(r)
	}()
	// A container vector with no child returns from SetValue before it can
	// look at the value at all, so the probe has to hand each one a real
	// child or it would measure the constructor rather than the writer.
	var v *Vector
	switch typ {
	case parquet.TypeVector:
		v = NewVectorVector(1, 4)
	case parquet.TypeArray:
		v = NewArrayVector(1, TypeInt64)
	case parquet.TypeRow:
		v = NewRowVector(1, []string{"f"}, []TypeID{TypeInt64})
	case parquet.TypeMap:
		v = NewMapVector(1, TypeString, TypeInt64)
	default:
		v = NewVector(typ, 1)
	}
	if err := v.SetValueChecked(0, "1"); err != nil {
		// An error rather than a panic is a VALUE the type could not take,
		// which is still a text arm: the writer looked at the text.
		return true
	}
	return true
}
