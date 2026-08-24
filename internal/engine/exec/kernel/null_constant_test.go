package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A nil constant is not the column type's ZERO.
//
// ResolveFilterKernel coerced it into one on every arm — toInt64(nil) is 0,
// toString(nil) is "", toBool(nil) is false, toDateInt32(nil) is day 0 — so a
// comparison against a NULL literal was answered as a comparison against that
// zero: `WHERE c_i64 = NULL` returned the rows holding 0 (#450). SQL says a
// comparison with NULL is UNKNOWN and a WHERE admits only TRUE, so the kernel
// must select nothing, for every type and every operator.
//
// The check is per TYPE deliberately. The defect was uniform across the type
// switch, and a single-type test would let the next type added reintroduce it.

// nullConstTypes is every type ResolveFilterKernel has an arm for. A type
// missing an arm returns a nil kernel, which the caller reports as "no
// comparison kernel for type" — also not a wrong answer, and asserted below.
var nullConstTypes = []batch.TypeID{
	batch.TypeBool, batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32,
	batch.TypeFloat64, batch.TypeString, batch.TypeBytes, batch.TypeTimestamp,
	batch.TypeIPv4, batch.TypeIPv6, batch.TypeCIDR, batch.TypeMAC,
	batch.TypePort, batch.TypeProtocol, batch.TypeDuration, batch.TypeUUID,
	batch.TypeDate, batch.TypeDecimal,
}

var nullConstOps = []CompareOp{OpEq, OpNe, OpLt, OpLe, OpGt, OpGe}

// zeroValuedVector builds a 4-row vector left at its zero value, which is
// exactly the value the coerced constant used to match.
func zeroValuedVector(t *testing.T, typ batch.TypeID) *batch.Vector {
	t.Helper()
	v := batch.NewVector(typ, 4)
	if typ == batch.TypeString || typ == batch.TypeBytes || typ == batch.TypeIPv6 ||
		typ == batch.TypeCIDR || typ == batch.TypeUUID {
		for i := 0; i < 4; i++ {
			v.BytesData.Set(i, nil)
		}
	}
	return v
}

func TestFilterKernelAgainstNullConstantMatchesNothing(t *testing.T) {
	for _, typ := range nullConstTypes {
		for _, op := range nullConstOps {
			t.Run(typ.String()+"_"+opLabel(op), func(t *testing.T) {
				k := ResolveFilterKernel(typ, op, nil)
				if k == nil {
					t.Fatalf("%s has an arm in ResolveFilterKernel for a non-nil constant "+
						"but returns no kernel for NULL; the caller would report the column "+
						"as having no comparison kernel", typ)
				}
				vec := zeroValuedVector(t, typ)
				out := make([]uint32, 0, 4)
				if sel := k(vec, nil, 4, out); len(sel) != 0 {
					t.Errorf("%s %s NULL selected %d of 4 zero-valued rows, want 0 — the nil "+
						"constant was read as the type's zero", typ, opLabel(op), len(sel))
				}
				// And again with an incoming selection vector, which is the
				// path a filter takes when it is not first in a chain.
				if sel := k(vec, []uint32{0, 1, 2, 3}, 4, out); len(sel) != 0 {
					t.Errorf("%s %s NULL selected %d of 4 pre-selected rows, want 0",
						typ, opLabel(op), len(sel))
				}
			})
		}
	}
}

func opLabel(op CompareOp) string {
	switch op {
	case OpEq:
		return "eq"
	case OpNe:
		return "ne"
	case OpLt:
		return "lt"
	case OpLe:
		return "le"
	case OpGt:
		return "gt"
	case OpGe:
		return "ge"
	default:
		return "op?"
	}
}
