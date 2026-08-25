package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInferCastTypeBooleanSpellings is the projection side of the agreement
// expr.castDestType's doc comment names.
//
// A CAST to BOOLEAN produces a real bool (expr.Cast.castToBool, #592), and the
// projection writes that value into the vector this function types. If the two
// ever disagreed about BOOLEAN the write would be refused by the #361
// silent-write guard — which is what `SELECT (s)::BOOLEAN` did before #592,
// for exactly that reason on the other side of the pair.
func TestInferCastTypeBooleanSpellings(t *testing.T) {
	for _, spelling := range []string{"BOOLEAN", "BOOL", "boolean", "bool", " Boolean "} {
		if got := inferCastType(spelling); got != parquet.TypeBool {
			t.Errorf("inferCastType(%q) = %v, want BOOL", spelling, got)
		}
	}
}
