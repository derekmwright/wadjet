// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"testing"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestArcNXCarrierWalkIsLinearInDepth bounds the declaration walk over the
// arms arc NX changed (#1386 #1392 #1450), each nested 20 deep: a bare
// NUMERIC cast over a bare NUMERIC cast (castDeclaredDecimal reads its
// operand through decimalArithOperand, which reads a bare cast through
// castDeclaredDecimal), an integer CAST under arithmetic beside a numeric, a
// chain of quotients over integer CASTs, unary minus over a wide literal,
// and a COALESCE chain over one. Each shape declares in well under the bound
// at base and at the tip; a walk that re-resolved a nested cast's operand per
// level would be exponential here and fail it.
func TestArcNXCarrierWalkIsLinearInDepth(t *testing.T) {
	const depth = 20
	nest := func(open, inner, close string) string {
		return strings.Repeat(open, depth) + inner + strings.Repeat(close, depth)
	}
	for _, tc := range []struct{ name, sql string }{
		{"bareNumericCast", nest("CAST(", "d152 * 1", " AS NUMERIC) * 1")},
		{"integerCastArith", nest("CAST(", "i64", " AS INTEGER) * 0.1 + d152")},
		{"integerCastQuotient", nest("CAST(", "i64", " AS INTEGER) / d152 + 1")},
		{"wideLiteralUnary", nest("-(", "14.0000000000000000001", ")")},
		{"wideLiteralCoalesce", nest("COALESCE(", "14.0000000000000000001", ", d152)")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			done := make(chan struct{})
			start := time.Now()
			go func() {
				nodeDeclaredType(node, nfdDecls())
				_ = pgCategoryOfDecl(nodeDeclaredType(node, nfdDecls()))
				close(done)
			}()
			select {
			case <-done:
				t.Logf("depth %d declared in %s", depth, time.Since(start))
			case <-time.After(2 * time.Second):
				t.Fatalf("declaring a %d-deep %s took more than 2s: the walk is not linear in depth", depth, tc.name)
			}
		})
	}
}
