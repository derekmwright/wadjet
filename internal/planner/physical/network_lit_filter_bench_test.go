package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkIPv4LiteralFilterPushdown is the regression benchmark for the
// #484/#485 review's blocking perf fix: compileCmp (internal/engine/expr/
// compile.go) started emitting *expr.CmpNetworkLit for `ipv4_col <op>
// 'literal'`, and extractFilterOps had no case for that node type, so the
// predicate fell to row-at-a-time evaluation instead of the vectorized
// kernel a plain *expr.Cmp got on this exact shape before #484 (measured
// 8.45ms -> 12.06ms, +43%, on 400k rows). This exercises the real planner
// entry point (buildFilterOp) end to end — SQL text through the compiled
// filter operator — rather than the kernel directly, so a future change
// that breaks pushdown for this shape shows up here the way it showed up
// in review.
func BenchmarkIPv4LiteralFilterPushdown(b *testing.B) {
	const rows = 400_000
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ip", Type: parquet.TypeIPv4},
	}
	bb := batch.NewRecordBatch(schema, rows)
	for i := 0; i < rows; i++ {
		bb.Columns[0].SetValue(i, int64(i))
		bb.Columns[1].SetValue(i, fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256))
	}

	node, err := plansql.ParseExpression("ip > '10.1.2.3'")
	if err != nil {
		b.Fatal(err)
	}
	p := &Planner{}
	op, err := p.buildFilterOp(logical.Predicate{ASTExpr: node}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		b.Fatal(err)
	}
	defer op.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb.Sel = nil // reset selection: a fresh 400k-row scan every rep
		if _, err := op.Execute(ctx, bb); err != nil {
			b.Fatal(err)
		}
	}
}
