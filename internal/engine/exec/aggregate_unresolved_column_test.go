package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #355: an aggregate that cannot resolve a name it reads must FAIL, not
// answer.
//
// The reported shape was `SELECT MAX(n) FROM (SELECT o_custkey AS n FROM
// orders)` on the stage DAG. There the subquery's rename emits no stage, so
// the scan delivered o_custkey and the aggregate asked for `n`; resolveIndices
// recorded index -1, MAX's kernel came back nil, no accumulator was ever
// allocated for it, and the single output row was emitted with the slot never
// written — NULL, from a plain MAX over a plain int column that DuckDB and the
// single-process path both answer 1499.
//
// The planner is where the rename is resolved (physical.resolveAggInputName).
// This is the backstop, and it is the rule this codebase keeps relearning
// (#312-#316, #327, #345): a name that does not resolve must error or fall
// back, never silently answer NULL.
func TestHashAggregateUnresolvedColumnIsLoud(t *testing.T) {
	schema := []parquet.Column{{Name: "o_custkey", Type: parquet.TypeInt64}}
	input := func() *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 3)
		for i, v := range []int64{1, 700, 1499} {
			b.Columns[0].Int64Data[i] = v
		}
		return b
	}

	tests := []struct {
		name string
		agg  *HashAggregate
		want string
	}{
		{
			// The reported query, reduced: the alias never became a column.
			name: "aggregate input names a column the input does not carry",
			agg: &HashAggregate{
				Aggs: []AggColumn{{Func: AggMax, InputCol: "n", OutputCol: "max(n)"}},
			},
			want: `aggregate input "n"`,
		},
		{
			// The louder half: an unresolvable group key used to serialize
			// as a NULL key, collapsing every row into one group.
			name: "GROUP BY key names a column the input does not carry",
			agg: &HashAggregate{
				GroupByCols: []string{"k"},
				Aggs:        []AggColumn{{Func: AggCount, OutputCol: "c"}},
			},
			want: `GROUP BY key "k"`,
		},
		{
			// The second operand of a two-column aggregate is read per row
			// by CORR/COVAR/MIN_BY/MAX_BY, so a miss there is a lookup too.
			name: "second aggregate input names a column the input does not carry",
			agg: &HashAggregate{
				Aggs: []AggColumn{{Func: AggCorr, InputCol: "o_custkey", InputCol2: "b", OutputCol: "c"}},
			},
			want: `aggregate input "b"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.agg.Init(context.Background()); err != nil {
				t.Fatalf("init: %v", err)
			}
			err := tt.agg.Consume(context.Background(), input())
			if err == nil {
				t.Fatalf("Consume succeeded — the aggregate answered a query over a column it could not "+
					"resolve. It must fail instead; want an error naming %s", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to name %s", err, tt.want)
			}
			if !strings.Contains(err.Error(), "o_custkey") {
				t.Errorf("error = %v, want it to list the columns the input DOES carry", err)
			}
		})
	}
}

// The shapes that legitimately resolve to no column must keep working, or the
// backstop above becomes an outage.
func TestHashAggregateResolvableInputsStillRun(t *testing.T) {
	schema := []parquet.Column{{Name: "o_custkey", Type: parquet.TypeInt64}}
	input := func() *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 3)
		for i, v := range []int64{1, 700, 1499} {
			b.Columns[0].Int64Data[i] = v
		}
		return b
	}

	// COUNT(*) has no input column at all — an empty InputCol is not a miss.
	countStar := &HashAggregate{Aggs: []AggColumn{{Func: AggCount, OutputCol: "c"}}}
	if err := countStar.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := countStar.Consume(context.Background(), input()); err != nil {
		t.Fatalf("COUNT(*) has no input column and must keep working: %v", err)
	}

	// A state-MERGE aggregate carries InputCol2 from the spec it was cloned
	// from, but reads only the encoded state its partial emitted. That stale
	// second name must not be treated as a lookup — CORR over a partial/final
	// split reaches the final with InputCol2 still naming the raw column.
	merge := &HashAggregate{
		Aggs: []AggColumn{{Func: AggCovarStateMerge, InputCol: "o_custkey", InputCol2: "gone", OutputCol: "c"}},
	}
	if err := merge.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := merge.Consume(context.Background(), input()); err != nil {
		t.Fatalf("a state-merge aggregate does not read InputCol2 and must not fail on it: %v", err)
	}
}
