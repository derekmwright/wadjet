package wshf

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The MULTI-FILE half of ADR-0010's shuffle-schema guard.
//
// shuffleWriter.writeChunk already refuses a chunk whose DECIMAL vector
// disagrees with its own file header (#533), and the ADR says in as many words
// that this covers the SINGLE-WRITER shape and only that shape: it fires where
// ONE task is handed batches at two scales, and cannot fire where each producer
// writes its own internally-consistent file and a downstream reader
// concatenates several of them. There is no writer at the point of
// reinterpretation.
//
// #685 was that gap, reached by an ordinary query: an ungrouped aggregate whose
// filter matched no rows in one task's files wrote a partial declaring
// DECIMAL(0,0) beside siblings declaring DECIMAL(38,2), and — one column over —
// declared the SUM leg of an AVG FLOAT64 where its siblings declared DECIMAL.
// Both producers are fixed; this is what makes the NEXT one a failed read
// instead of a wrong answer, in all six readers at once.
func TestSchemaGuardRefusesFilesThatDescribeDifferentRelations(t *testing.T) {
	col := func(name string, typ parquet.TypeID, prec, scale int) parquet.Column {
		return parquet.Column{Name: name, Type: typ, Precision: prec, Scale: scale, Nullable: true}
	}

	for _, tc := range []struct {
		name    string
		first   []parquet.Column
		second  []parquet.Column
		wantErr string // substring; "" means the pair must be accepted
	}{
		{
			name:   "same declaration is accepted",
			first:  []parquet.Column{col("s", parquet.TypeDecimal, 38, 2)},
			second: []parquet.Column{col("s", parquet.TypeDecimal, 38, 2)},
		},
		{
			// The #685 shape verbatim: the identity row's undeclared (p,s).
			name:    "undeclared decimal params are refused",
			first:   []parquet.Column{col("s", parquet.TypeDecimal, 38, 2)},
			second:  []parquet.Column{col("s", parquet.TypeDecimal, 0, 0)},
			wantErr: `column "s" is DECIMAL(0,0)`,
		},
		{
			// The #533 shape one stage over: two arms at real but different
			// scales, each file internally consistent.
			name:    "a different scale is refused",
			first:   []parquet.Column{col("v", parquet.TypeDecimal, 9, 2)},
			second:  []parquet.Column{col("v", parquet.TypeDecimal, 18, 4)},
			wantErr: `column "v" is DECIMAL(18,4)`,
		},
		{
			name:    "a different precision at the same scale is refused",
			first:   []parquet.Column{col("v", parquet.TypeDecimal, 9, 2)},
			second:  []parquet.Column{col("v", parquet.TypeDecimal, 38, 2)},
			wantErr: `column "v" is DECIMAL(38,2)`,
		},
		{
			// The TYPE half — #685's AVG leg, where the identity row declared
			// the SUM partial FLOAT64 beside siblings declaring DECIMAL. A
			// merge that resolved against the float batch first read the
			// DECIMAL vectors through a float kernel.
			name:    "a different type is refused",
			first:   []parquet.Column{col("__avg_sum#av", parquet.TypeDecimal, 38, 2)},
			second:  []parquet.Column{col("__avg_sum#av", parquet.TypeFloat64, 0, 0)},
			wantErr: `column "__avg_sum#av" is FLOAT64`,
		},
		{
			name:    "a different column count is refused",
			first:   []parquet.Column{col("s", parquet.TypeDecimal, 38, 2), col("n", parquet.TypeInt64, 0, 0)},
			second:  []parquet.Column{col("s", parquet.TypeDecimal, 38, 2)},
			wantErr: "declares 1 columns where an earlier file",
		},
		{
			name:    "a renamed column at the same position is refused",
			first:   []parquet.Column{col("s", parquet.TypeDecimal, 38, 2)},
			second:  []parquet.Column{col("other", parquet.TypeDecimal, 38, 2)},
			wantErr: `names column 0 "other"`,
		},
		{
			// Non-DECIMAL columns still have to agree on TYPE, but carry no
			// (p,s) to disagree about — the zero Precision/Scale a BIGINT
			// header parses with must not read as a disagreement.
			name:   "non-decimal columns are accepted",
			first:  []parquet.Column{col("n", parquet.TypeInt64, 0, 0), col("t", parquet.TypeString, 0, 0)},
			second: []parquet.Column{col("n", parquet.TypeInt64, 0, 0), col("t", parquet.TypeString, 0, 0)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g SchemaGuard
			if err := g.Check("first.wshf", tc.first); err != nil {
				t.Fatalf("the FIRST file was refused, which cannot be right — there is nothing "+
					"to disagree with yet: %v", err)
			}
			err := g.Check("second.wshf", tc.second)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("the pair was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("the pair was ACCEPTED — a reader that adopts the first header reads " +
					"the second file's bytes under a declaration that is not theirs, silently")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("refusal does not name the disagreement: %v", err)
			}
			if !strings.Contains(err.Error(), "second.wshf") {
				t.Errorf("refusal does not name the offending file: %v", err)
			}
		})
	}
}

// The check runs once per FILE, not once per batch: a chunk reader hands every
// batch of one file the same schema slice, and re-deriving the declaration for
// each of them would put an allocation on the shuffle-read hot path. What the
// shortcut must not do is let a disagreeing FILE through, so the two halves are
// asserted together.
func TestSchemaGuardChecksOncePerFileButStillChecksEveryFile(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeDecimal, Precision: 38, Scale: 2, Nullable: true}}
	var g SchemaGuard
	for i := 0; i < 3; i++ {
		// Three batches sharing one schema slice — what one file looks like.
		b := &batch.RecordBatch{Schema: schema, Len: 1}
		if err := g.CheckBatch("first.wshf", b); err != nil {
			t.Fatalf("batch %d of the first file was refused: %v", i, err)
		}
	}
	drifted := &batch.RecordBatch{
		Schema: []parquet.Column{{Name: "s", Type: parquet.TypeDecimal, Precision: 0, Scale: 0, Nullable: true}},
		Len:    1,
	}
	if err := g.CheckBatch("second.wshf", drifted); err == nil {
		t.Fatal("the second file's disagreeing header was ACCEPTED — the per-file shortcut is " +
			"skipping the comparison it exists to make cheap")
	}
	// A nil batch and an empty schema are no-ops, not refusals: an operator
	// with no columns has nothing to disagree about.
	if err := g.CheckBatch("x", nil); err != nil {
		t.Errorf("a nil batch was refused: %v", err)
	}
	if err := g.CheckBatch("x", &batch.RecordBatch{}); err != nil {
		t.Errorf("an empty schema was refused: %v", err)
	}
}
