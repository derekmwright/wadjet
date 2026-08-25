package worker

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// applyDeclaredScanSchema is the guard #503 turns on: a base-table parquet
// read with no declared schema is REFUSED, because the alternative is to type
// the columns from the file — which a parquet file cannot do for nine of this
// engine's types, and which for a file whose stored type CONTRADICTS the
// catalog decodes silently as whatever it holds.
//
// The distinction it draws is the whole guard: stage output legitimately
// carries no declaration (its types ride the WSHF payload), so the refusal
// has to see the difference, not just the absence.
func TestApplyDeclaredScanSchemaRefusesAnUndeclaredBaseTableRead(t *testing.T) {
	declared := []distributed.ColumnSpec{{Name: "c0", Type: int(parquet.TypeInt64)}}

	cases := []struct {
		name      string
		files     []string
		declared  []distributed.ColumnSpec
		wantErr   bool
		wantSetOn bool // the source received the declaration
	}{
		{
			name:    "base-table parquet with no declaration is refused",
			files:   []string{"tables/t/chunk_0000.parquet"},
			wantErr: true,
		},
		{
			name:      "base-table parquet WITH a declaration is applied",
			files:     []string{"tables/t/chunk_0000.parquet"},
			declared:  declared,
			wantSetOn: true,
		},
		{
			name:  "flat stage output carries its own types",
			files: []string{"queries/q/s/task.wshf"},
		},
		{
			name:  "partitioned stage output likewise",
			files: []string{"queries/q/s/partition=0000/task.wshf"},
		},
		{
			// classifyInputFiles has already failed the read by the time
			// this could matter; the guard must not invent a second verdict
			// over a list it cannot classify.
			name:  "an unclassifiable list is not this guard's to refuse",
			files: []string{"something-else"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &cachedFileStreamSource{}
			err := applyDeclaredScanSchema(src, "scan", "alias", tc.files, tc.declared)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("no error; an undeclared base-table read must be refused, not trusted")
				}
				if !strings.Contains(err.Error(), "no declared schema") {
					t.Errorf("error does not say what is missing: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if got := len(src.declaredSchema) > 0; got != tc.wantSetOn {
				t.Errorf("declaration applied = %v, want %v", got, tc.wantSetOn)
			}
		})
	}
}
