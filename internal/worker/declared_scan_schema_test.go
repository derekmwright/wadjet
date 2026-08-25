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

// The kill switch, on the #308 precedent (WADJET_FASTPATH_STRICT): this guard
// turns a read that USED to answer into a hard failure, so there has to be a
// way back to the old behavior for a deployment that hits an unfound plumbing
// gap. Turning it off restores trust-the-file — which is a read that can
// answer 167772165 for 10.0.0.5 — so the refusal names the switch and the
// bypass logs at Warn.
func TestDeclaredSchemaGuardHasAKillSwitch(t *testing.T) {
	files := []string{"tables/t/chunk_0000.parquet"}

	if err := applyDeclaredScanSchema(&cachedFileStreamSource{}, "scan", "alias", files, nil); err == nil {
		t.Fatal("the guard is off by default; it must refuse an undeclared base-table read")
	} else if !strings.Contains(err.Error(), "WADJET_DECLARED_SCHEMA_STRICT=0") {
		t.Errorf("the refusal does not name its way out: %v", err)
	}

	prev := DeclaredSchemaStrict.Set(false)
	t.Cleanup(func() { DeclaredSchemaStrict.Set(prev) })

	src := &cachedFileStreamSource{}
	if err := applyDeclaredScanSchema(src, "scan", "alias", files, nil); err != nil {
		t.Fatalf("with the switch off the read must proceed, not be refused: %v", err)
	}
	if len(src.declaredSchema) != 0 {
		t.Error("the switch-off path must leave the source undeclared — the file's types are " +
			"what it restores; declaring something here would be inventing types")
	}

	// A DECLARED read is unaffected: the switch governs the refusal, not
	// whether the catalog's types are applied when they are present.
	declared := []distributed.ColumnSpec{{Name: "c0", Type: int(parquet.TypeInt64)}}
	src = &cachedFileStreamSource{}
	if err := applyDeclaredScanSchema(src, "scan", "alias", files, declared); err != nil {
		t.Fatalf("a declared read must still be applied with the switch off: %v", err)
	}
	if len(src.declaredSchema) == 0 {
		t.Error("the switch turned off the declaration itself, not just the refusal")
	}
}
