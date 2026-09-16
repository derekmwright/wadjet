// SPDX-License-Identifier: MIT

package pgwire

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/queryroute"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// stubRoutedResult is a routed result with no execution behind it: the
// declaration alone, which is what the metas are derived from. The routed
// path's own result (coordinator.NewQueryRouter) is gated end to end in
// internal/server's wire arms; this file gates the derivation.
type stubRoutedResult struct {
	columns       []string
	schema        []parquet.Column
	unconstrained map[string]bool
	stringLength  map[string]int
}

func (r stubRoutedResult) ColumnNames() []string                     { return r.columns }
func (r stubRoutedResult) OutputSchema() []parquet.Column            { return r.schema }
func (r stubRoutedResult) WireUnconstrainedDecimal() map[string]bool { return r.unconstrained }
func (r stubRoutedResult) StringLength() map[string]int              { return r.stringLength }
func (r stubRoutedResult) Stream() queryroute.Stream                 { return queryroute.NewSliceStream(nil) }
func (r stubRoutedResult) Close() error                              { return nil }

// #416's wire face. routedColumnMetas answers from the result's OutputSchema(),
// which for a zero-row result used to be nil — no batch had been consumed to
// read it off — so this returned nil, sendRowDescription's fallback fired and
// the client was told OID 25 (text) for every column while the SAME statement
// through the embedded API declared real OIDs.
//
// SQLResult.Schema is now populated from the PLAN for a zero-row result
// (physical.declaredOutputSchema, carried on the terminal gather stage and
// on exec.CollectSink.SchemaHint), so the metas exist with no batches at all
// — which is the whole point: RowDescription is sent BEFORE any DataRow.
func TestRoutedColumnMetasFromPlanSchemaWithNoRows(t *testing.T) {
	res := stubRoutedResult{
		columns: []string{"c_custkey", "c_acctbal", "n"},
		schema: []parquet.Column{
			{Name: "c_custkey", Type: parquet.TypeInt64, Nullable: true},
			{Name: "c_acctbal", Type: parquet.TypeFloat64, Nullable: true},
			{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		},
		// No batches: a zero-row result.
	}
	metas := routedColumnMetas(res)
	if len(metas) != 3 {
		t.Fatalf("routedColumnMetas returned %d metas for a zero-row result, want 3 — "+
			"pgwire falls back to OID 25 (text) for every column when this is nil", len(metas))
	}
	want := []parquet.TypeID{parquet.TypeInt64, parquet.TypeFloat64, parquet.TypeInt64}
	for i, m := range metas {
		if m.Name != res.columns[i] {
			t.Errorf("meta %d is named %q, want %q", i, m.Name, res.columns[i])
		}
		if m.TypeID != want[i] {
			t.Errorf("column %q declared %v, want %v", m.Name, m.TypeID, want[i])
		}
		if oid := pgTypeOID(m.TypeName); oid == 25 && want[i] != parquet.TypeString {
			t.Errorf("column %q went out as OID 25 (text) despite declaring %v", m.Name, want[i])
		}
	}
}

// TestCoordColumnMetasStillNilWithoutASchema pins the other half of the
// contract: with nothing to declare from, the uniform text fallback is still
// what happens. A PARTIAL declaration would be worse, because a client cannot
// tell which columns were guessed.
func TestRoutedColumnMetasStillNilWithoutASchema(t *testing.T) {
	res := stubRoutedResult{columns: []string{"a", "b"}}
	if metas := routedColumnMetas(res); metas != nil {
		t.Fatalf("routedColumnMetas invented %d metas from no schema: %+v", len(metas), metas)
	}
}
