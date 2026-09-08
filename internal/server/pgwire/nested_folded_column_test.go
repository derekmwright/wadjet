package pgwire

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// nfcDB registers a table with a ROW column whose CATALOG name is CamelCase,
// and whose FIELDS are declared in an order that is not alphabetical — so a
// rendering that lost the declaration is visible in the bytes rather than
// merely unproven.
func nfcDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "Attrs", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "zeta", Type: parquet.TypeInt64},
			{Name: "alpha", Type: parquet.TypeString},
		}},
	}}
	if err := db.Catalog().CreateTable(ctx, "nst", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("nst", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "Attrs": map[string]any{"zeta": int64(9), "alpha": "A"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestPGWireNestedColumnResolvesFoldedReference is the regression gate for a
// ROW / ARRAY / MAP column losing its DECLARATION on the wire because the
// reference that named it was folded.
//
// `nestedColumnSchemas` builds its map from the catalog, so it is keyed by the
// catalog's spelling; `nestedColumnFor` looks it up by the OUTPUT column name,
// which for an unquoted reference is the folded spelling (#731). Over a
// CamelCase schema the lookup missed, and the positional fallback is a no-op
// on this path (`ordered` is deliberately nil for the catalog-lookup schema),
// so `formatPgValueTyped` was called with no declaration: a ROW then renders
// in SORTED-KEY order instead of declared field order, and an ARRAY and a MAP
// stop being distinguishable.
//
// That is a wrong VALUE on the wire, not a wrong name — the #471/#769 failure
// mode re-entered through the case door — and it is silent: the DataRow is
// well formed and the field count is right.
func TestPGWireNestedColumnResolvesFoldedReference(t *testing.T) {
	for _, tt := range []struct{ name, sql string }{
		{"unquoted reference to a CamelCase ROW column", "SELECT Attrs FROM nst"},
		{"delimited reference in the schema's own case", `SELECT "Attrs" FROM nst`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := nfcDB(t)
			srv := startTestServer(t, db)
			client := newPGClient(t, srv.Addr())
			client.startup("test", "wadjet")
			client.writeMsg('Q', append([]byte(tt.sql), 0))

			var row []byte
			for {
				typ, payload, err := client.readMsg()
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				switch typ {
				case 'D':
					row = append([]byte(nil), payload...)
				case 'E':
					t.Fatalf("%s was refused: %s", tt.sql, client.parseError(payload))
				}
				if typ == 'Z' {
					break
				}
			}
			client.terminate()

			// The declared field order is (zeta, alpha) = (9, A). Sorted-key
			// order — what a declaration-less render produces — is (A, 9).
			if !bytes.Contains(row, []byte("(9,A)")) {
				t.Fatalf("DataRow %q does not carry the ROW in DECLARED field order (9,A): "+
					"the column's declaration did not resolve, so it rendered with sorted keys",
					row)
			}
		})
	}
}

// nfcAmbiguousDB registers TWO tables whose ROW columns differ only in CASE —
// `nsa.Attrs` and `nsb.ATTRS` — and gives the two declarations DIFFERENT field
// names, so all three possible renderings of nsa's value {zeta: 9, alpha: "A"}
// are distinct on the wire:
//
//	no declaration    (A,9)   sorted keys — the miss, reported as the miss it is
//	nsa.Attrs's       (9,A)   declared field order
//	nsb.ATTRS's       (,)     two empty slots: compositeFieldOrder takes the
//	                          declared names and formatPgComposite finds
//	                          neither `gamma` nor `delta` in the value
//
// Two same-shaped field lists collapse the last two renderings into one, which
// is why the 25-run probe that found this read only 2 of 25 runs as wrong: half
// the wrong answers were wearing the right answer's bytes.
func nfcAmbiguousDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mk := func(table, col, f1, f2 string) {
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: col, Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: f1, Type: parquet.TypeInt64},
				{Name: f2, Type: parquet.TypeString},
			}},
		}}
		if err := db.Catalog().CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
		if err := ing.Ingest(ctx, []map[string]any{
			{"id": int64(1), col: map[string]any{f1: int64(9), f2: "A"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mk("nsa", "Attrs", "zeta", "alpha")
	mk("nsb", "ATTRS", "gamma", "delta")
	return db
}

// nfcAmbiguousSQL names BOTH tables, so nestedColumnSchemas puts `Attrs` and
// `ATTRS` in one map and has to decide what the folded output name `attrs`
// resolves to.
const nfcAmbiguousSQL = `SELECT a.Attrs FROM nsa a JOIN nsb b ON a.id = b.id`

// TestPGWireNestedAliasDropsAnAmbiguousFold is the ambiguity guard on the
// folded-spelling alias, asserted at the resolver rather than through the
// wire, so it is decided rather than sampled.
//
// The alias published FoldIdent(name) for every catalog name whose spelling is
// not already folded. Across relations two names can fold to ONE key —
// `catalog.checkDistinctColumnNames` refuses that only WITHIN a table — and
// both were "untaken", so both wrote `aliases["attrs"]` and the winner was
// whichever Go's map iteration reached last. The `conflicting` pass above
// cannot see it: it keys by the CATALOG spelling, so `Attrs` and `ATTRS` are
// two entries that are never compared, and both are TypeRow in any case.
//
// batch/schema.go item 3 already writes the rule down — "two columns matching
// is ambiguous and resolves to nothing, which the caller reports as the miss
// it is" — and this is that rule, carried by the hand-rolled map.
func TestPGWireNestedAliasDropsAnAmbiguousFold(t *testing.T) {
	c := &pgConn{db: nfcAmbiguousDB(t)}
	metas := []wadjet.ColumnMeta{{Name: "attrs", TypeID: parquet.TypeRow}}

	// Map iteration order is re-randomised per range, so one resolution says
	// nothing about the next: the assertion is over repetitions.
	for i := 0; i < 200; i++ {
		got := c.nestedColumnSchemas(nfcAmbiguousSQL, metas)
		if got == nil {
			t.Fatalf("run %d: nestedColumnSchemas returned nil, want the byte-exact entries", i)
		}
		if col, ok := got.byName["attrs"]; ok {
			t.Fatalf("run %d: the folded key \"attrs\" resolved to a declaration with fields %v — "+
				"two catalog names (nsa.Attrs, nsb.ATTRS) fold to it, so it is AMBIGUOUS and must "+
				"resolve to nothing; whichever one it picked is Go map iteration order, not a decision",
				i, nfcFieldNames(col))
		}
		// The byte-exact entries are untouched: dropping the alias must not
		// cost a delimited reference its own declaration.
		if _, ok := got.byName["Attrs"]; !ok {
			t.Fatalf("run %d: the byte-exact entry \"Attrs\" is gone", i)
		}
		if _, ok := got.byName["ATTRS"]; !ok {
			t.Fatalf("run %d: the byte-exact entry \"ATTRS\" is gone", i)
		}
	}
}

// nfcFieldNames renders a ROW column's declared field order for a failure
// message, so the report names WHICH table's declaration was taken.
func nfcFieldNames(col parquet.Column) []string {
	out := make([]string, len(col.Fields))
	for i, f := range col.Fields {
		out[i] = f.Name
	}
	return out
}

// TestPGWireNestedAmbiguousReferenceRendersIdenticallyEveryRun is the same
// guard at the WIRE, which is where the defect was measured: 25 identical runs
// of one query over one fixture returned `(9,A)` twice and `(A,9)` 23 times.
// Map-iteration-dependent wire bytes are not one of ADR-0013's eight legal
// classes of nondeterminism.
//
// AMENDED 2026-09-08 (arc A1, #965). The expected bytes moved from `(A,9)` to
// `(9,A)`, and it is the pin agreeing rather than the guard weakening.
//
// `(A,9)` was the sorted-key rendering — "no declaration bound" — and it was
// the only DETERMINISTIC answer available while the catalog walk was the only
// source of one: two catalog names folding to one key cannot choose between
// their declarations. That is still true and
// TestPGWireNestedAliasDropsAnAmbiguousFold still asserts it, at the resolver,
// with metas that carry no declaration of their own.
//
// What changed is that a RESULT now carries its own (wadjet.ColumnMeta.Fields,
// added for a bar, which no catalog describes at all). This query is not
// ambiguous to the PLANNER — it reads `nsa.Attrs` and nothing else — so the
// declaration is nsa's, `(9,A)` is the right rendering, and it is as
// deterministic as the plan is. Both halves of this gate still hold: the bytes
// are identical across 25 runs, and they are now identical AND right.
//
// The assertion is both halves: the bytes are the SAME every run, and they are
// the declaration-less rendering — the ambiguous reference resolving to
// nothing, which is what makes them the same.
func TestPGWireNestedAmbiguousReferenceRendersIdenticallyEveryRun(t *testing.T) {
	db := nfcAmbiguousDB(t)
	srv := startTestServer(t, db)

	seen := map[string]int{}
	var first []byte
	for i := 0; i < 25; i++ {
		client := newPGClient(t, srv.Addr())
		client.startup("test", "wadjet")
		client.writeMsg('Q', append([]byte(nfcAmbiguousSQL), 0))
		var row []byte
		for {
			typ, payload, err := client.readMsg()
			if err != nil {
				t.Fatalf("run %d: read: %v", i, err)
			}
			switch typ {
			case 'D':
				row = append([]byte(nil), payload...)
			case 'E':
				t.Fatalf("run %d: %s was refused: %s", i, nfcAmbiguousSQL, client.parseError(payload))
			}
			if typ == 'Z' {
				break
			}
		}
		client.terminate()
		seen[string(row)]++
		if i == 0 {
			first = row
		} else if !bytes.Equal(row, first) {
			t.Fatalf("run %d returned DIFFERENT wire bytes for the same query over the same "+
				"fixture: %q, run 0 returned %q. Tally so far: %v", i, row, first, seen)
		}
	}
	// (9,A) is nsa.Attrs's declared field order, which is the column this
	// query reads. (,) is nsb.ATTRS's — two empty slots, because
	// formatPgComposite would find neither `gamma` nor `delta` in the value —
	// and (A,9) is the sorted-key rendering that means no declaration bound at
	// all. Only the first is right, and it can only come from the PLAN.
	if !bytes.Contains(first, []byte("(9,A)")) {
		t.Fatalf("DataRow %q: want (9,A), nsa.Attrs's declared field order.\n"+
			"  (A,9) means no declaration bound — the catalog walk's ambiguous fold, which "+
			"the result's own declaration is supposed to answer before.\n"+
			"  (,)   means nsb.ATTRS's declaration was taken, which IS the map-iteration "+
			"defect this gate exists for.", first)
	}
}

// nfcContainerDB is the ARRAY and MAP arm of the CamelCase fixture: the alias
// is published for every nested type, not only ROW, and this asserts that
// rather than assuming it.
func nfcContainerDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "Tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "Props", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "key_value", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeString, Nullable: true},
			}}},
	}}
	if err := db.Catalog().CreateTable(ctx, "amt", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("amt", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "Tags": []any{"a", "b"}, "Props": map[string]any{"k": "v"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestPGWireNestedContainerColumnsResolveFoldedReferences covers the ARRAY and
// MAP half of the alias.
//
// The declaration is what keeps the two APART on the wire: the Go value
// GetValue hands back is a bare []any for an ARRAY and for a MAP alike, with
// no remembered structure of its own (#471). A folded reference that found no
// declaration therefore did not merely lose an ORDER, as it does for a ROW; it
// lost which CONTAINER the column is. The unquoted reference must render
// exactly what the delimited one renders.
func TestPGWireNestedContainerColumnsResolveFoldedReferences(t *testing.T) {
	db := nfcContainerDB(t)
	srv := startTestServer(t, db)

	render := func(sql string) []byte {
		t.Helper()
		client := newPGClient(t, srv.Addr())
		client.startup("test", "wadjet")
		client.writeMsg('Q', append([]byte(sql), 0))
		var row []byte
		for {
			typ, payload, err := client.readMsg()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			switch typ {
			case 'D':
				row = append([]byte(nil), payload...)
			case 'E':
				t.Fatalf("%s was refused: %s", sql, client.parseError(payload))
			}
			if typ == 'Z' {
				break
			}
		}
		client.terminate()
		return row
	}

	for _, tt := range []struct{ name, folded, delimited, want string }{
		{"ARRAY", "SELECT Tags FROM amt", `SELECT "Tags" FROM amt`, "{a,b}"},
		{"MAP", "SELECT Props FROM amt", `SELECT "Props" FROM amt`, "{k: v}"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			delim := render(tt.delimited)
			if !bytes.Contains(delim, []byte(tt.want)) {
				t.Fatalf("delimited reference: DataRow %q does not carry %q", delim, tt.want)
			}
			folded := render(tt.folded)
			if !bytes.Equal(folded, delim) {
				t.Fatalf("the folded reference renders %q where the delimited one renders %q — "+
					"the alias does not cover %s, so the column's declaration did not resolve",
					folded, delim, tt.name)
			}
		})
	}

	// And at the resolver, so the alias's TYPE coverage is asserted directly
	// rather than inferred from two byte strings agreeing.
	c := &pgConn{db: db}
	got := c.nestedColumnSchemas("SELECT Tags, Props FROM amt", []wadjet.ColumnMeta{
		{Name: "tags", TypeID: parquet.TypeArray},
		{Name: "props", TypeID: parquet.TypeMap},
	})
	if got == nil {
		t.Fatal("nestedColumnSchemas returned nil, want the folded aliases for Tags and Props")
	}
	for _, want := range []struct {
		name string
		typ  parquet.TypeID
	}{{"tags", parquet.TypeArray}, {"props", parquet.TypeMap}} {
		col, ok := got.byName[want.name]
		if !ok {
			t.Errorf("the folded key %q has no alias — a folded reference to it finds no declaration",
				want.name)
			continue
		}
		if col.Type != want.typ {
			t.Errorf("the alias for %q is a %s, want %s", want.name, col.Type, want.typ)
		}
		if col.ElementType == nil {
			t.Errorf("the alias for %q carries no ElementType, which is the declaration itself", want.name)
		}
	}
}
