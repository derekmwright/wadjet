package parquet

import (
	"bytes"
	"net"
	"strings"
	"testing"
)

// The pre-v0.18.0 file, and what the catalog has to say about it.
//
// A file this writer produces stamps its declared schema into the footer, so
// its IPv4/IPv6/MAC/UUID/BYTES/PORT/PROTOCOL/DURATION columns come back typed
// even though parquet's own schema cannot express them. A file written before
// that key existed carries no such statement, and a reader that types from
// the FILE reads those columns as the INT64/BYTE_ARRAY they are stored in —
// #396's symptom on data no migration can rewrite (#423).
//
// StripDeclaredSchema builds exactly that file, and SchemaAs is the reader
// side of the answer.
func TestSchemaAsRestoresTypesAFileCannotDeclare(t *testing.T) {
	declared := []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "c_ipv4", Type: TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: TypeIPv6, Nullable: true},
		{Name: "c_mac", Type: TypeMAC, Nullable: true},
		{Name: "c_uuid", Type: TypeUUID, Nullable: true},
		{Name: "c_port", Type: TypePort, Nullable: true},
		{Name: "c_proto", Type: TypeProtocol, Nullable: true},
		{Name: "c_dur", Type: TypeDuration, Nullable: true},
		{Name: "c_bytes", Type: TypeBytes, Nullable: true},
		{Name: "c_dec", Type: TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
	}
	rows := []map[string]any{{
		"id":      int64(1),
		"c_ipv4":  net.ParseIP("10.0.0.5").To4(),
		"c_ipv6":  net.ParseIP("2001:db8::5").To16(),
		"c_mac":   int64(187723558158341),
		"c_uuid":  bytes.Repeat([]byte{0x11}, 16),
		"c_port":  int32(443),
		"c_proto": int32(6),
		"c_dur":   int64(1500),
		"c_bytes": []byte{0xde, 0xad, 0xbe, 0xef},
		"c_dec":   int64(12345),
	}}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, Schema{Columns: declared}, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	current := buf.Bytes()

	legacy, err := StripDeclaredSchema(current)
	if err != nil {
		t.Fatalf("StripDeclaredSchema: %v", err)
	}
	if _, err := StripDeclaredSchema(legacy); err == nil {
		t.Error("stripping a file that has no declared-schema key succeeded; " +
			"a fixture that was never stamped must not pass for a stripped one")
	}

	// The premise: without the footer key the file reports raw storage form.
	// If this ever stops holding, the rest of the test proves nothing.
	legacyReader, err := NewReaderFromBytes(legacy)
	if err != nil {
		t.Fatalf("reader over the stripped file: %v", err)
	}
	fileTypes := typesByName(legacyReader.Schema().Columns)
	for _, name := range []string{"c_ipv4", "c_ipv6", "c_mac", "c_uuid", "c_port", "c_proto", "c_dur", "c_bytes"} {
		if got := fileTypes[name]; got == declaredType(declared, name) {
			t.Fatalf("the stripped file still reports %s as %s — StripDeclaredSchema did not "+
				"remove the statement, so this fixture is not the migration case", name, got)
		}
	}

	// The answer: the catalog's types, installed.
	got, err := legacyReader.SchemaAs(declared)
	if err != nil {
		t.Fatalf("SchemaAs: %v", err)
	}
	gotTypes := typesByName(got)
	for _, want := range declared {
		if gotTypes[want.Name] != want.Type {
			t.Errorf("column %s typed %s, want %s", want.Name, gotTypes[want.Name], want.Type)
		}
	}
	// DECIMAL is annotated in the file, so the footer already knew it — the
	// control that says this is not blanket relabelling.
	for _, c := range got {
		if c.Name != "c_dec" {
			continue
		}
		if c.Precision != 18 || c.Scale != 4 {
			t.Errorf("c_dec precision/scale = %d/%d, want 18/4", c.Precision, c.Scale)
		}
	}

	// A file that DOES carry the key agrees with the catalog, so the same
	// call is a no-op there — which is what makes it safe to apply always.
	currentReader, err := NewReaderFromBytes(current)
	if err != nil {
		t.Fatalf("reader over the stamped file: %v", err)
	}
	before := typesByName(currentReader.Schema().Columns)
	after, err := currentReader.SchemaAs(declared)
	if err != nil {
		t.Fatalf("SchemaAs over the stamped file: %v", err)
	}
	for name, typ := range typesByName(after) {
		if before[name] != typ {
			t.Errorf("column %s: the stamped file reported %s and the declaration made it %s — "+
				"the two disagree where they must agree", name, before[name], typ)
		}
	}
}

// Drift is an error, not garbage. A declaration the file's bytes cannot carry
// would otherwise decode one type's pages as another's — the failure mode
// retypeFromCatalog exists to refuse, reached here through the scan's door.
func TestSchemaAsRefusesDeclarationsTheFileCannotCarry(t *testing.T) {
	fileSchema := []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "txt", Type: TypeString, Nullable: true},
		{Name: "small", Type: TypeInt32, Nullable: true},
	}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, Schema{Columns: fileSchema}, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows([]map[string]any{
		{"id": int64(1), "txt": "hello", "small": int32(7)},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	legacy, err := StripDeclaredSchema(buf.Bytes())
	if err != nil {
		t.Fatalf("StripDeclaredSchema: %v", err)
	}
	r, err := NewReaderFromBytes(legacy)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	for _, tc := range []struct {
		name     string
		declared []Column
		wantIn   string
	}{
		{
			name:     "an integer leaf declared as a fixed-width address",
			declared: []Column{{Name: "id", Type: TypeIPv6}},
			wantIn:   "refusing to decode the file's bytes as a different type",
		},
		{
			name:     "an integer leaf declared as a UUID",
			declared: []Column{{Name: "small", Type: TypeUUID}},
			wantIn:   "refusing to decode the file's bytes as a different type",
		},
		{
			name:     "text declared as an integer",
			declared: []Column{{Name: "txt", Type: TypeInt64}},
			wantIn:   "refusing to decode the file's bytes as a different type",
		},
		{
			name:     "text declared as a network address",
			declared: []Column{{Name: "txt", Type: TypeIPv4}},
			wantIn:   "refusing to decode the file's bytes as a different type",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := r.SchemaAs(tc.declared)
			if err == nil {
				t.Fatal("SchemaAs accepted a declaration the file cannot carry — " +
					"the column would decode as something else, silently")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("error %q does not name the drift (want it to contain %q)", err, tc.wantIn)
			}
			if !strings.Contains(err.Error(), tc.declared[0].Name) {
				t.Errorf("error %q does not name the offending column %q", err, tc.declared[0].Name)
			}
		})
	}

	// The one drift the footer cannot settle: a BYTE_ARRAY leaf's entries are
	// individually sized, so "is every value sixteen bytes?" is a question
	// only the decode can answer. Admission lets it through by design —
	// refusing there could not name the offending row — and the READ fails.
	t.Run("text declared as a UUID fails at decode", func(t *testing.T) {
		declared := []Column{{Name: "txt", Type: TypeUUID, Nullable: true}}
		if _, err := r.SchemaAs(declared); err != nil {
			t.Fatalf("admission refused a width only the decode can check: %v", err)
		}
		if _, err := r.ReadRowsAs(declared, []string{"txt"}); err == nil {
			t.Fatal("reading five-byte strings as UUIDs succeeded — a wrong-width value " +
				"came back as a UUID instead of failing")
		}
	})
}

func typesByName(cols []Column) map[string]TypeID {
	out := make(map[string]TypeID, len(cols))
	for _, c := range cols {
		out[c.Name] = c.Type
	}
	return out
}

func declaredType(cols []Column, name string) TypeID {
	for _, c := range cols {
		if c.Name == name {
			return c.Type
		}
	}
	return TypeString
}
