package parquet

import (
	"bytes"
	"regexp"
	"testing"
)

// createdByShape is the Apache convention: `<library> version <semver>`,
// optionally followed by `(build <hash>)`. The test asserts the SHAPE and not
// the exact string, because an exact assertion would need updating every
// release — which is the maintenance trap #456 explicitly asks to avoid.
var createdByShape = regexp.MustCompile(
	`^wadjet version [0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?( \(build [0-9a-f]{1,12}(-dirty)?\))?$`)

// #456: the footer's created_by carried the constant "wadjet (native writer)",
// so no reader could tell WHICH wadjet wrote a file — and ADR-0018's two
// compatibility notes therefore have to tell an operator to find affected
// tables by ingest DATE against a release date.
func TestCreatedByCarriesAParsableVersion(t *testing.T) {
	got := CreatedBy()
	if !createdByShape.MatchString(got) {
		t.Fatalf("created_by = %q, want the Apache convention "+
			"`wadjet version <semver>` optionally with ` (build <hash>)` (#456)", got)
	}
	info := ParseCreatedBy(got)
	if !info.Ok {
		t.Fatalf("this package cannot parse its own stamp: %q", got)
	}
	if info.Library != "wadjet" {
		t.Errorf("library = %q, want wadjet", info.Library)
	}
	if info.Version == "" {
		t.Error("version is empty")
	}
	// One place, computed once: two calls in a process are the same string, so
	// two files written by one build compare equal (the #456 "Care" note about
	// byte-identical-file comparisons in the suite).
	if again := CreatedBy(); again != got {
		t.Errorf("CreatedBy is not stable within a process: %q then %q", got, again)
	}
}

// TestWrittenFilesCarryTheVersion is the end-to-end half: the string reaches
// the footer and comes back through both reader types.
func TestWrittenFilesCarryTheVersion(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: TypeInt64}}}
	write := func() []byte {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, schema, DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows([]map[string]any{{"id": int64(1)}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	a := write()
	fr, err := OpenFileReaderFromBytes(a)
	if err != nil {
		t.Fatal(err)
	}
	if got := fr.CreatedBy(); got != CreatedBy() {
		t.Errorf("FileReader.CreatedBy = %q, want %q", got, CreatedBy())
	}
	r, err := NewReader(bytes.NewReader(a), int64(len(a)))
	if err != nil {
		t.Fatal(err)
	}
	if got := r.CreatedBy(); got != CreatedBy() {
		t.Errorf("Reader.CreatedBy = %q, want %q", got, CreatedBy())
	}
	// Two files written by ONE build are byte-identical, which is what keeps
	// the suite's file-comparison tests valid across the length change (#456's
	// "Care" note).
	if b := write(); !bytes.Equal(a, b) {
		t.Error("two files written in one run are no longer byte-identical")
	}
}

// TestParseCreatedBy covers the writers a migration will actually meet,
// including the pre-#456 wadjet stamp, which must parse as NOT-ok rather than
// as a version — a migration that read a version out of it would key on
// nothing.
func TestParseCreatedBy(t *testing.T) {
	cases := []struct {
		in                  string
		lib, version, build string
		ok                  bool
	}{
		{in: "wadjet version 0.18.22 (build 8b693f30c1de)",
			lib: "wadjet", version: "0.18.22", build: "8b693f30c1de", ok: true},
		{in: "wadjet version 0.0.0-devel (build abc123-dirty)",
			lib: "wadjet", version: "0.0.0-devel", build: "abc123-dirty", ok: true},
		{in: "wadjet version 0.18.22", lib: "wadjet", version: "0.18.22", ok: true},
		{in: "parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)",
			lib: "parquet-mr", version: "1.13.1",
			build: "db4183109d5b734ec5930d870cdae161e408ddba", ok: true},
		{in: "parquet-cpp-arrow version 23.0.1",
			lib: "parquet-cpp-arrow", version: "23.0.1", ok: true},
		// The pre-#456 stamp: every wadjet file older than this change.
		{in: "wadjet (native writer)", lib: "wadjet (native writer)"},
		{in: "", lib: ""},
		{in: "wadjet version ", lib: "wadjet version"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := ParseCreatedBy(tc.in)
			if got.Ok != tc.ok || got.Library != tc.lib ||
				got.Version != tc.version || got.Build != tc.build {
				t.Errorf("ParseCreatedBy(%q) = %+v, want {Library:%q Version:%q Build:%q Ok:%v}",
					tc.in, got, tc.lib, tc.version, tc.build, tc.ok)
			}
		})
	}
}
