package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestCastDestTypeBooleanSpellings holds castDestType to the one answer it
// shares with physical.inferCastType. A nested `CAST(CAST(x AS BOOLEAN) AS
// BOOLEAN)` reads the inner cast's type from here, and the projection
// allocates the output vector from there: if the two disagreed about BOOLEAN,
// SetValue would refuse the write with the #361 guard.
//
// The other spellings are pinned for the 42846 MESSAGE they select, which is
// where the two tables deliberately part company — a DECIMAL destination is
// FLOAT64 to the projection and `numeric` to this refusal.
func TestCastDestTypeBooleanSpellings(t *testing.T) {
	for _, spelling := range []string{"BOOLEAN", "BOOL", "boolean", "bool", " Bool "} {
		got, ok := castDestType(spelling)
		if !ok || got != batch.TypeBool {
			t.Errorf("castDestType(%q) = (%v, %v), want (BOOL, true)", spelling, got, ok)
		}
	}
	for _, c := range []struct {
		spelling string
		want     batch.TypeID
		name     string
	}{
		{"bigint", batch.TypeInt64, "bigint"},
		{"double precision", batch.TypeFloat64, "double precision"},
		{"numeric", batch.TypeDecimal, "numeric"},
		{"decimal", batch.TypeDecimal, "numeric"},
		{"text", batch.TypeString, "text"},
		{"date", batch.TypeDate, "date"},
	} {
		got, ok := castDestType(c.spelling)
		if !ok || got != c.want {
			t.Errorf("castDestType(%q) = (%v, %v), want (%v, true)", c.spelling, got, ok, c.want)
			continue
		}
		if n := pgCastSourceName(got); n != c.name {
			t.Errorf("a %q source is named %q in the refusal, want %q", c.spelling, n, c.name)
		}
	}
	if _, ok := castDestType("vector(4)"); ok {
		t.Error("castDestType claims to know a spelling it does not map; an unknown one must decline")
	}
}

// TestParseBoolTextIsPostgresBoolin holds parseBoolText to
// `parse_bool_with_len` (src/backend/utils/adt/bool.c). Every entry is a live
// postgres:17-alpine transcript.
//
// The prefix rule is the load-bearing half: PostgreSQL accepts any non-empty
// prefix of "true"/"false"/"yes"/"no", so a stricter reader would raise 22P02
// for values PostgreSQL calls booleans. `'o'` is the one prefix it refuses,
// because it cannot choose between "on" and "off".
func TestParseBoolTextIsPostgresBoolin(t *testing.T) {
	accept := map[string]bool{
		"t": true, "tr": true, "tru": true, "true": true,
		"T": true, "TR": true, "TrUe": true, "TRUE": true,
		"y": true, "ye": true, "yes": true, "YES": true,
		"on": true, "ON": true, "oN": true,
		"1": true,
		"f": false, "fa": false, "fal": false, "fals": false, "false": false,
		"F": false, "FaLsE": false,
		"n": false, "no": false, "NO": false,
		"of": false, "off": false, "OFF": false,
		"0": false,
		// C isspace on both ends, and nothing else trimmed.
		"  true  ": true, "\ttrue\n": true, "\r\nfalse\v\f": false, " 1 ": true,
	}
	for in, want := range accept {
		got, ok := parseBoolText(in)
		if !ok {
			t.Errorf("parseBoolText(%q) refused; PostgreSQL answers %v", in, want)
			continue
		}
		if got != want {
			t.Errorf("parseBoolText(%q) = %v, want %v (PostgreSQL 17)", in, got, want)
		}
	}
	for _, in := range []string{
		"", "   ", "o", "O", "2", "01", "10", "-1", "truex", "ofx", "yess", "nope",
		"garbage", "tt", "ff", "onn", "offf", "true false", "'t'",
	} {
		if got, ok := parseBoolText(in); ok {
			t.Errorf("parseBoolText(%q) = (%v, true); PostgreSQL raises 22P02", in, got)
		}
	}
}
