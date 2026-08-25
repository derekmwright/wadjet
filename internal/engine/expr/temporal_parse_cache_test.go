package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestParseTemporalInt64OKCachesConsistently pins that routing the row-hot
// temporal fallback through dateEpochDaysCache/timestampEpochMsCache (rather
// than calling parseDateToEpochDaysOK/parseTimestampToEpochMsOK uncached
// every row) does not change the answer: the same (ref, literal) pair must
// answer identically on the cache-miss call and on every cache-hit call that
// follows, in both the epoch-DAYS and epoch-MILLISECONDS domains.
func TestParseTemporalInt64OKCachesConsistently(t *testing.T) {
	const dateRef = int64(10227)           // an ordinary TypeDate value
	const tsRef = int64(1_700_000_000_000) // an ordinary TypeTimestamp value (ms)

	for _, tc := range []struct {
		name    string
		ref     int64
		literal string
	}{
		{"date literal against a date-domain ref", dateRef, "1998-01-01"},
		{"timestamp literal against a timestamp-domain ref", tsRef, "1998-01-01T12:30:00"},
		// 0 is a legitimate result (the epoch itself), not a stand-in for
		// "did not parse" — the cache must keep the two apart.
		{"the epoch itself, date domain", dateRef, "1970-01-01"},
		{"the epoch itself, timestamp domain", tsRef, "1970-01-01T00:00:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			firstV, firstOK := parseTemporalInt64OK(tc.ref, tc.literal)
			if !firstOK {
				t.Fatalf("parseTemporalInt64OK(%d, %q) refused a valid literal", tc.ref, tc.literal)
			}
			for i := 0; i < 5; i++ {
				v, ok := parseTemporalInt64OK(tc.ref, tc.literal)
				if !ok || v != firstV {
					t.Fatalf("call %d: parseTemporalInt64OK(%d, %q) = (%d, %v), want (%d, true)",
						i, tc.ref, tc.literal, v, ok, firstV)
				}
			}
		})
	}
}

// TestParseTemporalInt64OKMalformedLiteralRefusesConsistently pins that a
// string which never parses as a date or a timestamp keeps refusing
// (ok=false) after the cache has been warmed the same way the uncached path
// always refused it — a stale or wrongly-typed cache entry could otherwise
// let a later call read a cached zero as a successful epoch-zero parse (the
// exact confusion temporalParseResult's separate ok field exists to prevent).
func TestParseTemporalInt64OKMalformedLiteralRefusesConsistently(t *testing.T) {
	const dateRef = int64(10227)
	const tsRef = int64(1_700_000_000_000)
	for _, ref := range []int64{dateRef, tsRef} {
		for i := 0; i < 5; i++ {
			if v, ok := parseTemporalInt64OK(ref, "not-a-date"); ok {
				t.Fatalf("call %d: parseTemporalInt64OK(%d, %q) = (%d, true), want ok=false",
					i, ref, "not-a-date", v)
			}
		}
	}
}

// TestParseTemporalInt64OKDateAndTimestampCachesDoNotCollide pins that the
// date-domain and timestamp-domain caches are keyed independently: the SAME
// literal text can appear in both, at different refs, and each domain must
// answer its own (parseDateToEpochDaysOK vs parseTimestampToEpochMsOK)
// reading rather than one leaking into the other's cache.
func TestParseTemporalInt64OKDateAndTimestampCachesDoNotCollide(t *testing.T) {
	const literal = "2005-06-15"
	const dateRef = int64(10227)
	const tsRef = int64(1_700_000_000_000)

	wantDays, ok := parseDateToEpochDaysOK(literal)
	if !ok {
		t.Fatalf("parseDateToEpochDaysOK(%q) refused", literal)
	}
	wantMs, ok := parseTimestampToEpochMsOK(literal)
	if !ok {
		t.Fatalf("parseTimestampToEpochMsOK(%q) refused", literal)
	}
	if wantDays == wantMs {
		t.Fatalf("test literal's day and millisecond readings coincide (%d) — pick a literal that tells the two caches apart", wantDays)
	}

	for i := 0; i < 3; i++ {
		if v, ok := parseTemporalInt64OK(dateRef, literal); !ok || v != wantDays {
			t.Fatalf("call %d: date-domain read = (%d, %v), want (%d, true)", i, v, ok, wantDays)
		}
		if v, ok := parseTemporalInt64OK(tsRef, literal); !ok || v != wantMs {
			t.Fatalf("call %d: timestamp-domain read = (%d, %v), want (%d, true)", i, v, ok, wantMs)
		}
	}
}

// TestTemporalFallbackOverMultipleRowsAndBatches is the end-to-end
// regression: a DATE column inside an IN list is exactly the shape that
// reaches compare()'s temporal fallback (boxedPair's classifyOperand leaves
// a DATE column boxUnknown, per its own doc comment, so it falls through to
// compare() rather than a declaration-driven rule). Running it over several
// rows and two separate batches exercises the SAME cached literals
// repeatedly, the way a real scan-pushed filter does, and a malformed
// member in the same list must keep refusing (never match, never corrupt
// the other members' answers) after the cache has warmed.
func TestTemporalFallbackOverMultipleRowsAndBatches(t *testing.T) {
	e := compileExprSQL(t, "d IN ('1998-01-01', 'not-a-date', '1999-03-06')")
	in, ok := e.(*In)
	if !ok {
		t.Fatalf("compiled to %T, want *In", e)
	}

	newBatch := func(days ...int32) *batch.RecordBatch {
		schema := []parquet.Column{{Name: "d", Type: parquet.TypeDate}}
		b := batch.NewRecordBatch(schema, len(days))
		for i, d := range days {
			b.Columns[0].SetValue(i, d)
		}
		b.Len = len(days)
		return b
	}
	epochDays := func(s string) int32 {
		v, ok := parseDateToEpochDaysOK(s)
		if !ok {
			t.Fatalf("test setup: %q does not parse as a date", s)
		}
		return int32(v)
	}
	d19980101 := epochDays("1998-01-01")
	d19990306 := epochDays("1999-03-06")
	other := epochDays("2000-12-25")

	// Two separate batches, run over several passes each, so the per-literal
	// cache is exercised well past its first (cache-miss) call.
	for _, batchDays := range [][]int32{
		{d19980101, other, d19990306},
		{other, d19980101, d19990306, other},
	} {
		b := newBatch(batchDays...)
		for pass := 0; pass < 3; pass++ {
			for row := 0; row < b.Len; row++ {
				want := batchDays[row] == d19980101 || batchDays[row] == d19990306
				if got := in.EvalBool(b, row); got != want {
					t.Fatalf("pass %d row %d (days=%d): IN = %v, want %v", pass, row, batchDays[row], got, want)
				}
			}
		}
	}
}
