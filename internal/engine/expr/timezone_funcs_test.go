package expr

import (
	"math"
	"strings"
	"testing"
	"time"
)

// TestPgPostmasterStartTime checks that the function reports this process's
// start: within a second of when the test binary began, and constant across
// calls (PostgreSQL's value does not move while the server runs).
func TestPgPostmasterStartTime(t *testing.T) {
	fn := DefaultRegistry.Lookup("pg_postmaster_start_time")
	if fn == nil {
		t.Fatal("pg_postmaster_start_time not registered")
	}
	got, ok := fn(nil).(string)
	if !ok {
		t.Fatalf("want timestamp text like now(), got %T", fn(nil))
	}
	// The engine's one instant rendering, which is PostgreSQL's: a space, no
	// `T` and no `Z`, with a fractional second only when there is one. It was
	// RFC3339 until #544's second pass; parsing it that way is what this
	// assertion is for.
	ts, err := time.Parse("2006-01-02 15:04:05.999", got)
	if err != nil {
		t.Fatalf("not the engine's timestamp rendering: %q (%v)", got, err)
	}
	if d := time.Since(ts); d < 0 || d > time.Minute {
		t.Fatalf("process start %v is %v away from now — not this process", ts, d)
	}
	if second := fn(nil).(string); second != got {
		t.Fatalf("value moved between calls: %q then %q", got, second)
	}
}

// TestEpoch covers EXTRACT(EPOCH FROM ts)'s rewrite target over each
// representation a timestamp reaches the scalar layer in.
func TestEpoch(t *testing.T) {
	fn := DefaultRegistry.Lookup("epoch")
	if fn == nil {
		t.Fatal("epoch not registered")
	}
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"rfc3339 utc", "2026-01-02T03:04:05Z", float64(1767323045)},
		{"rfc3339 offset keeps the instant", "2026-01-01T22:04:05-05:00", float64(1767323045)},
		{"space separated", "2026-01-02 03:04:05", float64(1767323045)},
		{"date only", "2026-01-02", float64(1767312000)},
		{"epoch seconds pass through", int64(1767323045), float64(1767323045)},
		{"unix epoch itself", "1970-01-01T00:00:00Z", float64(0)},
		{"before the epoch", "1969-12-31T23:59:59Z", float64(-1)},
		{"null in null out", nil, nil},
		{"unparseable", "not a timestamp", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fn([]any{tt.in}); got != tt.want {
				t.Fatalf("epoch(%v) = %v (%T), want %v", tt.in, got, got, tt.want)
			}
		})
	}
	if got := fn(nil); got != nil {
		t.Fatalf("epoch() with no argument = %v, want nil", got)
	}
}

// TestTimezoneUTCIsInstantPreserving pins the semantics AT TIME ZONE 'UTC'
// carries here: the instant is unchanged, so the EXTRACT(EPOCH FROM …) round
// trip is exact. That identity is what makes UTC the one zone this engine can
// answer without a naive-timestamp type.
func TestTimezoneUTCIsInstantPreserving(t *testing.T) {
	tzFn := DefaultRegistry.Lookup("timezone")
	if tzFn == nil {
		t.Fatal("timezone not registered")
	}
	epochFn := DefaultRegistry.Lookup("epoch")

	for _, zone := range []string{"UTC", "utc", "Utc", "GMT", "gmt", "Z", "Etc/UTC", "etc/gmt", "  UTC  "} {
		for _, in := range []any{
			"2026-01-02T03:04:05Z",
			"2026-01-01T22:04:05-05:00",
			int64(1767323045),
		} {
			shifted := tzFn([]any{zone, in})
			if shifted == nil {
				t.Fatalf("timezone(%q, %v) = nil", zone, in)
			}
			before := epochFn([]any{in})
			after := epochFn([]any{shifted})
			if before != after {
				t.Fatalf("timezone(%q, %v) moved the instant: epoch %v → %v", zone, in, before, after)
			}
			// The rendered form is UTC, not the input's original offset. The
			// engine's one rendering carries no zone AT ALL — which is what
			// makes it correct only here, where the value IS UTC — so the
			// check is that no offset survived, not that a `Z` did (#544).
			s, ok := shifted.(string)
			if !ok || strings.ContainsAny(s, "TZ+") {
				t.Fatalf("timezone(%q, %v) = %v, want a UTC-rendered timestamp "+
					"with no zone suffix", zone, in, shifted)
			}
		}
	}
}

// TestTimezoneRejectsNonUTC covers the runtime backstop. A literal zone is
// rejected at parse time with a message; a zone that is only knowable at run
// time returns NULL rather than a conversion with the wrong sign.
func TestTimezoneRejectsNonUTC(t *testing.T) {
	fn := DefaultRegistry.Lookup("timezone")
	for _, zone := range []string{"America/New_York", "Europe/London", "EST", "+05:00", "", "Not/AZone"} {
		if got := fn([]any{zone, "2026-01-02T03:04:05Z"}); got != nil {
			t.Errorf("timezone(%q, ts) = %v, want nil", zone, got)
		}
	}
	// Nulls and short argument lists stay null rather than panicking.
	for _, args := range [][]any{nil, {"UTC"}, {nil, "2026-01-02T03:04:05Z"}, {"UTC", nil}} {
		if got := fn(args); got != nil {
			t.Errorf("timezone(%v) = %v, want nil", args, got)
		}
	}
	if got := fn([]any{"UTC", "not a timestamp"}); got != nil {
		t.Errorf("timezone with an unparseable timestamp = %v, want nil", got)
	}
}

// TestDataGripStartupTimeChain evaluates the whole composition DataGrip sends
// —  round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC'))
// — through the registry, and checks it lands on this process's start.
func TestDataGripStartupTimeChain(t *testing.T) {
	start := DefaultRegistry.Lookup("pg_postmaster_start_time")(nil)
	shifted := DefaultRegistry.Lookup("timezone")([]any{"UTC", start})
	secs := DefaultRegistry.Lookup("epoch")([]any{shifted})
	rounded := DefaultRegistry.Lookup("round")([]any{secs})

	got, ok := rounded.(float64)
	if !ok {
		t.Fatalf("round() returned %T, want float64", rounded)
	}
	if delta := math.Abs(got - float64(time.Now().Unix())); delta > 60 {
		t.Fatalf("startup_time %v is %vs from now — not this process's start", got, delta)
	}
}
