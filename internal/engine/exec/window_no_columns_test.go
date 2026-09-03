package exec

import (
	"context"
	"strings"
	"testing"
)

// TestWindowWithNoWindowFunctionsIsRefused is the fixture for the
// impossibility the row-oriented spill path's deletion rests on (#460).
//
// That path was selected by `useColumnarRuns() == false`, which was
// `len(w.groups) == 0`, which — groups holding one entry per distinct
// (PARTITION BY, ORDER BY) pair across w.Columns — is `len(w.Columns) == 0`: a
// Window with no window functions. Both production constructors build their
// column list from a plan node's window expressions, so no query produces one,
// and an instrumented run over 100 windowed queries under a budget with the
// run floor lowered reached the path zero times. It is deleted.
//
// A design claim of the form "X cannot happen" is exactly where a regression
// hides, so the corpus attempts X. Every remaining path reads w.groups[0] or
// w.groups[len-1], so the answer has to be a refusal at the operator's
// boundary rather than an index panic three calls in.
func TestWindowWithNoWindowFunctionsIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		cols []WindowColumn
	}{
		{"nil column list", nil},
		{"empty column list", []WindowColumn{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWindow(tc.cols)
			err := w.Init(context.Background())
			if err == nil {
				t.Fatalf("a Window with no window functions initialized cleanly; every path below " +
					"reads w.groups[0], so this is an index panic waiting for the first batch")
			}
			if !strings.Contains(err.Error(), "no window functions") {
				t.Fatalf("refused, but not by name: %v", err)
			}
		})
	}
	// The control: one window function is enough, and it initializes.
	w := NewWindow([]WindowColumn{{Func: WinRowNumber, OutputCol: "r"}})
	if err := w.Init(context.Background()); err != nil {
		t.Fatalf("a Window with one window function must initialize: %v", err)
	}
}
