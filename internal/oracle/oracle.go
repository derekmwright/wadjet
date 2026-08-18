// Package oracle is the optimization-invariance differential harness
// (#287): every corpus query runs with all registered optimizations
// enabled (baseline), then once per optswitch toggle with just that
// optimization disabled, then with all of them disabled — and every
// configuration must produce identical results. An optimization that
// changes answers is a bug by definition; a divergence names the toggle,
// which is the defect localization.
//
// The harness is corpus-agnostic: callers supply the queries and a
// RunFunc, so the TPC-H and ClickBench suites (and later a query
// generator) plug in the same way.
package oracle

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Query is one corpus entry.
type Query struct {
	Name string
	SQL  string
	// CountOnly relaxes the comparison to row counts within Tolerance.
	// Reserved for queries whose row MEMBERSHIP legitimately shifts with
	// float accumulation order (threshold comparisons over float
	// aggregates, the TPC-H Q02/Q22 class) — cell-exact comparison would
	// flake on borderline rows even between two correct runs.
	CountOnly bool
	Tolerance int
}

// Result is the minimal result shape the harness compares. Rows are
// column-name-keyed, matching wadjet.QueryResult.
type Result struct {
	Columns []string
	Rows    []map[string]any
}

// RunFunc executes one SQL query under the currently-set toggles.
type RunFunc func(ctx context.Context, sql string) (*Result, error)

var limitRe = regexp.MustCompile(`(?i)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*;?\s*$`)

// ExpandLimits rewrites each LIMIT-ed query into two oracle entries: the
// stripped form (full-multiset compare — strictly stronger and tie-immune,
// same rationale as the DuckDB gate's stripLimit) and the verbatim form
// compared by row count only. At the LIMIT boundary any member of a tie
// group is admissible, so two correct runs can return different limited
// rows — but the row COUNT is deterministic, and running the verbatim
// query keeps LIMIT-dependent optimizations (top-N late materialization)
// exercised under the oracle. Queries already marked CountOnly keep that
// relaxation on the stripped form too.
func ExpandLimits(queries []Query) []Query {
	out := make([]Query, 0, len(queries)*2)
	for _, q := range queries {
		stripped := limitRe.ReplaceAllString(q.SQL, "")
		if stripped == q.SQL {
			out = append(out, q)
			continue
		}
		full := q
		full.Name = q.Name + "-nolimit"
		full.SQL = stripped
		out = append(out, full)
		verbatim := q
		verbatim.CountOnly = true
		out = append(out, verbatim)
	}
	return out
}

// RunDifferential drives the oracle. It temporarily forces every
// registered toggle on for the baseline (host env notwithstanding, so a
// stray kill switch can't silently weaken the oracle), restores prior
// state on return, and reports each divergence as a failed subtest named
// after the disabled toggle.
func RunDifferential(ctx context.Context, t *testing.T, queries []Query, run RunFunc) {
	toggles := optswitch.All()
	if len(toggles) == 0 {
		t.Fatal("no toggles registered — optswitch packages not linked?")
	}

	prev := make(map[string]bool, len(toggles))
	for _, tg := range toggles {
		prev[tg.Name] = tg.Set(true)
	}
	defer func() {
		for _, tg := range toggles {
			tg.Set(prev[tg.Name])
		}
	}()

	baseline := make(map[string]*Canon, len(queries))
	for _, q := range queries {
		res, err := run(ctx, q.SQL)
		if err != nil {
			t.Fatalf("baseline %s: %v", q.Name, err)
		}
		baseline[q.Name] = Canonicalize(res)
	}

	names := make([]string, len(toggles))
	for i, tg := range toggles {
		names[i] = tg.Name
	}
	t.Logf("oracle: %d queries × %d configurations (toggles: %s)",
		len(queries), len(toggles)+1, strings.Join(names, ", "))

	runConfig := func(label string, off []*optswitch.Toggle) {
		t.Run(label, func(t *testing.T) {
			for _, tg := range off {
				tg.Set(false)
			}
			defer func() {
				for _, tg := range off {
					tg.Set(true)
				}
			}()
			for _, q := range queries {
				res, err := run(ctx, q.SQL)
				if err != nil {
					t.Errorf("%s: query failed with %s disabled: %v", q.Name, label, err)
					continue
				}
				if diff := baseline[q.Name].Diff(Canonicalize(res), q); diff != "" {
					t.Errorf("%s diverges with %s disabled:\n%s", q.Name, label, diff)
				}
			}
		})
	}

	for _, tg := range toggles {
		runConfig(tg.Name, []*optswitch.Toggle{tg})
	}
	runConfig("all-off", toggles)
}

// Canon is a result in canonical comparable form. Exported so other
// differential harnesses (the standalone-vs-distributed oracle, #288)
// share the same comparison semantics as the kill-switch oracle.
//
// Rows are rendered at two float precisions. Rounding floats can land a
// value exactly on a rounding boundary, where one ULP of accumulation-
// order noise between two CORRECT runs flips the rendered digit (observed:
// SUM at 14903.55 rendering 14903.5 vs 14903.6). A divergence therefore
// only counts when it survives BOTH precisions — real bugs (missing rows,
// nulls, dropped limits, wrong groups) differ at every precision, while a
// boundary hit at two independent quanta simultaneously is vanishingly
// rare. The cost: a float-only error smaller than the coarse quantum
// (~1e-4 relative) is absorbed; row-membership and integer errors are
// unaffected.
type Canon struct {
	rows       []string // canonical per-row encodings at 6 sig digits, sorted
	coarseRows []string // same rows at 4 sig digits, sorted
}

// Canonicalize renders every row to a stable string: cells in Columns
// order, floats at 6 significant digits so accumulation-order noise
// between two correct runs doesn't register as divergence. Rows are
// sorted, so the comparison is order-insensitive.
func Canonicalize(res *Result) *Canon {
	c := CanonicalizeOrdered(res)
	sort.Strings(c.rows)
	sort.Strings(c.coarseRows)
	return c
}

// CanonicalizeOrdered is Canonicalize without the sort: rows stay in the
// order the engine returned them. Use it when a top-level ORDER BY makes
// the row SEQUENCE part of the answer — comparing sorted canonical forms
// there would accept a result that lost its ordering.
func CanonicalizeOrdered(res *Result) *Canon {
	rows := make([]string, len(res.Rows))
	coarse := make([]string, len(res.Rows))
	var sb, csb strings.Builder
	for i, row := range res.Rows {
		sb.Reset()
		csb.Reset()
		for j, col := range res.Columns {
			if j > 0 {
				sb.WriteByte('|')
				csb.WriteByte('|')
			}
			fine, rough := canonCell(row[col])
			sb.WriteString(fine)
			csb.WriteString(rough)
		}
		rows[i] = sb.String()
		coarse[i] = csb.String()
	}
	return &Canon{rows: rows, coarseRows: coarse}
}

// Rows returns the number of canonical rows.
func (b *Canon) Rows() int { return len(b.rows) }

func canonCell(v any) (fine, rough string) {
	switch tv := v.(type) {
	case nil:
		return "<null>", "<null>"
	case float64:
		return canonFloat(tv, 6), canonFloat(tv, 4)
	case float32:
		return canonFloat(float64(tv), 6), canonFloat(float64(tv), 4)
	case []byte:
		s := string(tv)
		return s, s
	default:
		s := fmt.Sprint(tv)
		return s, s
	}
}

func canonFloat(f float64, prec int) string {
	if f == 0 {
		f = 0 // collapse -0
	}
	return strconv.FormatFloat(f, 'g', prec, 64)
}

// Diff returns "" when other matches, else a description of the first
// difference.
func (b *Canon) Diff(other *Canon, q Query) string {
	if q.CountOnly {
		diff := len(other.rows) - len(b.rows)
		if diff < -q.Tolerance || diff > q.Tolerance {
			return fmt.Sprintf("row count %d, baseline %d (±%d allowed)",
				len(other.rows), len(b.rows), q.Tolerance)
		}
		return ""
	}
	if len(other.rows) != len(b.rows) {
		return fmt.Sprintf("row count %d, baseline %d", len(other.rows), len(b.rows))
	}
	for i := range b.rows {
		if b.rows[i] != other.rows[i] {
			// Fine-precision mismatch: confirm at coarse precision before
			// declaring divergence (see the Canon doc comment).
			if coarseEqual(b.coarseRows, other.coarseRows) {
				return ""
			}
			return fmt.Sprintf("first differing row (canonical order, index %d):\n  baseline: %s\n  got:      %s",
				i, b.rows[i], other.rows[i])
		}
	}
	return ""
}

func coarseEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
