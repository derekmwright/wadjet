package oracle

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// This file carries the determinism policy the generated-query differentials
// (#288 follow-on) compare under. A generated query is only worth running if a
// mismatch always means a defect, so every shape declares up front how much of
// its answer SQL actually pins down:
//
//   - no ORDER BY            → the row SEQUENCE is not part of the answer, so
//                              compare the multiset (CmpUnordered).
//   - ORDER BY, total order  → the sequence IS the answer and no two rows tie,
//                              so compare positionally (CmpOrdered).
//   - ORDER BY, ties possible→ compare the multiset AND the sequence of the
//                              ORDER BY KEY values (OrderKeys). Tied rows carry
//                              equal keys, so tie order can differ freely while
//                              a dropped ORDER BY still shows up.
//   - bare LIMIT             → which rows come back is genuinely arbitrary, so
//                              compare row counts only (CmpCount).
//
// CheckOrder is the absolute arm: it re-derives the ordering from the returned
// key columns and fails a result that is not actually sorted. It needs no
// second engine, so it catches an ORDER BY that both comparison arms drop the
// same way — the shape that made today's ORDER BY defects invisible to a
// two-arm compare.

// CmpMode selects how strictly two results are compared.
type CmpMode int

const (
	// CmpUnordered compares the row multiset; row order is ignored.
	CmpUnordered CmpMode = iota
	// CmpOrdered compares rows positionally: the sequence is part of the answer.
	CmpOrdered
	// CmpCount compares row counts only.
	CmpCount
)

func (m CmpMode) String() string {
	switch m {
	case CmpOrdered:
		return "ordered"
	case CmpCount:
		return "count-only"
	default:
		return "unordered"
	}
}

// OrderKey names an output column that carries one ORDER BY key's value,
// together with the direction the query asked for.
type OrderKey struct {
	Alias string
	Desc  bool
}

// CompareSpec is everything the harness needs to compare one query's results
// without producing a false positive.
type CompareSpec struct {
	Mode CmpMode
	// Limit, when > 0, is the trailing LIMIT both sides must respect.
	Limit int
	// OrderKeys are the projected ORDER BY keys, in ORDER BY sequence. Under
	// CmpUnordered they turn into a positional comparison of just those
	// columns, which is tie-immune. Empty when the keys are not projected.
	OrderKeys []OrderKey
}

// Compare returns "" when got matches want under spec, else a description of
// the first difference. want is the reference arm (DuckDB, the fast path, or
// the all-optimizations-on baseline).
func Compare(want, got *Result, spec CompareSpec) string {
	if spec.Limit > 0 {
		if n := len(got.Rows); n > spec.Limit {
			return fmt.Sprintf("got %d rows for LIMIT %d", n, spec.Limit)
		}
		if n := len(want.Rows); n > spec.Limit {
			return fmt.Sprintf("baseline returned %d rows for LIMIT %d", n, spec.Limit)
		}
	}
	if spec.Mode == CmpCount {
		if len(want.Rows) != len(got.Rows) {
			return fmt.Sprintf("row count %d, baseline %d", len(got.Rows), len(want.Rows))
		}
		return ""
	}

	if diff := missingColumns(want, got); diff != "" {
		return diff
	}
	// Compare on the reference arm's column list so a divergence is reported
	// in the columns the query asked for, and a column the other arm does not
	// carry is a failure rather than a silently-null cell.
	a := &Result{Columns: want.Columns, Rows: want.Rows}
	b := &Result{Columns: want.Columns, Rows: got.Rows}

	var ca, cb *Canon
	if spec.Mode == CmpOrdered {
		ca, cb = CanonicalizeOrdered(a), CanonicalizeOrdered(b)
	} else {
		ca, cb = Canonicalize(a), Canonicalize(b)
	}
	if diff := ca.Diff(cb, Query{}); diff != "" {
		return diff
	}

	// Under CmpUnordered the multiset compare above says nothing about row
	// sequence. Comparing just the ORDER BY key columns positionally does,
	// and tied rows carry equal keys so tie order cannot flake it.
	if spec.Mode == CmpUnordered && len(spec.OrderKeys) > 0 {
		cols := make([]string, len(spec.OrderKeys))
		for i, k := range spec.OrderKeys {
			cols[i] = k.Alias
		}
		ka := CanonicalizeOrdered(&Result{Columns: cols, Rows: want.Rows})
		kb := CanonicalizeOrdered(&Result{Columns: cols, Rows: got.Rows})
		if diff := ka.Diff(kb, Query{}); diff != "" {
			return "ORDER BY key sequence differs (rows themselves match, so this is an ordering divergence):\n" + diff
		}
	}
	return ""
}

func missingColumns(want, got *Result) string {
	have := map[string]bool{}
	for _, c := range got.Columns {
		have[c] = true
	}
	for _, c := range want.Columns {
		if !have[c] {
			return fmt.Sprintf("result has no column %q (baseline columns %v, got %v)", c, want.Columns, got.Columns)
		}
	}
	return ""
}

// CheckOrder reports the first row that breaks the ordering the query asked
// for, or "" when the sequence holds. It is an ABSOLUTE check — no second
// engine — so it catches an ORDER BY that every comparison arm drops the same
// way. Keys must name columns the result projects.
//
// NULL policy: PostgreSQL's, which is what wadjet implements and what the
// DuckDB arm CONFIGURES the reference engine to use
// (default_null_order='nulls_last_on_asc_first_on_desc') — NULLS LAST for ASC,
// NULLS FIRST for DESC. ADR-0012 makes PostgreSQL the authority on semantics,
// so the absolute checker has to encode the same rule the comparison arms are
// configured for.
//
// It did not. Until this was corrected, compareRowsByKeys placed NULLs last in
// BOTH directions and explicitly refused to flip a NULL-involving pair under
// DESC, so a DESC result with NULLs FIRST — the correct answer — was reported
// as an ordering failure. Nothing caught it because TPC-H contains no NULLs at
// all, so the fixed corpus and the shape fuzzer over it never put a NULL in a
// sort key. The type-matrix fixture, whose every column nulls on its own
// stride, hit it on its second generated seed.
func CheckOrder(res *Result, keys []OrderKey) string {
	if len(keys) == 0 || len(res.Rows) < 2 {
		return ""
	}
	for _, k := range keys {
		if !hasColumn(res, k.Alias) {
			return "" // key not projected: nothing to check against
		}
	}
	for i := 1; i < len(res.Rows); i++ {
		c, undecidable := compareRowsByKeys(res.Rows[i-1], res.Rows[i], keys)
		if undecidable {
			continue
		}
		if c > 0 {
			return fmt.Sprintf("rows are not in the order the query asked for: row %d sorts before row %d\n  row %d: %s\n  row %d: %s\n  ORDER BY %s",
				i, i-1, i-1, keyText(res.Rows[i-1], keys), i, keyText(res.Rows[i], keys), keySpec(keys))
		}
	}
	return ""
}

func hasColumn(res *Result, name string) bool {
	for _, c := range res.Columns {
		if c == name {
			return true
		}
	}
	return false
}

func keySpec(keys []OrderKey) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k.Alias
		if k.Desc {
			parts[i] += " DESC"
		}
	}
	return strings.Join(parts, ", ")
}

func keyText(row map[string]any, keys []OrderKey) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k.Alias, row[k.Alias])
	}
	return strings.Join(parts, " ")
}

// compareRowsByKeys returns -1/0/1 for a vs b under the key list. undecidable
// is true when two cells cannot be compared (mixed types), in which case the
// caller skips the pair rather than reporting a bogus ordering failure.
func compareRowsByKeys(a, b map[string]any, keys []OrderKey) (int, bool) {
	for _, k := range keys {
		c, ok := compareCells(a[k.Alias], b[k.Alias])
		if !ok {
			return 0, true
		}
		if c == 0 {
			continue
		}
		// A NULL-involving pair flips with DESC like any other: compareCells
		// puts NULL after every value, so negating it under DESC puts NULL
		// first, which is PostgreSQL's placement and the one the reference
		// engine is configured for.
		if k.Desc {
			c = -c
		}
		return c, false
	}
	return 0, false
}

// compareCells orders two result cells in ASCENDING order. NULL sorts after
// every value, which is NULLS LAST — the ASC half of PostgreSQL's placement.
// The DESC half comes from compareRowsByKeys negating this result.
func compareCells(a, b any) (int, bool) {
	if a == nil && b == nil {
		return 0, true
	}
	if a == nil {
		return 1, true
	}
	if b == nil {
		return -1, true
	}
	af, aNum := numeric(a)
	bf, bNum := numeric(b)
	if aNum && bNum {
		switch {
		case af < bf && !nearlyEqual(af, bf):
			return -1, true
		case af > bf && !nearlyEqual(af, bf):
			return 1, true
		default:
			return 0, true
		}
	}
	as, aStr := text(a)
	bs, bStr := text(b)
	if aStr && bStr {
		return strings.Compare(as, bs), true
	}
	return 0, false
}

// nearlyEqual absorbs last-ULP drift so a sort key computed slightly
// differently by two correct engines does not read as an ordering failure.
func nearlyEqual(a, b float64) bool {
	if a == b {
		return true
	}
	d := math.Abs(a - b)
	m := math.Max(math.Abs(a), math.Abs(b))
	return d/m < 1e-9
}

func numeric(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func text(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

// ParseCell converts one string cell (a CSV field from an external engine)
// into the Go type the reference result uses for that column, so canonical
// rendering compares like with like. ref is a non-nil sample value from the
// same column, or nil when the column was all-NULL on the reference side.
func ParseCell(s string, ref any) any {
	if s == "" || s == "<NULL>" {
		return nil
	}
	switch ref.(type) {
	case float64, float32:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case int, int8, int16, int32, int64:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		// An integer column rendered as "12.0" by the other engine still
		// compares equal once both sides are floats.
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case bool:
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
	}
	return s
}
