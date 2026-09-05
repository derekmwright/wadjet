package auth

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/config"
)

// A `query_limit` obligation narrows the query cost guard for one identity on
// one relation.
//
// `target` names WHICH of the guard's ceilings it sets and `value` is the
// number. An empty target means `max_scan_rows`, which is the only reading
// docs/security.md's obligation table ever gave it ("Value: row count").
//
//	obligations:
//	  - type: query_limit
//	    value: "1000000"          # max_scan_rows
//	  - type: query_limit
//	    target: max_scan_bytes
//	    value: "1073741824"
//
// The three ceilings are the three the guard already has. `require_filter_
// above_bytes` and `require_limit_above_rows` are deliberately NOT settable
// from a policy: they are shaped like advice to the query author, not like a
// ceiling on an identity, and an obligation that made a statement fail for
// missing a WHERE clause would be a worse message than one that names a size.
const (
	QueryLimitRows  = "max_scan_rows"
	QueryLimitBytes = "max_scan_bytes"
	QueryLimitFiles = "max_scan_files"
)

// QueryLimitObligation reads one query_limit obligation, or says why it cannot
// be enforced as written. ValidateABACPolicies calls it at config load and at
// hot reload so a policy that cannot be enforced does not load; the evaluator
// calls it again on a provider built in process.
func QueryLimitObligation(ob Obligation) (target string, n int64, err error) {
	target = strings.ToLower(strings.TrimSpace(ob.Target))
	if target == "" {
		target = QueryLimitRows
	}
	switch target {
	case QueryLimitRows, QueryLimitBytes, QueryLimitFiles:
	default:
		return "", 0, fmt.Errorf("query_limit names %q, which is not a query cost ceiling; "+
			"use one of %s, %s, %s (empty means %s)",
			ob.Target, QueryLimitRows, QueryLimitBytes, QueryLimitFiles, QueryLimitRows)
	}
	raw := strings.TrimSpace(ob.Value)
	if raw == "" {
		return "", 0, fmt.Errorf("query_limit on %s carries no value; write the ceiling as a "+
			"positive integer, e.g. value: \"1000000\"", target)
	}
	n, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return "", 0, fmt.Errorf("query_limit on %s has a value that is not a number: %q", target, raw)
	}
	if n <= 0 {
		return "", 0, fmt.Errorf("query_limit on %s is %d; a ceiling of zero or less would refuse "+
			"every query, which a policy says by denying the table instead", target, n)
	}
	return target, n, nil
}

// applyQueryLimit narrows lim by one obligation. lim is created on first use,
// so a decision with no query_limit obligation carries none.
func applyQueryLimit(lim *config.QueryLimits, ob Obligation) *config.QueryLimits {
	target, n, err := QueryLimitObligation(ob)
	if err != nil {
		// Unenforceable obligations are refused at load. One that reaches
		// here anyway (a provider built in process, bypassing the config
		// loader) is DROPPED rather than allowed to widen anything — the
		// evaluator has no way to report, and dropping a ceiling that names
		// no ceiling changes nothing.
		return lim
	}
	if lim == nil {
		lim = &config.QueryLimits{}
	}
	switch target {
	case QueryLimitRows:
		if lim.MaxScanRows == 0 || n < lim.MaxScanRows {
			lim.MaxScanRows = n
		}
	case QueryLimitBytes:
		if lim.MaxScanBytes == 0 || n < lim.MaxScanBytes {
			lim.MaxScanBytes = n
		}
	case QueryLimitFiles:
		if lim.MaxScanFiles == 0 || int(n) < lim.MaxScanFiles {
			lim.MaxScanFiles = int(n)
		}
	}
	return lim
}

// NarrowQueryLimits folds b into a, keeping the SMALLER of every ceiling
// either sets. Two policed relations in one statement each impose their own
// ceiling and the statement is held to the tighter.
func NarrowQueryLimits(a, b *config.QueryLimits) *config.QueryLimits {
	if b == nil {
		return a
	}
	if a == nil {
		cp := *b
		return &cp
	}
	out := *a
	if b.MaxScanRows > 0 && (out.MaxScanRows == 0 || b.MaxScanRows < out.MaxScanRows) {
		out.MaxScanRows = b.MaxScanRows
	}
	if b.MaxScanBytes > 0 && (out.MaxScanBytes == 0 || b.MaxScanBytes < out.MaxScanBytes) {
		out.MaxScanBytes = b.MaxScanBytes
	}
	if b.MaxScanFiles > 0 && (out.MaxScanFiles == 0 || b.MaxScanFiles < out.MaxScanFiles) {
		out.MaxScanFiles = b.MaxScanFiles
	}
	return &out
}
