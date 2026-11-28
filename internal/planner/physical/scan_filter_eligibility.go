// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// scanFilterTarget is the ELIGIBILITY GUARD for every scan-level filter
// derived from a hash join's build keys — the bloom scan filter and the
// dynamic min/max range (#1677). Such a filter prunes a row group by
// comparing the build's key values with the STORED column's statistics, so it
// is right only when BOTH hold:
//
//  1. the probe key the join compares is that stored column, unmodified
//     (line q); and
//  2. the filter's values are in exactly the stored column's type and
//     representation (line r): valType, the type the build carries key col's
//     values in (exec.HashJoin.ScanFilterValueTypes), is the stored column's
//     declared type, and that type is one whose representation the type ID
//     alone fixes (storedTypeCarries).
//
// It returns the catalog scan the filter may be installed on, or nil.
//
// It is fail-closed: it walks from the join's probe down to the scan and
// every hop must be proved, or nothing is installed. The hops it can prove:
//
//   - a row filter (the Filter family, a bloom pre-check): it narrows the
//     selection and never changes a value or adds a row — a row-filter
//     policy is one;
//   - a lower hash join's probe (exec.HashJoinProbe.ForwardsProbeColumn): an
//     INNER, LEFT, SEMI or ANTI join forwarding the probe's column, with no
//     build column the key could resolve to instead;
//   - a projection that passes the column through by name, unchanged and
//     alone (projectForwards);
//   - a pipelineSource, whose own operators are walked by the same rules.
//
// Anything else — a computed, cast, renamed or masked projection (a policy
// mask is a projection), a set operation, an aggregate, a window, a LIMIT, a
// lateral, a source that is not a base-table scan — is unproved. The scan
// itself must read col as a stored column, and must not be feeding the
// multi-consumer scan cache: a filter on the scan that fills the cache would
// prune the rows every other consumer replays.
//
// Losing a pruning opportunity is acceptable; a wrong prune is a silently
// wrong answer. Before this guard the attachment walked through any
// pipelineSource to the scan and ignored the operators in between, so
// `a.ssn IN (SELECT ssn …)` under a mask ranged the stored ssn by the masked
// build keys and pruned every row group, and `(SELECT y AS x FROM t) q WHERE
// q.x IN (…)` pruned t by its stored x. With only the first condition, `a.d
// IN (SELECT ts …)` — a stored DATE probed by TIMESTAMP build keys, compared
// at the join as the date's midnight — ranged the stored day counts by the
// build's milliseconds, and `a.m IN (SELECT i …)` ranged a NUMERIC(10,2)'s
// stored unscaled 300 by the integer 3: every row group went.
func scanFilterTarget(source exec.Source, ops []exec.UnaryOperator, col string, valType batch.TypeID) *catalogScanSource {
	if scanFilterGuardOffForTest {
		return unguardedScanTarget(source)
	}
	if col == "" {
		return nil
	}
	for i := len(ops) - 1; i >= 0; i-- {
		if !opForwardsColumn(ops[i], col) {
			return nil
		}
	}
	switch s := source.(type) {
	case *catalogScanSource:
		if s.cache != nil {
			return nil
		}
		for _, c := range s.requiredCols {
			if c == col {
				if !scanFilterTypeRuleOffForTest && !storedTypeCarries(s, col, valType) {
					return nil
				}
				return s
			}
		}
		return nil
	case *pipelineSource:
		return scanFilterTarget(s.source, s.ops, col, valType)
	}
	return nil
}

// storedTypeCarries reports whether a filter whose values are of type
// valType may be compared with the statistics of s's stored column col: the
// column is declared exactly once (by its own spelling, or failing that by one
// case-insensitive match), its declared type IS valType, and valType is a
// type whose representation in the build's recorded values and in the
// reconciled row-group statistics is fixed by the type ID alone and has been
// measured to agree: the integer carriers and their domain types (INT32,
// DATE, PORT, PROTOCOL as int32; INT64, TIMESTAMP in milliseconds, IPv4, MAC
// as int64), FLOAT64 and STRING — the types exec.HashJoin.updateKeyMinMax
// records a range for, plus the int-path bloom's integer types, less
// DURATION, whose statistics this line did not measure.
//
// That excludes, by not listing them: DECIMAL (its statistics are unscaled
// integers at the column's own scale, so a value at another scale or of
// another type reads as a different quantity), CIDR (its confirmed bounds are
// a separate domain, pqt.CidrInetBound), the containers ARRAY, ROW, MAP and
// VECTOR, and the types no build filter is recorded for today (BOOL, FLOAT32,
// BYTES, IPv6, UUID) — a future filter for one of them proves its
// representation and is listed here then. No implicit promotion is proved here, in either direction:
// DATE against TIMESTAMP, INTEGER against BIGINT, an integer against a float
// or a NUMERIC, a string against a network or UUID type — each is a different
// declared type and gets no filter (#1677 line r). Converting the filter to
// the stored type would keep the pruning; it is a possible later
// optimization, not a correctness requirement.
func storedTypeCarries(s *catalogScanSource, col string, valType batch.TypeID) bool {
	if valType == exec.KeyTypeUnresolved {
		return false
	}
	var decl *parquet.Column
	for i := range s.storedCols {
		if s.storedCols[i].Name == col {
			decl = &s.storedCols[i]
			break
		}
	}
	if decl == nil {
		for i := range s.storedCols {
			if strings.EqualFold(s.storedCols[i].Name, col) {
				if decl != nil {
					return false
				}
				decl = &s.storedCols[i]
			}
		}
	}
	if decl == nil || decl.Type != valType {
		return false
	}
	switch valType {
	case batch.TypeInt32, batch.TypeDate, batch.TypePort, batch.TypeProtocol,
		batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC,
		batch.TypeFloat64, batch.TypeString:
		return true
	}
	return false
}

// scanFilterTypeRuleOffForTest switches off the guard's second condition
// alone (storedTypeCarries), keeping line q's walk: the negative control for
// line r's type rule. TEST ONLY.
var scanFilterTypeRuleOffForTest bool

// scanFilterGuardOffForTest restores the walk the guard replaced (through any
// pipelineSource to the scan, ignoring every operator): the negative control
// for the guard's tests. TEST ONLY.
var scanFilterGuardOffForTest bool

func unguardedScanTarget(source exec.Source) *catalogScanSource {
	switch s := source.(type) {
	case *catalogScanSource:
		return s
	case *pipelineSource:
		return unguardedScanTarget(s.source)
	}
	return nil
}

// opForwardsColumn reports whether op is proved to hand its input's column
// col to its output unchanged, adding no rows (scanFilterTarget).
func opForwardsColumn(op exec.UnaryOperator, col string) bool {
	switch o := op.(type) {
	case *exec.Filter, *exec.ChainFilter, *exec.ColColFilter, *exec.InFilter,
		*exec.KernelFilter, *exec.LikeFilter, *exec.MatchNothingFilter,
		*exec.NullCheckFilter, *exec.OrFilter, *exec.BloomFilterOp:
		return true
	case *exec.HashJoinProbe:
		return o.ForwardsProbeColumn(col)
	case *exec.Project:
		return projectForwards(o, col)
	}
	return false
}

// projectForwards reports whether p passes its input column col through
// under the same name, as a plain copy, and is the only output a reference
// to col can resolve to. A rename, a computation, a cast, a mask or a
// positional source is not a proof.
func projectForwards(p *exec.Project, col string) bool {
	bare := bareColumn(col)
	found := false
	for _, pc := range p.Projections {
		if !strings.EqualFold(bareColumn(pc.Name), bare) {
			continue
		}
		if found || pc.Name != col || pc.DirectCopy != col || pc.Computed || pc.SourceIdxSet {
			return false
		}
		found = true
	}
	return found
}

// bareColumn is name without its qualifier: the part after the last dot.
func bareColumn(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}
