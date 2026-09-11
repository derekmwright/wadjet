package physical

import (
	"fmt"
	"os"
	"regexp"
	"sync/atomic"
)

// ExchangeSubsume gates dedupeSubsumedScanExchanges. Kill switch
// WADJET_EXCHANGE_SUBSUME=0. Exported atomic.Bool (ScalarAggSemijoin
// pattern) so tests can pin either arm.
var ExchangeSubsume atomic.Bool

func init() {
	ExchangeSubsume.Store(os.Getenv("WADJET_EXCHANGE_SUBSUME") != "0")
}

// dedupeSubsumedScanExchanges replaces filtered exchange B with raw sibling A.
// Each has one scan dependency on the same table; A is filter-free, B has
// FilterExprs; Exchange.Keys and Count match exactly. B has one consumer using
// it as RightDepStage, with no existing BuildFilterExprs.
// B's scan payload columns must be in A or referenced only by the residual;
// consumer RightKeys/JoinFilter references must all be in A's payload.
// Append the residual as a boolean column to A; the consumer filters build
// rows on it at read, equivalent to filtering at B's dropped scan.
// See docs/internals/subsumed-scan-exchanges.md for the design.
func dedupeSubsumedScanExchanges(stages []Stage) []Stage {
	if !ExchangeSubsume.Load() {
		return stages
	}
	idIndex := make(map[string]int, len(stages))
	consumers := make(map[string][]int) // stage ID -> indexes of stages depending on it
	for i := range stages {
		idIndex[stages[i].ID] = i
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], i)
		}
	}
	scanOf := func(s *Stage) *Stage {
		if s.Type != StageExchangeRepartition || s.Exchange == nil || len(s.Dependencies) != 1 {
			return nil
		}
		idx, ok := idIndex[s.Dependencies[0]]
		if !ok || stages[idx].Type != StageScan {
			return nil
		}
		return &stages[idx]
	}

	dropped := make(map[string]bool) // dropped stage IDs (exchange + its scan)
	flagSeq := 0
	for bi := range stages {
		b := &stages[bi]
		scanB := scanOf(b)
		if scanB == nil || len(scanB.FilterExprs) == 0 || len(consumers[scanB.ID]) != 1 {
			continue
		}
		// Sole consumer of B must use it as the build side.
		bCons := consumers[b.ID]
		if len(bCons) != 1 {
			continue
		}
		cons := &stages[bCons[0]]
		if cons.RightDepStage != b.ID || len(cons.BuildFilterExprs) > 0 {
			continue
		}
		for ai := range stages {
			a := &stages[ai]
			if ai == bi || dropped[a.ID] {
				continue
			}
			scanA := scanOf(a)
			if scanA == nil || len(scanA.FilterExprs) > 0 || scanA.TableName != scanB.TableName {
				continue
			}
			if !keysEqual(a.Exchange.Keys, b.Exchange.Keys) || a.Exchange.Count != b.Exchange.Count {
				continue
			}
			// …and hashed at the same TYPE. Two exchanges over one table with
			// one key list are only interchangeable when they also agree
			// about the width they hashed at (#615); serving a consumer from
			// a sibling that partitioned a cross-width key differently sends
			// its rows to partitions its join never looks in.
			if !keyTypesEqual(a.Exchange.KeyTypes, b.Exchange.KeyTypes, len(a.Exchange.Keys)) {
				continue
			}
			if !subsumePayloadCovered(scanA, scanB, cons) {
				continue
			}
			// Rewrite: flag col on A, consumer build reads A + filter.
			flag := fmt.Sprintf("%s%d", SlotSubsumeFlag, flagSeq)
			flagSeq++
			residual := conjoin(scanB.FilterExprs)
			a.Exchange.ComputedCols = append(a.Exchange.ComputedCols, ComputedCol{
				Name: flag,
				Expr: residual,
			})
			// The flag's inputs must be READ by A's scan without joining
			// the shipped payload: list them so the dispatcher widens the
			// parquet read and the worker drops them post-compute.
			aColSet := make(map[string]bool, len(scanA.Columns))
			for _, c := range scanA.Columns {
				aColSet[c] = true
			}
			for _, c := range scanB.Columns {
				if !aColSet[c] && identifierInExpr(residual, c) && !hasColumn(a.Exchange.ExtraReadCols, c) {
					a.Exchange.ExtraReadCols = append(a.Exchange.ExtraReadCols, c)
				}
			}
			cons.BuildFilterExprs = []string{flag}
			cons.RightDepStage = a.ID
			for i, d := range cons.Dependencies {
				if d == b.ID {
					cons.Dependencies[i] = a.ID
				}
			}
			dropped[b.ID] = true
			dropped[scanB.ID] = true
			break
		}
	}
	if len(dropped) == 0 {
		return stages
	}
	out := make([]Stage, 0, len(stages)-len(dropped))
	for _, s := range stages {
		if !dropped[s.ID] {
			out = append(out, s)
		}
	}
	return out
}

// subsumePayloadCovered checks the column-coverage conditions: B's payload
// columns are each either shipped by A or referenced only by B's residual
// filter, and the consumer's build-side references resolve against A's
// payload.
func subsumePayloadCovered(scanA, scanB, cons *Stage) bool {
	aCols := make(map[string]bool, len(scanA.Columns))
	for _, c := range scanA.Columns {
		aCols[c] = true
	}
	residual := conjoin(scanB.FilterExprs)
	for _, c := range scanB.Columns {
		if !aCols[c] && !identifierInExpr(residual, c) {
			return false
		}
	}
	// Residual must be evaluable at B's scan (its identifiers come from
	// scanB's own columns) — guaranteed by construction (planner pushed it
	// onto scanB). The consumer's build references must exist in A's
	// payload.
	for _, k := range cons.JoinRightKeys {
		if !aCols[baseColName(k)] {
			return false
		}
	}
	if cons.JoinFilter != "" {
		for _, id := range extractIdentifiers(cons.JoinFilter) {
			base := baseColName(id)
			// Build-side references must be shipped; probe-side identifiers
			// resolve against the probe schema, so only reject identifiers
			// that look like B's table columns and are missing from A.
			if hasColumn(scanB.Columns, base) && !aCols[base] {
				return false
			}
		}
	}
	return true
}

func conjoin(exprs []string) string {
	if len(exprs) == 1 {
		return exprs[0]
	}
	out := ""
	for i, e := range exprs {
		if i > 0 {
			out += " AND "
		}
		out += "(" + e + ")"
	}
	return out
}

var identRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.]*`)

// identifierInExpr reports whether col appears as a whole identifier (or
// the trailing segment of a qualified identifier) in expr.
func identifierInExpr(expr, col string) bool {
	for _, id := range extractIdentifiers(expr) {
		if baseColName(id) == col {
			return true
		}
	}
	return false
}

func extractIdentifiers(expr string) []string {
	return identRe.FindAllString(expr, -1)
}

func baseColName(id string) string {
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == '.' {
			return id[i+1:]
		}
	}
	return id
}

func hasColumn(cols []string, c string) bool {
	for _, x := range cols {
		if x == c {
			return true
		}
	}
	return false
}
