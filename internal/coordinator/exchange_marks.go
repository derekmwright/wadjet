// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"sort"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// exchangeMarkRegistry records, per stage-output FILE, the DECIMAL columns the
// task that wrote it wrote as columns created from an unconstrained numeric
// (ADR-0024 §10, distributed.ResultNotification.UnconstrainedColumns).
//
// A `.wshf` exchange header carries a column's name, type, precision and
// scale and nothing else, and it has no version field to grow one, so the
// mark cannot travel inside the file. It travels beside it: the producing
// task reports it with its files (noteTaskResult), and every task that reads
// those files is stamped with it at dispatch (stampTaskUnconstrained) — the
// way merge-on-read delete markers ride the file key — so the reader marks
// the batches it decodes (worker applyDeclaredScanSchema →
// exec.UnconstrainedStamp). Without it a column read back after a GROUP BY,
// a DISTINCT, a set operation, a window or a join printed its stored scale
// on the DAG arms only (`CAST(v AS TEXT) = '1'` counted 0 there and 1 on
// the single-process arms).
type exchangeMarkRegistry struct {
	mu      sync.RWMutex
	byFile  map[string][]string
	byQuery map[string][]string // task QueryID → files, for cleanup
}

func newExchangeMarkRegistry() *exchangeMarkRegistry {
	return &exchangeMarkRegistry{byFile: map[string][]string{}, byQuery: map[string][]string{}}
}

// record notes one successful task's files and marks.
func (r *exchangeMarkRegistry) record(res distributed.ResultNotification) {
	if r == nil || !res.Success || len(res.UnconstrainedColumns) == 0 {
		return
	}
	files := res.ResultFiles
	if len(files) == 0 && res.ResultPath != "" {
		files = []string{res.ResultPath}
	}
	if len(files) == 0 {
		return
	}
	marks := append([]string(nil), res.UnconstrainedColumns...)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range files {
		r.byFile[f] = marks
	}
	r.byQuery[res.QueryID] = append(r.byQuery[res.QueryID], files...)
}

// marks is the union of the marks recorded for files.
func (r *exchangeMarkRegistry) marks(files []string) []string {
	if r == nil || len(files) == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.byFile) == 0 {
		return nil
	}
	var set map[string]bool
	for _, f := range files {
		for _, n := range r.byFile[f] {
			if set == nil {
				set = make(map[string]bool)
			}
			set[n] = true
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// forgetQuery drops the files the named task QueryIDs reported: every
// query ID that starts with root (stage-scoped and repartition-side task
// query IDs embed the root query's).
func (r *exchangeMarkRegistry) forgetQuery(root string) {
	if r == nil || root == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for q, files := range r.byQuery {
		if !strings.Contains(q, root) {
			continue
		}
		for _, f := range files {
			delete(r.byFile, f)
		}
		delete(r.byQuery, q)
	}
}

// stampTaskUnconstrained gives every exchange read a task carries the marks
// its files were written with: an operator's input and build files, and a
// shuffle or gather task's own files. A read of base-table parquet has none
// recorded (its marks ride the declared schema, ColumnSpec.Unconstrained).
func stampTaskUnconstrained(t *distributed.Task, r *exchangeMarkRegistry) {
	if t == nil || r == nil {
		return
	}
	var own []string
	own = append(own, t.Files...)
	own = append(own, t.InputFiles...)
	for _, fs := range t.Inputs {
		own = append(own, fs...)
	}
	if m := r.marks(own); m != nil {
		t.Unconstrained = m
	}
	for i := range t.Operators {
		op := &t.Operators[i]
		if m := r.marks(op.InputFiles); m != nil {
			op.Unconstrained = m
		}
		if m := r.marks(op.BuildFiles); m != nil {
			op.BuildUnconstrained = m
		}
	}
}
