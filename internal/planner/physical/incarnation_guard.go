// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// IncarnationChangedError is the refusal of a statement an authorization
// decision of which may have been about another relation than the one the
// statement reads: the table's RECORD — the metadata enforcement reads for
// the column list — names another incarnation than the manifest the statement
// pinned, or is gone (ADR-0050 C3).
//
// 40001, the class this engine already gives a concurrent catalog change a
// client should retry (wadjet/dml.go, catalog/dml_commit.go). PostgreSQL does
// not reach this state: its DROP waits for the running statement. The
// refusal is FINAL: no other plan is built or run in its place.
type IncarnationChangedError struct {
	Table string
}

func (e *IncarnationChangedError) Error() string {
	return fmt.Sprintf("table %q was dropped or re-created under this statement; retry it", e.Table)
}

// SQLState makes the refusal a coded one (sqlerr.Coder).
func (e *IncarnationChangedError) SQLState() string { return "40001" }

// IsIncarnationChanged reports whether err is (or wraps) the refusal.
func IsIncarnationChanged(err error) bool {
	var ic *IncarnationChangedError
	return errors.As(err, &ic)
}

// incarnationGuard brackets every authorization decision a statement takes
// about a base table (ADR-0050 §3):
//
//   - C1, pin first: PinForDecision reads the table's manifest through the
//     statement's ManifestSnapshot — the pin every planner and scan of the
//     statement then shares — BEFORE the decision reads anything else.
//   - C2, verify after: VerifyIncarnations compares the table RECORD's
//     incarnation with the pinned manifest's, after the decision.
//
// An incarnation is minted once per CREATE and never reused, so equality at
// the verify means the name meant the pinned relation for every read between
// the pin and that moment: one verify after several decisions covers all of
// them. That is why a table is only DIRTY between a decision and the next
// verify, and a verify reads the record of the dirty tables alone.
type incarnationGuard struct {
	cat  *catalog.Catalog
	snap *ManifestSnapshot

	mu sync.Mutex
	// resolved memoizes the catalog spelling of each name a decision was
	// asked under (catalog.ResolveTableName reads the catalog's table list),
	// so a decision asked again costs no catalog read.
	resolved map[string]string
	// dirty is the set of catalog spellings decided since the last verify,
	// in first-decision order so a refusal names the first relation it finds.
	dirty  []string
	isDirt map[string]bool
}

type incarnationGuardKey struct{}

// WithIncarnationGuard turns the guard on for the statement ctx carries. The
// statement's ManifestSnapshot must already be on ctx (it is the pin); without
// one there is nothing to bracket against and ctx is returned unchanged. A
// ctx that already carries a guard keeps it — a re-execution of the same
// statement shares its pins (ADR-0050 C4).
//
// Only a door that supplies its own pin turns this on (the coordinator's
// ExecuteSQL). Every other door reaches PinForDecision and VerifyIncarnations
// with no guard on its context, where both do nothing.
func WithIncarnationGuard(ctx context.Context, cat *catalog.Catalog) context.Context {
	if cat == nil || incarnationGuardFrom(ctx) != nil {
		return ctx
	}
	snap := manifestSnapshotFromContext(ctx)
	if snap == nil {
		return ctx
	}
	return context.WithValue(ctx, incarnationGuardKey{}, &incarnationGuard{
		cat: cat, snap: snap,
		resolved: map[string]string{}, isDirt: map[string]bool{},
	})
}

// HasManifestSnapshot reports whether ctx carries a statement's manifest pin
// (WithManifestSnapshot), so a re-execution of the same statement can keep it
// rather than attach a new one (ADR-0050 C4).
func HasManifestSnapshot(ctx context.Context) bool {
	return manifestSnapshotFromContext(ctx) != nil
}

func incarnationGuardFrom(ctx context.Context) *incarnationGuard {
	if ctx == nil {
		return nil
	}
	g, _ := ctx.Value(incarnationGuardKey{}).(*incarnationGuard)
	return g
}

// incarnationGuardTestHook runs after PinForDecision has pinned a table, with
// the catalog spelling. Tests set it to drop and re-create the table between
// the pin and the decision it brackets. Nil in every non-test build.
var incarnationGuardTestHook func(table string)

// PinForDecision is C1: call it BEFORE an authorization decision about table,
// and before the decision reads the table's record. It pins the table's
// manifest in the statement's snapshot (a no-op read when a planner already
// did) and marks the table for the next VerifyIncarnations. Without a guard
// on ctx it does nothing.
func PinForDecision(ctx context.Context, table string) {
	g := incarnationGuardFrom(ctx)
	if g == nil || table == "" {
		return
	}
	g.mu.Lock()
	name, ok := g.resolved[table]
	g.mu.Unlock()
	if !ok {
		name = g.cat.ResolveTableName(table)
		g.mu.Lock()
		g.resolved[table] = name
		g.mu.Unlock()
	}
	// The pin. An error (no manifest under this name) is pinned too, and is
	// what every read of the table in this statement then returns.
	g.snap.Get(ctx, g.cat, name)
	g.mu.Lock()
	if !g.isDirt[name] {
		g.isDirt[name] = true
		g.dirty = append(g.dirty, name)
	}
	g.mu.Unlock()
	if incarnationGuardTestHook != nil {
		incarnationGuardTestHook(name)
	}
}

// VerifyIncarnations is C2: call it after decisions, before their effect can
// reach the client. For each table decided since the last verify it reads the
// table's record once and compares its incarnation with the pinned
// manifest's. A mismatch — or a record that is gone while the pin holds a
// relation — is IncarnationChangedError (C3). Without a guard, or with
// nothing decided since the last verify, it reads nothing.
//
// A table whose pin is itself an error (the name had no manifest when it was
// pinned) is not compared: no relation was pinned, and every read of the
// table in this statement returns that error, so no decision can be applied
// to another relation's files.
//
// Legacy tables, whose record and manifest were written before incarnations
// existed, carry "" on both and compare equal; once dropped and re-created,
// the record carries a minted incarnation and the pinned manifest still "".
func VerifyIncarnations(ctx context.Context) error {
	g := incarnationGuardFrom(ctx)
	if g == nil {
		return nil
	}
	g.mu.Lock()
	tables := g.dirty
	g.dirty = nil
	g.isDirt = map[string]bool{}
	g.mu.Unlock()
	for i, name := range tables {
		g.snap.mu.Lock()
		e := g.snap.byTbl[name]
		g.snap.mu.Unlock()
		if e == nil || e.err != nil || e.manifest == nil {
			continue
		}
		meta, err := g.cat.GetTable(ctx, name)
		if err != nil && !errors.Is(err, catalog.ErrTableNotFound) {
			// The catalog could not answer: the decision is not known to be
			// about the pinned relation. Keep the remaining tables dirty so
			// a later verify asks again, and report the read failure.
			g.redirty(tables[i:])
			return fmt.Errorf("verifying table %q under this statement: %w", name, err)
		}
		if meta == nil || meta.Incarnation != e.manifest.Incarnation {
			return &IncarnationChangedError{Table: name}
		}
	}
	return nil
}

func (g *incarnationGuard) redirty(tables []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, name := range tables {
		if !g.isDirt[name] {
			g.isDirt[name] = true
			g.dirty = append(g.dirty, name)
		}
	}
}
