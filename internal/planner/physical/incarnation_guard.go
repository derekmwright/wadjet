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

// isIncarnationChanged reports whether err is (or wraps) the refusal.
func isIncarnationChanged(err error) bool {
	var ic *IncarnationChangedError
	return errors.As(err, &ic)
}

// incarnationGuard holds a coordinator statement to one relation per table
// name (ADR-0050 §3, amended 2026-11-19: every statement, with or without an
// authorization provider). Two halves share it.
//
// Every READ of a table's record under the statement (the binder's column
// list and star expansion, the scan annotation, the physical planner's
// declared schema, the stage planner, a route's local pipeline, a subquery
// planned while the plan runs) is observed (catalog.TableReadObserver). The
// first read of a table pins its manifest in the statement's snapshot before
// the record reaches its reader (C1), and every read compares the record's
// incarnation with the pin's; a mismatch, or a record gone under a pinned
// relation, is kept and refused at the next verify (C2, C3). This reads no
// extra record: the record compared is the one the reader asked for.
//
// The authorization decisions bracket themselves too, as slice 2 built them,
// because a decision can be taken by name without reading the record:
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
	// changed is the first table a record read found under another
	// incarnation than the statement pinned (or gone under a pinned
	// relation). Never cleared: the refusal is final (C3).
	changed string
}

type incarnationGuardKey struct{}

// withIncarnationGuard turns the guard on for the statement ctx carries. The
// statement's ManifestSnapshot must already be on ctx (it is the pin); without
// one there is nothing to bracket against and ctx is returned unchanged. A
// ctx that already carries a guard keeps it — a re-execution of the same
// statement shares its pins (ADR-0050 C4).
//
// Only a door that supplies its own pin turns this on (the coordinator's
// ExecuteSQL). Every other door reaches PinForDecision and verifyIncarnations
// with no guard on its context, where both do nothing.
func withIncarnationGuard(ctx context.Context, cat *catalog.Catalog) context.Context {
	if cat == nil || incarnationGuardFrom(ctx) != nil {
		return ctx
	}
	snap := manifestSnapshotFromContext(ctx)
	if snap == nil {
		return ctx
	}
	g := &incarnationGuard{
		cat: cat, snap: snap,
		resolved: map[string]string{}, isDirt: map[string]bool{},
	}
	ctx = context.WithValue(ctx, incarnationGuardKey{}, g)
	return catalog.WithTableReadObserver(ctx, g.observeRead)
}

// observeRead sees every table-record read the statement makes. A record
// read pins the table's manifest (a memoized no-op once anything has) and
// must name the pinned incarnation. A read that found no record refuses only
// when a relation is pinned under the name: the table was dropped under the
// statement. A name with no record (a system relation, a spelling probe)
// pins nothing.
//
// A table whose pin is itself an error (no manifest under the name) is not
// compared: no relation's files can be read under it in this statement.
func (g *incarnationGuard) observeRead(ctx context.Context, name string, meta *catalog.TableMeta, err error) {
	switch {
	case err == nil && meta != nil:
		// The pin. Its own reads are not observed.
		m, merr := g.snap.Get(catalog.WithoutTableReadObserver(ctx), g.cat, name)
		if merr != nil || m == nil || m.Incarnation == meta.Incarnation {
			return
		}
	case errors.Is(err, catalog.ErrTableNotFound):
		g.snap.mu.Lock()
		e := g.snap.byTbl[name]
		g.snap.mu.Unlock()
		if e == nil || e.err != nil || e.manifest == nil {
			return
		}
	default:
		// The catalog could not answer: the reader sees that error itself.
		return
	}
	g.mu.Lock()
	if g.changed == "" {
		g.changed = name
	}
	g.mu.Unlock()
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
// did) and marks the table for the next verifyIncarnations. Without a guard
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

// verifyIncarnations is C2: call it after decisions, before their effect can
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
func verifyIncarnations(ctx context.Context) error {
	g := incarnationGuardFrom(ctx)
	if g == nil {
		return nil
	}
	g.mu.Lock()
	if g.changed != "" {
		name := g.changed
		g.mu.Unlock()
		return &IncarnationChangedError{Table: name}
	}
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

// The guard's operations as the AGPL side reaches them: through the
// PlanContext value, as it reaches WithManifestSnapshot (the seam's
// established shape; tools/licensecheck counts each as a member of the seam).
// PinForDecision stays a package-scope function: only the MIT side
// (internal/auth, and this package's policy reads) calls it.

// WithIncarnationGuard turns the guard on for the statement ctx carries
// (withIncarnationGuard).
func (PlanContext) WithIncarnationGuard(ctx context.Context, cat *catalog.Catalog) context.Context {
	return withIncarnationGuard(ctx, cat)
}

// VerifyIncarnations is C2 (verifyIncarnations).
func (PlanContext) VerifyIncarnations(ctx context.Context) error {
	return verifyIncarnations(ctx)
}

// IsIncarnationChanged reports whether err is (or wraps) the C3 refusal.
func (PlanContext) IsIncarnationChanged(err error) bool {
	return isIncarnationChanged(err)
}
