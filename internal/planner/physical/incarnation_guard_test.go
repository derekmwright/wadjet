// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func guardCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	cat := catalog.NewWithStore(objstore.NewMemStore(), "guard")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat
}

func guardCreate(t *testing.T, cat *catalog.Catalog, name string, cols ...string) {
	t.Helper()
	var schema parquet.Schema
	for _, c := range cols {
		schema.Columns = append(schema.Columns, parquet.Column{Name: c, Type: parquet.TypeInt64})
	}
	if err := cat.CreateTable(context.Background(), name, schema, nil); err != nil {
		t.Fatal(err)
	}
}

func guardRecreate(t *testing.T, cat *catalog.Catalog, name string, cols ...string) {
	t.Helper()
	if err := cat.DropTable(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	guardCreate(t, cat, name, cols...)
}

func guardCtx(cat *catalog.Catalog) context.Context {
	ctx := PlanContext{}.WithManifestSnapshot(context.Background(), NewManifestSnapshot())
	return withIncarnationGuard(ctx, cat)
}

// TestIncarnationGuardBrackets is C1-C3 at the primitive: a decision pinned
// before a drop and re-create is refused 40001 at the next verify; nothing
// changed, a verify passes; a verify covers only the decisions since the
// last one.
func TestIncarnationGuardBrackets(t *testing.T) {
	cat := guardCatalog(t)
	guardCreate(t, cat, "t", "id")
	ctx := guardCtx(cat)

	PinForDecision(ctx, "t")
	if err := verifyIncarnations(ctx); err != nil {
		t.Fatalf("nothing changed, verify refused: %v", err)
	}
	guardRecreate(t, cat, "t", "id", "ssn")
	// No decision since the last verify: nothing is asked, nothing refused.
	if err := verifyIncarnations(ctx); err != nil {
		t.Fatalf("a verify with no decision since the last one refused: %v", err)
	}
	PinForDecision(ctx, "t") // a decision about t, after the re-create
	err := verifyIncarnations(ctx)
	if !isIncarnationChanged(err) || sqlerr.StateOf(err) != "40001" {
		t.Fatalf("got %v (state %q), want the 40001 refusal", err, sqlerr.StateOf(err))
	}
	if want := `table "t" was dropped or re-created under this statement; retry it`; err.Error() != want {
		t.Fatalf("message %q, want %q", err.Error(), want)
	}
}

// TestIncarnationGuardEdges: a dropped (not re-created) table refuses; a
// name that had no manifest when pinned is not compared; a re-execution
// keeps the guard; a context without a guard or snapshot does nothing.
func TestIncarnationGuardEdges(t *testing.T) {
	cat := guardCatalog(t)
	guardCreate(t, cat, "t", "id")

	ctx := guardCtx(cat)
	PinForDecision(ctx, "t")
	if err := cat.DropTable(context.Background(), "t"); err != nil {
		t.Fatal(err)
	}
	if err := verifyIncarnations(ctx); !isIncarnationChanged(err) {
		t.Fatalf("a table dropped under the decision verified as %v", err)
	}

	ctx = guardCtx(cat)
	PinForDecision(ctx, "nope") // no such relation: the pin is the 42P01
	guardCreate(t, cat, "nope", "id")
	if err := verifyIncarnations(ctx); err != nil {
		t.Fatalf("a name pinned with no manifest was compared: %v", err)
	}

	if withIncarnationGuard(ctx, cat) != ctx {
		t.Fatal("a re-execution took a second guard")
	}
	bare := withIncarnationGuard(context.Background(), cat)
	if incarnationGuardFrom(bare) != nil {
		t.Fatal("a guard was installed with no manifest pin to bracket against")
	}
	PinForDecision(context.Background(), "t")
	if err := verifyIncarnations(context.Background()); err != nil {
		t.Fatalf("no guard, verify refused: %v", err)
	}
}

// TestIncarnationGuardLegacyTables: a table written before incarnations
// existed carries "" on its record and manifest and is not refused; dropped
// and re-created, its record carries a minted incarnation and is.
func TestIncarnationGuardLegacyTables(t *testing.T) {
	cat := guardCatalog(t)
	guardCreate(t, cat, "legacy", "id")
	kv := cat.KV()
	for _, key := range []string{cat.ClusterID() + ".table.legacy", cat.ClusterID() + ".manifest.legacy"} {
		raw, _, err := kv.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		delete(m, "incarnation")
		out, _ := json.Marshal(m)
		if _, err := kv.Put(key, out); err != nil {
			t.Fatal(err)
		}
	}
	ctx := guardCtx(cat)
	PinForDecision(ctx, "legacy")
	if err := verifyIncarnations(ctx); err != nil {
		t.Fatalf("a legacy table was refused: %v", err)
	}
	guardRecreate(t, cat, "legacy", "id")
	PinForDecision(ctx, "legacy")
	if err := verifyIncarnations(ctx); !isIncarnationChanged(err) {
		t.Fatalf("a re-created legacy table verified as %v", err)
	}
}

// TestIncarnationGuardDecisionSitesInPhysical: the binder's policed schema
// read and the column list a context policy projection is built over are
// decisions, pinned before their read.
func TestIncarnationGuardDecisionSitesInPhysical(t *testing.T) {
	cat := guardCatalog(t)
	guardCreate(t, cat, "t", "id")
	sites := map[string]func(ctx context.Context){
		"binder": func(ctx context.Context) {
			src := policedColumnSource{src: cat, deniedFor: func(string) map[string]bool { return nil }}
			_, _ = src.GetTable(ctx, "t")
		},
		"policy_projection_columns": func(ctx context.Context) {
			p := NewPlannerForContext(ctx, cat)
			_ = p.policyTableColumns(ctx, "t")
		},
	}
	for name, site := range sites {
		t.Run(name, func(t *testing.T) {
			ctx := guardCtx(cat)
			if _, err := manifestSnapshotFromContext(ctx).Get(ctx, cat, "t"); err != nil {
				t.Fatal(err)
			}
			guardRecreate(t, cat, "t", "id", "ssn")
			defer guardRecreate(t, cat, "t", "id")
			site(ctx)
			if err := verifyIncarnations(ctx); !isIncarnationChanged(err) {
				t.Fatalf("the %s read was not bracketed: verify = %v", name, err)
			}
		})
	}
}
