package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// prsSeed builds the fixture the way a deployment has it BEFORE a provider is
// attached: a catalog holding `Hits` with rows in it. The KV is returned so a
// second `wadjet.Open` sees the SAME catalog — that is what `runStandalone`
// does (`wadjet.Config{MetaKV: kv}`), and it is what makes an Open-time bind
// meaningful: without a shared KV every Open starts from an empty catalog and
// a policy naming any relation would refuse for that reason instead.
func prsSeed(t *testing.T, ctx context.Context) (objstore.Store, catalog.MetaKV) {
	t.Helper()
	store := objstore.NewMemStore()
	kv := catalog.NewMemKV()
	seed, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { seed.Close() })
	if err := seed.Catalog().CreateTable(ctx, prsTable, prsSchema(), nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := seed.NewIngester(prsTable, prsSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := ing.Ingest(ctx, prsRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return store, kv
}

// TestTheOpenAttachBindsOnItsOwn isolates the SIXTH attach site.
//
// `wadjet.Open(Config{AuthProvider: …})` assigned the provider into the new DB
// and never bound it — it was not among the five entries the attach rule was
// wired into, and it is the entry the shipped `wadjet mcp` command uses. A
// policy spelled `HITS` against a catalog `Hits` therefore matched nothing, and
// the embedded door returned the masked column in plaintext and the denied
// column at all, with `BindError()` nil (#882, round 3).
//
// Nothing else in this cell can bind: no `SetAuthProvider`, no HTTP server, no
// pgwire server. `Config.AuthProvider` is the only attach there is.
func TestTheOpenAttachBindsOnItsOwn(t *testing.T) {
	for _, tc := range []struct {
		name, resource string
		bindable       bool
	}{
		{"the catalog spelling binds", prsTable, true},
		{"the folded spelling binds", "hits", true},
		{"an upper-case spelling of the relation refuses at Open", "HITS", false},
		{"a mixed-case spelling that is neither refuses at Open", "hItS", false},
		{"a plain typo refuses at Open", "Hitz", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, kv := prsSeed(t, ctx)
			provider := prsProvider(t, tc.resource)

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store: store, Bucket: "test", MetaKV: kv, AuthProvider: provider,
			})
			if db != nil {
				t.Cleanup(func() { db.Close() })
			}

			if !tc.bindable {
				if err == nil {
					// Open accepted a set that names no relation. What that
					// costs is the point of this cell, so measure it rather
					// than report a missing error: the door discloses.
					id, aerr := provider.Authenticator().AuthenticateToken("analyst-key")
					if aerr != nil {
						t.Fatal(aerr)
					}
					res, qerr := db.Query(auth.ContextWithIdentity(ctx, id),
						`SELECT * FROM Hits ORDER BY WatchID`)
					got := "<no rows>"
					if res != nil {
						got = fmt.Sprintf("%v", res.Rows)
					}
					t.Fatalf("wadjet.Open accepted a policy set naming %q, which the catalog "+
						"does not hold, and the embedded door answered:\n  %s (err %v)",
						tc.resource, got, qerr)
				}
				if !strings.Contains(err.Error(), tc.resource) {
					t.Errorf("the refusal does not name the unresolvable relation %q: %v",
						tc.resource, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("a bindable policy set refused to open: %v", err)
			}
			if provider.BindError() != nil {
				t.Fatalf("a bindable policy set was remembered as unbound: %v", provider.BindError())
			}
			id, err := provider.Authenticator().AuthenticateToken("analyst-key")
			if err != nil {
				t.Fatal(err)
			}
			actx := auth.ContextWithIdentity(ctx, id)
			res, err := db.Query(actx, `SELECT * FROM Hits ORDER BY WatchID`)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := fmt.Sprintf("%v", res.Rows)
			if strings.Contains(got, prsSecret) {
				t.Errorf("the embedded door DISCLOSED the masked column in plaintext:\n  %s", got)
			}
			if strings.Contains(got, "700001") {
				t.Errorf("the embedded door returned the DENIED column:\n  %s", got)
			}
			if !strings.Contains(got, "***") {
				t.Errorf("the mask did not apply:\n  %s", got)
			}
			if _, err := db.Execute(actx, `UPDATE Hits SET Salary = 1 WHERE WatchID = 1`); err == nil {
				t.Error("a write to the denied column was PERMITTED")
			}
		})
	}
}
