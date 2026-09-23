// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestArcFR2AnUnopenableReaderInputIsRefusedOnTheCoordinator is #1245 on the
// coordinator's two paths: a reader whose input does not exist, matches no
// file or is a directory is refused at plan time with COPY's SQLSTATE
// (58P01 / 42809), on the fast path and on the DAG path alike, and EXPLAIN
// over it is refused too. At 962117da the error carried no SQLSTATE and
// EXPLAIN printed a plan.
func TestArcFR2AnUnopenableReaderInputIsRefusedOnTheCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	dir := t.TempDir()
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "fr2-ops", Name: "ops", Role: "ops"}},
		Roles:   []auth.RoleConfig{{Name: "ops", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	opsCtx := auth.ContextWithIdentity(ctx, &auth.Identity{
		Name: "ops", Role: "ops", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read", "write", "admin"}})

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	fast := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	fast.SetAuthProvider(provider)
	dag := tmdCoordinator(t, ctx, infra)
	dag.SetAuthProvider(provider)

	for _, d := range []struct {
		name string
		c    *Coordinator
	}{{"fastpath", fast}, {"dag", dag}} {
		for _, fn := range []struct{ name, ext string }{{"read_csv", "csv"}, {"read_json", "json"}, {"read_parquet", "parquet"}} {
			missing := filepath.Join(dir, "missing."+fn.ext)
			for _, c := range []struct{ name, sql, code string }{
				{"missing", fmt.Sprintf("SELECT * FROM %s('%s')", fn.name, missing), "58P01"},
				{"explain_missing", fmt.Sprintf("EXPLAIN SELECT * FROM %s('%s')", fn.name, missing), "58P01"},
				{"missing_in_a_cte", fmt.Sprintf("WITH c AS (SELECT * FROM %s('%s')) SELECT COUNT(*) FROM c", fn.name, missing), "58P01"},
				{"glob_matching_nothing", fmt.Sprintf("SELECT * FROM %s('%s/*.none')", fn.name, dir), "58P01"},
				{"directory", fmt.Sprintf("SELECT * FROM %s('%s')", fn.name, dir), "42809"},
			} {
				t.Run(d.name+"/"+fn.name+"/"+c.name, func(t *testing.T) {
					res, err := d.c.ExecuteSQL(opsCtx, c.sql)
					if err == nil && res != nil && res.Error != "" {
						err = fmt.Errorf("%s", res.Error)
					}
					if err == nil {
						t.Fatalf("answered; want %s", c.code)
					}
					if st := sqlerr.StateOf(err); st != c.code {
						t.Fatalf("SQLSTATE %q (%v), want %s", st, err, c.code)
					}
					if strings.Contains(err.Error(), "no dependencies and no ScanFiles") {
						t.Fatalf("reached the DAG stage pin instead of the refusal: %v", err)
					}
				})
			}
		}
	}
}
