// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestArcRPPastSampleWire(t *testing.T) {
	srv := setupJ1LateralDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, kind := range []string{"csv", "json"} {
		for _, row := range []int{101, 2049, 2200} {
			t.Run(fmt.Sprintf("%s/%d", kind, row), func(t *testing.T) {
				var body strings.Builder
				if kind == "csv" {
					body.WriteString("a\n")
				}
				for i := 1; i <= row; i++ {
					v := "7"
					if i == row {
						v = "oops"
					}
					if kind == "csv" {
						fmt.Fprintln(&body, v)
					} else if i == row {
						fmt.Fprintln(&body, `{"a":"oops"}`)
					} else {
						fmt.Fprintln(&body, `{"a":7}`)
					}
				}
				path := filepath.Join(t.TempDir(), "values."+kind)
				if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
					t.Fatal(err)
				}
				var count int64
				err := conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_%s('%s')", kind, path)).Scan(&count)
				var pe *pgconn.PgError
				if !errors.As(err, &pe) || pe.Code != "22P02" {
					t.Fatalf("want 22P02, got %v", err)
				}
				for _, part := range []string{path, fmt.Sprintf("row %d", row), `column "a"`} {
					if !strings.Contains(pe.Message, part) {
						t.Errorf("missing %q: %s", part, pe.Message)
					}
				}
				var one string
				if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != "1" {
					t.Fatalf("SELECT 1: %s, %v", one, err)
				}
			})
		}
	}
}
