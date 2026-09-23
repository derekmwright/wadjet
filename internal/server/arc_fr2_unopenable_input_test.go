// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestArcFR2AnUnopenableReaderInputIsRefusedOnEveryDoor is #1245: a file
// reader whose input cannot be opened is refused with the SQLSTATE
// PostgreSQL 17.11's COPY FROM and pg_read_file raise for the same path —
// 58P01 undefined_file for one that does not exist (and a glob that matches
// no file), 42501 insufficient_privilege for one that may not be read, 42809
// wrong_object_type for a directory — and EXPLAIN over it is refused as
// EXPLAIN over a missing relation is (42P01 on PostgreSQL), because the
// planner knows it without reading the input.
//
// Through v0.24.0 the error was the bare os error with no SQLSTATE on any
// door, and EXPLAIN printed `Scan: read_json`.
//
// The gRPC door carries no SQLSTATE for any statement (a door-wide gap, not
// this reader's), so there the MESSAGE is asserted.
func TestArcFR2AnUnopenableReaderInputIsRefusedOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	doors := append(append([]sec4Door{}, rig.doors...), tfGRPCDoor(t, ctx, rig))
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	noread := filepath.Join(dir, "noread")
	if err := os.WriteFile(noread+".csv", []byte("a\n1\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	canRead := false
	if f, err := os.Open(noread + ".csv"); err == nil {
		canRead = true // running as root: mode 000 does not stop the read
		f.Close()
	}
	type cell struct{ name, sql, code, text string }
	var cells []cell
	for _, fn := range []struct{ name, ext string }{{"read_csv", "csv"}, {"read_json", "json"}, {"read_parquet", "parquet"}} {
		cells = append(cells,
			cell{fn.name + "/missing", fmt.Sprintf("SELECT * FROM %s('%s.%s')", fn.name, missing, fn.ext), "58P01", "could not open file"},
			cell{fn.name + "/explain_missing", fmt.Sprintf("EXPLAIN SELECT * FROM %s('%s.%s')", fn.name, missing, fn.ext), "58P01", "could not open file"},
			cell{fn.name + "/missing_in_a_cte", fmt.Sprintf("WITH c AS (SELECT * FROM %s('%s.%s')) SELECT COUNT(*) FROM c", fn.name, missing, fn.ext), "58P01", "could not open file"},
			cell{fn.name + "/missing_in_a_scalar_subquery", fmt.Sprintf("SELECT (SELECT COUNT(*) FROM %s('%s.%s')) AS n", fn.name, missing, fn.ext), "58P01", "could not open file"},
			cell{fn.name + "/glob_matching_nothing", fmt.Sprintf("SELECT * FROM %s('%s/*.none')", fn.name, dir), "58P01", "no file matches"},
			cell{fn.name + "/directory", fmt.Sprintf("SELECT * FROM %s('%s')", fn.name, dir), "42809", "is a directory"},
			cell{fn.name + "/explain_directory", fmt.Sprintf("EXPLAIN SELECT * FROM %s('%s')", fn.name, dir), "42809", "is a directory"},
		)
	}
	if !canRead {
		cells = append(cells,
			cell{"read_csv/permission", fmt.Sprintf("SELECT * FROM read_csv('%s.csv')", noread), "42501", "permission denied"},
			cell{"read_csv/explain_permission", fmt.Sprintf("EXPLAIN SELECT * FROM read_csv('%s.csv')", noread), "42501", "permission denied"})
	}
	check := func(t *testing.T, d sec4Door, c cell) {
		t.Helper()
		rows, class, err := d.run(t, sec4Ops, c.sql)
		if err == nil {
			t.Errorf("%s/%s: answered %v; want %s", d.name, c.name, rows, c.code)
			return
		}
		if !strings.Contains(err.Error(), c.text) {
			t.Errorf("%s/%s: %v does not say %q", d.name, c.name, err, c.text)
		}
		if d.name != "grpc" && class != c.code {
			t.Errorf("%s/%s: SQLSTATE %q (%v), want %s", d.name, c.name, class, err, c.code)
		}
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			for _, c := range cells {
				check(t, d, c)
			}
		})
	}
	// With no plan-time schema (the http(s) path, a read-once input), the
	// SELECT is refused at the first batch with the same class; EXPLAIN reads
	// nothing there and is not refused — ADR-0039's first-batch boundary.
	t.Run("first_batch", func(t *testing.T) {
		t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
		for _, d := range doors {
			for _, c := range cells {
				if strings.Contains(c.name, "explain") {
					continue
				}
				check(t, d, c)
			}
		}
	})
}
