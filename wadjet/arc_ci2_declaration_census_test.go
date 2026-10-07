// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE DECLARATION WALK BY IDENTITY, COUNTED (ADR-0047 stage 2; RISKS R3) on
// the embedded engine — the door that stamps — over the group-key census
// cells (testdata/arc_ci1_census_cells.tsv) and the stage-2 table
// (../internal/coordinator/testdata/arc_ci2_declared_output_cells.tsv), with
// the same ss_t / ss_i fixture. Every reference the walk declares by its
// binding is also asked by the name rules, and the two must describe one
// column wherever the name rules answer: a binding whose ordinal named a
// different position than the plan carries would show here as a disagreement
// (R3: ordinals that do not stay put). The census is non-vacuous: the
// stage-2 table's bound references are declared by binding.
func TestArcCI2DeclarationByIdentityCensus(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2), s VARCHAR, o BOOLEAN, " +
			"d DATE, ts TIMESTAMP, u UUID, a ARRAY(INT))",
		"INSERT INTO ss_t VALUES (1, 3, 30, 1.5, 2.25, 'abc', true, DATE '2024-03-04', TIMESTAMP '2024-03-04 12:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000001' AS UUID), ARRAY[1,2]), " +
			"(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
		"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
		"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	var cells [][2]string
	for _, path := range []string{"testdata/arc_ci1_census_cells.tsv", "../internal/coordinator/testdata/arc_ci2_declared_output_cells.tsv"} {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			p := strings.SplitN(line, "\t", 3)
			if len(p) != 3 {
				t.Fatalf("malformed cell %q in %s", line, path)
			}
			cells = append(cells, [2]string{p[0], p[2]})
		}
		f.Close()
	}
	same := func(a, b parquet.Column) bool {
		return a.Type == b.Type && a.Precision == b.Precision && a.Scale == b.Scale
	}
	var byBinding, nameSilent int
	disagree := map[string]bool{}
	var cell string
	physical.DeclIdentityProbe = func(ref *plansql.ColRef, bound, named parquet.Column, ok bool) {
		byBinding++
		if !ok {
			nameSilent++
			return
		}
		if !same(bound, named) {
			disagree[fmt.Sprintf("%s: %s binding %v (%d,%d) names %v (%d,%d)", cell, ref.String(),
				bound.Type, bound.Precision, bound.Scale, named.Type, named.Precision, named.Scale)] = true
		}
	}
	t.Cleanup(func() { physical.DeclIdentityProbe = nil })
	for _, c := range cells {
		cell = c[0]
		_, _ = db.Query(ctx, c[1])
	}
	var keys []string
	for k := range disagree {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Errorf("a reference's binding and its name describe different columns: %s", k)
	}
	t.Logf("%d cells; %d references declared by binding, %d where the name rules answered nothing, %d disagreements",
		len(cells), byBinding, nameSilent, len(keys))
	if len(cells) < 2500 || byBinding < 1000 {
		t.Fatalf("the census is vacuous: %d cells, %d references declared by binding", len(cells), byBinding)
	}
}
