// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// plannerConfigFlags are the persistent planner flags this package resolves
// and must therefore hand to every database it opens. They are INSTANCE
// configuration (#1223): before the arc, `--bushy-join-reorder` reached the
// planner through a package variable that root.go's PersistentPreRunE stored
// for the whole process, so every command inherited it whether or not its own
// database carried it. With the variable gone, a `wadjet.Config` literal that
// omits the field is a command whose typed flag silently does nothing — which
// is exactly what the round-1 review measured on `wadjet query`.
var plannerConfigFlags = []string{"BushyJoinReorder"}

// TestEveryCLIDatabaseCarriesTheResolvedPlannerFlags closes the CLASS rather
// than the two doors the review found: it reads this package's own source and
// requires EVERY wadjet.Config composite literal to set each planner flag. A
// door added later inherits the requirement instead of inheriting the defect.
//
// It is a source assertion on purpose. The behavioural cell below can only
// reach the databases a test can open; this one reaches every literal,
// including `query`'s catalog-free path, whose MemStore holds no relation for
// a join to be reordered over and which therefore has no observable plan of
// its own to compare.
func TestEveryCLIDatabaseCarriesTheResolvedPlannerFlags(t *testing.T) {
	fset := token.NewFileSet()
	// The package's own directory, parsed with go/parser rather than walked:
	// a git worktree under .claude/worktrees/ is a second copy of this source
	// and a walking gate would read it too (CLAUDE.md).
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing package cli: %v", err)
	}

	found := 0
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				sel, ok := lit.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Config" {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "wadjet" {
					return true
				}
				found++
				set := map[string]bool{}
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); ok {
						set[key.Name] = true
					}
				}
				for _, field := range plannerConfigFlags {
					if !set[field] {
						t.Errorf("%s: this wadjet.Config sets no %s, so the command that opens it "+
							"accepts --%s (a PERSISTENT root flag, printed in its --help) and ignores it. "+
							"Planner configuration is the instance's now — hand it the resolved value (#1223).",
							fset.Position(lit.Pos()), field, flagNameFor(field))
					}
				}
				_ = filepath.Base(path)
				return true
			})
		}
	}
	// Non-vacuity: the three literals are serve_embedded.go's, shared_catalog.go's
	// (query / create-table / drop-table / shell / mcp) and root.go's catalog-free
	// query path. A refactor that leaves none is a gate that proves nothing.
	if found < 3 {
		t.Fatalf("found %d wadjet.Config literals in package cli, want at least 3 — "+
			"the walk stopped finding the doors it is meant to hold", found)
	}
}

// flagNameFor spells a Config field as the flag an operator types.
func flagNameFor(field string) string {
	switch field {
	case "BushyJoinReorder":
		return "bushy-join-reorder"
	default:
		return strings.ToLower(field)
	}
}

// TestTheSharedCLIDatabasePlansByTheResolvedFlag is the behavioural half: the
// database behind `query`, `create-table`, `drop-table`, `shell` and `mcp` —
// all five call openSharedDB, so one cell answers for all five — plans by the
// resolved flag, on and off.
//
// It fails at f1a7319d, where openSharedDB's Config omitted the field: the
// flag-on arm planned left-deep, byte-identical to flag-off, while `--help`
// advertised the flag (measured by the round-1 review).
func TestTheSharedCLIDatabasePlansByTheResolvedFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag bool
		want int64
	}{
		{name: "--bushy-join-reorder", flag: true, want: 1},
		{name: "the shipped default", flag: false, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCLIPlannerFlags(t, tc.flag)
			ctx := context.Background()
			db, release, err := openSharedDB(ctx, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
			if err != nil {
				t.Fatalf("openSharedDB: %v", err)
			}
			defer release()

			loadExpandingChain(t, db)
			before := logical.BushyJoinsPlanned.Load()
			res, err := db.Query(ctx, expandingChainSQL)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expanding chain returned %d rows, want 1", len(res.Rows))
			}
			if got := logical.BushyJoinsPlanned.Load() - before; got != tc.want {
				t.Fatalf("the shared CLI database planned %d bushy joins under %s, want %d — "+
					"the resolved flag did not reach the database the command opens (#1223)",
					got, tc.name, tc.want)
			}
		})
	}
}

// withCLIPlannerFlags resolves a real command line through the real root
// command, which is the point: the cell then measures what an OPERATOR's
// typed --bushy-join-reorder reaches, through the same resolution
// (ADR-0029) the commands use, rather than a package variable a test set by
// hand. resolveThroughTheRealCommand restores every bound variable after.
func withCLIPlannerFlags(t *testing.T, bushy bool) {
	t.Helper()
	root := t.TempDir()
	args := []string{
		"--storage-type=file",
		"--data-dir=" + filepath.Join(root, "data"),
		"--bucket=test",
		"--nats-store-dir=" + filepath.Join(root, "nats"),
	}
	if bushy {
		args = append(args, "--bushy-join-reorder")
	}
	if _, err := resolveThroughTheRealCommand(t, args); err != nil {
		t.Fatalf("resolving the command line: %v", err)
	}
}

// expandingChainSQL is the four-relation shape whose cheapest order only a
// bushy plan can express — fact_a ⋈ fact_b is a many-to-many explosion and
// each fact carries a small dimension — so the two regimes choose different
// join orders and the counter moves for one and not the other.
const expandingChainSQL = `SELECT count(*) AS n FROM fact_a ` +
	`JOIN dim_x ON a_x = x_id ` +
	`JOIN fact_b ON a_id = b_id ` +
	`JOIN dim_y ON b_y = y_id`

func loadExpandingChain(t *testing.T, db *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	two := func(a, b string) parquet.Schema {
		return parquet.Schema{Columns: []parquet.Column{
			{Name: a, Type: parquet.TypeInt64},
			{Name: b, Type: parquet.TypeInt64},
		}}
	}
	load := func(table string, schema parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatalf("CreateTable %s: %v", table, err)
		}
		ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", table, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", table, err)
		}
		// The DP cost model reads per-column NDV, which only ANALYZE computes.
		if _, err := db.Query(ctx, "ANALYZE TABLE "+table); err != nil {
			t.Fatalf("ANALYZE %s: %v", table, err)
		}
	}
	const factRows = 1000
	factA := make([]map[string]any, factRows)
	factB := make([]map[string]any, factRows)
	for i := 0; i < factRows; i++ {
		factA[i] = map[string]any{"a_id": int64(i % 10), "a_x": int64(i)}
		factB[i] = map[string]any{"b_id": int64(i % 10), "b_y": int64(i)}
	}
	dimX := make([]map[string]any, 10)
	dimY := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		dimX[i] = map[string]any{"x_id": int64(i), "x_v": int64(i)}
		dimY[i] = map[string]any{"y_id": int64(i), "y_v": int64(i)}
	}
	load("fact_a", two("a_id", "a_x"), factA)
	load("dim_x", two("x_id", "x_v"), dimX)
	load("fact_b", two("b_id", "b_y"), factB)
	load("dim_y", two("y_id", "y_v"), dimY)
}
