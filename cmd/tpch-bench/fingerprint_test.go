package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tpch "github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestFingerprintPassWritesSelfFileThatIsNotGroundTruth runs the whole pass
// against a generated SF0.01 fixture and checks the two things that make the
// artifact safe: the file it writes loads as a REGRESSION file, and the same
// bytes are refused when something tries to read them as ground truth.
func TestFingerprintPassWritesSelfFileThatIsNotGroundTruth(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t, ctx)
	out := filepath.Join(t.TempDir(), "fingerprint-self.json")

	// SF0.01 against SF100 ground truth: the gate must report itself
	// inactive rather than passing or failing on the wrong scale.
	if diverged := runFingerprintPass(ctx, nil, db, tpch.SF001, "generated SF0.01", out, 0); diverged != 0 {
		t.Fatalf("pass reported %d divergences over a fixture with no active gate", diverged)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read %s: %v", out, err)
	}
	f, err := tpch.ParseFingerprintFile(data, tpch.KindRegression)
	if err != nil {
		t.Fatalf("self-fingerprint file does not load as a regression file: %v", err)
	}
	if len(f.Queries) != len(tpch.TPCHQueries) {
		t.Fatalf("wrote %d entries, want %d", len(f.Queries), len(tpch.TPCHQueries))
	}
	for name, e := range f.Queries {
		if e.Engine != tpch.SelfEngine {
			t.Errorf("entry %s is stamped %q, not %q", name, e.Engine, tpch.SelfEngine)
		}
		if e.Fine == "" || e.Coarse == "" {
			t.Errorf("entry %s carries no digest", name)
		}
	}

	if _, err := tpch.ParseFingerprintFile(data, tpch.KindGroundTruth); err == nil {
		t.Fatal("Wadjet's own signatures were accepted as ground truth")
	} else if !strings.Contains(err.Error(), "cannot establish correctness") {
		t.Fatalf("refusal does not explain itself: %v", err)
	}

	// The stored file must not leak result values — the same constraint the
	// committed ground-truth file is held to.
	if strings.Contains(string(data), "value_sig") {
		t.Error("self-fingerprint file carries a value signature")
	}
}

func TestWadjetBuildIDNamesSomething(t *testing.T) {
	if id := wadjetBuildID(); id == "" {
		t.Fatal("build id is empty; an entry that cannot be traced to a build is not stampable")
	}
}

func openFixtureDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "tpch"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	data := tpch.Generate(tpch.SF001)
	for name, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		rows := data[name]
		if len(rows) == 0 {
			continue
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}
	return db
}
