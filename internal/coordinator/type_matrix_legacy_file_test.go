package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The #396 migration boundary, gated (#423).
//
// #396 — the DAG answering an IPv4 as 167772165 where the single-process
// engine answers 10.0.0.5 — was closed by making the file SELF-DESCRIBING:
// the writer stamps the declared schema into the footer under
// parquet.DeclaredSchemaKey and the reader overlays it, so a consumer that
// takes its types from the FILE gets the same nine types (IPv4, IPv6, MAC,
// UUID, BYTES, PORT, PROTOCOL, DURATION, CIDR) that parquet's own schema
// cannot express.
//
// That closes it only for files written from v0.18.0 on. A file written by
// an older build carries no such key, and on those the DAG kept answering
// raw storage form — the original symptom, unfixed, on exactly the data a
// migration cannot rewrite.
//
// TestTypeMatrixTwoPath cannot see this. It writes its fixture with the
// CURRENT writer on every run, so every file it reads carries the key, and
// the migration case cannot occur in it however the corpus grows. This gate
// is that suite over a fixture whose footer key has been REMOVED: the same
// corpus entries, the same comparison, over files that cannot say what they
// hold. It fails on the pre-#423 engine and passes once the catalog's schema
// reaches the worker's scan (Stage.ScanSchema → OpSpec.ColumnTypes →
// cachedFileStreamSource → parquet.Reader.SchemaAs).
//
// Both arms read the SAME store and the SAME catalog, so a divergence
// between them cannot be a difference in the data.
func TestTypeMatrixTwoPathWithoutDeclaredSchemaFooter(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the legacy-file two-path gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, func(t *testing.T, data []byte) []byte {
		t.Helper()
		out, err := parquet.StripDeclaredSchema(data)
		if err != nil {
			t.Fatalf("building a pre-v0.18.0 fixture: %v", err)
		}
		return out
	})
	coord := tmdCoordinator(t, ctx, infra)
	single, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open single-process arm over the shared catalog: %v", err)
	}
	t.Cleanup(func() { single.Close() })

	// The premise, asserted rather than assumed: a stripped file really does
	// report raw storage form for the nine. Without this the whole gate could
	// pass because the fixture was never the migration case.
	assertFixtureCannotDeclareItsTypes(t, ctx, infra)

	corpus := typematrix.Corpus()
	var compared, skipped int
	for _, q := range corpus {
		if !legacyBoundaryColumn(q.Col) {
			skipped++
			continue
		}
		q := q
		t.Run(q.Name, func(t *testing.T) {
			if p, ok := tmdCrashPins[q.Name]; ok {
				t.Skipf("process killer, tracked in %s:\n  %s", p.Issue, p.Reason)
			}
			// A shape that already diverges on the ORDINARY two-path gate
			// cannot say anything about the legacy-footer question this gate
			// exists for: both arms would be comparing a defect that has
			// nothing to do with where the column's type came from. The pin
			// is shared so it is stated once, and the ratchet that owns it
			// lives in TestTypeMatrixTwoPath.
			if p, _, ok := tmdPinFor(q.Name); ok {
				t.Skipf("diverges on the ordinary two-path gate too, tracked in %s:\n  %s",
					p.Issue, p.Reason)
			}
			aRes, aErr := tmdRunSingle(ctx, single, q.SQL)
			bRes, bErr := tmdRunDAG(ctx, coord, q.SQL)
			if aErr != nil && bErr != nil {
				t.Skipf("both arms refuse this shape: single=%v dag=%v", aErr, bErr)
			}
			if aErr != nil {
				t.Fatalf("the single-process arm FAILED on a query the stage DAG answered: %v\n  SQL: %s",
					aErr, q.SQL)
			}
			if bErr != nil {
				t.Fatalf("the stage DAG FAILED over a file with no declared-schema footer: %v\n  SQL: %s",
					bErr, q.SQL)
			}
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: q.Mode}); diff != "" {
				t.Fatalf("TWO-PATH DIVERGENCE over a pre-v0.18.0 file — the stage DAG took this "+
					"column's TYPE from the file, which cannot express it, while the "+
					"single-process engine took it from the catalog (#423)\n"+
					"  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					q.SQL, diff, tmdRender(aRes, 3), tmdRender(bRes, 3))
			}
		})
		compared++
	}
	t.Logf("legacy-file two-path gate: %d entries over the nine parquet-inexpressible types, "+
		"%d corpus entries skipped (their columns are types a file CAN declare)", compared, skipped)
	if compared == 0 {
		t.Fatal("no corpus entry targets one of the nine inexpressible types — this gate " +
			"cannot see the defect it exists for")
	}
}

// legacyBoundaryColumn reports whether a corpus entry targets a column whose
// type a parquet file cannot express on its own. Those are the columns the
// footer key exists for, and the only ones this gate can say anything about;
// every other type is annotated in the file and reads back correctly with or
// without the declaration.
func legacyBoundaryColumn(col string) bool {
	switch col {
	case "c_ipv4", "c_ipv6", "c_mac", "c_uuid", "c_bytes", "c_port", "c_proto", "c_dur", "c_cidr":
		return true
	}
	return false
}

// assertFixtureCannotDeclareItsTypes reads the fixture's own bytes back and
// requires that the file, on its own, reports the nine as the INT64 /
// BYTE_ARRAY leaves they are STORED in. That is what makes the rest of this
// gate meaningful: if the strip ever stops working, or the writer stops
// stamping, the two arms would agree for the wrong reason.
func assertFixtureCannotDeclareItsTypes(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	rc, _, err := infra.store.Get(ctx, "test", "tables/"+typematrix.Table+"/chunk_0000.parquet")
	if err != nil {
		t.Fatalf("reading the fixture back: %v", err)
	}
	defer rc.Close()
	data := make([]byte, 0, 1<<20)
	buf := make([]byte, 64<<10)
	for {
		n, rerr := rc.Read(buf)
		data = append(data, buf[:n]...)
		if rerr != nil {
			break
		}
	}
	r, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("opening the fixture: %v", err)
	}
	declared := make(map[string]parquet.TypeID)
	for _, c := range typematrix.Schema().Columns {
		declared[c.Name] = c.Type
	}
	for _, c := range r.Schema().Columns {
		if !legacyBoundaryColumn(c.Name) {
			continue
		}
		if c.Type == declared[c.Name] {
			t.Fatalf("the fixture still declares %s as %s on its own — the footer key was not "+
				"stripped, so this gate is running against a v0.18.0 file and proves nothing",
				c.Name, c.Type)
		}
	}
}
