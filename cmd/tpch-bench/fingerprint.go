package main

// SF100 value-level correctness pass.
//
// The timed runs above gate row counts (baseline-sf100.json) and log a
// per-column value signature. Neither catches a wrong VALUE with the right
// shape: #312 inflated Q05 revenues ~25x at an identical row count. This pass
// closes that by digesting the answers and holding them against
// benchmarks/tpch/fingerprint-sf100.json, an opaque per-query fingerprint
// captured from an external reference engine over the same parquet.
//
// It runs AFTER the timed runs and over the CORRECTNESS VARIANTS
// (tpch.CorrectnessQueries), not the canonical text the benchmark times: five
// of the 22 need a tiebreaker appended to their ORDER BY before a positional
// digest is legitimate. Keeping the pass separate is deliberate — the timed
// numbers are the deployment's primary artifact and must not pay for row
// materialization or digesting.
//
// The file it WRITES is a kind="regression" file stamped engine="wadjet". That
// distinction is load-bearing: a fingerprint Wadjet produced can tell you the
// answer CHANGED between two builds; only the external file can tell you the
// answer is RIGHT. tpch.ParseFingerprintFile refuses a Wadjet-stamped entry in
// the ground-truth file by name.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"

	tpch "github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/wadjet"
)

// fingerprintGateTag prefixes every line this pass emits so a deploy's logs
// can be grepped for the verdict without reading the whole run.
const fingerprintGateTag = "FINGERPRINT-GATE"

// runFingerprintPass executes the correctness variants, compares each answer
// against the embedded ground truth, and optionally writes Wadjet's own
// signatures to outPath. Returns the number of queries that diverged from
// ground truth (0 when the gate is inactive).
func runFingerprintPass(ctx context.Context, coord *coordinator.Coordinator, db *wadjet.DB,
	sf tpch.ScaleFactor, dataset, outPath string, perQueryTimeout time.Duration) int {

	corpus := tpch.CorrectnessQueries(sf)
	truth, err := tpch.GroundTruthSF100()
	gateActive := false
	switch {
	case errors.Is(err, tpch.ErrUnpopulated):
		log.Printf("%s: NOT ACTIVE — the ground-truth file carries no entries yet. "+
			"This run can only record what Wadjet answered; generate the reference fingerprints per "+
			"docs/design/sf100-value-fingerprints.md.", fingerprintGateTag)
	case err != nil:
		log.Printf("%s: NOT ACTIVE — ground truth unusable: %v", fingerprintGateTag, err)
	default:
		if err := truth.CheckCoversCorpus(corpus); err != nil {
			log.Printf("%s: NOT ACTIVE — ground truth is stale: %v", fingerprintGateTag, err)
		} else {
			gateActive = true
			log.Printf("%s: active — %d entries from %s", fingerprintGateTag, len(truth.Queries), truth.Generator)
		}
	}
	if sf != tpch.SF100 && gateActive {
		// The digests describe SF100 answers; anything else is a different
		// question and must not be reported as a pass or a failure.
		log.Printf("%s: NOT ACTIVE — this run is SF%g, the ground truth is SF100", fingerprintGateTag, float64(sf))
		gateActive = false
	}

	captured := make(map[string]tpch.FPEntry, len(corpus))
	diverged, unverified := 0, 0
	for _, q := range corpus {
		qCtx := ctx
		var cancel context.CancelFunc
		if perQueryTimeout > 0 {
			qCtx, cancel = context.WithTimeout(ctx, perQueryTimeout)
		}
		res, err := fingerprintQuery(qCtx, coord, db, q.SQL)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			// Reported apart from a divergence: a query that could not be
			// answered says nothing about the VALUES, and a worker outage
			// must not read as "Wadjet computed the wrong number".
			log.Printf("%s %s: UNVERIFIED — %v", fingerprintGateTag, q.Name, err)
			unverified++
			continue
		}
		sig := tpch.SignatureOf(res, q)
		captured[q.Name] = tpch.NewEntry(tpch.SelfEngine, wadjetBuildID(), dataset,
			time.Now().UTC().Format(time.RFC3339), q, sig)
		if !gateActive {
			log.Printf("%s %s: recorded %s", fingerprintGateTag, q.Name, sig)
			continue
		}
		want := truth.Queries[q.Name]
		if ok, detail := want.Match(sig); !ok {
			diverged++
			log.Printf("%s %s: DIVERGES from %s ground truth — %s", fingerprintGateTag, q.Name, want.Engine, detail)
			continue
		}
		log.Printf("%s %s: matches %s ground truth (%d rows, %s)", fingerprintGateTag, q.Name, want.Engine, sig.Rows.Rows, q.Mode)
	}

	if outPath != "" {
		writeSelfFingerprints(outPath, sf, dataset, captured)
	}
	switch {
	case !gateActive:
		log.Printf("%s: %d/%d queries recorded, %d unverified, gate inactive (no correctness verdict)",
			fingerprintGateTag, len(captured), len(corpus), unverified)
	case diverged == 0 && unverified == 0:
		log.Printf("%s: PASS — all %d queries match ground truth at the VALUE level", fingerprintGateTag, len(corpus))
	case diverged == 0:
		log.Printf("%s: INCOMPLETE — %d/%d queries match ground truth, %d could not be answered",
			fingerprintGateTag, len(corpus)-unverified, len(corpus), unverified)
	default:
		log.Printf("%s: FAIL — %d/%d queries diverge from ground truth (%d unverified)",
			fingerprintGateTag, diverged, len(corpus), unverified)
	}
	return diverged
}

// fingerprintQuery runs one correctness variant and materializes its rows.
//
// The row count is cross-checked against the coordinator's own count: a large
// result can be left on S3 with only its count returned, and digesting the
// batches that happened to come back inline would be a fingerprint of a
// fraction of the answer — a green gate over a partial read. That case is
// reported as UNVERIFIED, never as a signature.
func fingerprintQuery(ctx context.Context, coord *coordinator.Coordinator, db *wadjet.DB, sql string) (*oracle.Result, error) {
	if coord == nil {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &oracle.Result{Columns: res.Columns, Rows: res.Rows}, nil
	}
	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Error != "" {
		return nil, fmt.Errorf("%s", res.Error)
	}
	rows, err := res.Rows()
	if err != nil {
		return nil, fmt.Errorf("materializing rows: %w", err)
	}
	if int64(len(rows)) != res.TotalRows {
		return nil, fmt.Errorf("read %d of %d rows (result not fully materialized — a digest over part of the answer would be a false pass)",
			len(rows), res.TotalRows)
	}
	return &oracle.Result{Columns: res.Columns, Rows: rows}, nil
}

func writeSelfFingerprints(path string, sf tpch.ScaleFactor, dataset string, entries map[string]tpch.FPEntry) {
	f := &tpch.FingerprintFile{
		Version: 1,
		Kind:    tpch.KindRegression,
		Scale:   fmt.Sprintf("SF%g", float64(sf)),
		Generator: fmt.Sprintf("wadjet %s over %s at %s", wadjetBuildID(), dataset,
			time.Now().UTC().Format(time.RFC3339)),
		Note: "WADJET'S OWN ANSWERS, not ground truth. Comparing two of these files detects that a build " +
			"changed an answer; it cannot establish that either answer is correct. The ground-truth file is " +
			"benchmarks/tpch/fingerprint-sf100.json and only an external reference engine may write it.",
		Queries: entries,
	}
	data, err := tpch.MarshalFingerprintFile(f)
	if err != nil {
		log.Printf("%s: marshalling self-fingerprints: %v", fingerprintGateTag, err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("%s: writing %s: %v", fingerprintGateTag, path, err)
		return
	}
	log.Printf("%s: wrote %d self-fingerprints to %s (kind=%s)", fingerprintGateTag, len(entries), path, tpch.KindRegression)
}

// wadjetBuildID identifies the binary in a stamped entry. The VCS revision the
// Go toolchain embeds is the useful form; a build without one still has to
// name something, since an entry with no version cannot be traced.
func wadjetBuildID() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown-build"
	}
	rev, dirty := "", ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return "unknown-build"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	return rev + dirty
}
