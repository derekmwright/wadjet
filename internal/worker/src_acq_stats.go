package worker

import "time"

// Src-side acquisition counters (docs/benchmarks/
// straggler-mode-attribution-2026-08-16.md): the run-level straggler mode
// is entirely src_ms inflation — ops/sink constant, host CPU idle, tokens
// exonerated — and the per-minute tier ledgers are demand-confounded, so
// per-TASK attribution needs its own tallies. openNextFile records, per
// resolved tier, how many opens landed there and the end-to-end wall each
// took (prefetch-take waits, tiered fallthroughs, first byte). Folded into
// the "fragment task phases" line so a single straggler run names the
// guilty tier.
type acqTier uint8

const (
	acqPrefetch  acqTier = iota // file prefetcher (take wait + local open)
	acqBaseCache                // base-table NVMe cache
	acqTier0                    // same-worker LocalStageCache
	acqKV                       // NATS KV small-output fast path
	acqPeer                     // gRPC peer fetch
	acqS3                       // durable store (incl. upload-await + streaming open)
	acqTierCount
)

var acqTierNames = [acqTierCount]string{"prefetch", "basecache", "local", "kv", "peer", "s3"}

// srcAcqStats is written only by the producer goroutine that owns
// openNextFile and read at task finish, after the producer is done — no
// synchronization needed.
type srcAcqStats struct {
	files        [acqTierCount]int64
	ns           [acqTierCount]int64
	prefetchMiss int64

	// Prefetch overlap (perf(scan,worker) 2026-08-22). prefetchLeadNs is
	// the wall between the download workers spawning and the first take —
	// i.e. how much of the first file's download happened BEFORE the scan
	// asked for it, which on a join fragment is the build-load overlap.
	// prefetchAtInit records whether the start was at source Init (the new
	// default) or at the first file open (WADJET_PREFETCH_AT_INIT=0), so a
	// run's stats line names which arm produced the lead. The manifest
	// wrapper shares one instance across inner sources; the lead keeps the
	// largest, since that is the one that covered the build.
	prefetchLeadNs int64
	prefetchAtInit bool
	prefetchRan    bool
}

// notePrefetchLead records the first-take lead for one source.
func (a *srcAcqStats) notePrefetchLead(d time.Duration, atInit bool) {
	if a == nil {
		return
	}
	a.prefetchRan = true
	if atInit {
		a.prefetchAtInit = true
	}
	if ns := d.Nanoseconds(); ns > a.prefetchLeadNs {
		a.prefetchLeadNs = ns
	}
}

func (a *srcAcqStats) note(t acqTier, d time.Duration) {
	if t >= acqTierCount {
		return
	}
	a.files[t]++
	a.ns[t] += d.Nanoseconds()
}

// attrs renders the nonzero tiers as log attrs.
func (a *srcAcqStats) attrs() []any {
	if a == nil {
		return nil
	}
	var out []any
	for t := acqTier(0); t < acqTierCount; t++ {
		if a.files[t] > 0 {
			out = append(out, "acq_"+acqTierNames[t]+"_files", a.files[t],
				"acq_"+acqTierNames[t]+"_ms", a.ns[t]/1e6)
		}
	}
	if a.prefetchMiss > 0 {
		out = append(out, "acq_prefetch_miss", a.prefetchMiss)
	}
	if a.prefetchRan {
		started := 0
		if a.prefetchAtInit {
			started = 1
		}
		out = append(out, "prefetch_started_before_build", started,
			"prefetch_lead_ms", a.prefetchLeadNs/1e6)
	}
	return out
}

// srcAcqReporter is implemented by sources whose acquisition tallies ride
// the fragment phases line.
type srcAcqReporter interface{ srcAcqAttrs() []any }
