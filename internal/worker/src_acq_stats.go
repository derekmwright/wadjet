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

	// Durable-object wait (peer_exchange.go awaitDurableObject): how often
	// this task blocked on a producer's in-flight background upload, how
	// many S3 re-polls that cost, and the wall it burned. The 2026-08-22
	// window-2 analysis could only infer this from the 500ms quantization
	// of task durations; these make it a grep.
	durableWaits     int64
	durableWaitPolls int64
	durableWaitNs    int64

	// Peer-tier hint outcomes, per task. hits: a hinted fetch served the
	// file from the producer's NVMe. misses: a hint existed and the fetch
	// failed, so the read fell through to the durable tier.
	peerHits   int64
	peerMisses int64

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

func (a *srcAcqStats) noteDurableWait(d time.Duration, polls int) {
	if a == nil {
		return
	}
	a.durableWaits++
	a.durableWaitPolls += int64(polls)
	a.durableWaitNs += d.Nanoseconds()
}

func (a *srcAcqStats) notePeer(hit bool) {
	if a == nil {
		return
	}
	if hit {
		a.peerHits++
		return
	}
	a.peerMisses++
}

// notable reports whether the tally holds a signal worth an INFO line
// regardless of how long the task ran. A durable wait qualifies: the task
// stalled on another worker's upload, which is exactly the case the
// fragment-phase log floor would otherwise hide (these tasks emit a
// handful of rows and never reach the floor).
func (a *srcAcqStats) notable() bool { return a != nil && a.durableWaits > 0 }

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
	if a.durableWaits > 0 {
		out = append(out, "durable_waits", a.durableWaits,
			"durable_wait_polls", a.durableWaitPolls,
			"durable_wait_ms", a.durableWaitNs/1e6)
	}
	if a.peerHits > 0 {
		out = append(out, "peer_hits", a.peerHits)
	}
	if a.peerMisses > 0 {
		out = append(out, "peer_misses", a.peerMisses)
	}
	return out
}

// srcAcqReporter is implemented by sources whose acquisition tallies ride
// the fragment phases line. srcAcqNotable lets a short task escalate that
// line to INFO when the tally itself is the finding.
type srcAcqReporter interface {
	srcAcqAttrs() []any
	srcAcqNotable() bool
}
