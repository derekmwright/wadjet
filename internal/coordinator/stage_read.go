package coordinator

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// coordReadTier names the tier that served one coordinator-side stage-output
// read. The names mirror the worker's acquisition tiers
// (internal/worker/src_acq_stats.go acqTierNames) so one grep vocabulary
// covers both sides of a stage boundary.
type coordReadTier uint8

const (
	coordReadKV   coordReadTier = iota // NATS KV small-output fast path
	coordReadPeer                      // producing worker's local copy, over the peer data plane
	coordReadS3                        // durable store
	coordReadTierCount
)

// coordReadNone marks "no read happened" (empty producer output). Kept out
// of the counter range on purpose — it is not a tier.
const coordReadNone coordReadTier = 255

var coordReadTierNames = [coordReadTierCount]string{"kv", "peer", "s3"}

func (t coordReadTier) String() string {
	if t == coordReadNone {
		return "none"
	}
	if t < coordReadTierCount {
		return coordReadTierNames[t]
	}
	return "unknown"
}

// coordPeerReads gates the coordinator's Tier-1.5 stage-output read: fetch
// the producing worker's local copy over the same PeerExchange data plane
// workers use for shuffle inputs, instead of waiting for that worker's S3
// upload to land. Off restores the pre-2026-08-22 KV→S3 path.
//
// Advisory, like every other peer hint: the tier is skipped whenever the
// registry, the token or the producer is missing, and any fetch failure
// falls through to the durable copy. It can change which BYTES-IDENTICAL
// copy of an object is read and nothing else — never the object, never the
// result.
var coordPeerReads = optswitch.Register("coord-peer-reads", "WADJET_COORD_PEER_READS",
	"coordinator-side stage-output reads try the producing worker's local copy (peer tier) before S3")

// peerStageReadTimeout bounds one coordinator-side peer fetch: dial the
// producing worker, open the FetchShuffle stream, and read the whole
// payload. This tier only ever serves what the coordinator itself reads —
// an ~80-byte scalar, occasionally a few-MB stage/probe-split result — and
// the read sits on the query's dispatch-blocking critical path, so it must
// fail fast rather than borrow a bound sized for full shuffle transfers.
// For scale: dataplane.peerServeAcquireTimeout (the producer's serve-slot
// wait) is 10s and dataplane.PeerFetchIdleTimeout (the per-chunk stall
// bound) is 15s — both far above what reading a handful of KB should ever
// cost. On expiry this is an ordinary miss, same as any other fetch
// failure: the caller falls through to S3. var so tests can shrink it.
var peerStageReadTimeout = 2 * time.Second

// fetchResultDataTiered retrieves one stage-output / result blob for
// coordinator-side consumption and reports which tier served it.
//
// Tier order, cheapest-correct first:
//
//	kv    the producer mirrored payloads <= natsKVResultThreshold into the
//	      shared NATS KV bucket at task-finish time (worker
//	      finishStageOutputAsync / writeUnpartitionedWSHF). Best case, but
//	      the bucket is a 5-minute-TTL, 1 GB-capped cache: a Put that lost
//	      the cap race or a long query both miss.
//	peer  the producing worker still holds the file on local NVMe (its
//	      LocalStageCache is what keeps peer fetches serviceable for the
//	      whole query). This is the copy that exists FIRST — it is written
//	      before the result notification the coordinator is reacting to.
//	s3    the durable copy. Under every --shuffle-durability mode this is
//	      still written for the objects the coordinator reads
//	      (executeStageDAG registers scalar producers in coordReadStages and
//	      annotateTaskPeerLocations keeps their uploads eager), so the
//	      fallthrough is always available; it is just not always there YET,
//	      which is the 0.5–1.06 s of whole-cluster idle SF100 window 4 §7
//	      measured on the scalar-substitution barrier.
//
// queryID is unused (it always was): the peer tier derives the root from the
// key's "queries/<id>/" prefix, which is the identity both sides agree on.
//
// tryPeer gates the peer tier for this call. fetchStageOutputData's re-poll
// loop (peer_locations.go) passes false on every iteration after the first:
// the producer was already asked once, and re-dialing it every 500ms for up
// to 15s buys nothing — either its local copy answers on the first try, or
// the re-poll's job is waiting out the durable upload, not the producer.
// Every other caller (the initial call in that same loop, and
// fetchResultData's single-shot reads) passes true.
func (c *Coordinator) fetchResultDataTiered(ctx context.Context, _ string, path string, tryPeer bool) ([]byte, coordReadTier, error) {
	if c.resultKV != nil {
		if entry, err := c.resultKV.Get(ctx, natsKVKey(path)); err == nil {
			c.noteStageReadTier(coordReadKV)
			return entry.Value(), coordReadKV, nil
		}
	}
	if tryPeer {
		if data, ok := c.fetchFromProducerPeer(ctx, path); ok {
			c.noteStageReadTier(coordReadPeer)
			return data, coordReadPeer, nil
		}
	}
	store := c.catalog.Store()
	reader, _, err := store.Get(ctx, c.config.ResultBucket, path)
	if err != nil {
		return nil, coordReadS3, fmt.Errorf("fetching result from S3: %w", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, coordReadS3, err
	}
	c.noteStageReadTier(coordReadS3)
	return data, coordReadS3, nil
}

// fetchFromProducerPeer streams one stage-output object from the worker that
// produced it, over the PeerExchange data plane. ok=false means "this tier
// declined" — switch off, no registry, not query scratch, streaming
// disabled for the query, no minted token, unknown or dead producer, a
// fetch/stream failure, a timeout, or a payload with the wrong magic — and
// the caller must fall through to the durable copy. Nothing in here may
// turn a readable object into an error.
//
// The token is the query's fetch token, minted by annotateTaskPeerLocations
// for the same root and recorded by every worker that ran one of its tasks;
// the producer validates it in Executor.ResolveShuffleFile before serving.
// A query whose tasks were never annotated (streaming exchange off, or the
// ErrInputLost re-execution) has no token, so the tier declines rather than
// minting one the producer could not match.
func (c *Coordinator) fetchFromProducerPeer(ctx context.Context, path string) ([]byte, bool) {
	if !coordPeerReads.On() || c.peerFiles == nil || c.peerClient == nil || c.workers == nil {
		return nil, false
	}
	root := distributed.ScratchQueryID(path)
	if root == "" || c.streamingDisabledFor(root) {
		return nil, false
	}
	token := c.peerFiles.ExistingTokenFor(root)
	if token == "" {
		return nil, false
	}
	workerID := c.peerFiles.Lookup(path)
	if workerID == "" {
		return nil, false
	}
	addr := c.workers.PeerAddr(workerID)
	if addr == "" {
		return nil, false
	}
	fetchCtx, cancel := context.WithTimeout(ctx, peerStageReadTimeout)
	defer cancel()
	rc, err := c.peerClient.FetchShuffle(fetchCtx, addr, root, path, token)
	if err != nil {
		c.stageReadPeerMisses.Add(1)
		c.logger.Debug("coordinator peer read declined; falling through to the durable copy",
			"key", path, "producer", workerID, "addr", addr, "error", err)
		return nil, false
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		c.stageReadPeerMisses.Add(1)
		c.logger.Debug("coordinator peer read failed mid-stream; falling through to the durable copy",
			"key", path, "producer", workerID, "addr", addr, "error", err)
		return nil, false
	}
	if !isPeerStagePayload(data) {
		c.stageReadPeerMisses.Add(1)
		magic := data
		if len(magic) > 4 {
			magic = magic[:4]
		}
		c.logger.Debug("coordinator peer read returned a non-shuffle payload; falling through to the durable copy",
			"key", path, "producer", workerID, "addr", addr, "magic", magic)
		return nil, false
	}
	return data, true
}

// isPeerStagePayload reports whether data starts with a magic the
// coordinator's decode paths actually handle: raw WSHF or an s2-wrapped
// WSHC envelope, both of which wshf.Decompress + wshf.DecodeBatches read.
//
// WSHZ (zstd) is deliberately excluded: it is the S3-upload envelope
// (docs/design/exchange-zstd-wire.md — chosen only when
// WADJET_EXCHANGE_ZSTD=1 uploads to S3; peer-wire compression stays s2 and
// on-disk stays raw WSHF), so a peer serving it — or garbage, or a short
// read — means the local copy is not what this tier expects. That is a
// tier failure to fall through on, never a payload for decode to choke on:
// turning a readable-but-wrong-format object into a query error is exactly
// what this check exists to prevent.
func isPeerStagePayload(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	codec, ok := wshf.CodecForMagic([4]byte{data[0], data[1], data[2], data[3]})
	return ok && codec != wshf.CodecZstd
}

// noteStageReadTier tallies one served coordinator-side stage read.
func (c *Coordinator) noteStageReadTier(t coordReadTier) {
	if t < coordReadTierCount {
		c.stageReadTiers[t].Add(1)
	}
}

// StageReadTierCounts reports how many coordinator-side stage-output reads
// each tier served, plus how many peer attempts fell through to the durable
// copy. Observability + test assertions; the per-read tier also rides the
// "scalar substitution" log line.
func (c *Coordinator) StageReadTierCounts() (kv, peer, s3, peerMisses int64) {
	return c.stageReadTiers[coordReadKV].Load(),
		c.stageReadTiers[coordReadPeer].Load(),
		c.stageReadTiers[coordReadS3].Load(),
		c.stageReadPeerMisses.Load()
}
