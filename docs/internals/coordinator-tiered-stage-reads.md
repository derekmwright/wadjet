# Coordinator tiered stage reads

Source: internal/coordinator/stage_read.go — fetchResultDataTiered, moved 2026-09-11 (#1026)

fetchResultDataTiered retrieves one stage-output / result blob for
coordinator-side consumption and reports which tier served it.

Tier order, cheapest-correct first:

	kv    the producer mirrored payloads <= natsKVResultThreshold into the
	      shared NATS KV bucket at task-finish time (worker
	      finishStageOutputAsync / writeUnpartitionedWSHF). Best case, but
	      the bucket is a 5-minute-TTL, 1 GB-capped cache: a Put that lost
	      the cap race or a long query both miss.
	peer  the producing worker still holds the file on local NVMe (its
	      LocalStageCache is what keeps peer fetches serviceable for the
	      whole query). This is the copy that exists FIRST — it is written
	      before the result notification the coordinator is reacting to.
	s3    the durable copy. Under every --shuffle-durability mode this is
	      still written for the objects the coordinator reads
	      (executeStageDAG registers scalar producers in coordReadStages and
	      annotateTaskPeerLocations keeps their uploads eager), so the
	      fallthrough is always available; it is just not always there YET,
	      which is the 0.5–1.06 s of whole-cluster idle SF100 window 4 §7
	      measured on the scalar-substitution barrier.

queryID is unused (it always was): the peer tier derives the root from the
key's "queries/<id>/" prefix, which is the identity both sides agree on.

tryPeer gates the peer tier for this call. fetchStageOutputData's re-poll
loop (peer_locations.go) passes false on every iteration after the first:
the producer was already asked once, and re-dialing it every 500ms for up
to 15s buys nothing — either its local copy answers on the first try, or
the re-poll's job is waiting out the durable upload, not the producer.
Every other caller (the initial call in that same loop, and
fetchResultData's single-shot reads) passes true.
