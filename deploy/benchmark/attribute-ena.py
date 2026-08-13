#!/usr/bin/env python3
"""Attribute per-worker ENA throttle events to stage-DAG phases.

The barrier-overlap arc's step-0 instrument (docs: memory kickoff
2026-08-13; evidence: results/20260812-233343 showed chronic bursty
bw_out_allowance_exceeded at stage barriers). This joins the two
per-run evidence streams every arm already produces:

  1. worker journals (wlogs/wlog-*.gz): `ena-poll` samples — cumulative
     rx/tx bytes + ENA allowance-exceeded counters, 10s cadence
     (30s/60s on pre-2026-08-13 artifacts; the tool only diffs
     consecutive samples, so cadence is discovered, not assumed).
  2. coordinator journal (coord-journal.gz): stage-DAG lifecycle —
     stage dispatch events, per-task results (stage_id, worker_id,
     ts), shuffle side completion, query open/close.

and answers: which stage classes are active when the NIC trips its
outbound allowance — repartition shuffle writes, stage-output PUTs
(join/aggregate/sort), scan, or no stage at all (barrier gap /
between-queries, i.e. result upload + peer serving territory).

Attribution model: a worker's counter delta over a sample interval is
split across the piecewise-constant active-stage signatures during
that interval, proportional to sub-duration. Bursts are impulsive, so
proportional smearing is an approximation — its error shrinks with
cadence (the reason the sampler moved to 10s). The top-burst table
reports raw per-interval facts with no smearing.

Usage:
  attribute-ena.py --artifacts DIR            # expects coord-journal.gz,
                                              # wlogs/, distributed-*.txt
  attribute-ena.py --coord coord-journal.gz --wlogs wlogs/ \
      [--results distributed-SF100-*.txt] [--top 20] [--tsv out.tsv]
"""
import argparse
import glob
import gzip
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

SLOG_RE = re.compile(r'time=(\S+) level=\w+ msg=("([^"]*)"|\S+)(.*)$')
KV_RE = re.compile(r'(\w+)=("[^"]*"|\S+)')
SYSLOG_TS_RE = re.compile(r"^([A-Z][a-z]{2}) +(\d+) (\d\d):(\d\d):(\d\d) ")
MONTHS = {m: i + 1 for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split())}
QLABEL_RE = re.compile(r"^(Q\d+)\s.*\s(OK|FAIL)\s*$")
ENA_NUM_RE = re.compile(r"(\w+)[=:] ?(\d+)")

# stage_id prefix -> phase class
def stage_kind(stage_id):
    if stage_id.startswith("shuffle-") or "exchange-repartition" in stage_id:
        return "repartition"
    for p, k in (("scan", "scan"), ("join", "join"), ("final_aggregate", "aggregate"),
                 ("aggregate", "aggregate"), ("sort", "sort"), ("window", "window")):
        if stage_id.startswith(p):
            return k
    return "other"


def zopen(path):
    return gzip.open(path, "rt", errors="replace") if path.endswith(".gz") \
        else open(path, errors="replace")


def slog_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def syslog_ts(line, year):
    m = SYSLOG_TS_RE.match(line)
    if not m:
        return None
    mon, day, hh, mm, ss = m.groups()
    return datetime(year, MONTHS[mon], int(day), int(hh), int(mm), int(ss),
                    tzinfo=timezone.utc).timestamp()


class Run:
    def __init__(self, qid, t0):
        self.qid, self.t0, self.t_end, self.label = qid, t0, None, None
        self.stages = {}  # stage_id -> dict(kind, start, end)

    def touch(self, sid, ts):
        st = self.stages.get(sid)
        if st is None:
            st = self.stages[sid] = dict(kind=stage_kind(sid), start=ts, end=ts)
        st["start"] = min(st["start"], ts)
        st["end"] = max(st["end"], ts)
        return st


def parse_coord(path):
    """Returns (runs, year). Stage span = dispatch event -> last task
    result (shuffle sides: -> 'shuffle side complete')."""
    runs, cur, year = [], None, None
    with zopen(path) as f:
        for raw in f:
            i = raw.find("time=")
            if i < 0:
                continue
            m = SLOG_RE.match(raw[i:].strip())
            if not m:
                continue
            ts = slog_ts(m.group(1))
            if year is None:
                year = datetime.fromtimestamp(ts, timezone.utc).year
            msg = m.group(3) if m.group(3) is not None else m.group(2)
            kv = {k: v.strip('"') for k, v in KV_RE.findall(m.group(4))}
            if msg == "stage-DAG dispatch" and "query" in kv:
                cur = Run(kv["query"], ts)
                runs.append(cur)
            elif cur is None:
                continue
            elif msg in ("dispatchScanFilterStage", "dispatchScanAggregateStage",
                         "dispatchFinalAggregateFanout", "dispatching compute stage",
                         "task result") and "stage_id" in kv:
                cur.touch(kv["stage_id"], ts)
            elif msg == "shuffle side complete" and kv.get("query_id") == cur.qid:
                cur.touch("shuffle-" + kv.get("side", "?"), ts)
            elif msg in ("gather: fused wait returned", "gather: wait returned") \
                    and kv.get("query") == cur.qid:
                cur.t_end = ts
                cur = None
    return runs, year


def label_runs(runs, results_path):
    labels = []
    if results_path:
        with zopen(results_path) as f:
            for line in f:
                m = QLABEL_RE.match(line.strip())
                if m:
                    labels.append(m.group(1))
    n = len(runs)
    half = n // 2 if labels and len(labels) == n else None
    for i, r in enumerate(runs):
        base = labels[i] if i < len(labels) else f"run{i}"
        rnum = 1 + (i // half if half else 0)
        r.label = f"{base}-R{rnum}" if half else base


def parse_wlog(path, year):
    """Returns (worker_id, samples). Sample = (ts, {counter: value}).
    Only the rich `if=... rx=... tx=...` sampler lines are used; the
    counter-only lines from the short-lived 60s duplicate are skipped."""
    wid, samples = None, []
    with zopen(path) as f:
        for line in f:
            if wid is None and "worker_id=" in line:
                mm = re.search(r"worker_id=(\S+)", line)
                if mm:
                    wid = mm.group(1)
            if " ena-poll" not in line or " rx=" not in line:
                continue
            ts = syslog_ts(line, year)
            if ts is None:
                continue
            vals = {k: int(v) for k, v in ENA_NUM_RE.findall(
                line.split("ena-poll", 1)[1])}
            if "rx" in vals and "tx" in vals:
                samples.append((ts, vals))
    return wid, samples


def intervals(samples):
    """Consecutive-sample deltas; drops counter resets (negative)."""
    out = []
    for (t0, a), (t1, b) in zip(samples, samples[1:]):
        if t1 <= t0:
            continue
        d = {k: b.get(k, 0) - a.get(k, 0) for k in b}
        if any(v < 0 for v in d.values()):
            continue
        out.append((t0, t1, d))
    return out


def active_signature(runs, t):
    """Phase signature at instant t: sorted '+'-joined kinds of active
    stage spans, else 'barrier-gap' (query open, no stage active) or
    'no-query' (between queries: result upload / discovery / idle)."""
    kinds, in_query = set(), False
    for r in runs:
        t_end = r.t_end if r.t_end is not None else r.t0
        if r.t0 <= t <= t_end:
            in_query = True
            for st in r.stages.values():
                if st["start"] <= t <= st["end"]:
                    kinds.add(st["kind"])
    if kinds:
        return "+".join(sorted(kinds))
    return "barrier-gap" if in_query else "no-query"


def change_points(runs, t0, t1):
    pts = {t0, t1}
    for r in runs:
        for x in (r.t0, r.t_end if r.t_end is not None else r.t0):
            if t0 < x < t1:
                pts.add(x)
        for st in r.stages.values():
            for x in (st["start"], st["end"]):
                if t0 < x < t1:
                    pts.add(x)
    return sorted(pts)


def query_at(runs, t):
    for r in runs:
        if r.t0 <= t <= (r.t_end if r.t_end is not None else r.t0):
            return r
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--artifacts", help="dir with coord-journal.gz, wlogs/, distributed-*.txt")
    ap.add_argument("--coord")
    ap.add_argument("--wlogs")
    ap.add_argument("--results")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--tsv", help="write every (worker, interval) row here")
    args = ap.parse_args()

    if args.artifacts:
        args.coord = args.coord or os.path.join(args.artifacts, "coord-journal.gz")
        args.wlogs = args.wlogs or os.path.join(args.artifacts, "wlogs")
        if not args.results:
            hits = sorted(glob.glob(os.path.join(args.artifacts, "distributed-*.txt")))
            args.results = hits[0] if hits else None
    if not args.coord or not args.wlogs:
        ap.error("need --artifacts or both --coord and --wlogs")

    runs, year = parse_coord(args.coord)
    label_runs(runs, args.results)
    print(f"coordinator: {len(runs)} query runs "
          f"({sum(len(r.stages) for r in runs)} stage spans)")

    workers = {}  # wid -> intervals
    for path in sorted(glob.glob(os.path.join(args.wlogs, "wlog-*"))):
        wid, samples = parse_wlog(path, year)
        wid = wid or os.path.basename(path)
        workers[wid] = intervals(samples)
        cad = (samples[-1][0] - samples[0][0]) / max(1, len(samples) - 1) \
            if len(samples) > 1 else 0
        print(f"  {wid}: {len(samples)} ena samples (~{cad:.0f}s cadence) "
              f"from {os.path.basename(path)}")

    # ---- per-worker totals -------------------------------------------------
    print("\nPER-WORKER TOTALS (whole capture)")
    print(f"{'worker':22} {'tx_GB':>8} {'rx_GB':>8} {'out_exc':>9} {'in_exc':>9} {'pps_exc':>8}")
    for wid, ivs in sorted(workers.items()):
        tot = defaultdict(int)
        for _, _, d in ivs:
            for k, v in d.items():
                tot[k] += v
        print(f"{wid:22} {tot['tx']/1e9:8.1f} {tot['rx']/1e9:8.1f} "
              f"{tot.get('bw_out_allowance_exceeded', 0):9d} "
              f"{tot.get('bw_in_allowance_exceeded', 0):9d} "
              f"{tot.get('pps_allowance_exceeded', 0):8d}")

    # ---- signature attribution (duration-proportional smearing) -----------
    sig = defaultdict(lambda: defaultdict(float))  # signature -> counters
    rows = []          # tsv rows
    bursts = []        # (out_exc, wid, t0, t1, d, sig, qlabels)
    per_query = defaultdict(lambda: defaultdict(float))
    for wid, ivs in workers.items():
        for t0, t1, d in ivs:
            pts = change_points(runs, t0, t1)
            span = t1 - t0
            for a, b in zip(pts, pts[1:]):
                mid = (a + b) / 2
                s = active_signature(runs, mid)
                frac = (b - a) / span
                sig[s]["seconds"] += b - a
                for k, v in d.items():
                    sig[s][k] += v * frac
                q = query_at(runs, mid)
                if q:
                    per_query[q.label]["out_exc"] += \
                        d.get("bw_out_allowance_exceeded", 0) * frac
                    per_query[q.label]["tx"] += d.get("tx", 0) * frac
            oe = d.get("bw_out_allowance_exceeded", 0)
            s_mid = active_signature(runs, (t0 + t1) / 2)
            q = query_at(runs, (t0 + t1) / 2)
            if oe > 0:
                bursts.append((oe, wid, t0, t1, d, s_mid, q.label if q else "-"))
            if args.tsv:
                rows.append((wid, t0, t1, s_mid, q.label if q else "-", d))

    print("\nATTRIBUTION BY ACTIVE-STAGE SIGNATURE"
          " (counter deltas smeared duration-proportionally; see header)")
    print(f"{'signature':28} {'sec':>7} {'tx_GB':>8} {'out_exc':>9} {'out_exc/s':>9} {'in_exc':>8}")
    for s, c in sorted(sig.items(), key=lambda x: -x[1].get("bw_out_allowance_exceeded", 0)):
        sec = c["seconds"]
        oe = c.get("bw_out_allowance_exceeded", 0)
        print(f"{s:28} {sec:7.0f} {c.get('tx', 0)/1e9:8.1f} {oe:9.0f} "
              f"{oe/sec if sec else 0:9.1f} {c.get('bw_in_allowance_exceeded', 0):8.0f}")

    print(f"\nTOP {args.top} BURST INTERVALS (raw, no smearing)")
    print(f"{'when(UTC)':>8} {'sec':>4} {'worker':22} {'out_exc':>8} {'tx_MB':>7} "
          f"{'query':9} signature")
    for oe, wid, t0, t1, d, s, ql in sorted(bursts, reverse=True)[:args.top]:
        when = datetime.fromtimestamp(t0, timezone.utc).strftime("%H:%M:%S")
        print(f"{when:>8} {t1-t0:4.0f} {wid:22} {oe:8d} {d.get('tx',0)/1e6:7.0f} "
              f"{ql:9} {s}")

    print("\nPER-QUERY OUTBOUND THROTTLE (all workers, duration-smeared)")
    ranked = sorted(per_query.items(), key=lambda x: -x[1]["out_exc"])
    print(f"{'query':9} {'out_exc':>9} {'tx_GB':>8}")
    for ql, c in ranked[:args.top]:
        print(f"{ql:9} {c['out_exc']:9.0f} {c['tx']/1e9:8.1f}")

    if args.tsv:
        with open(args.tsv, "w") as f:
            f.write("worker\tt0\tt1\tsignature\tquery\ttx\trx\tout_exc\tin_exc\tpps_exc\n")
            for wid, t0, t1, s, ql, d in rows:
                f.write(f"{wid}\t{t0:.0f}\t{t1:.0f}\t{s}\t{ql}\t{d.get('tx',0)}\t"
                        f"{d.get('rx',0)}\t{d.get('bw_out_allowance_exceeded',0)}\t"
                        f"{d.get('bw_in_allowance_exceeded',0)}\t"
                        f"{d.get('pps_allowance_exceeded',0)}\n")
        print(f"\nwrote {len(rows)} interval rows to {args.tsv}")


if __name__ == "__main__":
    sys.exit(main())
