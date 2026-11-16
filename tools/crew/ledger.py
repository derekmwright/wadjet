#!/usr/bin/env python3
"""The crew ledger (docs/testing/ROUTING.md): one row per item per attempt.

  ledger.py cost <workflow transcript dir> [...]   per-agent tokens and API-rate cost
  ledger.py add '<json row>'                       append a row to ledger.jsonl
  ledger.py report                                 the routing profile so far

Rates are $/MTok (input, output, cache read). Cache write is taken as 1.25x input.
A rate marked assumed is not from the published table; fix it here when it is.
"""
import collections, glob, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.path.join(HERE, "ledger.jsonl")
RATES = {  # model-id substring -> (input, output, cache_read)
    "opus-5-5": (4.00, 20.00, 0.20),
    "sonnet-5-5": (2.00, 10.00, 0.10),
    "haiku-5-5": (0.10, 0.50, 0.005),  # cache read assumed
}


def cost(dirs):
    grand = 0.0
    for d in dirs:
        total = 0.0
        print("==", d.rstrip("/").split("/")[-1])
        for f in sorted(glob.glob(os.path.join(d, "agent-*.jsonl")), key=os.path.getmtime):
            u, model, seen, t0, t1 = collections.Counter(), None, set(), None, None
            for line in open(f):
                try:
                    j = json.loads(line)
                except ValueError:
                    continue
                ts = j.get("timestamp")
                if ts:
                    t0, t1 = t0 or ts, ts
                m = j.get("message") or {}
                us = m.get("usage")
                if j.get("type") != "assistant" or not us or m.get("id") in seen:
                    continue
                seen.add(m.get("id"))
                model = m.get("model")
                for k in ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
                    u[k] += us.get(k) or 0
            rate = next((r for k, r in RATES.items() if k in (model or "")), None)
            if not rate:
                print("   no rate for", model)
                continue
            i, o, cr = rate
            c = (u["input_tokens"] * i + u["output_tokens"] * o + u["cache_read_input_tokens"] * cr
                 + u["cache_creation_input_tokens"] * i * 1.25) / 1e6
            total += c
            print(f"   {model:20s} turns {len(seen):3d} out {u['output_tokens']:6d} cache_w {u['cache_creation_input_tokens']:8d}"
                  f" cache_r {u['cache_read_input_tokens']:9d}  ${c:5.2f}  {(t0 or '')[11:16]}-{(t1 or '')[11:16]}")
        print(f"   total ${total:.2f}")
        grand += total
    print(f"all: ${grand:.2f}")


def rows():
    return [json.loads(l) for l in open(LEDGER) if l.strip()] if os.path.exists(LEDGER) else []


def band(d):
    return "0-2" if d <= 2 else "3-4" if d <= 4 else "5-6" if d <= 6 else "7+"


def report():
    g = collections.defaultdict(list)
    for r in rows():
        g[(r["class"], band(sum(r["difficulty"].values())), r["builder"])].append(r)
    print(f"{'class':16s} {'D':4s} {'builder':18s} {'n':>2s} {'clean@1':>7s} {'rounds':>6s} {'$total':>7s} {'missed':>6s} {'slips':>5s} {'stops':>5s}")
    for (cls, b, builder), rs in sorted(g.items()):
        n = len(rs)
        clean1 = sum(1 for r in rs if r["outcome"] == "clean" and r["rounds"] == 1)
        print(f"{cls:16s} {b:4s} {builder:18s} {n:2d} {clean1:4d}/{n:<2d} {sum(r['rounds'] for r in rs)/n:6.1f}"
              f" {sum(r['cost']['total'] for r in rs)/n:7.2f} {sum(r.get('side_effects_missed', 0) for r in rs):6d}"
              f" {sum(r.get('brief_deviations', 0) for r in rs):5d} {sum(1 for r in rs if r['outcome'] == 'stopped'):5d}")
    print(f"{len(rows())} rows. A cell with n < 3 is an anecdote, not a rate.")


if len(sys.argv) > 2 and sys.argv[1] == "cost":
    cost(sys.argv[2:])
elif len(sys.argv) == 3 and sys.argv[1] == "add":
    r = json.loads(sys.argv[2])
    for k in ("date", "item", "issue", "kind", "class", "difficulty", "builder", "outcome", "rounds", "cost"):
        assert k in r, "missing " + k
    open(LEDGER, "a").write(json.dumps(r, sort_keys=True) + "\n")
elif len(sys.argv) == 2 and sys.argv[1] == "report":
    report()
else:
    sys.exit(__doc__)
