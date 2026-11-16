#!/usr/bin/env python3
"""Tier-benchmark contamination audit (docs/testing/TIER-BENCHMARK.md).

Usage: contamination-audit.py <arm-id>=<workflow transcript dir> [...]

For each arm, reads the BUILDER's transcript (the first agent of the run) and
reports every tool call that names another arm: its id, its notes, its branch
or its worktree. An arm with a hit is void. Also reports when a builder was
merely SHOWN another arm's artifact in a tool result (a listing), which is a
leak in the setup, not a use.
"""
import glob, json, os, sys

arms = dict(a.split("=", 1) for a in sys.argv[1:])
if len(arms) < 2:
    sys.exit(__doc__)
bad = False
for arm, d in arms.items():
    files = sorted(glob.glob(os.path.join(d, "agent-*.jsonl")), key=os.path.getmtime)
    if not files:
        print(f"{arm}: no transcript in {d}"); bad = True; continue
    others = []
    for o, od in arms.items():
        if o != arm:
            others += [o, os.path.basename(od.rstrip("/"))]
    used, shown = [], 0
    for line in open(files[0]):
        try:
            content = (json.loads(line).get("message") or {}).get("content")
        except ValueError:
            continue
        for c in content if isinstance(content, list) else []:
            text = json.dumps(c.get("input") if c.get("type") == "tool_use" else c.get("content"))
            if not any(o in text for o in others):
                continue
            if c.get("type") == "tool_use":
                used.append(text[:200])
            elif c.get("type") == "tool_result":
                shown += 1
    print(f"{arm}: {len(used)} call(s) naming another arm, {shown} result(s) showing another arm's artifact")
    for u in used:
        print("   ", u)
    bad = bad or bool(used)
sys.exit(1 if bad else 0)
