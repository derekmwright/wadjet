#!/usr/bin/env python3
"""Reproduce the official ClickBench ranking for a wadjet results.json.

Implements index.html's exact scoring (verified 2026-08-17 against the
repo at 4c6e5d44): hot = min(run2, run3); per-query ratio
(t+0.01)/(baseline+0.01) with per-run baselines over the field; missing
results penalized 2x max(300s, own worst); combined =
exp(0.1*ln(load>=5s ? ratio : 1) + 0.1*ln(size>=1GB ? ratio : 2)
    + 0.2*ln(cold) + 0.6*ln(hot)).
Field = data.generated.js entries for the chosen machine (non-fake).

Usage: rank.py <clickbench-repo-dir> <results.json> [machine]
"""
import json, math, sys

repo, ours_path = sys.argv[1], sys.argv[2]
machine = sys.argv[3] if len(sys.argv) > 3 else 'c6a.4xlarge'

s = open(repo + '/data.generated.js').read()
s = s[s.find('['):]
field = [e for e in json.loads(s[:s.rfind(']')+1])
         if e.get('machine') == machine and not e.get('fake')]
ours = json.load(open(ours_path))
ours['system'] = 'Wadjet (this run)'
field.append(ours)
NQ = len(ours['result'])

def sel(t, metric):
    if t is None:
        return None
    if metric == 'cold':
        return t[0]
    return min(t[1], t[2]) if (t[1] is not None and t[2] is not None) else None

baseline = [[min((e['result'][q][r] for e in field
                  if e['result'][q] is not None and e['result'][q][r] is not None), default=None)
             for r in range(3)] for q in range(NQ)]

def rel(e, metric):
    own = [sel(t, metric) for t in e['result']]
    fallback = 2 * max(300, max((x for x in own if x is not None), default=0))
    acc = used = 0
    for q in range(NQ):
        bt = sel(baseline[q], metric)
        if bt is None:
            continue
        ct = own[q] if own[q] is not None else fallback
        acc += math.log((0.01 + ct) / (0.01 + bt))
        used += 1
    return math.exp(acc / used)

min_load = min(e['load_time'] for e in field if e.get('load_time') and e['load_time'] > 5)
min_size = min(e['data_size'] for e in field if e.get('data_size') and e['data_size'] > 1e9)

rows = []
for e in field:
    h, c = rel(e, 'hot'), rel(e, 'cold')
    lt, ds = e.get('load_time'), e.get('data_size')
    comb = math.exp(0.1 * math.log(lt / min_load if lt and lt >= 5 else 1) +
                    0.1 * math.log(ds / min_size if ds and ds >= 1e9 else 2) +
                    0.2 * math.log(c) + 0.6 * math.log(h))
    rows.append((e['system'], comb, h, c))

for label, k in (('combined', 1), ('hot', 2), ('cold', 3)):
    v = sorted(rows, key=lambda r: r[k])
    for i, r in enumerate(v, 1):
        if r[0] == 'Wadjet (this run)':
            print(f"{label:8s}: #{i}/{len(v)}  x{r[k]:.1f}")

v = sorted(rows, key=lambda r: r[1])
idx = next(i for i, r in enumerate(v) if r[0] == 'Wadjet (this run)')
print("\ncombined neighborhood:")
for i in range(max(0, idx - 4), min(len(v), idx + 5)):
    m = '>>' if i == idx else '  '
    print(f"{m} #{i+1:3d} {v[i][0][:42]:42s} x{v[i][1]:.2f} (hot x{v[i][2]:.1f} cold x{v[i][3]:.1f})")
