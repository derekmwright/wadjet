#!/usr/bin/env python3
"""Judge an SF100 A/B pair (or a single arm) from distributed-SF100-*.txt files.

Usage:
  judge-pair.py <control-file-or-dir> [treatment-file-or-dir] [--trino <trino-comparison.txt>]

Parses per-run per-query walls (handles both `12.345s` and `899ms` units —
Q06 broke the naive regex on 2026-08-12 with the first sub-second SF100
query), verifies row identity across every arm/run, and prints:
  - per-arm run totals + R2/R1
  - pair per-query delta table + totals (when a treatment is given)
  - optional per-query comparison against a Trino comparison transcript
    (queries that FAILED on Trino are excluded from totals and marked)

Judge order reminder (docs/design conventions): rows first, mechanism
markers second (wlogs/profiles — not this script's job), walls LAST.
"""
import re
import sys
import glob
import os

# Wall formats seen in results files: `12.345s`, `899ms`, `3m47.376s`.
# The optional minutes group must come first or "3m47.376s" silently
# parses as 47.376s (dropped 3 minutes — hid the 2026-08-13 control
# Q22-R2 stall from the pair table).
QLINE = re.compile(r'^(Q\d+) .*?(?:(\d+)m)?([\d.]+)(ms|s)\s+(\d+) rows.*?(OK|FAIL)$', re.M)
TRINO_Q = re.compile(r'(q\d+)\s+wall_ms=(\d+)')
TRINO_FAIL = re.compile(r'(q\d+)\s+FAILED')


def resolve(path):
    if os.path.isdir(path):
        hits = glob.glob(os.path.join(path, 'distributed-SF100-*.txt'))
        if not hits:
            sys.exit(f"no distributed-SF100-*.txt under {path}")
        return hits[0]
    return path


def parse(path):
    """Return [ {Q: (wall_s, rows, status)} per run ]."""
    runs = []
    for chunk in open(resolve(path)).read().split('=== Run ')[1:]:
        seen = {}
        for q, mins, w, unit, rows, st in QLINE.findall(chunk):
            if q not in seen:  # result file repeats queries in a summary table
                wall = float(w) / 1000 if unit == 'ms' else float(w)
                wall += 60 * int(mins) if mins else 0
                seen[q] = (wall, int(rows), st)
        runs.append(seen)
    if not runs:
        sys.exit(f"no runs parsed from {path}")
    return runs


def total(run):
    return sum(w for w, _, _ in run.values())


def arm_summary(name, runs):
    for i, r in enumerate(runs):
        bad = [q for q, (_, rows, st) in r.items() if st != 'OK' or rows == 0]
        print(f"{name} R{i+1}: n={len(r)} total={total(r):.1f}s"
              + (f"  BAD={bad}" if bad else ""))
    if len(runs) >= 2 and total(runs[0]) > 0:
        print(f"{name} R2/R1 = {total(runs[1])/total(runs[0]):.3f}")


def main():
    args = sys.argv[1:]
    trino_path = None
    if '--trino' in args:
        i = args.index('--trino')
        trino_path = args[i + 1]
        del args[i:i + 2]
    if not args:
        sys.exit(__doc__)

    ctl = parse(args[0])
    trt = parse(args[1]) if len(args) > 1 else None

    arm_summary("control" if trt else "arm", ctl)
    if trt:
        arm_summary("treatment", trt)
        # Row identity across every arm/run.
        mismatch = []
        for q in sorted(ctl[0]):
            rows = {r[q][1] for r in ctl + trt if q in r}
            if len(rows) != 1:
                mismatch.append((q, sorted(rows)))
        n = sum(len(r) for r in ctl + trt)
        print(f"rows: {'%d/%d IDENTICAL' % (n, n) if not mismatch else f'MISMATCH {mismatch}'}")

        print(f"\n{'Q':5} {'ctlR1':>8} {'trtR1':>8} {'d1':>7} {'ctlR2':>8} {'trtR2':>8} {'d2':>7}")
        for q in sorted(ctl[0], key=lambda x: int(x[1:])):
            c1, t1 = ctl[0][q][0], trt[0][q][0]
            row = f"{q:5} {c1:8.1f} {t1:8.1f} {100*(t1-c1)/c1:+6.0f}%"
            if len(ctl) > 1 and len(trt) > 1:
                c2, t2 = ctl[1][q][0], trt[1][q][0]
                row += f" {c2:8.1f} {t2:8.1f} {100*(t2-c2)/c2:+6.0f}%"
            print(row)
        c = sum(total(r) for r in ctl)
        t = sum(total(r) for r in trt)
        print(f"\npair total: {c:.1f} -> {t:.1f} ({100*(t-c)/c:+.1f}%)")

    if trino_path:
        txt = open(trino_path).read()
        truns = []
        for chunk in txt.split('=== Run ')[1:]:
            truns.append({q.upper(): int(ms) / 1000 for q, ms in TRINO_Q.findall(chunk)})
        failed = {q.upper() for q in TRINO_FAIL.findall(txt)}
        subject = trt or ctl
        print(f"\n--- vs Trino ({os.path.basename(trino_path)}; "
              f"FAILED on Trino, excluded: {sorted(failed) or 'none'}) ---")
        for ri in range(min(len(truns), len(subject))):
            ts = sum(v for q, v in truns[ri].items() if q not in failed)
            ws = sum(w for q, (w, _, _) in subject[ri].items()
                     if q in truns[ri] and q not in failed)
            wins = sum(1 for q in truns[ri]
                       if q not in failed and q in subject[ri]
                       and subject[ri][q][0] < truns[ri][q])
            common = sum(1 for q in truns[ri] if q not in failed and q in subject[ri])
            print(f"R{ri+1}: Trino {ts:.1f}s vs Wadjet {ws:.1f}s = {ws/ts:.2f}x"
                  f"  (Wadjet wins {wins}/{common})")


if __name__ == '__main__':
    main()
