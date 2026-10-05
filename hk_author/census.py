#!/usr/bin/env python3
"""Count // blocks with more than 20 consecutive comment lines.

The file set is the bundle's changed, non-test Go files. Missing files count
as zero. Whitespace before // is permitted; blank lines and code end a block.
Run from the repository: census.py TREE [--files FILE_LIST].
"""
import argparse
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('tree', type=Path)
parser.add_argument('--files', type=Path)
args = parser.parse_args()
files = (args.files.read_text() if args.files else subprocess.check_output(
    ['git', 'diff', '--name-only', 'db4c0a7d..33e2fb92', '--', '*.go'], text=True)).splitlines()
counts = {}
for name in sorted(files):
    if name.endswith('_test.go'):
        continue
    path = args.tree / name
    lines = path.read_text().splitlines() if path.exists() else []
    blocks, length = 0, 0
    for line in lines + ['']:
        if re.match(r'^\s*//', line):
            length += 1
        else:
            blocks += length > 20
            length = 0
    counts[name] = blocks
print(json.dumps({'total': sum(counts.values()), 'files': counts}, indent=2))
