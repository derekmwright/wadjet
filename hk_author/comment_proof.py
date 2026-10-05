#!/usr/bin/env python3
"""Check Go edits are comment lines and leave all non-comment tokens intact."""
import re
import subprocess
from pathlib import Path


def program(text):
    # Strings and runes are retained verbatim, including comment-like text.
    token = re.compile(r'`[^`]*`|"(?:\\.|[^"\\])*"|\x27(?:\\.|[^\x27\\])*\x27|//[^\n]*|/\*[\s\S]*?\*/|\s+|[^\s]', re.M)
    return [m[0] for m in token.finditer(text)
            if not m[0].isspace() and not m[0].startswith(('//', '/*'))]


files = subprocess.check_output(['git', 'diff', '--name-only', '33e2fb92', '--', '*.go'], text=True).splitlines()
for name in files:
    before = subprocess.check_output(['git', 'show', '33e2fb92:' + name], text=True)
    after = Path(name).read_text()
    assert len(before.splitlines()) == len(after.splitlines()), name + ': line count changed'
    assert program(before) == program(after), name + ': program tokens differ'
    assert re.findall(r'`([^`]+)`', before) == re.findall(r'`([^`]+)`', after), name + ': literal example changed'
    diff = subprocess.check_output(['git', 'diff', '--unified=0', '33e2fb92', '--', name], text=True)
    for line in diff.splitlines():
        if line.startswith(('---', '+++')):
            continue
        if line.startswith(('-', '+')):
            assert line[1:].lstrip().startswith('//'), name + ': changed non-comment line: ' + line
print(f'{len(files)} Go files: all changed lines are // comments; non-comment tokens identical to 33e2fb92')
fmt = subprocess.check_output(['gofmt', '-l'] + files, text=True) if files else ''
assert not fmt, 'gofmt reports: ' + fmt
print('gofmt -l changed Go files: clean')
print('All backtick-delimited examples unchanged; no comment lines removed')
subprocess.run(['git', 'diff', '--check'], check=True)
print('git diff --check: clean')
