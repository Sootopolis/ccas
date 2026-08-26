#!/usr/bin/env python3
"""Enforce the two mechanical rules in docs/documentation-standard.md.

1. No comment block over MAX_BLOCK lines outside docs/. Blocks carrying @param
   or @return are per-field reference material and exempt (section 3).
2. Every markdown link and every docs/... path cited from Scala resolves
   (section 6: a reference may only replace an explanation if the target holds it).
"""
import os, re, sys

MAX_BLOCK = 15
SKIP_DIRS = ('/.git', '/target', '/.bloop', '/.metals', '/.idea', '/out', '/logs')
failures = []


def walk(exts):
    for root, dirs, files in os.walk('.'):
        if any(d in root for d in SKIP_DIRS):
            continue
        for fn in sorted(files):
            if fn.endswith(exts):
                yield os.path.join(root, fn)


def check_blocks():
    for path in walk('.scala'):
        lines = open(path).read().split('\n')
        start, buf = None, []
        for i, line in enumerate(lines):
            if line.strip().startswith(('//', '/*', '*')):
                if start is None:
                    start = i + 1
                buf.append(line)
            else:
                flush(path, start, buf)
                start, buf = None, []
        flush(path, start, buf)


def flush(path, start, buf):
    if start is None or len(buf) <= MAX_BLOCK:
        return
    if any('@param' in b or '@return' in b for b in buf):
        return
    failures.append(f'{path}:{start}: comment block is {len(buf)} lines (max {MAX_BLOCK}). '
                    f'Move the rationale to docs/adr/ and leave a pointer.')


def check_pointers():
    for path in walk('.md'):
        root = os.path.dirname(path)
        text = re.sub(r'`[^`\n]*`', '', open(path).read())   # ignore inline code spans
        for m in re.finditer(r'\[[^\]]*\]\(([^)#\s]+)(?:#[^)]*)?\)', text):
            target = m.group(1)
            if target.startswith(('http://', 'https://', 'mailto:')):
                continue
            if not os.path.exists(os.path.normpath(os.path.join(root, target))):
                failures.append(f'{path}: unresolved link -> {target}')
    for path in walk('.scala'):
        for m in re.finditer(r'docs/[A-Za-z0-9._/-]+\.md', open(path).read()):
            if not os.path.exists(m.group(0)):
                failures.append(f'{path}: unresolved doc pointer -> {m.group(0)}')


check_blocks()
check_pointers()

if failures:
    print('Documentation standard violations (docs/documentation-standard.md):\n')
    for f in failures:
        print(f'  {f}')
    sys.exit(1)
print('Documentation standard: OK')
