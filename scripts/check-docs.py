#!/usr/bin/env python3
"""Enforce the mechanical rules in docs/documentation-standard.md.

These are ceilings that catch essays, not the review budgets. Section 3's tighter
numbers (`//` <= 3, member scaladoc <= 10) stay review rules: a regex cannot tell a
member scaladoc from a class one, nor judge whether five lines of `//` earn their
place. The ceilings here are set off the repo's own distribution -- 99% of `//`
blocks are within MAX_LINE_BLOCK, and every scaladoc within MAX_DOC_BLOCK -- so a
failure means something genuinely new, not pre-existing debt.

  1. No comment block over its ceiling outside docs/. `@param` / `@return` lines are
     per-field reference material and do not count toward it (section 3).
  2. Every markdown link, and every docs/... path cited from Scala, resolves
     (section 6: a reference may only replace an explanation if the target holds it).
"""
import os, re, sys

MAX_LINE_BLOCK = 8       # consecutive `//` lines
MAX_DOC_BLOCK = 15       # scaladoc / block comment
MAX_CLAUDE_WORDS = 2500  # section 6
SKIP_DIRS = {'.git', 'target', '.bloop', '.metals', '.idea', 'out', 'logs'}
LINK = re.compile(r'\[[^\]]*\]\(([^)#\s]+)(?:#[^)\s]*)?(?:\s+"[^"]*")?\)')
DOC_PATH = re.compile(r'docs/[A-Za-z0-9._/-]+\.md')
ADR_DIR = os.path.join('.', 'docs', 'adr')
ADR_FILE = re.compile(r'\b(\d{4}-[A-Za-z0-9-]+\.md)')
# The status line marks a DEAD decision with the past participle; "Supersedes" on a live ADR's
# status must not match it. A citing line may use any form of the word.
STATUS_DEAD = re.compile(r'\bsuperseded\b', re.I)
CITE_MARKER = re.compile(r'supersed', re.I)

failures = []


def walk(ext):
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fn in sorted(files):
            if fn.endswith(ext):
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
    """Measure one block, discounting the per-field reference lines it may carry."""
    if start is None:
        return
    line_comment = all(b.strip().startswith('//') for b in buf)
    ceiling = MAX_LINE_BLOCK if line_comment else MAX_DOC_BLOCK
    # Drop `@param x ...` / `@return ...` and their wrapped continuation lines: only
    # the surrounding prose counts, so one tag cannot exempt an essay.
    prose, in_tag = [], False
    for b in buf:
        body = b.strip().lstrip('*').lstrip('/').strip()
        if body.startswith(('@param', '@return', '@throws', '@tparam')):
            in_tag = True
            continue
        if in_tag and body and not body.startswith('@'):
            continue          # continuation of the tag above
        in_tag = False
        prose.append(b)
    if len(prose) <= ceiling:
        return
    kind = 'line-comment block' if line_comment else 'comment block'
    failures.append(f'{path}:{start}: {kind} is {len(prose)} lines (max {ceiling}). '
                    f'Move the rationale to docs/adr/ and leave a pointer.')


def check_pointers():
    for path in walk('.md'):
        root = os.path.dirname(path)
        text = re.sub(r'`[^`\n]*`', '', open(path).read())   # ignore inline code spans
        for m in LINK.finditer(text):
            target = m.group(1)
            if target.startswith(('http://', 'https://', 'mailto:')):
                continue
            if not os.path.exists(os.path.normpath(os.path.join(root, target))):
                failures.append(f'{path}: unresolved link -> {target}')
    for path in walk('.scala'):
        for m in DOC_PATH.finditer(open(path).read()):
            if not os.path.exists(m.group(0)):
                failures.append(f'{path}: unresolved doc pointer -> {m.group(0)}')


def check_claude_size():
    words = len(open('CLAUDE.md').read().split())
    if words > MAX_CLAUDE_WORDS:
        failures.append(f'CLAUDE.md: {words} words (max {MAX_CLAUDE_WORDS}). '
                        f'Move architecture prose to docs/ and link to it.')


def status_line(path):
    """The ADR's `**Status:**` line, joined with the line after it so a wrapped status still reads whole."""
    lines = open(path).read().split('\n')
    for i, l in enumerate(lines):
        if l.startswith('**Status:**'):
            return ' '.join(lines[i:i + 2])
    return ''


def check_adr_currency():
    """Immutability is not currency: a superseded ADR still resolves, so a bare pointer into one
    hands the reader a decision that no longer holds. Require a forward link on the ADR, and require
    every citation from outside docs/adr/ to say the target is superseded."""
    if not os.path.isdir(ADR_DIR):
        return
    superseded = set()
    for fn in sorted(os.listdir(ADR_DIR)):
        if not ADR_FILE.fullmatch(fn):
            continue
        status = status_line(os.path.join(ADR_DIR, fn))
        if not STATUS_DEAD.search(status):
            continue
        superseded.add(fn)
        forward = [m for m in ADR_FILE.findall(status) if m != fn]
        if not forward:
            failures.append(f'docs/adr/{fn}: status says superseded but links to no successor. '
                            f'Add the link so a reader can walk to the live decision.')
    if not superseded:
        return
    for ext in ('.scala', '.md'):
        for path in walk(ext):
            if os.path.normpath(path).startswith(os.path.normpath(ADR_DIR)):
                continue          # ADRs legitimately discuss one another
            for n, line in enumerate(open(path).read().split('\n'), 1):
                for fn in ADR_FILE.findall(line):
                    if fn in superseded and not CITE_MARKER.search(line):
                        failures.append(f'{path}:{n}: cites superseded ADR {fn} without saying so. '
                                        f'Point at its successor, or say "superseded" on this line.')


check_blocks()
check_pointers()
check_claude_size()
check_adr_currency()

if failures:
    print('Documentation standard violations (docs/documentation-standard.md):\n')
    for f in failures:
        print(f'  {f}')
    sys.exit(1)
print('Documentation standard: OK')
