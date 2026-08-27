#!/usr/bin/env python3
"""Enforce the mechanical rules in docs/documentation-standard.md.

Two kinds of rule, and they are treated differently on purpose.

*Judgement* rules are the length ceilings. A regex cannot tell a member scaladoc from
a class one, nor judge whether five lines of `//` earn their place, so these catch
essays rather than measuring quality -- and they are WAIVABLE. Put

    docs-standard: allow long-block -- <reason>

anywhere inside the block. The waiver is honoured and counted; `--report` lists them.
A rule waived often is a rule that is wrong, and that is the point: the waivers are
the evidence the next review runs on, so nobody has to argue from taste.

*Correctness* rules are pointer resolution and ADR currency. A broken pointer is never
a judgement call, so those are NOT waivable and no marker suppresses them.

Run with --report for the review agenda: live measurements against the standard's
Baseline, every waiver with its reason, and which rules currently have no tension at
all -- a rule that never fires and is never waived costs reading attention and buys
nothing, so the review should consider deleting it.
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
# Waivers apply to judgement rules only. The reason is mandatory: an unexplained waiver
# is indistinguishable from a bypass, and carries no evidence into the next review.
WAIVER = re.compile(r'docs-standard:\s*allow\s+([a-z-]+)\s*(?:--|—)\s*(\S.*?)\s*$')

failures = []
waivers = []      # (rule, path, line, reason)
fired = set()     # rules that produced at least one failure or waiver this run


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
    waived = next((WAIVER.search(b) for b in buf if WAIVER.search(b)), None)
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
    if waived and waived.group(1) == 'long-block':
        waivers.append(('long-block', path, start, waived.group(2)))
        fired.add('long-block')
        return
    fired.add('long-block')
    kind = 'line-comment block' if line_comment else 'comment block'
    failures.append(f'{path}:{start}: {kind} is {len(prose)} lines (max {ceiling}). '
                    f'Move the rationale to docs/adr/, leave a pointer, or waive it inline with '
                    f'"docs-standard: allow long-block -- <reason>" if the ceiling is wrong here.')


def check_pointers():
    for path in walk('.md'):
        root = os.path.dirname(path)
        text = re.sub(r'`[^`\n]*`', '', open(path).read())   # ignore inline code spans
        for m in LINK.finditer(text):
            target = m.group(1)
            if target.startswith(('http://', 'https://', 'mailto:')):
                continue
            if not os.path.exists(os.path.normpath(os.path.join(root, target))):
                fired.add('pointer'); failures.append(f'{path}: unresolved link -> {target}')
    for path in walk('.scala'):
        for m in DOC_PATH.finditer(open(path).read()):
            if not os.path.exists(m.group(0)):
                fired.add('pointer'); failures.append(f'{path}: unresolved doc pointer -> {m.group(0)}')


def check_claude_size():
    words = len(open('CLAUDE.md').read().split())
    if words > MAX_CLAUDE_WORDS:
        fired.add('claude-size'); failures.append(f'CLAUDE.md: {words} words (max {MAX_CLAUDE_WORDS}). '
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
            fired.add('adr-currency'); failures.append(f'docs/adr/{fn}: status says superseded but links to no successor. '
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
                        fired.add('adr-currency'); failures.append(f'{path}:{n}: cites superseded ADR {fn} without saying so. '
                                        f'Point at its successor, or say "superseded" on this line.')


ALL_RULES = {
    'long-block':   'judgement — comment-block ceilings',
    'claude-size':  'judgement — CLAUDE.md word budget',
    'pointer':      'correctness — links and cited doc paths resolve',
    'adr-currency': 'correctness — superseded ADRs are marked and linked forward',
}


def measure():
    """Live numbers for the same things the standard's Baseline records, so a review compares
    like with like instead of re-deriving them by hand."""
    # Two populations, because the Baseline uses two and mixing them silently understates
    # compliance: the comment share is src/main, the block distribution is all of src.
    comment = total = 0
    sizes = []
    for path in walk('.scala'):
        if not path.startswith(os.path.join('.', 'src')):
            continue
        in_main = path.startswith(os.path.join('.', 'src', 'main'))
        lines = open(path).read().split('\n')
        if in_main:
            total += len(lines)
        run = 0
        for l in lines:
            st = l.strip()
            if in_main and st.startswith(('//', '/*', '*')):
                comment += 1
            if st.startswith('//'):
                run += 1
            else:
                if run:
                    sizes.append(run)
                run = 0
        if run:
            sizes.append(run)
    sizes.sort()

    def within(n):
        return 100 * sum(1 for x in sizes if x <= n) // len(sizes) if sizes else 100

    return {
        'comment share of src/main': f'{100 * comment // total}%  (Baseline: 16%; 1% in March 2026)',
        'src // blocks within the review budget of 3': f'{within(3)}%  (Baseline: 88%)',
        f'src // blocks within the gate ceiling of {MAX_LINE_BLOCK}': f'{within(MAX_LINE_BLOCK)}%  (Baseline: 99%)',
        'CLAUDE.md words': f'{len(open("CLAUDE.md").read().split())}  (budget: {MAX_CLAUDE_WORDS})',
    }


def report():
    print('Documentation standard — review agenda\n')
    print('Measurements (compare against the Baseline section of the standard):')
    for k, v in measure().items():
        print(f'  {k}: {v}')

    print(f'\nWaivers ({len(waivers)}):')
    if not waivers:
        print('  none. A judgement rule with no waivers is either well-calibrated or unexercised —')
        print('  the difference shows in whether it ever fires at all, below.')
    for rule, path, line, reason in sorted(waivers):
        print(f'  [{rule}] {path}:{line} — {reason}')
    by_rule = {}
    for rule, *_ in waivers:
        by_rule[rule] = by_rule.get(rule, 0) + 1
    for rule, n in sorted(by_rule.items()):
        if n >= 5:
            print(f'\n  {rule} waived {n}x. That is evidence the threshold is wrong, not that the')
            print('  code is. Change the number or delete the rule; do not add a sixth waiver.')

    quiet = sorted(set(ALL_RULES) - fired)
    print(f'\nRules that did not fire on this tree ({len(quiet)}):')
    for rule in quiet:
        print(f'  {rule} — {ALL_RULES[rule]}')
    if quiet:
        print('\n  One clean run proves nothing — a healthy tree is exactly when nothing fires. This')
        print('  list is evidence only across several reviews: a rule that has never fired and has')
        print('  never been waived over that span costs reading attention and buys nothing, so')
        print('  delete it. The default action of a review is deletion; the document earns growth.')


check_blocks()
check_pointers()
check_claude_size()
check_adr_currency()

if '--report' in sys.argv:
    report()
    sys.exit(0)

if failures:
    print('Documentation standard violations (docs/documentation-standard.md):\n')
    for f in failures:
        print(f'  {f}')
    sys.exit(1)
print('Documentation standard: OK')
