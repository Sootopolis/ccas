#!/usr/bin/env python3
"""Show that each test fails with the code it guards broken, as /review-change asks.

    python3 scripts/inject-faults.py faults.json

faults.json is a list of faults, each one exact edit and the test that should catch it:

    [{"name": "range check dropped", "file": "src/main/scala/ccas/analysis/tables/RecruitmentCriteria.scala",
      "old": "d < 0.0 || d > 1.0", "new": "d < 0.0",
      "suite": "ccas.cli.TestRecruitmentCriteriaCommand", "test": "a value out of range is asked again"}]

Run it from the root of the tree under test; `file` is relative to it. Each named test is first run unbroken, since
one already failing would seem to catch every fault; a test that does not pass skips its faults. Then each fault is
applied alone, the test run
with `-Werror` off (a fault often leaves a value unused), and the file restored, even on Ctrl-C. Every `old` must
occur exactly once in its file, checked for all faults before any is applied. `test` is ZIO Test's `-t` filter,
which matches any test whose name contains it.

Each fault gets one verdict: caught, missed, did not compile (a fault the compiler refuses shows nothing about the
test), no test ran (the filter matched nothing), unclear, or skipped. Exits 0 only when every fault was caught, 2 when the
spec is unusable. INJECT_FAULTS_SBT replaces the `sbt` command, for the tests of this script.
"""
import json, os, re, shlex, signal, subprocess, sys

ANSI = re.compile(r'\x1b\[[0-9;?]*[A-Za-z]')
SUMMARY = re.compile(r'(\d+) tests passed\. (\d+) tests failed\.')
KEYS = ('name', 'file', 'old', 'new', 'suite', 'test')
NO_WERROR = 'set Compile/scalacOptions ~= (_.filterNot(_ == "-Werror"))'


def problems(faults):
    """Why the spec can't be run as given; empty when it can."""
    if not isinstance(faults, list) or not faults:
        return ['the spec must be a non-empty JSON list of faults']
    found = []
    for i, fault in enumerate(faults):
        label = f'fault {i + 1}'
        if not isinstance(fault, dict):
            found.append(f'{label}: not a JSON object')
            continue
        label = f"fault {i + 1} ({fault.get('name', 'unnamed')})"
        missing = [k for k in KEYS if not isinstance(fault.get(k), str) or not fault[k]]
        if missing:
            found.append(f"{label}: missing {', '.join(missing)}")
            continue
        if '"' in fault['test']:
            found.append(f'{label}: test contains a double quote, which the -t argument cannot carry')
            continue
        if fault['old'] == fault['new']:
            found.append(f'{label}: old and new are the same, so nothing would break')
            continue
        try:
            with open(fault['file']) as f:
                count = f.read().count(fault['old'])
        except OSError as e:
            found.append(f'{label}: cannot read {fault["file"]}: {e.strerror}')
            continue
        if count != 1:
            found.append(f"{label}: old text found {count} times in {fault['file']}, not once")
    return found


def sbt_command(suite, test):
    sbt = shlex.split(os.environ.get('INJECT_FAULTS_SBT', 'sbt'))
    # `--server` keeps sbt's thin client, which joins separate arguments into one command, out of the way.
    return sbt + ['--server', '-batch', NO_WERROR, f'testOnly {suite} -- -t "{test}"']


def verdict(returncode, output):
    """The verdict on one run, and the detail that backs it."""
    text = ANSI.sub('', output)
    if 'Compilation failed' in text:
        errors = [l for l in text.splitlines() if l.startswith('[error] -- ')]
        return 'did not compile', (errors[0][len('[error] -- '):].strip() if errors else 'compilation failed')
    summaries = SUMMARY.findall(text)
    passed = sum(int(p) for p, _ in summaries)
    failed = sum(int(f) for _, f in summaries)
    if failed:
        return 'caught', f'{failed} failed, {passed} passed'
    if 'No tests were executed' in text or (summaries and not passed):
        return 'no test ran', 'the test filter matched nothing'
    if returncode == 0 and passed:
        return 'missed', f'{passed} passed'
    tail = [l for l in text.splitlines() if l.strip()][-3:]
    return 'unclear', f'exit {returncode}: ' + ' | '.join(tail)


def test(target):
    try:
        r = subprocess.run(sbt_command(*target), capture_output=True, text=True)
    except OSError as e:
        return 'unclear', f'cannot run sbt: {e.strerror}'
    return verdict(r.returncode, r.stdout + r.stderr)


def run(fault):
    with open(fault['file']) as f:
        original = f.read()
    try:
        with open(fault['file'], 'w') as f:
            f.write(original.replace(fault['old'], fault['new']))
        return test((fault['suite'], fault['test']))
    finally:
        with open(fault['file'], 'w') as f:
            f.write(original)


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    try:
        with open(argv[1]) as f:
            faults = json.load(f)
    except (OSError, ValueError) as e:
        print(f'cannot read {argv[1]}: {e}', file=sys.stderr)
        return 2
    found = problems(faults)
    if found:
        print('Nothing was run:', *found, sep='\n  ', file=sys.stderr)
        return 2
    # As KeyboardInterrupt does, so `run`'s `finally` restores the file when the process is told to stop.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    unproven = set()
    for target in dict.fromkeys((f['suite'], f['test']) for f in faults):
        outcome, detail = test(target)
        if outcome != 'missed':
            unproven.add(target)
            unbroken = 'fails' if outcome == 'caught' else outcome   # nothing to catch: no fault is in yet
            print(f'[unbroken {target[0]} -t "{target[1]}"] {unbroken}: {detail}', flush=True)
    verdicts = []
    for fault in faults:
        if (fault['suite'], fault['test']) in unproven:
            print(f"[{fault['name']}] skipped: its test does not pass unbroken", flush=True)
            verdicts.append('skipped')
            continue
        outcome, detail = run(fault)
        verdicts.append(outcome)
        print(f"[{fault['name']}] {outcome}: {detail}", flush=True)
    caught = verdicts.count('caught')
    print(f'{caught} of {len(faults)} faults caught.')
    return 0 if caught == len(faults) else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
