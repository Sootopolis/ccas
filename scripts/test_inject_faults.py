#!/usr/bin/env python3
"""Tests for inject-faults.py.

Each test writes a tree to a temp directory and runs the script there with a stub in place of sbt. The stub prints
output trimmed from real sbt runs (2026-11-17), chosen by a marker in the source as the fault leaves it, and logs the
arguments and source it saw, so a test can check what ran against what.
"""
import json, os, subprocess, sys, tempfile, textwrap, unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'inject-faults.py')
SOURCE = 'src/A.scala'
ORIGINAL = 'object A {\n  val x = 1\n}\n'

STUB = textwrap.dedent('''\
    import json, sys
    source = open('src/A.scala').read()
    with open('sbt.log', 'a') as log:
        log.write(json.dumps({'args': sys.argv[1:], 'source': source}) + '\\n')
    if 'BROKEN_BUILD' in source:
        print('[error] -- [E008] Not Found Error: src/A.scala:2:13 ')
        print('[error] one error found')
        print('[error] (Compile / compileIncremental) Compilation failed')
        sys.exit(1)
    if 'TEST_FAILS' in source or 'ALWAYS_FAILS' in source:
        print('+ A')
        print('  - a test')
        print('\\x1b[31m0 tests passed. 1 tests failed. 0 tests ignored.\\x1b[0m')
        print('[error] (Test / testOnly) sbt.TestsFailedException: Tests unsuccessful')
        sys.exit(1)
    if sys.argv[-1].endswith('-t "nothing"'):
        print('No tests were executed')
        print('[success] Total time: 5 s')
        sys.exit(0)
    print('+ A')
    print('  + a test')
    print('\\x1b[32m1 tests passed. 0 tests failed. 0 tests ignored.\\x1b[0m')
    print('[success] Total time: 4 s')
''')


def fault(name, new, old='val x = 1', test='a test'):
    return {'name': name, 'file': SOURCE, 'old': old, 'new': new, 'suite': 'ccas.TestA', 'test': test}


class InjectFaults(unittest.TestCase):

    def run_with(self, faults, source=ORIGINAL):
        """Run the script on `faults` in a fresh tree holding `source`; the exit code, output and stub log."""
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = root.name
        os.makedirs(os.path.join(root.name, 'src'))
        for path, text in [(SOURCE, source), ('stub.py', STUB), ('faults.json', json.dumps(faults))]:
            with open(os.path.join(root.name, path), 'w') as f:
                f.write(text)
        env = dict(os.environ, INJECT_FAULTS_SBT=f'"{sys.executable}" stub.py')
        r = subprocess.run([sys.executable, SCRIPT, 'faults.json'], cwd=root.name, env=env,
                           capture_output=True, text=True)
        log_path = os.path.join(root.name, 'sbt.log')
        log = []
        if os.path.exists(log_path):
            with open(log_path) as f:
                log = [json.loads(l) for l in f]
        return r.returncode, r.stdout + r.stderr, log

    def source(self):
        with open(os.path.join(self.root, SOURCE)) as f:
            return f.read()

    def test_a_fault_the_test_catches_is_reported_caught_and_exits_0(self):
        code, out, _ = self.run_with([fault('x broken', 'val x = 1 // TEST_FAILS')])
        self.assertEqual(code, 0, out)
        self.assertIn('[x broken] caught: 1 failed, 0 passed', out)
        self.assertIn('1 of 1 faults caught.', out)

    def test_a_fault_the_test_misses_is_reported_missed_and_exits_1(self):
        code, out, _ = self.run_with([fault('x changed', 'val x = 2')])
        self.assertEqual(code, 1, out)
        self.assertIn('[x changed] missed: 1 passed', out)

    def test_a_fault_that_does_not_compile_is_not_reported_caught(self):
        code, out, _ = self.run_with([fault('x unbuildable', 'val x = BROKEN_BUILD')])
        self.assertEqual(code, 1, out)
        self.assertIn('[x unbuildable] did not compile: [E008] Not Found Error: src/A.scala:2:13', out)

    def test_a_test_filter_matching_nothing_is_reported_and_its_faults_skipped(self):
        code, out, log = self.run_with([fault('x changed', 'val x = 2', test='nothing')])
        self.assertEqual(code, 1, out)
        self.assertIn('[unbroken ccas.TestA -t "nothing"] no test ran: the test filter matched nothing', out)
        self.assertIn('[x changed] skipped: its test does not pass unbroken', out)
        self.assertEqual(len(log), 1, 'only the unbroken run')

    def test_a_test_failing_unbroken_skips_its_faults_rather_than_reporting_them_caught(self):
        code, out, log = self.run_with([fault('x broken', 'val x = 2')], source=ORIGINAL + '// ALWAYS_FAILS\n')
        self.assertEqual(code, 1, out)
        self.assertNotIn('caught:', out)
        self.assertIn('[unbroken ccas.TestA -t "a test"] fails: 1 failed, 0 passed', out)
        self.assertIn('[x broken] skipped', out)
        self.assertEqual(len(log), 1, 'only the unbroken run')

    def test_each_test_runs_unbroken_once_then_once_per_fault_with_only_that_fault_applied(self):
        faults = [fault('first', 'val x = 1 // TEST_FAILS'), fault('second', 'val x = 1 // TEST_FAILS second')]
        code, out, log = self.run_with(faults)
        self.assertEqual(code, 0, out)
        self.assertEqual([entry['source'] for entry in log],
                         [ORIGINAL,
                          ORIGINAL.replace('val x = 1', 'val x = 1 // TEST_FAILS'),
                          ORIGINAL.replace('val x = 1', 'val x = 1 // TEST_FAILS second')])

    def test_sbt_runs_the_named_suite_and_test_with_werror_off(self):
        _, _, log = self.run_with([fault('x broken', 'val x = 1 // TEST_FAILS')])
        self.assertEqual(log[-1]['args'], ['--server', '-batch',
                                           'set Compile/scalacOptions ~= (_.filterNot(_ == "-Werror"))',
                                           'testOnly ccas.TestA -- -t "a test"'])

    def test_the_file_is_restored_after_every_fault(self):
        faults = [fault('caught', 'val x = 1 // TEST_FAILS'), fault('missed', 'val x = 2'),
                  fault('unbuildable', 'val x = BROKEN_BUILD')]
        self.run_with(faults)
        self.assertEqual(self.source(), ORIGINAL)

    def test_the_file_is_restored_when_sbt_ends_without_a_verdict(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        os.makedirs(os.path.join(root.name, 'src'))
        with open(os.path.join(root.name, SOURCE), 'w') as f:
            f.write(ORIGINAL)
        with open(os.path.join(root.name, 'faults.json'), 'w') as f:
            json.dump([fault('x broken', 'val x = 2')], f)
        # The unbroken run would fail first, so make it pass: the stub refuses only once the fault is in.
        stub = 'import sys\nif "val x = 2" in open("src/A.scala").read(): sys.exit(127)\nprint("1 tests passed. 0 tests failed.")\n'
        with open(os.path.join(root.name, 'stub.py'), 'w') as f:
            f.write(stub)
        env = dict(os.environ, INJECT_FAULTS_SBT=f'"{sys.executable}" stub.py')
        r = subprocess.run([sys.executable, SCRIPT, 'faults.json'], cwd=root.name, env=env,
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn('[x broken] unclear: exit 127', r.stdout)
        with open(os.path.join(root.name, SOURCE)) as f:
            self.assertEqual(f.read(), ORIGINAL)

    def test_old_text_not_found_exactly_once_stops_the_run_before_any_file_changes(self):
        faults = [fault('fine', 'val x = 2'), fault('absent', 'val y = 2', old='val y = 1'),
                  fault('twice', 'q', old='\n')]
        code, out, log = self.run_with(faults)
        self.assertEqual(code, 2, out)
        self.assertIn('fault 2 (absent): old text found 0 times in src/A.scala, not once', out)
        self.assertIn('fault 3 (twice): old text found 3 times in src/A.scala, not once', out)
        self.assertEqual(log, [])
        self.assertEqual(self.source(), ORIGINAL)

    def test_a_fault_missing_a_field_or_changing_nothing_stops_the_run(self):
        incomplete = fault('no suite', 'val x = 2')
        del incomplete['suite']
        code, out, log = self.run_with([incomplete, fault('no-op', 'val x = 1')])
        self.assertEqual(code, 2, out)
        self.assertIn('fault 1 (no suite): missing suite', out)
        self.assertIn('fault 2 (no-op): old and new are the same, so nothing would break', out)
        self.assertEqual(log, [])

    def test_a_test_name_with_a_double_quote_stops_the_run(self):
        code, out, log = self.run_with([fault('quoted', 'val x = 2', test='says "hi"')])
        self.assertEqual(code, 2, out)
        self.assertIn('test contains a double quote', out)
        self.assertEqual(log, [])


if __name__ == '__main__':
    unittest.main()
