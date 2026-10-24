#!/usr/bin/env python3
"""Tests for check-docs.py.

Each test writes a minimal tree to a temp directory and runs the script there, asserting the exit code and the
message -- the contract the hook and CI depend on. The message matters as much as the code: a waiver that fails to
take still exits 1, as an over-long block, so a test asserting only the code would pass with the waiver check gone.
"""
import os, subprocess, sys, tempfile, unittest

CHECKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'check-docs.py')
OK = 'Documentation standard: OK'
RULE_HEADER = '---\npaths:\n  - "src/**"\n---\n'
SCALA = 'src/A.scala'


def comments(n, extra=''):
    """A Scala object holding a run of `n` `//` lines, `extra` among them."""
    lines = [f'  // line {i}' for i in range(n - 1 if extra else n)] + ([f'  // {extra}'] if extra else [])
    return 'object A {\n' + '\n'.join(lines) + '\n  val x = 1\n}\n'


def scaladoc(prose, tags=0):
    """A scaladoc of `prose` lines, delimiters included, plus `tags` `@param` lines each with a continuation."""
    body = ['  * prose'] * (prose - 2) + ['  * @param p a parameter', '  *   continued'] * tags
    return '/**\n' + '\n'.join(body) + '\n  */\nobject A\n'


class CheckDocs(unittest.TestCase):

    def run_in(self, files, *args):
        """Write `files` ({path: text}, or None for an empty directory) into a fresh tree; run the checker there."""
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        for path, text in files.items():
            full = os.path.join(root.name, path)
            if text is None:
                os.makedirs(full, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, 'w', newline='') as f:   # newline='' writes `\r\n` as given
                f.write(text)
        r = subprocess.run([sys.executable, CHECKER, *args], cwd=root.name, capture_output=True, text=True)
        return r.returncode, r.stdout + r.stderr

    def assertPasses(self, files, *args):
        code, out = self.run_in(files, *args)
        self.assertEqual(code, 0, out)
        return out

    def assertFails(self, files, message):
        code, out = self.run_in(files)
        self.assertEqual(code, 1, out)
        self.assertIn(message, out)

    def test_a_tree_that_follows_every_rule_passes(self):
        out = self.assertPasses({
            'CLAUDE.md': 'Rules. See [the guide](docs/guide.md).\n',
            'docs/guide.md': 'See [the decision](adr/0001-live.md) and `docs/adr/0001-live.md`.\n',
            'docs/adr/0001-live.md': '# A decision\n\n**Status:** Accepted.\n',
            '.claude/rules/r.md': RULE_HEADER + '\nA rule.\n',
            SCALA: '// Why this line is strange. See docs/adr/0001-live.md.\nobject A\n',
        })
        self.assertIn(OK, out)

    # Comment-block length

    def test_a_run_of_nine_line_comments_fails(self):
        self.assertFails({SCALA: comments(9)}, 'line-comment block is 9 lines (max 8)')

    def test_a_run_of_eight_line_comments_passes(self):
        self.assertPasses({SCALA: comments(8)})

    def test_a_sixteen_line_scaladoc_fails(self):
        self.assertFails({SCALA: scaladoc(16)}, 'comment block is 16 lines (max 15)')

    def test_param_and_return_lines_do_not_count_towards_a_scaladoc(self):
        self.assertPasses({SCALA: scaladoc(15, tags=3)})

    # Waivers

    def test_a_waived_long_block_passes_and_the_report_lists_its_reason(self):
        waived = comments(9, 'docs-standard: allow long-block -- the cases belong together')
        out = self.assertPasses({SCALA: waived}, '--report')
        self.assertIn('[long-block] ./src/A.scala:2 — the cases belong together', out)

    def test_a_waiver_without_a_reason_fails(self):
        self.assertFails({SCALA: comments(9, 'docs-standard: allow long-block')},
                         'waiver not applied — unparseable marker')

    def test_a_waiver_naming_a_rule_that_is_not_waivable_fails(self):
        self.assertFails({SCALA: comments(9, 'docs-standard: allow pointer -- it is fine')},
                         'waiver not applied — unknown rule "pointer"')

    def test_the_report_flags_a_rule_waived_five_times(self):
        waived = comments(9, 'docs-standard: allow long-block -- one of five')
        out = self.assertPasses({f'src/A{i}.scala': waived for i in range(5)}, '--report')
        self.assertIn('long-block waived 5x', out)

    # Pointers

    def test_a_markdown_link_to_a_missing_file_fails(self):
        self.assertFails({'docs/a.md': 'See [b](missing.md).\n'}, 'unresolved link -> missing.md')

    def test_a_link_resolves_from_the_directory_of_the_file_holding_it(self):
        self.assertPasses({'docs/a.md': 'See [b](b.md).\n', 'docs/b.md': 'B.\n'})

    def test_a_link_inside_inline_code_is_not_checked(self):
        self.assertPasses({'docs/a.md': 'Write `[b](missing.md)` for a link.\n'})

    def test_a_docs_path_cited_from_scala_that_does_not_exist_fails(self):
        self.assertFails({SCALA: '// See docs/adr/0001-gone.md.\nobject A\n'},
                         'unresolved doc pointer -> docs/adr/0001-gone.md')

    # ADR currency

    SUPERSEDED = {
        'docs/adr/0001-old.md': '# Old\n\n**Status:** Superseded by [0002](0002-new.md).\n',
        'docs/adr/0002-new.md': '# New\n\n**Status:** Accepted. Supersedes [0001](0001-old.md).\n',
    }

    def test_a_superseded_adr_that_links_to_no_successor_fails(self):
        self.assertFails({'docs/adr/0001-old.md': '# Old\n\n**Status:** Superseded.\n'},
                         'status says superseded but links to no successor')

    def test_citing_a_superseded_adr_without_saying_so_fails(self):
        self.assertFails({**self.SUPERSEDED, SCALA: '// See docs/adr/0001-old.md.\nobject A\n'},
                         'cites superseded ADR 0001-old.md without saying so')

    def test_a_citation_that_says_the_adr_is_superseded_passes(self):
        self.assertPasses({**self.SUPERSEDED, SCALA: '// See docs/adr/0001-old.md (superseded by 0002).\nobject A\n'})

    # Instruction-file size

    def test_claude_md_over_its_word_budget_fails(self):
        self.assertFails({'CLAUDE.md': 'word ' * 2501}, 'CLAUDE.md: 2501 words (max 2500)')

    def test_a_rules_file_over_its_word_budget_fails(self):
        rule = RULE_HEADER + 'word ' * (751 - len(RULE_HEADER.split()))
        self.assertFails({'.claude/rules/r.md': rule, SCALA: 'object A\n'}, 'r.md: 751 words (max 750)')

    # Rule headers

    def rule(self, header, **files):
        return {'.claude/rules/r.md': header + '\nA rule.\n', SCALA: 'object A\n', **files}

    def test_a_rules_file_without_a_paths_header_fails(self):
        self.assertFails(self.rule(''), 'loads in every session: put it in CLAUDE.md')

    def test_a_paths_header_with_no_entries_fails(self):
        self.assertFails(self.rule('---\npaths:\n---\n'), 'the header must be a `paths:` list')

    def test_an_unquoted_pattern_fails(self):
        self.assertFails(self.rule('---\npaths:\n  - **/*.scala\n---\n'), 'the header must be a `paths:` list')

    def test_a_brace_pattern_fails_as_one_pattern_per_line(self):
        self.assertFails(self.rule('---\npaths:\n  - "src/*.{scala,md}"\n---\n'), 'write one pattern per line')

    def test_a_pattern_into_a_missing_directory_fails(self):
        self.assertFails(self.rule('---\npaths:\n  - "nope/**"\n---\n'), '"nope/**" matches no file')

    def test_every_pattern_must_match_a_file_not_just_one(self):
        self.assertFails(self.rule('---\npaths:\n  - "src/**"\n  - "nope/**"\n---\n'), '"nope/**" matches no file')

    def test_a_pattern_that_matches_only_directories_fails(self):
        self.assertFails(self.rule('---\npaths:\n  - "empty/**"\n---\n', **{'empty/sub': None}),
                         '"empty/**" matches no file')

    def test_a_pattern_alive_only_under_build_output_fails(self):
        files = self.rule('---\npaths:\n  - "**/OnlyHere.scala"\n---\n',
                          **{'target/gen/OnlyHere.scala': 'object OnlyHere\n'})
        self.assertFails(files, '"**/OnlyHere.scala" matches no file')

    def test_a_header_tolerates_crlf_trailing_spaces_comments_and_blank_lines(self):
        header = '---  \r\npaths:\r\n  # the sources\r\n\r\n  - "src/**"  # every one\r\n---\r\n'
        self.assertPasses(self.rule(header))


if __name__ == '__main__':
    unittest.main()
