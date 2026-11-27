import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from fuzz_nightly import MAX_INLINE_BYTES, fuzz, issue, main, progress

DEADLINE = ["fuzz: elapsed: 5m0s, execs: 11336598 (0/sec), new interesting: 0 (total: 390)",
            "--- FAIL: FuzzParse (300.12s)", "    context deadline exceeded", "FAIL"]
CLEAN = ["fuzz: elapsed: 3s, execs: 100 (33/sec), new interesting: 1 (total: 9)",
         "fuzz: elapsed: 5s, execs: 2500 (800/sec), new interesting: 3 (total: 11)", "PASS"]


class FakeGo:
    """Scripted stand-in for `go test -fuzz`: each call plays the next step.

    A step is (exit code, files Go writes to the target's corpus, output).
    """

    def __init__(self, *steps):
        self.steps = list(steps)
        self.calls = 0

    def __call__(self, target, package, budget, root):
        self.calls += 1
        code, writes, lines = self.steps.pop(0)
        for name, content in writes:
            path = root / package / "testdata" / "fuzz" / target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            # Bytes, not text: Go writes LF on every platform.
            path.write_bytes(content.encode("utf-8"))
        return code, lines


class FuzzNightlyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def commit(self, package, target, name, content="go test fuzz v1\nstring(\"old\")\n"):
        path = self.root / package / "testdata" / "fuzz" / target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))

    def test_clean_run_passes_without_an_issue(self):
        result = fuzz("FuzzParse", "internal/query", "5m", self.root, run=FakeGo((0, [], CLEAN)))
        self.assertEqual(result["outcome"], "pass")
        self.assertEqual((result["execs"], result["interesting"], result["corpus"]), (2500, 3, 11))
        self.assertIsNone(issue([result], self.root, ""))

    def test_deadline_without_reproducer_is_retried_not_reported(self):
        go = FakeGo((1, [], DEADLINE), (0, [], CLEAN))
        result = fuzz("FuzzParse", "internal/query", "5m", self.root, run=go)
        self.assertEqual((result["outcome"], result["exit_codes"], go.calls), ("pass", [1, 0], 2))
        self.assertIsNone(issue([result], self.root, ""))

    def test_committed_reproducers_are_never_reported_as_new(self):
        # The regression that filed a stale crasher as a fresh finding.
        self.commit("internal/auth", "FuzzParseKeys", "41848af6075164d9")
        self.commit("internal/query", "FuzzParse", "seed")
        go = FakeGo((1, [], DEADLINE), (1, [], DEADLINE))
        result = fuzz("FuzzParse", "internal/query", "5m", self.root, run=go)
        self.assertEqual(result["outcome"], "broken")
        self.assertEqual(result["reproducers"], [])

    def test_repeated_failure_without_reproducer_is_infrastructure_not_a_finding(self):
        result = fuzz("FuzzParse", "internal/query", "5m", self.root,
                      run=FakeGo((1, [], DEADLINE), (1, [], DEADLINE)))
        label, title, body = issue([result], self.root, "https://example/run/1")
        self.assertEqual(label, "fuzz-infra")
        self.assertEqual(title, "Nightly fuzzing could not complete FuzzParse")
        self.assertIn("not a finding", body)
        self.assertIn("context deadline exceeded", body)
        self.assertIn("exit codes 1, 1", body)
        self.assertNotIn("Fuzzing found", body)

    def test_new_reproducer_is_a_finding_inlined_into_the_issue(self):
        crasher = 'go test fuzz v1\nstring("`z`")\n'
        go = FakeGo((1, [("9cf17e32bc6a97b9", crasher)], ["--- FAIL: FuzzParse"]))
        result = fuzz("FuzzParse", "internal/query", "5m", self.root, run=go)
        self.assertEqual((result["outcome"], go.calls), ("finding", 1))
        self.assertEqual(result["reproducers"], ["internal/query/testdata/fuzz/FuzzParse/9cf17e32bc6a97b9"])
        label, title, body = issue([result], self.root, "")
        self.assertEqual((label, title), ("fuzz", "Fuzzing found a failing input in FuzzParse"))
        self.assertIn("~~~~\n" + crasher.rstrip("\n") + "\n~~~~", body)
        self.assertIn("go test -run 'FuzzParse/<file>' internal/query", body)

    def test_finding_on_the_retry_is_still_a_finding(self):
        go = FakeGo((1, [], DEADLINE), (1, [("abc", "go test fuzz v1\nint(7)\n")], []))
        result = fuzz("FuzzWindowFor", "internal/aggregate", "5m", self.root, run=go)
        self.assertEqual((result["outcome"], result["exit_codes"]), ("finding", [1, 1]))

    def test_findings_outrank_broken_targets_in_one_issue(self):
        finding = fuzz("FuzzA", "p", "1m", self.root, run=FakeGo((1, [("x", "go test fuzz v1\n")], [])))
        broken = fuzz("FuzzB", "q", "1m", self.root, run=FakeGo((2, [], []), (2, [], [])))
        label, title, body = issue([finding, broken], self.root, "")
        self.assertEqual((label, title), ("fuzz", "Fuzzing found a failing input in FuzzA"))
        self.assertIn("`FuzzB` could not run", body)

    def test_large_reproducers_are_truncated_in_the_issue(self):
        go = FakeGo((1, [("big", "x" * (MAX_INLINE_BYTES * 3))], []))
        result = fuzz("FuzzA", "p", "1m", self.root, run=go)
        body = issue([result], self.root, "")[2]
        self.assertIn(f"truncated at {MAX_INLINE_BYTES} of {MAX_INLINE_BYTES * 3} bytes", body)
        self.assertLess(len(body), MAX_INLINE_BYTES * 2)

    def test_progress_comes_from_the_last_progress_line(self):
        self.assertEqual(progress(CLEAN), dict(execs=2500, interesting=3, corpus=11))
        self.assertEqual(progress(["build failed"]), dict(execs=None, interesting=None, corpus=None))

    def test_main_collects_only_new_reproducers_and_sets_the_exit_code(self):
        self.commit("internal/query", "FuzzParse", "committed")
        out = self.root / "out"
        go = FakeGo((0, [], CLEAN), (1, [("fresh", "go test fuzz v1\nstring(\"q\")\n")], []))
        env = {"GITHUB_ACTIONS": "", "GITHUB_STEP_SUMMARY": "", "GITHUB_OUTPUT": str(self.root / "gh-out")}
        with mock.patch.dict(os.environ, env), mock.patch("sys.stdout"):
            code = main(["--root", str(self.root), "--out", str(out), "1m",
                         "FuzzHashLabels:internal/aggregate", "FuzzParse:internal/query"], run=go)
        self.assertEqual(code, 1)
        crashers = sorted(p.relative_to(out / "crashers").as_posix()
                          for p in (out / "crashers").rglob("*") if p.is_file())
        self.assertEqual(crashers, ["internal/query/testdata/fuzz/FuzzParse/fresh"])
        report = json.loads((out / "report.json").read_text(encoding="utf-8"))
        self.assertEqual([r["outcome"] for r in report], ["pass", "finding"])
        self.assertEqual((out / "issue-label").read_text(encoding="utf-8"), "fuzz")
        self.assertEqual((self.root / "gh-out").read_text(encoding="utf-8"), "issue=true\n")

    def test_clean_night_exits_zero_and_files_nothing(self):
        out = self.root / "out"
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "", "GITHUB_OUTPUT": ""}), mock.patch("sys.stdout"):
            code = main(["--root", str(self.root), "--out", str(out), "1m", "FuzzA:p"],
                        run=FakeGo((0, [], CLEAN)))
        self.assertEqual(code, 0)
        self.assertFalse((out / "issue.md").exists())
        self.assertFalse((out / "crashers").exists())

    def test_malformed_target_specs_are_rejected(self):
        for spec in ["FuzzParse", "Parse:./internal/query", ":./internal/query", "FuzzParse:"]:
            with self.subTest(spec=spec), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                main(["1m", spec], run=FakeGo())


if __name__ == "__main__":
    unittest.main()
