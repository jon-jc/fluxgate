#!/usr/bin/env python3
"""Run nightly fuzz targets and tell a finding apart from a run that failed.

Go defines a finding as a file. Every failing input the fuzzer can reproduce is
written to <package>/testdata/fuzz/<Target>/, where plain `go test` replays it
deterministically. A non-zero exit that leaves no new file there is something
else: the coordinator reporting its own -fuzztime deadline as "context deadline
exceeded", a worker killed by the runner, or a package that no longer builds.

Treating the two alike once filed a "fuzzing found a failing input" issue for a
toolchain timeout, and attached as its reproducer a crasher that had been fixed
and committed weeks earlier, because the upload collected every corpus file
rather than new ones. So each target is classified by what appeared on disk, a
failure that leaves nothing behind is retried once, and only new reproducers
are collected -- and inlined into the issue, which outlives the artifact.
"""

import argparse
from collections import deque
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

PROGRESS = re.compile(r"execs: (\d+) .*new interesting: (\d+) \(total: (\d+)\)")
MAX_INLINE_BYTES = 4096
LOG_TAIL_LINES = 25
RETAINED_LINES = 500


def corpus(directory):
    """Files under a target's corpus directory, relative to that directory."""
    if not directory.is_dir():
        return set()
    return {p.relative_to(directory) for p in directory.rglob("*") if p.is_file()}


def go_test(target, package, budget, root):
    """Run one fuzz target from root, echoing output; return (exit code, lines)."""
    command = ["go", "test", "-run", "^$", "-fuzz", f"^{target}$", "-fuzztime", budget, package]
    lines = deque(maxlen=RETAINED_LINES)
    with subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, errors="replace") as process:
        for line in process.stdout:
            sys.stdout.write(line)
            lines.append(line.rstrip("\n"))
    return process.returncode, list(lines)


def progress(lines):
    """Executions, new coverage and corpus size from the last progress line."""
    for line in reversed(lines):
        match = PROGRESS.search(line)
        if match:
            execs, interesting, total = map(int, match.groups())
            return dict(execs=execs, interesting=interesting, corpus=total)
    return dict(execs=None, interesting=None, corpus=None)


def fuzz(target, package, budget, root, run=go_test, attempts=2):
    """Fuzz one target and classify the outcome by what it left on disk."""
    directory = root / package / "testdata" / "fuzz" / target
    before = corpus(directory)
    codes = []
    for _ in range(attempts):
        code, lines = run(target, package, budget, root)
        codes.append(code)
        new = sorted(corpus(directory) - before)
        if new:
            outcome = "finding"
        elif code == 0:
            outcome = "pass"
        else:
            continue
        break
    else:
        outcome, new = "broken", []
    files = [(directory / name) for name in new]
    return dict(target=target, package=package, outcome=outcome, exit_codes=codes,
                reproducers=[f.relative_to(root).as_posix() for f in files],
                log_tail=lines[-LOG_TAIL_LINES:] if outcome == "broken" else [],
                **progress(lines))


def describe(result):
    if result["outcome"] == "pass":
        return "passed" if len(result["exit_codes"]) == 1 else "passed on retry"
    return {"finding": "**finding**", "broken": "**could not run**"}[result["outcome"]]


def summary(results, budget):
    rows = [f"### Nightly fuzzing ({budget} per target)", "",
            "| Target | Outcome | Executions | New coverage | Attempts |",
            "| --- | --- | ---: | ---: | ---: |"]
    for r in results:
        execs = "n/a" if r["execs"] is None else f"{r['execs']:,}"
        found = "n/a" if r["interesting"] is None else f"{r['interesting']} (corpus {r['corpus']})"
        rows.append(f"| `{r['target']}` | {describe(r)} | {execs} | {found} | {len(r['exit_codes'])} |")
    return "\n".join(rows) + "\n"


def fenced(text):
    # Go quotes corpus values with strconv, so backticks can appear verbatim;
    # tildes cannot close a fence opened with four of them.
    return "~~~~\n" + text.rstrip("\n") + "\n~~~~"


def inline(root, path):
    data = (root / path).read_bytes()
    text = data[:MAX_INLINE_BYTES].decode("utf-8", errors="replace")
    if len(data) > MAX_INLINE_BYTES:
        text += f"\n... truncated at {MAX_INLINE_BYTES} of {len(data)} bytes; see the fuzz-reproducers artifact"
    return fenced(text)


def issue(results, root, run_url):
    """Return (label, title, body) for a night that needs attention, else None."""
    findings = [r for r in results if r["outcome"] == "finding"]
    broken = [r for r in results if r["outcome"] == "broken"]
    if not findings and not broken:
        return None
    names = lambda rs: ", ".join(r["target"] for r in rs)
    if findings:
        label, title = "fuzz", f"Fuzzing found a failing input in {names(findings)}"
    else:
        label, title = "fuzz-infra", f"Nightly fuzzing could not complete {names(broken)}"
    parts = []
    for r in findings:
        parts.append(f"## `{r['target']}` (`{r['package']}`)\n\n"
                     "Go wrote the reproducer below. Commit it at the same path with the fix "
                     "and it runs on every pull request as an ordinary test:\n\n"
                     f"    go test -run '{r['target']}/<file>' {r['package']}")
        for path in r["reproducers"]:
            parts.append(f"#### `{path}`\n\n{inline(root, path)}")
    for r in broken:
        codes = ", ".join(map(str, r["exit_codes"]))
        parts.append(f"## `{r['target']}` could not run (`{r['package']}`)\n\n"
                     f"It failed {len(r['exit_codes'])} times (exit codes {codes}) without writing a "
                     "reproducer, so this is not a finding: there is no input that fails. Usual causes "
                     "are the fuzzing coordinator reporting its own deadline, a worker killed by the "
                     "runner, or a package that no longer builds. The end of the last attempt:\n\n"
                     + fenced("\n".join(r["log_tail"]) or "(no output)"))
    if run_url:
        parts.append(f"Run: {run_url}")
    return label, title, "\n\n".join(parts) + "\n"


def write_outputs(results, root, out, budget, run_url):
    out.mkdir(parents=True, exist_ok=True)
    for r in results:
        for path in r["reproducers"]:
            target = out / "crashers" / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / path, target)
    (out / "report.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    table = summary(results, budget)
    (out / "summary.md").write_text(table, encoding="utf-8")
    report = issue(results, root, run_url)
    if report:
        for name, value in zip(("issue-label", "issue-title", "issue.md"), report):
            (out / name).write_text(value, encoding="utf-8")
    for variable, text in (("GITHUB_STEP_SUMMARY", table),
                           ("GITHUB_OUTPUT", f"issue={'true' if report else 'false'}\n")):
        if os.environ.get(variable):
            with open(os.environ[variable], "a", encoding="utf-8") as handle:
                handle.write(text)
    return report


def main(argv=None, run=go_test):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("fuzz-out"),
                        help="directory for report.json, summary.md, crashers/ and the issue")
    parser.add_argument("--root", type=Path, default=Path("."), help="module root")
    parser.add_argument("--run-url", default=os.environ.get("RUN_URL", ""))
    parser.add_argument("budget", help="per-target -fuzztime, for example 5m")
    parser.add_argument("targets", nargs="+", metavar="Target:package")
    args = parser.parse_args(argv)

    specs = []
    for spec in args.targets:
        target, separator, package = spec.partition(":")
        if not (separator and target.startswith("Fuzz") and package):
            parser.error(f"expected Target:package, got {spec!r}")
        specs.append((target, package))

    grouped = bool(os.environ.get("GITHUB_ACTIONS"))
    results = []
    for target, package in specs:
        # Every target runs even after one fails, so one night reports every
        # broken boundary rather than the first one in the list.
        if grouped:
            print(f"::group::{target} ({args.budget})", flush=True)
        result = fuzz(target, package, args.budget, args.root, run=run)
        if grouped:
            print("::endgroup::", flush=True)
            if result["outcome"] != "pass":
                print(f"::error title={target}::{describe(result).strip('*')}", flush=True)
        results.append(result)

    report = write_outputs(results, args.root, args.out, args.budget, args.run_url)
    print(summary(results, args.budget))
    return 1 if report else 0


if __name__ == "__main__":
    sys.exit(main())
