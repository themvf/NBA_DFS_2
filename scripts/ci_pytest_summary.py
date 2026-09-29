"""Summarize a pytest JUnit XML report for CI, and refuse unexpected skips.

Used by .github/workflows/tests.yml after pytest runs. It does two things:

1. Writes a pass/fail/skip table to $GITHUB_STEP_SUMMARY (or stdout locally).
2. Exits non-zero when a test SKIPPED that is not in EXPECTED_SKIPS below.

The second part matters because a skip looks green. `pytest.importorskip` on a
module inside this repo turns an import error into a skip, and a test that
quietly stops running is the failure this CI exists to catch (see
docs/nfl-dfs-reliability-program.md, item C4). A new skip therefore has to be
added here, with its reason, before CI accepts it. A known failure should be
marked `@pytest.mark.xfail(reason=...)` in the test itself; those are listed in
the summary but do not fail CI (an unexpected pass still counts as a pass).

Pytest's own exit code still decides failures; this script only adds the skip
check, so run it with `if: always()` and let both steps report.

Usage:
    python scripts/ci_pytest_summary.py pytest-results.xml
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET

# Tests allowed to skip in CI, keyed by "<file>::<test name>", with the reason.
# Both are opt-in back-tests that need network data (and, for score comps, the
# production database); CI deliberately does not set their opt-in variables.
EXPECTED_SKIPS = {
    "tests/test_nfl_archetype_epa_audit.py::test_no_one_directional_disagreement_cluster":
        "Opt-in EPA audit: needs an nflverse PBP season (NFL_PBP_CACHE or "
        "NFL_PBP_ALLOW_DOWNLOAD=1). CI has no cache and does not download.",
    "tests/test_nfl_score_comps.py::test_backtest_calibrated_and_accurate":
        "Opt-in back-test (NFL_SCORE_COMPS_RUN=1): reads the production "
        "database and downloads nflverse seasons.",
}


def _node_id(case: ET.Element) -> str:
    classname = case.get("classname", "")
    name = case.get("name", "")
    # JUnit classnames look like "tests.test_file" or "tests.test_file.TestClass".
    parts = classname.split(".")
    module_parts, class_parts = parts, []
    for i, part in enumerate(parts):
        if part.startswith("test_"):
            module_parts, class_parts = parts[: i + 1], parts[i + 1:]
            break
    path = "/".join(module_parts) + ".py"
    return "::".join([path, *class_parts, name])


def main(xml_path: str) -> int:
    root = ET.parse(xml_path).getroot()
    cases = root.iter("testcase")
    passed, failed, skipped, known = [], [], [], []
    for case in cases:
        node = _node_id(case)
        if case.find("failure") is not None or case.find("error") is not None:
            element = case.find("failure")
            if element is None:
                element = case.find("error")
            first_line = ((element.get("message") or "").splitlines() or [""])[0]
            failed.append((node, first_line[:200]))
        elif case.find("skipped") is not None:
            element = case.find("skipped")
            reason = element.get("message") or ""
            # An xfail marker carries its reason in the test file itself, so it
            # is already an explicit, documented known failure. List it, don't
            # treat it as a silent skip.
            if element.get("type") == "pytest.xfail":
                known.append((node, reason))
            else:
                skipped.append((node, reason))
        else:
            passed.append(node)

    unexpected = [(node, reason) for node, reason in skipped
                  if node.split("[")[0] not in EXPECTED_SKIPS]
    stale = sorted(set(EXPECTED_SKIPS) - {node.split("[")[0] for node, _ in skipped})

    lines = []
    status = "all passed" if not failed and not unexpected else "problems found"
    lines.append(f"## Python tests: {status}")
    lines.append("")
    lines.append(f"{len(passed)} passed, {len(failed)} failed, {len(skipped)} skipped, "
                 f"{len(known)} known failures (xfail).")
    if known:
        lines += ["", "### Known failures (xfail, reason recorded in the test)", ""]
        lines += [f"- `{node}`: {reason}" for node, reason in known]
    if failed:
        lines += ["", "### Failed", "", "| Test | Message |", "|---|---|"]
        lines += [f"| `{node}` | {msg.replace('|', '/')} |" for node, msg in failed]
    if unexpected:
        lines += ["", "### Unexpected skips (CI fails on these)", "",
                  "A skip here means the test stopped running. Fix it, or add it to "
                  "EXPECTED_SKIPS in scripts/ci_pytest_summary.py with the reason.", ""]
        lines += [f"- `{node}`: {reason}" for node, reason in unexpected]
    if skipped:
        lines += ["", "### Skipped", ""]
        for node, reason in skipped:
            why = EXPECTED_SKIPS.get(node.split("[")[0], "UNEXPECTED")
            lines.append(f"- `{node}`: {why} (pytest said: {reason})")
    if stale:
        lines += ["", "### Expected skips that ran (informational)", ""]
        lines += [f"- `{node}` is listed in EXPECTED_SKIPS but did not skip." for node in stale]

    text = "\n".join(lines) + "\n"
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(text)
    print(text)
    for node, reason in unexpected:
        print(f"::error title=Unexpected pytest skip::{node} skipped: {reason}")
    return 1 if unexpected else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
