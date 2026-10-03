"""Every failure inside a workflow job must turn the job red or be named.

Three patterns let a failure pass unseen (audit of 2026-09-29):

1. `python -c "...; from X import main; main()"` -- if `main()` ever returns a
   non-zero int instead of raising, the step is green. `sys.exit(main())` is
   free when `main()` returns None and correct when it returns an int, so every
   inline entrypoint call in a workflow must be wrapped in it.
2. `continue-on-error: true` -- the step's failure is invisible unless a later
   step reads `steps.<id>.outcome` (or dumps `toJSON(steps)`). A fail-soft step
   must therefore have an `id` that a later step in the same job references.
3. No `timeout-minutes` -- a hung API call holds the job for GitHub's 6-hour
   default, blocks its concurrency group, and /health reads "Running now" all
   day. Every job declares a timeout.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
INLINE_MAIN = re.compile(r"from\s+[\w.]+\s+import\s+main\s*;\s*(?P<call>(?:sys\.exit\()?main\(\)\)?)")

# Workflows the 2026-09-29 audit was told to leave to another owner. Each entry
# is checked both ways: the file must still carry the named gap (a stale entry
# fails CI, the same rule as EXPECTED_SKIPS), and no other file may.
KNOWN_GAPS: dict[str, dict[str, str]] = {
    "refresh_tennis.yml": {
        "fail_soft": "tennis owned separately as of 2026-09-29",
        "timeout": "tennis owned separately as of 2026-09-29",
    },
    "refresh_tennis_settlement.yml": {
        "fail_soft": "tennis owned separately as of 2026-09-29",
        "timeout": "tennis owned separately as of 2026-09-29",
    },
    "load_mlb_slate.yml": {"timeout": "MLB DFS owned separately as of 2026-09-29"},
    "refresh_mlb_stats.yml": {"timeout": "MLB DFS entrypoint (ingest.mlb_stats) owned separately as of 2026-09-29"},
}


def _partition(found: list[str], gap: str) -> tuple[list[str], list[str]]:
    """Split findings into (unexpected, known); a known gap with no finding is stale."""
    known_files = {name for name, gaps in KNOWN_GAPS.items() if gap in gaps}
    unexpected = [item for item in found if item.split(" :: ")[0] not in known_files]
    seen = {item.split(" :: ")[0] for item in found}
    stale = sorted(known_files - seen)
    return unexpected, stale


def _jobs(root: Path):
    for wf in sorted(root.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8")) or {}
        for job_name, job in (doc.get("jobs") or {}).items():
            yield wf.name, job_name, job


def _step_label(wf: str, job_name: str, step: dict, index: int) -> str:
    return f"{wf} :: {job_name} :: {step.get('name') or step.get('id') or f'step {index}'}"


def inline_main_calls_without_exit(root: Path = WORKFLOWS) -> list[str]:
    bad = []
    for wf, job_name, job in _jobs(root):
        for index, step in enumerate(job.get("steps") or []):
            run = step.get("run") or ""
            for match in INLINE_MAIN.finditer(run):
                if not match.group("call").startswith("sys.exit("):
                    bad.append(_step_label(wf, job_name, step, index))
    return bad


def _text(value) -> str:
    if isinstance(value, dict):
        return " ".join(_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_text(v) for v in value)
    return str(value) if value is not None else ""


def unreported_fail_soft_steps(root: Path = WORKFLOWS) -> list[str]:
    bad = []
    for wf, job_name, job in _jobs(root):
        steps = job.get("steps") or []
        for index, step in enumerate(steps):
            if step.get("continue-on-error") is not True:
                continue
            label = _step_label(wf, job_name, step, index)
            step_id = step.get("id")
            if not step_id:
                bad.append(f"{label} (no id)")
                continue
            later = " ".join(_text({k: v for k, v in s.items() if k != "id"}) for s in steps[index + 1:])
            if f"steps.{step_id}.outcome" in later or f"steps.{step_id}.conclusion" in later \
                    or "toJSON(steps)" in later:
                continue
            bad.append(f"{label} (no later step reads steps.{step_id}.outcome)")
    return bad


def jobs_without_timeout(root: Path = WORKFLOWS) -> list[str]:
    return [f"{wf} :: {job_name}" for wf, job_name, job in _jobs(root)
            if not isinstance(job.get("timeout-minutes"), int)]


def test_every_inline_entrypoint_call_is_wrapped_in_sys_exit() -> None:
    assert inline_main_calls_without_exit() == [], (
        "wrap the inline entrypoint as `sys.exit(main())` so a non-zero return "
        "turns the step red")


def test_every_fail_soft_step_is_named_by_a_later_step() -> None:
    unexpected, stale = _partition(unreported_fail_soft_steps(), "fail_soft")
    assert unexpected == [], (
        "give the continue-on-error step an id and have a later step in the same "
        "job read steps.<id>.outcome (or dump toJSON(steps)) into the summary")
    assert stale == [], "KNOWN_GAPS lists a fail_soft gap that no longer exists; remove the entry"


def test_every_job_declares_a_timeout() -> None:
    unexpected, stale = _partition(jobs_without_timeout(), "timeout")
    assert unexpected == [], (
        "add timeout-minutes so a hung API call cannot hold the job (and its "
        "concurrency group) for GitHub's 6-hour default")
    assert stale == [], "KNOWN_GAPS lists a timeout gap that no longer exists; remove the entry"


def test_the_checks_catch_the_bugs_and_allow_the_fixes(tmp_path: Path) -> None:
    (tmp_path / "bad.yml").write_text(
        "jobs:\n  j:\n    steps:\n"
        "      - name: bare\n        run: python -c \"from ingest.x import main; main()\"\n"
        "      - name: hidden\n        continue-on-error: true\n        run: python -m x\n"
        "      - name: unread\n        id: unread\n        continue-on-error: true\n        run: python -m y\n",
        encoding="utf-8")
    (tmp_path / "ok.yml").write_text(
        "jobs:\n  j:\n    timeout-minutes: 10\n    steps:\n"
        "      - name: wrapped\n        run: python -c \"import sys; from ingest.x import main; sys.exit(main())\"\n"
        "      - name: optional\n        id: opt\n        continue-on-error: true\n        run: python -m x\n"
        "      - name: report\n        if: always()\n        env:\n          OUTCOME: ${{ steps.opt.outcome }}\n"
        "        run: echo \"$OUTCOME\"\n"
        "      - name: dumped\n        id: dumped\n        continue-on-error: true\n        run: python -m y\n"
        "      - name: table\n        if: always()\n        env:\n          STEP_RESULTS: ${{ toJSON(steps) }}\n"
        "        run: echo ok\n",
        encoding="utf-8")
    assert inline_main_calls_without_exit(tmp_path) == ["bad.yml :: j :: bare"]
    assert unreported_fail_soft_steps(tmp_path) == [
        "bad.yml :: j :: hidden (no id)",
        "bad.yml :: j :: unread (no later step reads steps.unread.outcome)",
    ]
    assert jobs_without_timeout(tmp_path) == ["bad.yml :: j"]
