"""No workflow step may pipe a command without pipefail.

GitHub's default `run` shell is `bash -e {0}`: in `cmd | tee log` the step's
exit code is tee's, so a failing `cmd` turns the step green. Only an explicit
`shell: bash` runs `bash --noprofile --norc -eo pipefail {0}`. This pattern hid
failures in three workflows (availability monitor/freeze, pipeline health,
daily failure sweep) before it was caught on 2026-09-29.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
PIPE = re.compile(r"(?<![|])\|(?![|])")


def _unquoted(script: str) -> str:
    # A '|' inside a quoted python -c "..." or a regex is not a shell pipe.
    script = re.sub(r"'[^'\n]*'", "''", script)
    return re.sub(r'"[^"\n]*"', '""', script)


def piped_steps_without_pipefail(root: Path = WORKFLOWS) -> list[str]:
    bad = []
    for wf in sorted(root.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8")) or {}
        wf_shell = ((doc.get("defaults") or {}).get("run") or {}).get("shell")
        for job_name, job in (doc.get("jobs") or {}).items():
            job_shell = ((job.get("defaults") or {}).get("run") or {}).get("shell") or wf_shell
            for index, step in enumerate(job.get("steps") or []):
                run = step.get("run")
                if not run or (step.get("shell") or job_shell) == "bash" or "pipefail" in run:
                    continue
                lines = [line for line in _unquoted(run).splitlines()
                         if line.strip() and not line.strip().startswith("#")]
                if any(PIPE.search(line) for line in lines):
                    bad.append(f"{wf.name} :: {job_name} :: {step.get('name') or f'step {index}'}")
    return bad


def test_every_piped_step_uses_pipefail() -> None:
    assert piped_steps_without_pipefail() == [], (
        "these steps pipe a command without pipefail; add `shell: bash` so a failing "
        "command fails the step")


def test_the_check_catches_the_bug_and_allows_the_fixes(tmp_path: Path) -> None:
    (tmp_path / "bad.yml").write_text(
        "jobs:\n  j:\n    steps:\n      - name: swallowed\n        run: python -m x | tee log\n", encoding="utf-8")
    (tmp_path / "ok.yml").write_text(
        "jobs:\n  j:\n    steps:\n"
        "      - name: explicit bash\n        shell: bash\n        run: python -m x | tee log\n"
        "      - name: set pipefail\n        run: |\n          set -o pipefail\n          python -m x | tee log\n"
        "      - name: or is not a pipe\n        run: python -m x || echo failed\n"
        "      - name: quoted pipe\n        run: python -c \"print('a|b')\"\n", encoding="utf-8")
    (tmp_path / "defaults.yml").write_text(
        "defaults:\n  run:\n    shell: bash\njobs:\n  j:\n    steps:\n      - run: a | b\n", encoding="utf-8")
    assert piped_steps_without_pipefail(tmp_path) == ["bad.yml :: j :: swallowed"]
