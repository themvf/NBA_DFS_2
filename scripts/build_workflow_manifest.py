"""Write web/src/data/workflow-manifest.json from .github/workflows/*.yml.

The /health checklist needs every workflow's triggers (its own cron schedule,
manual dispatch, workflow_run) to say when a job should next run and whether
it is overdue. The web app cannot read .github/ at runtime (Vercel's root is
web/), so the schedule is compiled into a checked-in manifest.
tests/test_workflow_manifest.py fails CI when a workflow changes and this was
not re-run:

    python scripts/build_workflow_manifest.py
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
OUT = ROOT / "web" / "src" / "data" / "workflow-manifest.json"


def _triggers(doc: dict) -> dict:
    # YAML 1.1 reads a bare `on:` key as the boolean True.
    on = doc.get("on", doc.get(True))
    if isinstance(on, str):
        on = {on: None}
    elif isinstance(on, list):
        on = {name: None for name in on}
    return on or {}


def build() -> dict:
    entries = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        on = _triggers(doc)
        crons = [item["cron"] for item in (on.get("schedule") or []) if isinstance(item, dict) and "cron" in item]
        run = on.get("workflow_run") or {}
        entries.append({
            "file": path.name,
            "name": str(doc.get("name") or path.stem),
            "crons": crons,
            "dispatch": "workflow_dispatch" in on,
            "afterWorkflows": list(run.get("workflows") or []) if isinstance(run, dict) else [],
            "push": "push" in on,
            "pullRequest": "pull_request" in on,
        })
    return {"generatedBy": "scripts/build_workflow_manifest.py", "workflows": entries}


def render() -> str:
    return json.dumps(build(), indent=2) + "\n"


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render(), encoding="utf-8", newline="\n")
    print(f"wrote {OUT.relative_to(ROOT)} ({len(build()['workflows'])} workflows)")
