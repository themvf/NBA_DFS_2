"""A failing CFB integrity pass must not stop /health from being recorded."""
from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "pipeline_health.yml"
FRESHNESS = "python -m model.pipeline_health"
INTEGRITY_PASSES = ("python -m ingest.cfb_movements --full", "python -m ingest.cfb_capture_audit --full")


def steps():
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return doc["jobs"]["pipeline-health"]["steps"]


def index_of(command):
    return next(i for i, step in enumerate(steps()) if command in (step.get("run") or ""))


def test_the_freshness_reading_runs_before_any_integrity_pass():
    freshness = index_of(FRESHNESS)
    assert all(index_of(command) > freshness for command in INTEGRITY_PASSES)


def test_each_integrity_pass_runs_even_after_an_earlier_step_fails():
    for command in INTEGRITY_PASSES:
        assert steps()[index_of(command)].get("if") == "${{ !cancelled() }}", command
