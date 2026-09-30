"""The /health checklist reads every workflow's schedule from a checked-in
manifest. A workflow edited without regenerating it would give the checklist a
stale schedule, so the manifest must match the workflow files exactly."""
from pathlib import Path

from scripts.build_workflow_manifest import OUT, build, render, workflow_files


def test_workflow_manifest_is_current():
    assert OUT.exists(), "run: python scripts/build_workflow_manifest.py"
    current = OUT.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert current == render(), "workflow manifest is stale; run: python scripts/build_workflow_manifest.py"


def test_manifest_lists_every_workflow_with_its_triggers():
    manifest = build()["workflows"]
    workflows = Path(".github/workflows")
    # GitHub runs .yaml as well as .yml; a workflow the manifest cannot see is a
    # job the checklist never judges.
    files = sorted(p.name for p in [*workflows.glob("*.yml"), *workflows.glob("*.yaml")])
    assert [w["file"] for w in manifest] == files
    assert [p.name for p in workflow_files()] == files
    by_file = {w["file"]: w for w in manifest}
    # `on:` parses as True in YAML 1.1; triggers must still be read.
    assert by_file["refresh_nfl_dfs_projections.yml"]["crons"], "scheduled workflows carry their crons"
    assert by_file["refresh_nfl_dfs_projections.yml"]["dispatch"] is True
    assert by_file["refresh_nfl_dfs_research.yml"]["afterWorkflows"], "workflow_run triggers are kept"
