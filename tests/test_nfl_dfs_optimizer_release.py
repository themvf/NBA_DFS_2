"""The calibrated opt-in release must name the study the shadow job is pinned to."""
import json
from pathlib import Path

import pytest

from ingest.nfl_dfs_optimizer_release import RELEASE_PATH, RELEASE_VERSION, SHADOW_CONFIG, pinned_study


def test_committed_release_names_the_shadow_pin():
    config = json.loads(SHADOW_CONFIG.read_text())
    release = json.loads(RELEASE_PATH.read_text())
    assert release["studyId"] == config["study_run_id"]
    assert release["studyDigest"] == config["output_digest"]
    assert release["version"] == RELEASE_VERSION
    # A position may be offered only when the study freezes its candidate.
    for position, policy in release["positions"].items():
        if policy["enabledForOptIn"]:
            assert policy["shadowCandidate"], position


def test_pinned_study_resolves_from_the_config():
    config = json.loads(SHADOW_CONFIG.read_text())
    study, report = pinned_study()
    assert report["run_id"] == config["study_run_id"]
    assert (study / "report.json").exists()


def test_pinned_study_refuses_a_config_that_disagrees_with_its_report(tmp_path: Path):
    config = json.loads(SHADOW_CONFIG.read_text())
    config["output_digest"] = "0" * 64
    path = tmp_path / "shadow.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        pinned_study(path)
