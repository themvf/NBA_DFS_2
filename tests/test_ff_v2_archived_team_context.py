from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from ingest.ff_v2_archived_team_context import (
    ARCHIVE_BUNDLES,
    ROSTER_FILES,
    _expand_roster,
    _git_blob_sha,
    validate_payload,
)


def test_all_archive_sources_predate_their_bundle_cutoff() -> None:
    for bundle in ARCHIVE_BUNDLES:
        assert bundle.files
        assert all(item.published_at <= bundle.cutoff for item in bundle.files)
        assert all(len(item.sha256) == 64 for item in bundle.files)
        assert all(item.git_blob_sha is None or len(item.git_blob_sha) == 40 for item in bundle.files)
        assert all(bool(item.commit_sha) != bool(item.release_tag) for item in bundle.files)


def test_payload_requires_both_sha256_and_git_blob_identity() -> None:
    payload = b"immutable archive bytes"
    spec = ROSTER_FILES[2020]
    matching = type(spec)(
        **{
            **spec.__dict__,
            "sha256": __import__("hashlib").sha256(payload).hexdigest(),
            "git_blob_sha": _git_blob_sha(payload),
        }
    )
    validate_payload(matching, payload)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        validate_payload(matching, payload + b"tampered")


def test_release_asset_requires_sha256_without_fake_git_identity() -> None:
    payload = b"immutable release bytes"
    spec = ROSTER_FILES[2020]
    matching = type(spec)(
        **{
            **spec.__dict__,
            "commit_sha": None,
            "release_tag": "archive-release",
            "sha256": __import__("hashlib").sha256(payload).hexdigest(),
            "git_blob_sha": None,
        }
    )
    validate_payload(matching, payload)


def test_annual_roster_expands_only_to_observed_regular_season_weeks() -> None:
    roster = pd.DataFrame(
        [{"season": 2020, "gsis_id": "RB1", "position": "RB", "team": "TB"}]
    )
    pbp = pd.DataFrame(
        [
            {"season": 2020, "season_type": "REG", "week": 1},
            {"season": 2020, "season_type": "REG", "week": 3},
            {"season": 2020, "season_type": "POST", "week": 19},
        ]
    )
    expanded = _expand_roster(roster, pbp, 2020)
    assert expanded[["gsis_id", "week"]].to_dict("records") == [
        {"gsis_id": "RB1", "week": 1},
        {"gsis_id": "RB1", "week": 3},
    ]


def test_archive_cutoffs_are_timezone_aware() -> None:
    for bundle in ARCHIVE_BUNDLES:
        assert bundle.cutoff.tzinfo is not None
        assert bundle.cutoff <= datetime(2022, 9, 7, 23, 59, 59, tzinfo=timezone.utc)
