"""WP10 hygiene: duplicate-identity guard, study provenance, --tonight scope."""

from __future__ import annotations

import pytest

from ingest.nfl_dfs_projections import assert_unique_identities
from model.nfl_dfs_study_provenance import provenance


def test_duplicate_roster_identity_fails_the_build_with_the_ids():
    ok = [{"id": 560, "normalized_name": "pukanacua", "team_abbrev": "LAR", "position": "WR"},
          {"id": 561, "normalized_name": "pukanacua", "team_abbrev": "LAR", "position": "TE"}]
    assert_unique_identities(ok)                      # same name, different position: fine
    dup = ok + [{"id": 34, "normalized_name": "pukanacua", "team_abbrev": "LAR", "position": "WR"}]
    with pytest.raises(ValueError) as exc:
        assert_unique_identities(dup)
    assert "560,34" in str(exc.value) and "ff_dedupe_identities" in str(exc.value)


def test_provenance_records_digest_rows_and_latest_labelled_week():
    rows = [{"season": 2026, "week": 1, "x": 1}, {"season": 2026, "week": 2, "x": 2}, {"season": 2025, "week": 18, "x": 3}]
    p = provenance(rows)
    assert p["dataset_rows"] == 3 and p["max_labelled_week"] == [2026, 2]
    assert len(p["dataset_digest"]) == 64
    assert provenance(rows[:2])["dataset_digest"] != p["dataset_digest"]   # one more labelled week changes it


def test_opponent_tonight_applies_v1_to_carries_only_and_caps_targets():
    src = open("model/nfl_dfs_workload_opponent.py", encoding="utf-8").read()
    body = src.split("def tonight(")[1].split("def main(")[0]
    assert 'use_v1 = field == "carries"' in body
    assert "targets_capped_at_attempts" in body
    assert '{"mean": v["v1"]} if v and "v1" in v else None' not in body
