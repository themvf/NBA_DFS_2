from __future__ import annotations

from datetime import datetime, timezone

import pytest

from model.ff_v2_backtest import (
    ARTIFACT_KINDS,
    build_run_manifest,
    prediction_view,
    score_artifacts,
)


def test_prediction_view_exposes_only_prior_training_and_preseason_features() -> None:
    view = prediction_view(
        [
            {
                "season": 2024,
                "observedAt": "2025-01-05T16:00:00-05:00",
                "team": "TB",
                "plays": 65,
            }
        ],
        [
            {
                "season": 2025,
                "availableAt": "2025-08-20T12:00:00-04:00",
                "entityId": "TB:2025:1",
                "quarterback": "QB1",
                "actualValue": 70,
            }
        ],
        cutoff="2025-09-03T23:59:59-04:00",
        evaluation_season=2025,
    )

    assert view["trainingRows"][0]["plays"] == 65
    assert "actualValue" not in view["evaluationFeatures"][0]


@pytest.mark.parametrize(
    ("training_season", "observed_at", "feature_available_at"),
    [
        (2025, "2025-01-05T16:00:00-05:00", "2025-08-20T12:00:00-04:00"),
        (2024, "2025-09-04T00:00:00-04:00", "2025-08-20T12:00:00-04:00"),
        (2024, "2025-01-05T16:00:00-05:00", "2025-09-04T00:00:00-04:00"),
    ],
)
def test_prediction_view_rejects_future_information(
    training_season: int,
    observed_at: str,
    feature_available_at: str,
) -> None:
    with pytest.raises(ValueError):
        prediction_view(
            [{"season": training_season, "observedAt": observed_at}],
            [{"season": 2025, "availableAt": feature_available_at, "entityId": "x"}],
            cutoff="2025-09-03T23:59:59-04:00",
            evaluation_season=2025,
        )


@pytest.mark.parametrize("artifact_kind", ARTIFACT_KINDS)
def test_all_product_artifact_scopes_share_one_chronological_scorer(artifact_kind: str) -> None:
    result = score_artifacts(
        artifact_kind,
        [
            {
                "entityId": "entity-1",
                "season": 2025,
                "availableAt": "2025-09-03T23:00:00-04:00",
                "value": 12.0,
            }
        ],
        [
            {
                "entityId": "entity-1",
                "season": 2025,
                "observedAt": "2025-09-07T16:00:00-04:00",
                "value": 10.0,
            }
        ],
        cutoff="2025-09-03T23:59:59-04:00",
        evaluation_season=2025,
    )

    assert result["artifactKind"] == artifact_kind
    assert result["n"] == 1
    assert result["absoluteErrorTotal"] == 2.0
    assert result["signedErrorTotal"] == 2.0
    assert len(result["scoreDigest"]) == 64


def test_artifact_scorer_rejects_post_cutoff_prediction_and_identity_mismatch() -> None:
    with pytest.raises(ValueError, match="post-cutoff"):
        score_artifacts(
            "team_week",
            [{"entityId": "a", "season": 2025, "availableAt": "2025-09-04T00:00:00-04:00", "value": 1}],
            [{"entityId": "a", "season": 2025, "observedAt": "2025-09-07T16:00:00-04:00", "value": 1}],
            cutoff="2025-09-03T23:59:59-04:00",
            evaluation_season=2025,
        )

    with pytest.raises(ValueError, match="identities differ"):
        score_artifacts(
            "team_week",
            [{"entityId": "a", "season": 2025, "availableAt": "2025-09-03T23:00:00-04:00", "value": 1}],
            [{"entityId": "b", "season": 2025, "observedAt": "2025-09-07T16:00:00-04:00", "value": 1}],
            cutoff="2025-09-03T23:59:59-04:00",
            evaluation_season=2025,
        )


class _FakeDatabase:
    def execute(self, sql: str, params: tuple[str]):
        del params
        if "ff_v2_team_week_facts" in sql:
            return [
                {
                    "season": 2020,
                    "week": 1,
                    "game_id": "2020_01_A_B",
                    "team": "A",
                    "fact_digest": "a" * 64,
                    "observed_at": datetime(2020, 9, 10, tzinfo=timezone.utc),
                },
                {
                    "season": 2021,
                    "week": 1,
                    "game_id": "2021_01_A_B",
                    "team": "A",
                    "fact_digest": "b" * 64,
                    "observed_at": datetime(2021, 9, 9, tzinfo=timezone.utc),
                },
            ]
        if "ff_v2_roster_weeks" in sql:
            return [
                {"season": 2020, "row_count": 10, "unique_players": 8},
                {"season": 2021, "row_count": 12, "unique_players": 9},
            ]
        raise AssertionError(sql)


def test_run_manifest_is_deterministic_and_uses_expanding_prior_seasons_only() -> None:
    context = {
        "runId": "9077ad91-e258-5e47-beb8-f41b68c6651b",
        "artifactDigest": "c" * 64,
        "seasons": [2020, 2021],
        "sources": {
            "schedule:all": {"sourceSnapshotId": 1},
            "play-by-play:2020": {"sourceSnapshotId": 2},
            "play-by-play:2021": {"sourceSnapshotId": 3},
        },
    }
    cutoffs = {
        2020: "2020-09-09T23:59:59-04:00",
        2021: "2021-09-08T23:59:59-04:00",
    }

    first = build_run_manifest(_FakeDatabase(), context, seed=7, cutoffs=cutoffs)
    second = build_run_manifest(_FakeDatabase(), context, seed=7, cutoffs=cutoffs)

    assert first["runId"] == second["runId"]
    assert first["outputDigest"] == second["outputDigest"]
    assert first["splits"][0]["trainingSeasons"] == []
    assert first["splits"][0]["scorable"] is False
    assert first["splits"][1]["trainingSeasons"] == [2020]
    assert first["splits"][1]["scorable"] is True
    assert first["splits"][1]["trainingRowCounts"]["team_week"] == 1
    assert first["splits"][1]["evaluationRowCounts"]["player_week"] == 12
