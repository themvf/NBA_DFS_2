from datetime import datetime, timezone
import hashlib
import json

import pandas as pd

from ingest.nfl_context_publish import prepare_publication


def test_publication_builds_context_from_revisioned_facts(tmp_path) -> None:
    path = tmp_path / "play_by_play_2025.parquet"
    base = {
        "game_id": "g1",
        "season_type": "REG",
        "posteam": "NYG",
        "drive": 1,
        "qtr": 1,
        "qb_kneel": 0,
        "qb_spike": 0,
        "two_point_attempt": 0,
        "score_differential": 0,
        "penalty_type": None,
    }
    rows = pd.DataFrame(
        [
            {
                **base,
                "play_id": 1,
                "play_type": "run",
                "desc": "Runner left guard for 4 yards",
                "game_seconds_remaining": 3600,
            },
            {
                **base,
                "play_id": 2,
                "play_type": "no_play",
                "desc": "PENALTY NYG False Start, No Play",
                "game_seconds_remaining": 3580,
            },
            {
                **base,
                "play_id": 3,
                "play_type": "pass",
                "desc": "Pass incomplete short right",
                "game_seconds_remaining": 3550,
            },
            {
                **base,
                "play_id": 4,
                "play_type": "run",
                "desc": "Runner right end for 5 yards",
                "game_seconds_remaining": 3520,
            },
        ]
    )
    rows.to_parquet(path, index=False)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "latest.json").write_text(
        json.dumps(
            {
                "url": "https://example.test/pbp.parquet",
                "response_hash": digest,
                "source_published_at": "2026-08-13T12:26:09+00:00",
            }
        ),
        encoding="utf-8",
    )
    publication = prepare_publication(
        path,
        target_game="2026_02_NYG_LA",
        teams=["NYG"],
        observed_at=datetime(2026, 9, 23, tzinfo=timezone.utc),
    )
    assert len(publication.facts) == 4
    assert publication.facts[1].snap_execution == "no_snap"
    context = publication.contexts[0]
    # The false-start row breaks adjacency. Only plays 3 -> 4 form an interval.
    assert context.denominator == 1
    assert context.value == 30
    assert context.fact_release_id == publication.fact_release_id
    assert context.source_snapshot_ids == (publication.source_observation_id,)


def test_fact_release_identity_does_not_depend_on_observation_time(tmp_path) -> None:
    path = tmp_path / "pbp.parquet"
    pd.DataFrame(
        [
            {
                "game_id": "g1",
                "play_id": 1,
                "season_type": "REG",
                "desc": "End of game",
                "play_type": "game_end",
            }
        ]
    ).to_parquet(path, index=False)
    first = prepare_publication(
        path,
        target_game="game",
        teams=[],
        observed_at=datetime(2026, 9, 23, tzinfo=timezone.utc),
    )
    second = prepare_publication(
        path,
        target_game="game",
        teams=[],
        observed_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    assert first.fact_release_id == second.fact_release_id
    assert first.source_observation_id == second.source_observation_id


def test_fact_only_publication_does_not_require_a_target(tmp_path) -> None:
    path = tmp_path / "pbp.parquet"
    pd.DataFrame(
        [{"game_id": "g1", "play_id": 1, "desc": "End of game", "play_type": "game_end"}]
    ).to_parquet(path, index=False)
    publication = prepare_publication(
        path,
        target_game=None,
        teams=[],
        observed_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    assert len(publication.facts) == 1
    assert publication.contexts == ()
