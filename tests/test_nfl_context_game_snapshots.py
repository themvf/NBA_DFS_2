from datetime import datetime, timezone
import hashlib
import json

import pandas as pd

from ingest.nfl_context_game_snapshots import prepare_game_publication


def test_one_history_release_builds_many_target_snapshots(tmp_path) -> None:
    path = tmp_path / "history.parquet"
    rows = []
    for team in ("NYG", "LA"):
        for play_id, seconds in ((1, 3600), (2, 3570)):
            rows.append(
                {
                    "game_id": f"old-{team}",
                    "play_id": play_id,
                    "season_type": "REG",
                    "desc": "Runner left guard for 4 yards",
                    "play_type": "run",
                    "posteam": team,
                    "drive": 1,
                    "qtr": 1,
                    "qb_kneel": 0,
                    "qb_spike": 0,
                    "two_point_attempt": 0,
                    "score_differential": 0,
                    "game_seconds_remaining": seconds,
                    "penalty_type": None,
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "latest.json").write_text(
        json.dumps({"response_hash": digest}), encoding="utf-8"
    )
    publication = prepare_game_publication(
        path,
        targets=[("game-1", ["NYG", "LA"]), ("game-2", ["NYG", "LA"])],
        observed_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    assert len(publication.facts) == 4
    assert len(publication.contexts) == 4
    assert {context.target_id for context in publication.contexts} == {"game-1", "game-2"}
