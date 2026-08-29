from model.ff_champion_baseline import SCORING_TYPES, build_manifest


def _board(scoring: str, ranking_set_id: int) -> dict:
    return {
        "scoring": scoring,
        "ranking_set_id": ranking_set_id,
        "ranking_set_name": f"2026 Independent Model ({scoring})",
        "source_snapshot_id": 99,
        "source_response_hash": "source-hash",
        "source_request_params": {"model_version": "ff-independent-v1.14"},
        "created_at": "2026-08-28T00:00:00+00:00",
    }


def _rankings(offset: int = 0) -> list[dict]:
    return [
        {
            "player_id": offset + index,
            "our_rank": index,
            "position_rank": index,
            "tier": 1,
            "our_projected_points": 300 - index,
            "projection_low": 250 - index,
            "projection_high": 350 - index,
            "expected_games": 17,
            "confidence": 0.8,
        }
        for index in range(1, 101)
    ]


def test_manifest_is_reproducible_and_records_required_evidence() -> None:
    boards = [_board(scoring, index + 1) for index, scoring in enumerate(SCORING_TYPES)]
    rankings = {index + 1: _rankings(index * 1_000) for index in range(3)}
    first = build_manifest(
        season=2026,
        model_version="ff-independent-v1.14",
        boards=boards,
        rankings_by_set=rankings,
        frozen_at="2026-08-28T12:00:00+00:00",
    )
    replay = build_manifest(
        season=2026,
        model_version="ff-independent-v1.14",
        boards=list(reversed(boards)),
        rankings_by_set={key: list(reversed(value)) for key, value in rankings.items()},
        frozen_at="2026-08-29T12:00:00+00:00",
    )
    assert first["championModelVersion"] == "ff-independent-v1.14"
    assert [board["scoring"] for board in first["boards"]] == list(SCORING_TYPES)
    assert all(board["playerCount"] == 100 for board in first["boards"])
    assert all(len(board["projectionDigest"]) == 64 for board in first["boards"])
    assert all(len(board["orderDigest"]) == 64 for board in first["boards"])
    assert first["combinedDigest"] == replay["combinedDigest"]
    assert first["projectionBehaviorChanged"] is False
