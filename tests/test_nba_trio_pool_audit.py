import pytest

from research.nba_trio_math_runner import grade, signed
from research.nba_trio_pool_audit import build, top_three_capture


def fixture():
    members = [
        {"event_id": "book", "player_key": f"p{i:02d}", "player": f"Player {i}", "market_line": 30 - i}
        for i in range(13)
    ]
    identity = {
        "game_date": "2026-11-04", "mode": "prospective", "source_snapshot_sha256": "source",
        "lock_at": "2026-11-05T00:00:00Z",
    }
    forecast = signed({**identity, "model_sha256": "v3", "rows": members, "modeled_pool_n": 13})
    ids = [f"book:{row['player_key']}" for row in members]
    record = signed({
        **identity, "forecast_sha256": forecast["sha256"], "base_model_sha256": "v3",
        "protocol_sha256": "protocol", "model_sha256": "math", "pool_n": 12, "modeled_pool_n": 13,
        "slate_schedule": [{"event_id": "espn"}],
        "players": [
            {"id": key, "player": row["player"], "espn_event_id": "espn", "band": "high",
             "raw_p90": 40, "adjusted_p90": 39, "compound_mean": 25, "v3_mean": 24,
             "market_line": row["market_line"]}
            for key, row in zip(ids, members[:12])
        ],
        "portfolios": {"market_baseline": {"trios": [[ids[0], ids[2], ids[i]] for i in range(3, 8)]}},
    })
    diagnostics = {
        "snapshot_sha256": "source", "game_date": identity["game_date"],
        "rows": [
            {**row, "status": "verified", "espn_event_id": "espn", "actual": {"points": points}}
            for row, points in zip(members, [38, 35, 25, 10, 9, 8, 7, 6, 5, 4, 3, 2, 41])
        ],
    }
    scoreboard = {"events": [{"id": "espn", "status": {"type": {"completed": True}}}]}
    return record, forecast, diagnostics, scoreboard


def test_gap_reconciles_for_original_frozen_pools():
    record, forecast, diagnostics, board = fixture()
    audit = build(record, forecast, diagnostics, grade(record, diagnostics, board))
    assert audit["full_pool"]["best_total"] == 114
    assert audit["top_12"]["best_total"] == 98
    assert audit["pool_gap"] == 16
    result = audit["portfolios"]["market_baseline"]
    assert result["best_entry"] == 73
    assert result["selection_gap"] == 25
    assert result["total_gap"] == audit["pool_gap"] + result["selection_gap"] == 41
    assert result["fractional_top_three"] == 1
    assert not result["tie_compatible_top_three"]
    assert not audit["provisional"]


def test_unresolved_players_and_wrong_grade():
    record, forecast, diagnostics, board = fixture()
    original_grade = grade(record, diagnostics, board)
    diagnostics["rows"][12]["status"] = "dnp"
    audit = build(record, forecast, diagnostics, original_grade)
    assert audit["full_pool"]["unresolved"] == {"dnp": 1}
    assert audit["provisional"]
    diagnostics["rows"][3]["status"] = "dnp"
    with pytest.raises(ValueError, match="diagnostics disagree"):
        build(record, forecast, diagnostics, original_grade)
    audit = build(record, forecast, diagnostics, grade(record, diagnostics, board))
    result = audit["portfolios"]["market_baseline"]
    assert result["best_entry"] is result["total_gap"] is result["fractional_top_three"] is None


def test_boundary_ties_count_fractionally_and_allow_any_tied_player():
    points = {"a": 40, "b": 34, "c": 28, "d": 28, "e": 25}
    assert top_three_capture(["a", "b", "d"], points) == (2.5, True)
    assert top_three_capture(["a", "c", "d"], points) == (2.0, False)
