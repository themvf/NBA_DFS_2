from datetime import datetime, timezone
import json
import math

import pytest

from research.nfl_longest_touchdown_weekly_grade import proportional_baseline, summarize, update
from tests.test_nfl_longest_touchdown import play, settled_snapshot

NOW = datetime(2025, 9, 3, tzinfo=timezone.utc)
GAME = {"game_id": "2025_01_ATL_NO", "season": 2025, "week": 1, "kickoff": "2025-09-01T00:00:00Z",
        "away": "ATL", "home": "NO"}


def pub(game=GAME, sha="req-1", kickoff=None):
    return {"game": {**game, "kickoff": kickoff or game["kickoff"]}, "decisionAt": "2025-08-31T00:00:00Z",
            "modelVersion": "test-v1", "noTd": 0.1,
            "provenance": {"request_sha256": sha, "implementation_sha256": "impl"},
            "players": [{"name": "Back", "identity": "rb", "longestShare": 0.5, "anyTd": 0.6, "longTd": 0.2},
                        {"name": "Wideout", "identity": "wr", "longestShare": 0.3, "anyTd": 0.2, "longTd": 0.1}],
            "residual": [{"name": "OTHER:ATL", "identity": "OTHER:ATL", "longestShare": 0.1, "anyTd": 0.1, "longTd": 0.0}]}


def index(*games):
    return {"season": 2025, "weeks": [{"week": 1, "games": list(games)}], "priorSeasons": []}


def test_graded_game_scores_model_against_any_td_baseline():
    snapshot = settled_snapshot([play(1, field=20, td=True), play(2, field=45, td=True, actor="wr", position="WR")])
    store, report = update(None, index(pub()), snapshot, NOW)
    assert report["graded"] == ["2025_01_ATL_NO"]
    record = store["records"][0]
    assert record["winners"] == ["Wideout"] and record["longest_yards"] == 45
    assert record["top_pick"] == "Back" and not record["top_hit"] and record["winner_rank"] == 2
    assert record["log_loss"] == pytest.approx(-math.log(0.3))
    # baseline: 0.9 TD mass split 0.6/0.2/0.1 of 0.9 any-TD -> wr gets 0.2
    assert record["baseline_log_loss"] == pytest.approx(-math.log(0.2))
    assert record["any_td_expected"] == pytest.approx(0.8) and record["any_td_scored"] == 2
    assert record["long_td_scored"] == 1 and record["named_players"] == 2


def test_baseline_keeps_no_td_mass_and_handles_no_scoring_rows():
    forecast = {"no_scrimmage_td_probability": 0.25,
                "players": [{"identity": "a", "any_td_probability": 0.0, "longest_td_win_share": 0.5},
                            {"identity": "b", "any_td_probability": 0.0, "longest_td_win_share": 0.25}]}
    shares = [p["longest_td_win_share"] for p in proportional_baseline(forecast)["players"]]
    assert shares == pytest.approx([0.375, 0.375])


def test_needs_review_is_stored_unscored_and_not_duplicated():
    rows = [play(1, field=20, td=True),
            play(2, field=70, td=True, description="Run for 70 yards TOUCHDOWN. LATERAL to wr")]
    snapshot = settled_snapshot(rows)
    store, report = update(None, index(pub()), snapshot, NOW)
    assert report["needs_review"] == ["2025_01_ATL_NO"]
    record = store["records"][0]
    assert record["status"] == "needs_review" and "log_loss" not in record
    week = store["weeks"][0]
    assert week["graded"] == 0 and week["logLoss"] is None
    assert week["needsReview"][0]["plays"] == [rows[1]["description"]]
    again, report = update(store, index(pub()), snapshot, NOW)
    assert again["records"] == store["records"] and report["unchanged"] == ["2025_01_ATL_NO"]


def test_review_resolved_by_corrected_pbp_appends_and_keeps_history():
    bad = settled_snapshot([play(1, field=20, td=True),
                            play(2, field=70, td=True, description="Run for 70 yards TOUCHDOWN. LATERAL to wr")])
    store, _ = update(None, index(pub()), bad, NOW)
    fixed = settled_snapshot([play(1, field=20, td=True)])
    store, report = update(store, index(pub()), fixed, NOW)
    assert [r["status"] for r in store["records"]] == ["needs_review", "graded"]
    assert report["graded"] == ["2025_01_ATL_NO"] and store["weeks"][0]["needsReview"] == []


def test_unfinished_and_future_games_write_nothing():
    unfinished = {"plays": [play(1, field=20, td=True)],
                  "games": [{"game_id": "2025_01_ATL_NO", "completed": False}], "game_coverage": {}}
    store, report = update(None, index(pub()), unfinished, NOW)
    assert store["records"] == [] and report["awaiting"] == ["2025_01_ATL_NO"]
    store, report = update(None, index(pub(kickoff="2025-09-10T00:00:00Z")), unfinished, NOW)
    assert store["records"] == [] and report == {"graded": [], "needs_review": [], "awaiting": [], "unchanged": []}


def test_graded_game_is_final_and_changed_forecast_is_rejected():
    snapshot = settled_snapshot([play(1, field=20, td=True)])
    store, _ = update(None, index(pub()), snapshot, NOW)
    later = settled_snapshot([play(1, field=80, td=True, actor="wr", position="WR")])
    again, report = update(store, index(pub()), later, NOW)
    assert again["records"] == store["records"] and report["unchanged"] == ["2025_01_ATL_NO"]
    with pytest.raises(ValueError, match="changed after grading"):
        update(store, index(pub(sha="req-2")), snapshot, NOW)


def test_week_summary_counts_top_hits_against_their_own_probability():
    records = [{"game_id": g, "season": 2025, "week": 1, "away": "A", "home": "B", "kickoff": k, "status": "graded",
                "reason": None, "top_hit": hit, "top_probability": 0.2, "log_loss": 2.0, "baseline_log_loss": 2.5,
                "uniform_log_loss": 3.0, "any_td_expected": 4.0, "any_td_scored": 5, "long_td_expected": 0.5,
                "long_td_scored": 1, "longest_yards": 30, "winners": ["X"], "winner_probability": 0.2,
                "winner_rank": 1, "outcomes": 20, "top_pick": "X"}
               for g, k, hit in (("g1", "2025-09-01", True), ("g2", "2025-09-02", False))]
    week = summarize(records)[0]
    assert week["topHits"] == 1 and week["topExpected"] == pytest.approx(0.4)
    assert week["logLoss"] == 2.0 and week["baselineLogLoss"] == 2.5 and week["anyTdScored"] == 10


LATERAL = "Run for 70 yards TOUCHDOWN. LATERAL to wr"


def test_hand_ruling_settles_a_review_and_is_recorded():
    bad = settled_snapshot([play(1, field=20, td=True), play(2, field=70, td=True, description=LATERAL)])
    store, _ = update(None, index(pub()), bad, NOW)
    ruling = {2: {"play_id": 2, "description": LATERAL, "ruling": "touchdown", "scorer": "wr",
                  "team": "ATL", "yards": 70, "note": "film"}}
    store, report = update(store, index(pub()), bad, NOW, {"2025_01_ATL_NO": ruling})
    assert [r["status"] for r in store["records"]] == ["needs_review", "graded"]
    record = store["records"][-1]
    assert record["winners"] == ["Wideout"] and record["longest_yards"] == 70
    assert record["resolved_td_plays"][0]["ruling"] == "touchdown"
    assert store["weeks"][0]["games"][0]["reviewed"] and store["weeks"][0]["needsReview"] == []
    no_td = {2: {**ruling[2], "ruling": "no_touchdown"}}
    store, _ = update(None, index(pub()), bad, NOW, {"2025_01_ATL_NO": no_td})
    assert store["records"][0]["winners"] == ["Back"] and store["records"][0]["longest_yards"] == 20


def test_ruling_on_a_changed_play_or_a_partial_ruling_does_not_score():
    other = "Pass for 60 yards TOUCHDOWN. LATERAL to rb"
    bad = settled_snapshot([play(1, field=20, td=True), play(2, field=70, td=True, description=LATERAL),
                            play(3, field=60, td=True, description=other)])
    stale = {2: {"play_id": 2, "description": "old text", "ruling": "no_touchdown", "note": "x"}}
    store, report = update(None, index(pub()), bad, NOW, {"2025_01_ATL_NO": stale})
    assert report["needs_review"] == ["2025_01_ATL_NO"] and len(store["records"][0]["quarantined_td_plays"]) == 2
    partial = {2: {"play_id": 2, "description": LATERAL, "ruling": "no_touchdown", "note": "x"}}
    store, report = update(None, index(pub()), bad, NOW, {"2025_01_ATL_NO": partial})
    assert report["needs_review"] == ["2025_01_ATL_NO"]
    assert [q["play_id"] for q in store["records"][0]["quarantined_td_plays"]] == [3]


def test_review_file_rejects_incomplete_rulings(tmp_path):
    from research.nfl_longest_touchdown_weekly_grade import load_reviews
    path = tmp_path / "reviews.json"
    base = {"game_id": "g", "play_id": 2, "description": "d", "note": "n"}
    for bad in ({**base, "ruling": "touchdown", "team": "ATL", "yards": 10},
                {**base, "ruling": "maybe"}, {**base, "ruling": "no_touchdown", "note": ""}):
        path.write_text(json.dumps({"reviews": [bad]}))
        with pytest.raises(ValueError):
            load_reviews(path)
    path.write_text(json.dumps({"reviews": [{**base, "ruling": "no_touchdown"}] * 2}))
    with pytest.raises(ValueError, match="Two rulings"):
        load_reviews(path)
    assert load_reviews(tmp_path / "missing.json") == {}


def test_checked_in_reviews_load():
    from research.nfl_longest_touchdown_weekly_grade import REVIEWS, load_reviews
    assert 3670 in load_reviews(REVIEWS)["2026_05_NYG_WAS"]
