"""Every DraftKings rushing-yards under, 2023-2025 (research/nfl_rushing_under_history.py)."""
from datetime import datetime, timedelta, timezone

from research import nfl_rushing_under as ru
from research import nfl_rushing_under_history as hist

KICK = datetime(2024, 10, 13, 17, 0, tzinfo=timezone.utc)
GAME = {"game_id": "2024_06_SF_ATL", "week": 6, "kickoff": KICK, "completed": True,
        "home_name": "Atlanta Falcons", "away_name": "San Francisco 49ers"}


def _cap(player, line, minutes_before=90, over=-115, under=-105):
    return {"event_id": "e", "player": player, "dk": {"line": line, "over": over, "under": under},
            "captured_at": KICK - timedelta(minutes=minutes_before), "commence_time": KICK,
            "home_team_name": "Atlanta Falcons", "away_team_name": "San Francisco 49ers"}


def test_past_season_lines_are_kept_and_keyed_by_season_week():
    caps = [_cap("Bijan Robinson", 70.5), _cap("Tyler Allgeier", 25.5)]
    names = {(GAME["game_id"], "bijanrobinson"): ["b"], (GAME["game_id"], "tylerallgeier"): ["t"]}
    stats = {(6, "b"): {"rushing_yards": 90}, (6, "t"): {"rushing_yards": 10}}
    bets, pending = hist.season_bets(2024, [GAME], caps, names, stats, {GAME["game_id"]})
    by = {b["player"]: b for b in bets}
    assert pending == 0 and by["Bijan Robinson"]["result"] == "loss" and by["Tyler Allgeier"]["result"] == "win"
    assert {b["week"] for b in bets} == {202406} and {b["nfl_week"] for b in bets} == {6}
    assert {b["season"] for b in bets} == {2024}


def test_week_19_and_later_are_outside_the_population():
    playoff = {**GAME, "week": 19}
    assert hist.season_bets(2024, [playoff], [_cap("A Back", 40.5)], {}, {}, set())[0] == []


def _bet(season, week, result, line=30.5):
    pnl = {"win": 100 / 110, "loss": -1.0, "push": 0.0, "void": None}[result]
    return {"game_id": f"g{season}{week}", "season": season, "week": season * 100 + week, "nfl_week": week,
            "result": result, "pnl": pnl, "line": line, "fair_over": 0.5, "in_final_capture": True,
            "void_reason": "no_stat_row" if result == "void" else None}


def _season(season, wins, losses, line=30.5):
    return [_bet(season, w, r, line) for w in range(1, 19) for r in ["win"] * wins + ["loss"] * losses]


def test_pass_needs_the_interval_above_zero_and_two_positive_seasons():
    good = _season(2023, 30, 20) + _season(2024, 30, 20) + _season(2025, 30, 20)
    out = hist.summarize(good, final=True)
    assert out["floors_met"] and out["positive_seasons"] == 3 and out["verdict_if_first"] == "PASS"
    assert hist.summarize(good, final=False)["verdict_if_first"] is None


def test_one_great_season_cannot_carry_a_pass():
    lopsided = _season(2023, 45, 5) + _season(2024, 25, 27) + _season(2025, 25, 27)
    out = hist.summarize(lopsided, final=True)
    assert out["primary"]["roi_interval"][0] > 0 and out["positive_seasons"] == 1
    assert out["verdict_if_first"] == "NO_EDGE"


def test_losing_and_thin_samples():
    bad = _season(2023, 20, 30) + _season(2024, 20, 30) + _season(2025, 20, 30)
    assert hist.summarize(bad, final=True)["verdict_if_first"] == "LOSES"
    assert hist.summarize(bad[:300], final=True)["verdict_if_first"] == "INCONCLUSIVE"


def test_secondary_compares_small_and_large_lines_on_the_same_weeks():
    bets = _season(2023, 30, 20, line=20.5) + _season(2023, 20, 30, line=80.5)
    sec = hist.summarize(bets)["secondary_small_lines"]
    assert sec["small"]["bets"] == sec["large"]["bets"] == 900
    assert sec["difference_roi"] > 0 and sec["difference_interval"][0] > 0


def test_settlement_and_line_rules_are_the_forward_studys():
    """Same functions, not copies: a change to the forward rules shows up here."""
    assert hist.ru is ru
    assert hist.REGISTRATION["settlement"] == {**ru.REGISTRATION["settlement"],
                                               "source": "nflverse stats_player_week for the line's season, regular season"}


def test_registered_constants_are_frozen():
    """Registered before any 2023-2025 rushing line was pulled: changing any of these is a new version."""
    r = hist.REGISTRATION
    assert r["study"] == "nfl-rushing-under-history-v1" and r["status"] == "registered"
    assert r["registered_at"] == "2026-10-31T21:10:00+00:00"
    assert (r["seasons"], r["weeks"], r["book"], r["market"], r["stat"], r["side"], r["stake"]) == \
        ([2023, 2024, 2025], [1, 18], "draftkings", "player_rush_yds", "rushing_yards", "under", 1.0)
    assert r["event_match_window_minutes"] == 120
    assert r["floors"] == {"graded_bets": 2000, "game_weeks": 36}
    assert r["bootstrap"] == {"resamples": 2000, "seed": 20261031, "cluster": "season_game_week", "interval": 0.95}
    assert r["stability"] == {"min_positive_seasons": 2}
    assert r["secondary"]["split_line"] == 60.0
    assert set(r["verdict"]["outcomes"]) == {"PASS", "NO_EDGE", "LOSES", "INCONCLUSIVE"}
    from ingest.nfl_prop_backfill import LEAD_MINUTES
    assert LEAD_MINUTES == 90
