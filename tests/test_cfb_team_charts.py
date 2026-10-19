"""Synthetic tests for model/cfb_team_charts.py (no database)."""
from __future__ import annotations

import itertools
import random

import pytest

from model import cfb_team_charts as tc


# ── market ratings ───────────────────────────────────────────────────────────
TRUE = {1: 14.0, 2: 7.0, 3: 0.0, 4: -6.0, 5: -15.0}


def _schedule(weeks: int = 4, hfa: float = 3.0, noise: float = 0.0, seed: int = 1) -> list[dict]:
    rng = random.Random(seed)
    games = []
    pairs = list(itertools.combinations(TRUE, 2))
    for w in range(1, weeks + 1):
        for i, (a, b) in enumerate(pairs):
            home, away = (a, b) if (w + i) % 2 else (b, a)
            neutral = i % 5 == 0
            margin = TRUE[home] - TRUE[away] + (0 if neutral else hfa) + rng.gauss(0, noise)
            games.append({"week": w, "home_team_id": home, "away_team_id": away, "hname": f"T{home}",
                          "aname": f"T{away}", "hclass": "fbs", "aclass": "fbs", "season_type": "regular",
                          "neutral_site": neutral, "home_spread": -margin})
    return games


def test_fit_recovers_known_ratings_and_home_edge():
    games = _schedule()
    fit = tc.fit_ratings(games, 4, ridge=1e-6)
    mean = sum(TRUE.values()) / len(TRUE)
    for t, r in TRUE.items():
        assert fit["ratings"][t] == pytest.approx(r - mean, abs=0.05)
    assert fit["hfa"] == pytest.approx(3.0, abs=0.05)
    assert fit["mae"] == pytest.approx(0.0, abs=0.05)


def test_ridge_shrinks_toward_zero_and_keeps_order():
    fit = tc.fit_ratings(_schedule(noise=1.5), 4, ridge=5.0)
    r = fit["ratings"]
    assert [t for t in sorted(r, key=lambda t: -r[t])] == [1, 2, 3, 4, 5]
    assert abs(r[1]) < TRUE[1] - sum(TRUE.values()) / 5
    assert sum(r.values()) == pytest.approx(0.0, abs=1e-6)


def test_market_ratings_rows_and_risers():
    games = _schedule(weeks=3)
    # Team 5 gets better in week 3's lines only: it should be the biggest riser.
    for g in games:
        if g["week"] == 3 and 5 in (g["home_team_id"], g["away_team_id"]):
            g["home_spread"] += -10 if g["home_team_id"] == 5 else 10
    rows = tc.market_ratings(games)
    assert {r["week"] for r in rows} == {1, 2, 3}
    assert len([r for r in rows if r["week"] == 3]) == 5
    assert tc.biggest_risers(rows, 1) == ["T5"]
    fits = tc.market_rating_fits(games)
    assert [f["week"] for f in fits] == [1, 2, 3] and fits[-1]["games"] == 30


def test_rating_games_filters_non_fbs_and_missing_spreads():
    games = _schedule(weeks=1)
    games[0]["aclass"] = "fcs"
    games[1]["home_spread"] = None
    games[2]["season_type"] = "postseason"
    assert len(tc._rating_games(games)) == len(games) - 3


# ── comebacks ────────────────────────────────────────────────────────────────
def _play(gsr, off, os_, ds, period=None, scoring=True, drive=1, num=1):
    period = period or min(4, (3600 - gsr) // 900 + 1)
    return {"game_id": 1, "period": period, "game_seconds_remaining": gsr, "drive_number": drive,
            "play_number": num, "offense_team_id": off, "offense_score": os_, "defense_score": ds, "scoring": scoring}


GAME = {"game_id": 1, "week": 3, "home_team_id": 10, "away_team_id": 20, "home": "Home U", "away": "Away St",
        "home_score": 24, "away_score": 21, "reg_home": 24, "reg_away": 21, "overtime": False,
        "home_line": [0, 3, 7, 14], "away_line": [14, 7, 0, 0]}


def _game_plays():
    return [
        _play(3300, 20, 7, 0), _play(2900, 20, 14, 0),              # away up 14 in Q1
        _play(2500, 20, 21, 0),                                     # away up 21 in Q2
        _play(2000, 10, 3, 21),                                     # home FG
        _play(1200, 10, 10, 21),                                    # Q3 TD
        _play(800, 10, 17, 21), _play(100, 10, 24, 21),             # Q4
        _play(2400, 20, 99, 0, scoring=False),                      # a junk running score
    ]


def test_find_comebacks_largest_deficit_and_minute():
    audit = {}
    rows = tc.find_comebacks([GAME], _game_plays(), audit=audit)
    assert len(rows) == 1
    r = rows[0]
    assert (r["winner"], r["loser"], r["deficit"]) == ("Home U", "Away St", 21)
    assert r["minute"] == pytest.approx((3600 - 2500) / 60)
    assert r["path"][-1] == (60.0, 3)
    assert audit == {"games": 1, "excluded": 0}


def test_illegal_score_jump_is_rejected_and_game_left_out():
    plays = _game_plays() + [_play(2450, 20, 39, 0)]   # an 18-point jump: rejected
    steps, rejected = tc.score_steps(plays, 10)
    assert rejected == 1 and max(b for _, _, b in steps) == 21
    # A missing scoring play means the steps cannot add up: the game is left out.
    audit = {}
    assert tc.find_comebacks([GAME], [p for p in plays if p["game_seconds_remaining"] != 100], audit=audit) == []
    assert audit == {"games": 1, "excluded": 1}


def test_quarter_line_mismatch_excludes_game():
    g = dict(GAME, home_line=[0, 10, 0, 14])   # same total, wrong quarters
    assert tc.find_comebacks([g], _game_plays()) == []


def test_wire_to_wire_winner_is_not_a_comeback():
    g = dict(GAME, home_score=21, away_score=24, reg_home=21, reg_away=24, home_line=None, away_line=None)
    plays = [_play(3300, 20, 7, 0), _play(2000, 20, 14, 0), _play(1000, 10, 7, 14), _play(500, 20, 21, 7),
             _play(200, 10, 14, 21), _play(50, 20, 24, 14), _play(10, 10, 21, 24)]
    assert tc.find_comebacks([g], plays) == []


# ── aggregations ─────────────────────────────────────────────────────────────
def test_fourth_down_teams_rates_and_minimum():
    rows = [{"team_id": 1, "team": "A", "play_type": "Rush", "gained_distance": True, "n": 6},
            {"team_id": 1, "team": "A", "play_type": "Pass Incompletion", "gained_distance": False, "n": 2},
            {"team_id": 1, "team": "A", "play_type": "Punt", "gained_distance": False, "n": 2},
            {"team_id": 1, "team": "A", "play_type": "Penalty", "gained_distance": False, "n": 9},
            {"team_id": 2, "team": "B", "play_type": "Rush", "gained_distance": True, "n": 3}]
    out = tc.fourth_down_teams(rows, min_decisions=8)
    assert len(out) == 1
    assert (out[0]["decisions"], out[0]["go"], out[0]["conv"]) == (10, 8, 6)
    assert out[0]["go_pct"] == pytest.approx(80.0)


def test_drive_team_rows_red_zone_and_points():
    rows = [{"team_id": 1, "team": "A", "drive_result": "TD", "reached_red_zone": True, "n": 3, "offense_points": 21},
            {"team_id": 1, "team": "A", "drive_result": "FG", "reached_red_zone": True, "n": 1, "offense_points": 3},
            {"team_id": 1, "team": "A", "drive_result": "TD", "reached_red_zone": False, "n": 1, "offense_points": 7},
            {"team_id": 1, "team": "A", "drive_result": "PUNT", "reached_red_zone": False, "n": 5, "offense_points": 0},
            {"team_id": 1, "team": "A", "drive_result": "END OF HALF", "reached_red_zone": False, "n": 2,
             "offense_points": 0}]
    (a,) = tc.drive_team_rows(rows)
    assert (a["drives"], a["trips"], a["rz_td"]) == (10, 4, 3)
    assert a["rz_td_pct"] == pytest.approx(75.0)
    assert a["points_per_drive"] == pytest.approx(3.1)


def test_turnover_ats_rows_join():
    games = [{"season_type": "regular", "home_spread": -7.0, "home_score": 30, "away_score": 20, "neutral_site": False,
              "vegas_total": 50, "home_team_id": 1, "away_team_id": 2, "hname": "A", "aname": "B",
              "hclass": "fbs", "aclass": "fbs", "hconf": "X", "aconf": "Y"}] * 4
    turnovers = [{"team_id": 1, "team": "A", "games": 4, "giveaways": 2, "takeaways": 6},
                 {"team_id": 2, "team": "B", "games": 4, "giveaways": 6, "takeaways": 2},
                 {"team_id": 3, "team": "C", "games": 2, "giveaways": 0, "takeaways": 0}]
    out = {r["team"]: r for r in tc.turnover_ats_rows(turnovers, games)}
    assert set(out) == {"A", "B"}
    assert out["A"]["margin_pg"] == pytest.approx(1.0) and out["A"]["cover_pct"] == 100.0
    assert out["B"]["cover_pct"] == 0.0


# ── charts ───────────────────────────────────────────────────────────────────
def _scatter_rows(xk, yk):
    rng = random.Random(3)
    return [{"team": f"Team {i}", xk: rng.uniform(-1, 1), yk: rng.uniform(20, 80)} for i in range(30)]


def test_every_chart_writes_png(tmp_path):
    games = _schedule()
    ratings = tc.market_ratings(games)
    fits = tc.market_rating_fits(games)
    comebacks = tc.find_comebacks([GAME], _game_plays())
    fourth = [{"team": f"T{i}", "go_pct": 100 - i * 4, "go": 10 - i // 3, "decisions": 10, "conv": 5}
              for i in range(25)]
    drives = [{"team": f"T{i}", "trips": 20, "rz_td": 20 - i // 2, "rz_td_pct": 100 * (20 - i // 2) / 20}
              for i in range(30)]
    made = {
        "a.png": tc.ratings_chart(ratings, tmp_path / "a.png", teams=["T1", "T3", "T5"], fits=fits),
        "b.png": tc.top_ratings_chart(ratings, tmp_path / "b.png"),
        "c.png": tc.turnovers_vs_ats_chart(_scatter_rows("margin_pg", "cover_pct"), tmp_path / "c.png"),
        "d.png": tc.fourth_down_coaches_chart(fourth, tmp_path / "d.png"),
        "e.png": tc.red_zone_teams_chart(drives, tmp_path / "e.png"),
        "f.png": tc.biggest_comebacks_chart(comebacks, tmp_path / "f.png", {"games": 1, "excluded": 0}),
        "g.png": tc.comeback_flow_chart(comebacks[0], tmp_path / "g.png"),
        "h.png": tc.explosive_vs_points_chart(_scatter_rows("explosive_pct", "points_per_drive"), tmp_path / "h.png"),
    }
    for name, ok in made.items():
        assert ok, name
        assert (tmp_path / name).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_every_chart_returns_false_when_empty(tmp_path):
    assert tc.ratings_chart([], tmp_path / "a.png") is False
    assert tc.top_ratings_chart([], tmp_path / "b.png") is False
    assert tc.turnovers_vs_ats_chart([], tmp_path / "c.png") is False
    assert tc.fourth_down_coaches_chart([], tmp_path / "d.png") is False
    assert tc.red_zone_teams_chart([], tmp_path / "e.png") is False
    assert tc.biggest_comebacks_chart([], tmp_path / "f.png") is False
    assert tc.comeback_flow_chart(None, tmp_path / "g.png") is False
    assert tc.explosive_vs_points_chart([], tmp_path / "h.png") is False
    assert not list(tmp_path.iterdir())
