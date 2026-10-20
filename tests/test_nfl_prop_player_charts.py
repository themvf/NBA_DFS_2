"""Offline tests for model/nfl_prop_player_charts.py: pure computations and that every chart writes a PNG."""

from __future__ import annotations

import random

import pytest

from model import nfl_prop_player_charts as pc


def _line(key, week, line, actual, event="g1", name=None, team="AAA", p_over=0.5):
    return {"player_key": key, "name": name or key.title(), "team": team, "week": week, "line": line,
            "actual": actual, "event_id": event, "p_over": p_over}


# ── over / under / push ──────────────────────────────────────────────────────
def test_classify_handles_pushes_and_missing_rows():
    assert pc.classify(5, 4.5) == "over"
    assert pc.classify(4, 4.5) == "under"
    assert pc.classify(40, 40) == "push"
    assert pc.classify(None, 4.5) is None      # no stat row: not a zero


def test_tally_counts_pushes_and_missing_apart_from_the_graded_lines():
    rows = [_line("a", 1, 4.5, 6), _line("b", 1, 4.5, 2), _line("c", 1, 40, 40), _line("d", 1, 3.5, None)]
    t = pc.tally(rows)
    assert (t["over"], t["under"], t["push"], t["missing"], t["graded"]) == (1, 1, 1, 1, 2)
    assert t["over_rate"] == 0.5


def test_unloaded_games_are_dropped_whole_but_an_inactive_player_is_counted_missing():
    rows = [_line("a", 1, 4.5, 6, event="g1"), _line("b", 1, 3.5, None, event="g1"),
            _line("c", 1, 4.5, None, event="g2"), _line("d", 1, 2.5, None, event="g2")]
    kept, dropped = pc.drop_unloaded_games(rows)
    assert dropped == 1
    assert {r["player_key"] for r in kept} == {"a", "b"}


# ── player ranking ───────────────────────────────────────────────────────────
def test_player_table_needs_the_minimum_graded_lines_and_ignores_pushes():
    rows = [_line("star", w, 4.5, 6) for w in range(1, 5)]                      # 4 of 4 over
    rows += [_line("thin", w, 4.5, 6) for w in range(1, 4)]                     # only 3 lines
    rows += [_line("pushy", w, 4, 4) for w in range(1, 4)] + [_line("pushy", 4, 4.5, 6)]   # pushes do not count
    rows += [_line("cold", w, 4.5, 2) for w in range(1, 6)] + [_line("cold", 6, 4.5, None)]
    table = pc.player_table(rows, min_lines=4)
    keys = [p["player_key"] for p in table]
    assert keys == ["star", "cold"]
    star, cold = table
    assert (star["overs"], star["n"], star["rate"]) == (4, 4, 1.0)
    assert (cold["overs"], cold["n"], cold["rate"]) == (0, 5, 0.0)
    assert cold["diff"] == pytest.approx(-2.5)


def test_player_table_takes_the_team_on_his_latest_line():
    rows = [_line("mover", w, 3.5, 5, team="OLD") for w in (1, 2, 3)] + [_line("mover", 4, 3.5, 1, team="NEW")]
    assert pc.player_table(rows, min_lines=4)[0]["team"] == "NEW"


def test_ends_never_repeat_a_player_when_the_list_is_short():
    table = [{"name": str(i)} for i in range(15)]
    top, bottom = pc.ends(table, 12)
    assert len(top) == 12 and len(bottom) == 3
    assert not {p["name"] for p in top} & {p["name"] for p in bottom}


# ── anytime touchdown calibration ────────────────────────────────────────────
def test_wilson_interval_brackets_the_rate():
    lo, hi = pc.wilson(20, 100)
    assert lo < 0.2 < hi
    assert lo == pytest.approx(0.1334, abs=1e-3) and hi == pytest.approx(0.2888, abs=1e-3)
    assert pc.wilson(0, 10)[0] == 0.0


def test_td_bins_group_by_priced_chance():
    rows = ([{"implied": 0.05, "scored": i < 1} for i in range(10)]
            + [{"implied": 0.25, "scored": i < 3} for i in range(10)])
    bins = pc.td_bins(rows)
    assert [(b["lo"], b["n"], b["scored"]) for b in bins] == [(0.0, 10, 1), (0.2, 10, 3)]
    assert bins[1]["rate"] == pytest.approx(0.3)
    assert bins[1]["ci"][0] < 0.3 < bins[1]["ci"][1]


def test_implied_yes_and_long_shots():
    assert pc.implied_yes(300) == pytest.approx(0.25)
    assert pc.implied_yes(-150) == pytest.approx(0.6)
    rows = [{"implied": 0.03, "scored": True, "week": 1, "name": "A"},
            {"implied": 0.01, "scored": False, "week": 1, "name": "B"},
            {"implied": 0.02, "scored": True, "week": 2, "name": "C"}]
    assert [r["name"] for r in pc.long_shots(rows, 5)] == ["C", "A"]


def test_team_defense_markets_are_recognised():
    assert pc.is_team_defense("Chicago Bears D/ST")
    assert not pc.is_team_defense("DJ Moore")


# ── longest touchdowns ───────────────────────────────────────────────────────
def _play(**kw):
    base = {"game_id": "g1", "week": 1, "home_team": "AAA", "away_team": "BBB", "play_type": "pass",
            "touchdown": 1, "pass_touchdown": 0, "rush_touchdown": 0, "return_touchdown": 0,
            "two_point_attempt": 0, "td_team": "AAA", "td_player_name": "A.Catcher", "td_player_id": "00-1",
            "passer_player_name": "Q.Back", "yards_gained": 10, "return_yards": 0, "interception": 0,
            "desc": "x"}
    return base | kw


def test_td_plays_excludes_nullified_plays_and_two_point_tries():
    plays = [_play(pass_touchdown=1, yards_gained=75),
             _play(play_type="no_play", pass_touchdown=1, yards_gained=90),          # penalty wiped it out
             _play(two_point_attempt=1, touchdown=0, yards_gained=2),
             _play(play_type="run", rush_touchdown=1, yards_gained=12, td_player_name="R.Back", td_team="BBB"),
             _play(play_type="kickoff", return_touchdown=1, yards_gained=0, return_yards=99,
                   td_player_name="K.Return", td_team="BBB"),
             _play(play_type="pass", return_touchdown=1, interception=1, yards_gained=0, return_yards=40,
                   td_player_name="D.Back", td_team="BBB"),
             _play(touchdown=0, yards_gained=60)]
    tds = pc.td_plays(plays)
    assert [(t["player"], t["yards"], t["kind"]) for t in tds] == [
        ("K.Return", 99, "kick return"), ("A.Catcher", 75, "pass from Back"),
        ("D.Back", 40, "interception return"), ("R.Back", 12, "rush")]
    assert tds[0]["opp"] == "AAA" and tds[1]["opp"] == "BBB"


def test_game_longest_counts_finished_games_only_and_zero_when_nobody_scored():
    plays = [_play(pass_touchdown=1, yards_gained=30), _play(desc="END GAME", touchdown=0, play_type=None),
             _play(game_id="g2", desc="END GAME", touchdown=0, play_type=None),
             _play(game_id="g3", pass_touchdown=1, yards_gained=80)]          # still in progress
    games = pc.final_games(plays)
    assert games == {"g1", "g2"}
    assert pc.game_longest(pc.td_plays(plays), games) == {"g1": 30, "g2": 0}


def test_full_names_replace_short_names_by_player_id():
    tds = [{"player": "A.Catcher", "player_id": "00-1"}, {"player": "Z.Nobody", "player_id": None}]
    out = pc.with_full_names(tds, {"00-1": "Alan Catcher"})
    assert [t["player"] for t in out] == ["Alan Catcher", "Z.Nobody"]


def test_grade_summary_counts_graded_games_only():
    grades = [(5, [{"status": "graded", "top_hit": True, "outcomes": 20},
                   {"status": "graded", "top_hit": False, "outcomes": 20},
                   {"status": "needs_review", "top_hit": None}])]
    g = pc.grade_summary(grades)
    assert (g["games"], g["hits"], g["weeks"]) == (2, 1, [5])
    assert g["chance"] == pytest.approx(0.05)
    assert pc.grade_summary([]) is None


# ── every chart writes a PNG ─────────────────────────────────────────────────
def _market_rows(yards: bool, n=300, seed=3):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        line = rng.choice([24.5, 38.5, 55.5, 70.5]) if yards else rng.choice([1.5, 2.5, 3.5, 4.5, 5.5])
        actual = max(0, round(line + rng.gauss(0, 20 if yards else 1.8)))
        rows.append(_line(f"p{i % 40}", 1 + i // 40, line, actual if i % 25 else None, event=f"g{i % 12}",
                          name=f"Player {i % 40}", p_over=0.5))
    return rows


def test_every_chart_writes_a_png(tmp_path):
    for market, yards in (("player_receptions", False), ("player_reception_yds", True)):
        rows = _market_rows(yards)
        assert pc.line_scatter_chart(rows, tmp_path / f"{market}.png", market, 1, n_labels=3 if yards else 0)
        assert pc.players_chart(pc.player_table(rows), tmp_path / f"{market}_players.png", market, (1, 8))
    rng = random.Random(5)
    td = []
    for i in range(400):
        price = rng.choice([-150, 120, 250, 400, 900, 2000])
        p = pc.implied_yes(price)
        td.append({"week": 1 + i % 6, "name": f"P{i}", "team": "AAA", "price": price, "implied": p,
                   "scored": rng.random() < p * 0.9})
    assert pc.td_chart(td, tmp_path / "td.png", dropped=1, missing=12)
    plays = [_play(game_id=f"g{i}", week=1 + i % 6, pass_touchdown=1, yards_gained=rng.randint(1, 90),
                   td_player_name=f"P.Layer{i}") for i in range(40)]
    plays += [_play(game_id=f"g{i}", desc="END GAME", touchdown=0, play_type=None) for i in range(40)]
    tds = pc.td_plays(plays)
    grades = {"games": 14, "hits": 6, "weeks": [5], "chance": 0.04}
    assert pc.longest_td_chart(tds, pc.game_longest(tds, pc.final_games(plays)), tmp_path / "long.png", grades)
    for f in ("player_receptions.png", "player_reception_yds_players.png", "td.png", "long.png"):
        assert (tmp_path / f).stat().st_size > 10_000


def test_charts_return_false_without_data(tmp_path):
    assert not pc.line_scatter_chart([], tmp_path / "a.png", "player_receptions")
    assert not pc.players_chart([], tmp_path / "b.png", "player_receptions")
    assert not pc.td_chart([], tmp_path / "c.png")
    assert not pc.longest_td_chart([], {}, tmp_path / "d.png")
