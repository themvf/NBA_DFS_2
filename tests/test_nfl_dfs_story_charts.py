"""Offline tests for model/nfl_dfs_story_charts.py: pure computations and that every chart writes a PNG."""

from __future__ import annotations

import random

from model import nfl_dfs_story_charts as sc


def _players(n=40, seed=1):
    rng = random.Random(seed)
    pos = ["QB", "RB", "WR", "TE", "DST"]
    return [{"name": f"Player {i}", "own": round(50 * rng.random() ** 2, 2), "points": round(40 * rng.random(), 1),
             "position": pos[i % 5], "team": "AAA", "salary": 5000} for i in range(n)]


# ── scored_players ───────────────────────────────────────────────────────────
def test_scored_players_prefers_contest_points_then_builds_and_never_zero_fills():
    own = [{"player_name": "A", "normalized_name": "a", "drafted_pct": 10.0, "fpts": 12.0},
           {"player_name": "B", "normalized_name": "b", "drafted_pct": 5.0, "fpts": None},
           {"player_name": "C", "normalized_name": "c", "drafted_pct": 2.0, "fpts": None}]
    builds = {"a": {"points": 99.0, "position": "WR"}, "b": {"points": 7.5, "position": "RB"}}
    players, missing = sc.scored_players(own, builds)
    by = {p["name"]: p for p in players}
    assert by["A"]["points"] == 12.0          # contest's own score wins over the builds
    assert by["B"]["points"] == 7.5           # builds fill the gap
    assert "C" not in by and missing == 1     # no score anywhere: left out and counted, not 0
    assert by["A"]["position"] == "WR"


# ── chalk split ──────────────────────────────────────────────────────────────
def test_chalk_split_top_ten_against_rest_above_floor():
    players = [{"name": f"c{i}", "own": 30 + i, "points": 20.0} for i in range(10)]
    players += [{"name": "r1", "own": 5.0, "points": 10.0}, {"name": "r2", "own": 2.0, "points": 4.0},
                {"name": "tiny", "own": 0.5, "points": 40.0}]
    s = sc.chalk_split(players)
    assert s["chalk_avg"] == 20.0 and s["chalk_n"] == 10
    assert s["rest_avg"] == 7.0 and s["rest_n"] == 2     # the 0.5% player is below the 1% floor
    assert s["chalk_20"] == 10


def test_chalk_split_needs_both_groups():
    assert sc.chalk_split([{"name": "a", "own": 10, "points": 1}] * 5) is None


def test_crowd_labels_busts_and_smashes():
    players = [{"name": "chalk bust", "own": 40, "points": 3}, {"name": "chalk hit", "own": 35, "points": 30},
               {"name": "smash", "own": 2, "points": 35}, {"name": "low dud", "own": 1, "points": 2},
               {"name": "mid", "own": 10, "points": 22}]
    busts, smashes = sc.crowd_labels(players)
    assert [p["name"] for p in busts] == ["chalk bust"]
    assert [p["name"] for p in smashes] == ["smash"]


# ── first place with ties ────────────────────────────────────────────────────
def test_first_place_single_winner():
    e = [{"rank": 2, "username": "b", "lineup_text": "x"}, {"rank": 1, "username": "a", "lineup_text": "y"}]
    top = sc.first_place(e)
    assert top["username"] == "a" and top["tied"] == 1 and top["distinct_lineups"] == 1


def test_first_place_tie_is_counted_and_stable():
    e = [{"rank": 1, "username": "zed", "lineup_text": "L1"}, {"rank": 1, "username": "amy", "lineup_text": "L1"},
         {"rank": 1, "username": "bob", "lineup_text": "L2"}, {"rank": 4, "username": "c", "lineup_text": "L3"}]
    top = sc.first_place(e)
    assert top["username"] == "amy" and top["tied"] == 3 and top["distinct_lineups"] == 2
    assert sc.first_place([]) is None


def test_lineup_summary_stack_and_totals():
    roster = [{"slot": "QB", "name": "Q", "team": "NE", "position": "QB", "salary": 6000, "points": 30, "own": 4},
              {"slot": "WR", "name": "W", "team": "NE", "position": "WR", "salary": 5000, "points": 20, "own": 10},
              {"slot": "TE", "name": "T", "team": "NE", "position": "TE", "salary": 4000, "points": 10, "own": 6},
              {"slot": "RB", "name": "R", "team": "DET", "position": "RB", "salary": 8000, "points": 25, "own": 30}]
    s = sc.lineup_summary(roster, "QB/WR/BB")
    assert s["salary"] == 23000 and s["own"] == 50 and s["points"] == 85
    assert s["stack"] == "NE QB + WR + TE, bring-back" and s["stacked"] == 2
    roster[3]["points"] = None
    assert sc.lineup_summary(roster)["points"] is None    # a missing seat score is not a zero


def test_published_summary_skips_duplicates_and_missing():
    rows = [{"season": 2024, "week": 7, "players": [{"draft_pct": 10}, {"draft_pct": 5}], "flags": []},
            {"season": 2024, "week": 9, "players": [{"draft_pct": 10}], "flags": ["duplicate_of_week_9"]},
            {"season": 2024, "week": 10, "players": [{"draft_pct": None}], "flags": []},
            {"season": 2023, "week": 1, "players": [{"draft_pct": 3}], "flags": ["salary_over_cap"]}]
    out = sc.published_summary(rows)
    assert [(r["season"], r["week"], r["own"]) for r in out] == [(2024, 7, 15.0), (2023, 1, 3.0)]


# ── over rate ────────────────────────────────────────────────────────────────
def _prop_rows(n_games=30, per=6, p=0.5, seed=4):
    rng = random.Random(seed)
    rows = []
    for g in range(n_games):
        for _ in range(per):
            rows.append({"line": 50.5, "actual": 60.0 if rng.random() < p else 40.0, "p_over": 0.5, "event_id": f"g{g}"})
    return rows


def test_over_summary_excludes_pushes_and_missing():
    rows = _prop_rows()
    rows += [{"line": 4.0, "actual": 4.0, "p_over": 0.5, "event_id": "g0"},
             {"line": 4.5, "actual": None, "p_over": 0.5, "event_id": "g1"}]
    s = sc.over_summary(rows, n_boot=300)
    assert s["n"] == 180 and s["pushes"] == 1 and s["missing"] == 1 and s["games"] == 30


def test_over_summary_interval_contains_rate_and_is_sane():
    s = sc.over_summary(_prop_rows(p=0.6), n_boot=500)
    lo, hi = s["ci"]
    assert 0 <= lo < s["over"] < hi <= 1
    assert hi - lo < 0.25
    glo, ghi = s["gap_ci"]
    assert abs((s["over"] - s["implied"]) - s["gap"]) < 1e-12 and glo < s["gap"] < ghi


def test_over_summary_empty():
    assert sc.over_summary([{"line": 1, "actual": None, "p_over": .5, "event_id": "x"}]) is None


# ── charts write PNGs ────────────────────────────────────────────────────────
def test_every_chart_writes_png(tmp_path):
    players = _players()
    assert sc.crowd_chart(players, tmp_path / "a.png", 5, 160000, missing=1)
    weeks = [{"week": w, **sc.chalk_split(_players(seed=w))} for w in (1, 2, 3)]
    assert sc.chalk_report_chart(weeks, tmp_path / "b.png")
    roster = [{"slot": s, "name": f"Name Number{i}", "team": "NE", "position": s if s != "FLEX" else "WR",
               "salary": 5000, "points": 20.0, "own": 3.0 * i} for i, s in enumerate(sc.CLASSIC_SLOTS)]
    lineups = [{"week": w, "entries": 100000, "roster": roster, "summary": sc.lineup_summary(roster),
                "tied": 1 if w != 2 else 3, "distinct_lineups": 1, "username": "u"} for w in (1, 2)]
    assert sc.winning_lineups_chart(lineups, tmp_path / "c.png")
    pub = [{"season": s, "week": w, "own": 80 + 7 * w} for s in (2023, 2024) for w in range(1, 6)]
    assert sc.winners_history_chart(pub, [{"season": 2026, "week": 1, "own": 90}], tmp_path / "d.png")
    summ = {"player_rush_yds": sc.over_summary(_prop_rows(p=0.4), n_boot=200),
            "player_receptions": sc.over_summary(_prop_rows(seed=9), n_boot=200)}
    assert sc.prop_chart(summ, tmp_path / "e.png", (1, 5))
    for name in "abcde":
        assert (tmp_path / f"{name}.png").stat().st_size > 10_000


def test_charts_return_false_without_data(tmp_path):
    assert not sc.crowd_chart([], tmp_path / "a.png", 5)
    assert not sc.chalk_report_chart([], tmp_path / "b.png")
    assert not sc.winning_lineups_chart([], tmp_path / "c.png")
    assert not sc.winners_history_chart([], [], tmp_path / "d.png")
    assert not sc.prop_chart({}, tmp_path / "e.png")
    assert not list(tmp_path.iterdir())
