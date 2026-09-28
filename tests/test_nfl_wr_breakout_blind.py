"""Star-WR absence breakout study: scoring, event classification and gates."""
import numpy as np
import pytest

from model import nfl_absence_reallocation as v1
from model import nfl_wr_breakout_blind as b

POS = {"wr1": "WR", "wr2": "WR", "wr3": "WR", "te": "TE", "rb": "RB", "qb": "QB"}
# targets, carries, receiving yards, rushing yards
LINES = {"wr1": (10, 0, 90, 0), "wr2": (5, 0, 60, 0), "wr3": (2, 0, 20, 0),
         "te": (4, 0, 40, 0), "rb": (3, 15, 20, 70), "qb": (0, 4, 0, 20)}


def game(week, statuses, stats):
    g = v1.TeamGame(2016, week, "AAA", "BBB")
    for pid, status in statuses.items():
        g.status[pid] = status
        g.position[pid] = POS[pid]
        g.name[pid] = pid
    for pid, (targets, carries, rec_yds, rush_yds) in stats.items():
        receptions = targets * 0.7
        g.stats[pid] = {f: 0.0 for f in v1.STAT_FIELDS} | {
            "targets": float(targets), "carries": float(carries), "receptions": receptions,
            "receiving_yards": float(rec_yds), "rushing_yards": float(rush_yds),
            "fantasy_points_ppr": receptions + rec_yds / 10 + rush_yds / 10}
    g.has_stats = True
    return g


def season(n, absent=(), missing=None, lines=None):
    games = []
    for w in range(1, n + 1):
        out = set(missing or ()) | ({"wr1"} if w in absent else set())
        statuses = {p: ("INA" if p in out else "ACT") for p in POS}
        stats = {p: (lines or {}).get((w, p), line) for p, line in LINES.items() if p not in out}
        games.append(game(w, statuses, stats))
    return games


def test_dk_points_adds_the_100_yard_bonuses():
    g = game(1, {"wr1": "ACT", "rb": "ACT"}, {"wr1": (10, 0, 120, 0), "rb": (0, 20, 0, 100)})
    assert b.dk_points(g, "wr1") == pytest.approx(7.0 + 12.0 + 3.0)
    assert b.dk_points(g, "rb") == pytest.approx(10.0 + 3.0)
    assert b.dk_points(g, "nobody") == 0.0


def test_boom_rates_weight_recent_active_games_only():
    booms = {(1, "wr2"): (12, 0, 200, 0)}          # a 20+ game two games back
    games = season(3, lines=booms)
    games[2].status["wr2"] = "INA"                  # inactive in the most recent game
    window = list(reversed(games))                  # most recent first
    rates = b.boom_rates(window)
    w = v1.recency_weights(3)
    assert rates["wr2"][20.0] == pytest.approx(w[2] / (w[1] + w[2]))
    assert "qb" in rates and rates["qb"][20.0] == 0.0


def test_team_games_classifies_first_game_under_way_and_control():
    games = b.team_games({"AAA": season(12, absent=(10, 11))}, (2016,))
    by_week = {g["week"]: g for g in games}
    assert by_week[10]["kind"] == "absence" and by_week[10]["first_game"] is True
    assert by_week[11]["kind"] == "absence" and by_week[11]["first_game"] is False
    assert by_week[9]["kind"] == "control" and by_week[12]["kind"] == "control"
    assert [s["id"] for s in by_week[10]["stars"]] == ["wr1"]
    roles = {r["id"]: r["role"] for r in by_week[10]["rows"]}
    assert roles == {"wr2": "lead_WR", "wr3": "other_WR", "te": "TE"}


def test_a_missing_non_star_pass_catcher_is_neither_absence_nor_control():
    games = b.team_games({"AAA": season(10, missing={"wr2"})}, (2016,))
    # wr2 never plays, so he never becomes established and cannot be a donor:
    assert all(g["kind"] == "control" for g in games)
    games = season(10)
    games[9].status["wr2"] = "INA"
    games[9].stats.pop("wr2")
    # weeks 1-2 have no established players yet; week 10 has a non-star absent
    assert [g["week"] for g in b.team_games({"AAA": games}, (2016,))] == list(range(3, 10))


def test_gap_vs_control_arithmetic():
    def ev(rows):
        return {"rows": [{"role": "lead_WR", "p": {20.0: p, 25.0: p}, "dk": 30.0 if y else 0.0} for y, p in rows]}
    absence = [ev([(1, 0.2), (0, 0.2)]), ev([(1, 0.1)])]      # booms 2/3, expected 0.5/3
    control = [ev([(0, 0.1), (0, 0.1)]), ev([(1, 0.2)])]      # booms 1/3, expected 0.4/3
    out = b.gap_vs_control(absence, control)
    assert out["gap"] == pytest.approx((2 - 0.5) / 3)
    assert out["control_gap"] == pytest.approx((1 - 0.4) / 3)
    assert out["effect"] == pytest.approx(0.3)
    team = b.any_boom(absence, control)
    assert team["any_boom_rate"] == 1.0
    assert team["expected"] == pytest.approx(np.mean([1 - 0.8 * 0.8, 0.1]))


def test_verdict_gates():
    good = {"ci": [0.005, 0.04], "effect": 0.02, "events": 90, "rows": 560}
    assert b.verdict(good, 0.028)["verdict"] == "PROMOTE"
    assert b.verdict({**good, "ci": [-0.001, 0.04]}, 0.028)["verdict"] == "NOT_PROMOTED"
    assert b.verdict({**good, "effect": 0.013}, 0.028)["verdict"] == "NOT_PROMOTED"
    assert b.verdict({**good, "events": 79}, 0.028)["verdict"] == "INSUFFICIENT"
    assert b.verdict({**good, "rows": 499}, 0.028)["verdict"] == "INSUFFICIENT"


def test_frozen_setting_matches_the_registration():
    assert (b.STAR_MIN_TARGETS, b.BOOM_DK, b.SECONDARY_BOOM_DK) == (7.0, 20.0, 25.0)
    assert b.RECIPIENT_POSITIONS == frozenset({"WR", "TE"})
    assert b.DISCOVERY_EFFECT == 0.0281
    assert (b.GATE_MIN_EVENTS, b.GATE_MIN_ROWS) == (80, 500)
    assert b.BLIND_SEASONS == (2014, 2015, 2016, 2017, 2018)
