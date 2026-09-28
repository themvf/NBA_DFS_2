"""Starting-TE absence study: event classification, the TE-room pie and gates."""
import numpy as np
import pytest

from model import nfl_absence_reallocation as v1
from model import nfl_te_backup_blind as t

POS = {"te1": "TE", "te2": "TE", "wr1": "WR", "rb": "RB"}
# targets, receiving yards
LINES = {"te1": (6, 60), "te2": (1, 8), "wr1": (8, 90), "rb": (3, 20)}


def game(week, statuses, stats):
    g = v1.TeamGame(2016, week, "AAA", "BBB")
    for pid, status in statuses.items():
        g.status[pid] = status
        g.position[pid] = POS[pid]
        g.name[pid] = pid
    for pid, (targets, yds) in stats.items():
        receptions = targets * 0.7
        g.stats[pid] = {f: 0.0 for f in v1.STAT_FIELDS} | {
            "targets": float(targets), "receptions": receptions, "receiving_yards": float(yds),
            "fantasy_points_ppr": receptions + yds / 10}
    g.has_stats = True
    return g


def season(n, absent=(), lines=None):
    games = []
    for w in range(1, n + 1):
        out = {"te1"} if w in absent else set()
        statuses = {p: ("INA" if p in out else "ACT") for p in POS}
        stats = {p: (lines or {}).get((w, p), line) for p, line in LINES.items() if p not in out}
        games.append(game(w, statuses, stats))
    return games


def by_week(events):
    return {e["week"]: e for e in events}


def test_events_classify_first_game_under_way_and_control():
    ev = by_week(t.team_events({"AAA": season(12, absent=(10, 11))}, (2016,)))
    assert ev[10]["kind"] == "absence" and ev[10]["first_game"] is True
    assert ev[11]["kind"] == "absence" and ev[11]["first_game"] is False
    assert ev[9]["kind"] == "control" and ev[12]["kind"] == "control"
    assert [s["id"] for s in ev[10]["starters"]] == ["te1"]
    # only TEs are rows; the backup is the lead remaining TE
    assert [r["id"] for r in ev[10]["rows"]] == ["te2"]
    assert [r["id"] for r in ev[9]["rows"]] == ["te1", "te2"]


def test_the_te_room_pie_counts_only_tight_ends():
    ev = by_week(t.team_events({"AAA": season(12, absent=(10,))}, (2016,)))[10]
    pie = ev["pie"]
    assert pie["budget"] == pytest.approx(7.0)       # te1 6 + te2 1; WR/RB targets excluded
    assert pie["active"] == pytest.approx(1.0)
    assert pie["reserve"] == pytest.approx(0.0)
    # leftover 6, phi 0.5 -> 3, capped at 3x a 1-target baseline -> 3
    pred = t.predict(ev, 0.5)
    assert pred["targets"][0] == pytest.approx(4.0)
    ppu = ev["rows"][0]["ppu"]
    assert pred["points"][0] == pytest.approx(ev["rows"][0]["points_base"] + 3.0 * ppu)
    base = t.predict(ev, None)
    assert base["targets"][0] == pytest.approx(1.0)


def test_a_small_te_is_not_a_starter():
    lines = {(w, "te1"): (2, 20) for w in range(1, 13)}
    events = t.team_events({"AAA": season(12, absent=(10,), lines=lines)}, (2016,))
    # te1 at 2 targets a game is a donor but not a starter: neither absence nor control
    assert 10 not in by_week(events)


def test_errors_split_lead_and_others():
    ev = {"pie": None, "rows": [
        {"id": "a", "targets_base": 3.0, "points_base": 8.0, "ppu": 2.0, "actual_targets": 5.0, "actual_points": 12.0},
        {"id": "b", "targets_base": 1.0, "points_base": 2.0, "ppu": 2.0, "actual_targets": 0.0, "actual_points": 0.0}]}
    lead, others = t.errors(ev, None, "lead"), t.errors(ev, None, "others")
    assert lead["points"].tolist() == [4.0] and lead["targets"].tolist() == [2.0]
    assert others["points"].tolist() == [-2.0]


def test_big_game_compares_lead_te_with_control_te2():
    def ev(rows):
        return {"rows": [{"dk": dk, "p_boom": p} for dk, p in rows]}
    first = [ev([(15.0, 0.1)]), ev([(5.0, 0.1), (20.0, 0.9)])]          # lead: 1/2 hit, expected 0.1
    control = [ev([(30.0, 0.9), (13.0, 0.2)]), ev([(0.0, 0.5), (1.0, 0.2)])]  # TE2: 1/2 hit, expected 0.2
    out = t.big_game(first, control)
    assert out["rows"] == 2 and out["control_rows"] == 2
    assert out["boom_rate"] == 0.5 and out["expected_rate"] == pytest.approx(0.1)
    assert out["effect"] == pytest.approx((0.5 - 0.1) - (0.5 - 0.2))


def test_gates():
    pts = {"mse_ci": [-5.0, -0.5], "mae_ci": [-0.3, 0.4], "events": 75}
    tgt = {"mse_delta": -1.0}
    assert t.verdict(pts, tgt, 0.5)["verdict"] == "PROMOTE"
    assert t.verdict({**pts, "mse_ci": [-5.0, 0.1]}, tgt, 0.5)["verdict"] == "NOT_PROMOTED"
    assert t.verdict({**pts, "mae_ci": [-0.3, 0.6]}, tgt, 0.5)["verdict"] == "NOT_PROMOTED"
    assert t.verdict(pts, {"mse_delta": 0.1}, 0.5)["verdict"] == "NOT_PROMOTED"
    assert t.verdict({**pts, "events": 69}, tgt, 0.5)["verdict"] == "INSUFFICIENT"
    big = {"rows": 75, "effect": 0.05, "ci": [0.001, 0.1]}
    assert t.big_game_verdict(big)["verdict"] == "PROMOTE"
    assert t.big_game_verdict({**big, "ci": [-0.001, 0.1]})["verdict"] == "NOT_PROMOTED"
    assert t.big_game_verdict({**big, "effect": 0.040})["verdict"] == "NOT_PROMOTED"
    assert t.big_game_verdict({**big, "rows": 69})["verdict"] == "INSUFFICIENT"


def test_frozen_setting_matches_the_registration():
    assert (t.STARTER_MIN_TARGETS, t.BOOM_DK, t.FROZEN_PHI, t.MAE_MARGIN) == (4.0, 12.0, 0.5, 0.50)
    assert t.TE_ROOM.budget_only == frozenset({"TE"}) and t.TE_ROOM.recipients == frozenset({"TE"})
    assert (t.BIG_GAME_DISCOVERY_EFFECT, t.BIG_GAME_MIN_SHARE_OF_DISCOVERY) == (0.0814, 0.5)
    assert t.GATE_MIN_EVENTS == 70
    assert t.BLIND_SEASONS == (2014, 2015, 2016, 2017, 2018)
    assert np.isclose(t.BIG_GAME_MIN_SHARE_OF_DISCOVERY * t.BIG_GAME_DISCOVERY_EFFECT, 0.0407)
