"""RB-room absence reallocation (v2): points accounting and pool scoping."""
import pytest

from model import nfl_absence_reallocation as v1
from model import nfl_rb_absence_reallocation as m


def game(week, statuses, stats, positions):
    g = v1.TeamGame(2024, week, "AAA", "BBB")
    for pid, status in statuses.items():
        g.status[pid] = status
        g.position[pid] = positions[pid]
        g.name[pid] = pid
    for pid, (carries, targets, rush_yds, rec_yds) in stats.items():
        g.stats[pid] = {f: 0.0 for f in v1.STAT_FIELDS} | {
            "carries": float(carries), "targets": float(targets), "rushing_yards": float(rush_yds),
            "receptions": targets * 0.8, "receiving_yards": float(rec_yds),
            "fantasy_points_ppr": rush_yds / 10 + targets * 0.8 + rec_yds / 10}
    g.has_stats = True
    return g


POS = {"rb1": "RB", "rb2": "RB", "wr": "WR", "te": "TE", "qb": "QB"}


def season(n, absent_week=None):
    games = []
    for w in range(1, n + 1):
        out = w == absent_week
        statuses = {p: "ACT" for p in POS} | ({"rb1": "INA"} if out else {})
        stats = {"rb2": (6, 2, 24, 12), "wr": (0, 8, 0, 70), "te": (0, 5, 0, 40), "qb": (4, 0, 20, 0)}
        if not out:
            stats["rb1"] = (16, 4, 70, 30)
        games.append(game(w, statuses, stats, POS))
    return games


def test_rb_target_budget_counts_only_the_rb_room():
    state = v1.team_states(season(9), m.RB_TARGETS)[8]
    assert state.budget == pytest.approx(6.0)  # rb1 4 + rb2 2, not the WR/TE targets


def test_event_moves_both_carries_and_rb_targets_to_the_backup():
    teams = {"AAA": season(10, absent_week=10)}
    [event] = m.build_events(teams, (2024,))
    [row] = event["rows"]
    assert row["id"] == "rb2"
    base = m.predict(event, None, 0.0)
    full = m.predict(event, 1.0, 1.0)
    assert full["carries"][0] - base["carries"][0] == pytest.approx(16.0)
    assert full["targets"][0] - base["targets"][0] == pytest.approx(4.0)
    carries_only = m.predict(event, 1.0, 0.0)
    assert carries_only["targets"][0] == pytest.approx(base["targets"][0])
    # Points: own per-carry (2.4/6 = 0.4) and per-target (1.6+1.2=2.8 / 2 = 1.4) rates.
    assert full["points"][0] - base["points"][0] == pytest.approx(16 * 0.4 + 4 * 1.4)


def test_quarterback_rushing_is_not_inherited_by_the_backup():
    teams = {"AAA": season(10, absent_week=10)}
    [event] = m.build_events(teams, (2024,))
    assert event["carries"]["budget"] == pytest.approx(22.0)  # rb1 16 + rb2 6, QB carries excluded
    assert [d["id"] for d in event["carries"]["donors"]] == ["rb1"]


def test_no_event_without_a_material_rb_absence():
    games = season(10)
    games[9].status["wr"] = "INA"
    games[9].stats.pop("wr")
    assert m.build_events({"AAA": games}, (2024,)) == []


def test_verdict_requires_points_mechanism_and_sample():
    pts = {"mse_ci": [-5.0, -1.0], "mae_ci": [-0.1, 0.03], "events": 250, "rows": 600}
    good, bad = {"mse_delta": -0.3}, {"mse_delta": 0.1}
    assert m.verdict(pts, good, good)["verdict"] == "PROMOTE"
    assert m.verdict({**pts, "mse_ci": [-5.0, 0.2]}, good, good)["verdict"] == "NOT_PROMOTED"
    assert m.verdict({**pts, "mae_ci": [-0.1, 0.16]}, good, good)["verdict"] == "NOT_PROMOTED"
    assert m.verdict(pts, bad, good)["verdict"] == "NOT_PROMOTED"
    assert m.verdict(pts, good, bad)["verdict"] == "NOT_PROMOTED"
    assert m.verdict(pts, good, None)["verdict"] == "PROMOTE"
    assert m.verdict({**pts, "events": 150}, good, good)["verdict"] == "INSUFFICIENT"


def test_selection_uses_squared_error_and_breaks_ties_conservatively():
    teams = {"AAA": season(10, absent_week=10)}
    events = m.build_events(teams, (2024,))
    sel = m.select(events)
    assert set(sel["points_mse"]) == {m.key(c) for c in m.CONFIGS}
    best = min(sel["points_mse"].values())
    tied = [c for c in m.CONFIGS if sel["points_mse"][m.key(c)] == best]
    assert (sel["phi_carries"], sel["phi_targets"]) == sorted(tied, key=lambda c: (c[1], c[0]))[0]
