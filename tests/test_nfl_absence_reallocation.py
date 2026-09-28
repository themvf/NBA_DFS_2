"""Fixed-pie absence reallocation: allocation arithmetic and walk-forward guards."""
import pytest

from model import nfl_absence_reallocation as m


def rec(pid, group, base):
    return {"id": pid, "group": group, "base": base}


def test_allocates_exactly_the_leftover_when_uncapped():
    recipients = [rec("a", "WR", 6.0), rec("b", "TE", 3.0), rec("c", "RB", 1.0)]
    donors = [rec("d", "WR", 8.0)]
    out = m.allocate_fixed_pie(recipients, donors, team_budget=33.0, active_sum=24.0, reserve=3.0, phi=1.0, alpha=0.0)
    assert out["leftover"] == pytest.approx(6.0)
    assert sum(out["gains"].values()) == pytest.approx(6.0)
    assert out["gains"]["a"] == pytest.approx(3.6)  # 6 * 6/10
    assert out["dropped"] == 0


def test_absence_already_in_the_baselines_leaves_nothing_to_hand_out():
    # Teammates' averages already absorbed him: nothing is left on the table.
    recipients = [rec("a", "WR", 9.0), rec("b", "TE", 6.0)]
    out = m.allocate_fixed_pie(recipients, [rec("d", "WR", 8.0)], 33.0, 31.0, 3.0, 1.0, 0.0)
    assert out["leftover"] == 0
    assert all(v == 0 for v in out["gains"].values())


def test_leftover_never_exceeds_what_the_donors_had():
    out = m.allocate_fixed_pie([rec("a", "WR", 5.0)], [rec("d", "WR", 2.0)], 40.0, 10.0, 0.0, 1.0, 0.0)
    assert out["leftover"] == pytest.approx(2.0)


def test_phi_hands_out_a_fraction():
    out = m.allocate_fixed_pie([rec("a", "WR", 5.0)], [rec("d", "WR", 4.0)], 30.0, 22.0, 4.0, 0.5, 0.0)
    assert out["leftover"] == pytest.approx(4.0)
    assert out["gains"]["a"] == pytest.approx(2.0)


def test_alpha_routes_to_the_donors_position_group():
    recipients = [rec("wr", "WR", 2.0), rec("te", "TE", 8.0)]
    donors = [rec("d", "WR", 5.0)]
    full = m.allocate_fixed_pie(recipients, donors, 30.0, 20.0, 5.0, 1.0, 1.0)
    assert full["gains"]["wr"] == pytest.approx(5.0)
    assert full["gains"]["te"] == pytest.approx(0.0)
    half = m.allocate_fixed_pie(recipients, donors, 30.0, 20.0, 5.0, 1.0, 0.5)
    assert half["gains"]["wr"] == pytest.approx(2.5 + 2.5 * 0.2)
    assert half["gains"]["te"] == pytest.approx(2.5 * 0.8)


def test_no_same_group_recipient_spreads_to_everyone():
    out = m.allocate_fixed_pie([rec("te", "TE", 4.0), rec("rb", "RB", 4.0)], [rec("d", "WR", 6.0)],
                               30.0, 20.0, 4.0, 1.0, 1.0)
    assert out["gains"]["te"] == pytest.approx(3.0)
    assert out["gains"]["rb"] == pytest.approx(3.0)


def test_cap_drops_and_counts_the_excess():
    out = m.allocate_fixed_pie([rec("a", "WR", 1.0)], [rec("d", "WR", 10.0)], 30.0, 10.0, 0.0, 1.0, 0.0)
    assert out["gains"]["a"] == pytest.approx(3.0)  # 4x cap
    assert out["dropped"] == pytest.approx(7.0)
    assert out["allocated"] == pytest.approx(3.0)


def test_rejects_invalid_inputs():
    with pytest.raises(ValueError):
        m.allocate_fixed_pie([rec("a", "WR", 0.0)], [rec("d", "WR", 1.0)], 30, 10, 0, 1, 0)
    with pytest.raises(ValueError):
        m.allocate_fixed_pie([rec("a", "WR", 1.0)], [rec("d", "WR", 1.0)], float("nan"), 10, 0, 1, 0)
    with pytest.raises(ValueError):
        m.allocate_fixed_pie([rec("a", "WR", 1.0)], [rec("d", "WR", 1.0)], 30, 10, 0, 1.5, 0)


def test_old_additive_transfer_ignores_the_budget():
    gains = m.allocate_additive([rec("a", "WR", 6.0), rec("b", "WR", 4.0)], [rec("d", "WR", 10.0)])
    assert sum(gains.values()) == pytest.approx(10.0)


# --- walk-forward --------------------------------------------------------------

def game(week, statuses, stats, season=2024, has_stats=True, positions=None):
    g = m.TeamGame(season, week, "AAA", "BBB")
    for pid, status in statuses.items():
        g.status[pid] = status
        g.position[pid] = (positions or {}).get(pid, "WR")
        g.name[pid] = pid
    if has_stats:
        for pid, (targets, carries) in stats.items():
            g.stats[pid] = {f: 0.0 for f in m.STAT_FIELDS} | {"targets": float(targets), "carries": float(carries),
                                                                "receptions": targets * 0.6, "fantasy_points_ppr": targets}
        g.has_stats = True
    return g


def season_of(n, absent_week=None, qb_carries=0):
    games = []
    positions = {"x": "WR", "y": "WR", "z": "TE", "rb": "RB", "qb": "QB"}
    for w in range(1, n + 1):
        out = w == absent_week
        statuses = {"x": "INA" if out else "ACT", "y": "ACT", "z": "ACT", "rb": "ACT", "qb": "ACT"}
        stats = {"y": (6, 0), "z": (4, 0), "rb": (2, 15), "qb": (0, qb_carries)}
        if not out:
            stats["x"] = (9, 0)
        games.append(game(w, statuses, stats, positions=positions))
    return games


def test_baseline_ignores_the_target_game_and_everything_after():
    games = season_of(10)
    before = m.team_states(games, m.POOLS["targets"])[8].baselines["y"].base
    games[8].stats["y"]["targets"] = 50.0
    games[9].stats["y"]["targets"] = 50.0
    after = m.team_states(games, m.POOLS["targets"])[8].baselines["y"].base
    assert before == pytest.approx(after) == pytest.approx(6.0)


def test_carries_budget_excludes_quarterback_runs():
    games = season_of(9, qb_carries=7)
    state = m.team_states(games, m.POOLS["carries"])[8]
    assert state.budget == pytest.approx(15.0)


def test_event_hands_the_absent_players_volume_to_teammates():
    games = season_of(10, absent_week=10)
    teams = {"AAA": games}
    events, _ = m.build_events(teams, m.POOLS["targets"], (2024,))
    assert len(events) == 1
    event = events[0]
    assert [d["id"] for d in event["donors"]] == ["x"]
    assert event["budget"] == pytest.approx(21.0)
    assert event["reserve"] == pytest.approx(0.0)
    out = m.allocate_fixed_pie(event["recipients"], event["donors"], event["budget"], event["active"],
                               event["reserve"], 1.0, 0.0)
    assert out["leftover"] == pytest.approx(9.0)


def test_reserve_prefers_full_strength_games():
    games = season_of(12)
    for w in (4, 6):  # two earlier absences; an unmodelled call-up caught his targets
        games[w].status["x"] = "INA"
        games[w].stats["u"] = games[w].stats.pop("x")
    states = m.team_states(games, m.POOLS["targets"])
    assert states[4].residual == pytest.approx(9.0) and not states[4].full_strength
    # Six full-strength games remain in the window, so the absences do not
    # inflate the reserve and shrink the next absence's hand-out.
    assert m.reserve_for(states, 11) == pytest.approx(0.0)
    for w in (7, 8, 9, 10):
        games[w].status["x"] = "INA"
        games[w].stats["u"] = games[w].stats.pop("x")
    states = m.team_states(games, m.POOLS["targets"])
    assert m.reserve_for(states, 11) > 0  # fewer than 3 full-strength games: falls back to all


def test_verdict_requires_all_three_gates():
    good = {"delta": -0.2, "ci": [-0.3, -0.1], "events": 300, "rows": 1000}
    pts = {"delta": -0.05, "ci": [-0.2, 0.04]}
    assert m.verdict(good, pts)["verdict"] == "PROMOTE"
    assert m.verdict({**good, "ci": [-0.3, 0.01]}, pts)["verdict"] == "NOT_PROMOTED"
    assert m.verdict(good, {"delta": -0.01, "ci": [-0.1, 0.06]})["verdict"] == "NOT_PROMOTED"
    assert m.verdict({**good, "events": 150}, pts)["verdict"] == "INSUFFICIENT"
