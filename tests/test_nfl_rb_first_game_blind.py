"""First-game RB absence blind test: activity proxy, population and gates."""
import pytest

from model import nfl_absence_reallocation as v1
from model import nfl_rb_absence_reallocation as m
from model import nfl_rb_first_game_blind as b

POS = {"rb1": "RB", "rb2": "RB", "rb3": "RB", "wr": "WR", "qb": "QB"}


def game(week, statuses, stats):
    g = v1.TeamGame(2016, week, "AAA", "BBB")
    for pid, status in statuses.items():
        g.status[pid] = status
        g.position[pid] = POS[pid]
        g.name[pid] = pid
    for pid, (carries, targets, rush_yds, rec_yds) in stats.items():
        g.stats[pid] = {f: 0.0 for f in v1.STAT_FIELDS} | {
            "carries": float(carries), "targets": float(targets), "rushing_yards": float(rush_yds),
            "receptions": targets * 0.8, "receiving_yards": float(rec_yds),
            "fantasy_points_ppr": rush_yds / 10 + targets * 0.8 + rec_yds / 10}
    g.has_stats = True
    return g


def season(n, absent=()):
    """rb1 is the starter; he misses the weeks in `absent`."""
    games = []
    for w in range(1, n + 1):
        out = w in absent
        statuses = {p: "ACT" for p in POS} | ({"rb1": "INA"} if out else {})
        stats = {"rb2": (6, 2, 24, 12), "rb3": (2, 1, 8, 5), "wr": (0, 8, 0, 70), "qb": (4, 0, 20, 0)}
        if not out:
            stats["rb1"] = (16, 4, 70, 30)
        games.append(game(w, statuses, stats))
    return games


def test_franchise_codes_collapse_every_nflverse_spelling():
    assert {b.franchise(c) for c in ("STL", "SL", "LA")} == {"LA"}
    assert {b.franchise(c) for c in ("SD", "LAC")} == {"LAC"}
    assert {b.franchise(c) for c in ("OAK", "LV")} == {"LV"}
    assert [b.franchise(c) for c in ("ARZ", "BLT", "CLV", "HST", "JAC")] == ["ARI", "BAL", "CLE", "HOU", "JAX"]
    assert b.franchise("KC") == "KC"


def test_name_normalization_ignores_accents_punctuation_and_suffixes():
    assert b.norm_name("Le'Veon Bell") == b.norm_name("LeVeon Bell")
    assert b.norm_name("Todd Gurley II") == b.norm_name("Todd Gurley")
    assert b.norm_name("José Ramírez") == "joseramirez"
    assert b.norm_name(None) == ""


def test_proxy_status():
    assert b.proxy_status("ACT", True) == "ACT"
    assert b.proxy_status("ACT", False) == "INA"
    assert b.proxy_status("RES", False) == "RES"
    assert b.proxy_status("RES", True) == "ACT"
    assert b.proxy_status("CUT", False) == "CUT"


def test_with_proxy_marks_dressed_but_absent_players_inactive():
    g = game(1, {"rb1": "ACT", "rb2": "ACT", "rb3": "RES"}, {"rb2": (6, 2, 24, 12)})
    teams = {"AAA": [g]}
    played = {(2016, 1, "AAA"): {"rb2"}}
    [p] = b.with_proxy(teams, played, set())["AAA"]
    assert p.status == {"rb1": "INA", "rb2": "ACT", "rb3": "RES"}
    assert g.status["rb1"] == "ACT"  # the true-status copy is untouched
    [q] = b.with_proxy(teams, played, {(2016, 1, "AAA")})["AAA"]
    assert q.has_stats is False     # no snap data: the game is dropped, not guessed


def test_first_game_flag_and_lead_back():
    teams = {"AAA": season(12, absent=(10, 11))}
    events = b.annotated_events(teams, (2016,))
    by_week = {e["week"]: e for e in events}
    assert by_week[10]["first_game"] is True
    assert by_week[11]["first_game"] is False
    assert by_week[10]["lead_id"] == "rb2"
    assert b.primary(events) == [by_week[10]]


def test_lead_back_is_scored_after_allocating_the_whole_backfield():
    teams = {"AAA": season(10, absent=(10,))}
    [event] = b.annotated_events(teams, (2016,))
    full = m.predict(event, 1.0, 0.0)["carries"]
    ids = [r["id"] for r in event["rows"]]
    lead = event["lead"]
    assert ids == ["rb2", "rb3"]
    # rb2 gets 6/8 of rb1's 16 carries, not all of them.
    assert full[lead] - event["rows"][lead]["carries_base"] == pytest.approx(16 * 6 / 8)
    err = b.lead_errors(event, (1.0, 0.0))
    assert err["carries"].shape == (1,)
    assert err["carries"][0] == pytest.approx(event["rows"][lead]["actual_carries"] - full[lead])
    assert b.other_errors(event, (1.0, 0.0))["carries"].shape == (1,)


def test_verdict_gates():
    pts = {"mse_ci": [-20.0, -3.0], "mae_ci": [-0.4, 0.3], "events": 200}
    good, bad = {"mse_delta": -10.0}, {"mse_delta": 1.0}
    assert b.verdict(pts, good, 0.5)["verdict"] == "PROMOTE"
    assert b.verdict({**pts, "mse_ci": [-20.0, 0.5]}, good, 0.5)["verdict"] == "NOT_PROMOTED"
    assert b.verdict({**pts, "mae_ci": [-0.4, 0.6]}, good, 0.5)["verdict"] == "NOT_PROMOTED"
    assert b.verdict(pts, bad, 0.5)["verdict"] == "NOT_PROMOTED"
    assert b.verdict({**pts, "events": 149}, good, 0.5)["verdict"] == "INSUFFICIENT"


def test_frozen_setting_matches_the_registration():
    assert b.FROZEN == (0.5, 0.5)
    assert b.MAE_MARGIN == 0.50
    assert b.BLIND_SEASONS == (2014, 2015, 2016, 2017, 2018)
    assert b.GATE_MIN_EVENTS == 150
