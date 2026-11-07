import math

from model import first_td_stats as fs


def _g(season, side, outcome="offense", kind="rush", pid="p1", qb=False, home_implied=27.0, away_implied=20.0):
    return {"season": season, "home": "H", "away": "A", "home_implied": home_implied, "away_implied": away_implied,
            "outcome": outcome, "side": side, "kind": kind if outcome == "offense" else None,
            "pid": pid if outcome == "offense" else None, "name": pid, "qb": qb}


def test_base_rates_shares_and_favourite():
    games = [_g(2026, "home"), _g(2026, "away", kind="rec", pid="p2"), _g(2026, "home", outcome="non_offense"),
             _g(2026, "home", qb=True), _g(2026, None, outcome="no_td")]
    row = fs.base_rates(games, [2026])[0]
    assert row["season"] == "2026" and row["games"] == 4  # no-TD game left out
    assert row["rush"] == 0.5 and row["rec"] == 0.25 and row["non_offense"] == 0.25
    assert row["qb_rush"] == 0.5  # one of two rushing first TDs by a passer
    assert row["favourite_first"] == 0.75 and row["home_first"] == 0.75


def test_unknown_qb_is_left_out_not_counted_as_no():
    games = [_g(2023, "home", qb=None), _g(2023, "home", qb=None)]
    row = fs.base_rates(games, [2023])[0]
    assert row["qb_rush"] is None and row["qb_known"] == 0


def test_leaders_counts_players_and_teams():
    games = [_g(2026, "home", pid="x"), _g(2026, "home", pid="x"), _g(2026, "away", pid="y")]
    out = fs.leaders(games, 2026)
    assert out["players"] == [{"name": "x", "team": "H", "firsts": 2}]  # one first TD is not listed
    assert out["teams"][0] == {"team": "H", "firsts": 2, "games": 3}
    assert out["tied"] is None


def test_long_tie_at_the_cut_is_summarised():
    games = [_g(2026, "home", pid="star") for _ in range(3)]
    games += [_g(2026, "home", pid=f"p{i}") for i in range(25) for _ in range(2)]
    out = fs.leaders(games, 2026)
    assert [p["name"] for p in out["players"]] == ["star"]
    assert out["tied"] == {"firsts": 2, "players": 25}


def test_track_record_top_pick_and_calibration():
    rows = [
        {"dist": {"a": 0.20, "b": 0.08, "other:home": 0.6, "non_offense": 0.12}, "scorer": "a", "prob": 0.20},
        {"dist": {"a": 0.18, "b": 0.03, "other:home": 0.7, "non_offense": 0.09}, "scorer": None, "prob": 0.09},
    ]
    r = fs.track_record(rows)
    assert r["games"] == 2 and r["top_pick_hit"] == 0.5
    assert math.isclose(r["top_pick_stated"], 0.19)
    assert math.isclose(r["log_loss"], (-math.log(0.20) - math.log(0.09)) / 2)
    bins = {b["from"]: b for b in r["calibration"]}
    assert bins[0.0]["forecasts"] == 1 and bins[0.05]["forecasts"] == 1
    assert bins[0.15]["forecasts"] == 1 and bins[0.15]["happened"] == 0.0
    assert bins[0.20]["forecasts"] == 1 and bins[0.20]["happened"] == 1.0 and bins[0.20]["to"] is None
    assert fs.track_record([]) is None
