import math

from model import nfl_first_td_dk as dk


def _prices():
    prices = {f"Home Player{i}": 900 + 100 * i for i in range(7)}
    prices.update({f"Away Runner{i}": 1000 + 100 * i for i in range(7)})
    prices.update({"No Touchdown": 5000, "Kansas City Chiefs D/ST": 3000, "Arizona Cardinals D/ST": 4000,
                   "New Guy": 6000, "Mystery Man": 8000})
    return prices


def _cands():
    cands = {f"h{i}": {"name": f"H.Player{i}", "team": "KC", "td": i, "vol": 10.0 * i} for i in range(7)}
    cands.update({f"a{i}": {"name": f"Away Runner{i}", "team": "ARI", "td": 0, "vol": 5.0} for i in range(7)})
    return cands


ROSTER = [{"pid": "new1", "name": "New Guy", "team": "AZ"}, {"pid": "x", "name": "Somebody Else", "team": "KC"}]


def test_reading_sums_to_one_and_drops_no_touchdown():
    read = dk.distribution(_prices(), _cands(), ROSTER, "KC", "ARI")
    dist = read["dist"]
    assert math.isclose(sum(dist.values()), 1.0, abs_tol=1e-12)
    implied = {n: 100 / (p + 100) for n, p in _prices().items() if n != "No Touchdown"}
    total = sum(implied.values())
    assert math.isclose(dist["non_offense"], (implied["Kansas City Chiefs D/ST"] + implied["Arizona Cardinals D/ST"]) / total)
    assert math.isclose(dist["h0"], implied["Home Player0"] / total * (1 - dk.UNLISTED_SHARE))
    assert max((p for p in dist if p not in ("non_offense", "other:home", "other:away")), key=dist.get) == "h0"


def test_roster_names_a_listed_player_we_do_not_carry_and_the_rest_goes_to_everyone_else():
    read = dk.distribution(_prices(), _cands(), ROSTER, "KC", "ARI")
    assert read["players"]["new1"] == {"name": "New Guy", "team": "AZ", "american": 6000}
    assert read["unmatched"] == ["Mystery Man"]
    dist = read["dist"]
    implied = {n: 100 / (p + 100) for n, p in _prices().items() if n != "No Touchdown"}
    held = (1 - dist["non_offense"]) - sum(v for k, v in dist.items() if k not in ("non_offense", "other:home", "other:away"))
    assert math.isclose(dist["other:home"] + dist["other:away"], held)
    assert dist["other:home"] + dist["other:away"] > implied["Mystery Man"] / sum(implied.values()) * 0.99
    # the Arizona alias counts New Guy on the away side
    away = sum(dist[p] for p in dist if p.startswith("a")) + dist["new1"]
    home = sum(dist[f"h{i}"] for i in range(7))
    assert math.isclose(read["p_home_first"], home / (home + away))


def test_a_thin_list_is_not_read():
    thin = {k: v for k, v in list(_prices().items())[:5]}
    assert dk.distribution(thin, _cands(), ROSTER, "KC", "ARI") is None


def test_row_is_a_savable_forecast():
    read = dk.distribution(_prices(), _cands(), ROSTER, "KC", "ARI")
    g = {"game_id": "2026_11_ARI_KC", "season": 2026, "week": 11, "away": "ARI", "home": "KC",
         "commence_time": None, "away_implied": 17.0, "home_implied": 27.0}
    row = dk.row(g, read, None, _cands(), 10)
    assert len(row["players"]) == 10 and row["players"][0]["name"] == "H.Player0"
    assert row["players"][0]["prob"] == row["players"][0]["dk_prob"] == read["dist"]["h0"]
    assert math.isclose(row["other_offense"] + row["non_offense"] + sum(read["dist"][p] for p in read["players"]), 1.0)
    assert row["availability"] is None and row["context"]["unmatched"] == ["Mystery Man"]


def test_compare_is_descriptive_until_the_review():
    pairs = [{"ours": 0.10, "dk": 0.20, "ours_top": False, "dk_top": True}] * 3
    out = dk.compare(pairs)
    assert out["games"] == 3 and out["interval"] is None and out["status"] == "descriptive until the review"
    assert math.isclose(out["diff"], math.log(2)) and out["dk_top"] == 3 and out["ours_top"] == 0
    full = dk.compare(pairs * 20)
    assert full["status"] == "review" and full["interval"][0] > 0
    assert dk.compare([]) == {"version": "nfl-first-td-dk-v1", "games": 0, "review_games": 60}


def test_constants_are_frozen():
    assert (dk.VERSION_DK, dk.UNLISTED_SHARE, dk.MIN_PLAYERS, dk.REVIEW_GAMES) == ("nfl-first-td-dk-v1", 0.01, 12, 60)
    assert dk.BOOTSTRAP == {"resamples": 2000, "seed": 20261116}
