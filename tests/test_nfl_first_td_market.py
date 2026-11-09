import math

from model import nfl_first_td_market as mk


def test_price_matching_by_initial_and_unique_surname():
    names = {"p1": "Kenneth Walker III", "p2": "Marquise Brown", "p3": "Josh Allen", "p4": "Kyren Williams",
             "p5": "Blake Williams"}
    prices = {"Kenneth Walker": 330, "Hollywood Brown": 1600, "Josh Allen": 900, "Kyren Williams": 475,
              "No Scorer": 1200, "Kansas City Chiefs D/ST": 3500}
    m = mk.match_prices(names, prices)
    assert m == {"p1": 330, "p2": 1600, "p3": 900, "p4": 475}  # Blake Williams: surname not unique, not matched


def test_grade_game_records_bets_rank_group_and_opening():
    dist = {"a": 0.20, "b": 0.08, "c": 0.02, "other:home": 0.5, "other:away": 0.14, "non_offense": 0.06}
    names = {"a": "Alpha One", "b": "Beta Two", "c": "Gamma Three"}
    prices = {"Alpha One": 300, "Beta Two": 2000, "Gamma Three": 9000}
    lab = {"outcome": "offense", "side": "home", "pid": "b", "kind": "rec"}
    row = mk.grade_game(dist, names, prices, lab, {"b": "TE"}, {"receiving": "KC", "td": False}, "KC", "NYJ")
    assert [p["pid"] for p in row["priced"]] == ["a", "b", "c"]
    assert row["scorer_rank"] == 2 and math.isclose(row["scorer_prob"], 0.08) and row["scorer_group"] == "TE"
    assert row["receiving_team"] == "KC" and row["receiving_scored_first"] is True and row["opening_drive_td"] is False
    # 0.08 x 21 = 1.68 (edge +68%), 0.02 x 91 = 1.82 (+82%), 0.20 x 4 = 0.80 (-20%)
    s = mk.summarize([{**row, "dk_players": 3}])
    first = s["bets"][0]
    assert first == {"edge_over": 0.0, "bets": 2, "wins": 1, "units": 19.0, "roi": 9.5}
    assert s["scorer_rank"]["top3"] == 1 and s["scorer_groups"] == {"TE": 1}
    assert s["opening"]["receiving_scored_first"] == 1 and s["opening"]["games_without_opening_td"] == 1


def test_scorer_outside_distribution_and_non_offense():
    dist = {"a": 0.3, "other:home": 0.6, "non_offense": 0.1}
    off = mk.grade_game(dist, {"a": "A B"}, {}, {"outcome": "offense", "side": "away", "pid": "z"}, {}, None, "H", "V")
    assert off["scorer_rank"] is None and off["scorer_group"] == "other" and off["receiving_scored_first"] is None
    ret = mk.grade_game(dist, {"a": "A B"}, {}, {"outcome": "non_offense", "side": "home"}, {}, None, "H", "V")
    assert ret["scorer"] is None and ret["scorer_group"] == "return_or_defense"


def test_market_chip_only_when_far_above_the_book():
    assert mk.market_chip(0.149, 5000) is not None  # 14.9% vs 2.0% implied
    assert mk.market_chip(0.20, 360) is None  # close to the book
    assert mk.market_chip(0.10, None) is None
    assert mk.market_chip(0.149, 5000)["code"] == "MARKET_FAR_LOWER"
