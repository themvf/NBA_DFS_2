import json
from pathlib import Path

from research import nfl_rushing_underdog_under as study

REG = json.loads((Path(__file__).resolve().parents[1] / "docs" / "nfl-rushing-underdog-under-study.json").read_text())


def q(line, under_price, over=-110):
    return {"line": line, "over": over, "under": under_price}


def row(game_id="2026_11_ARI_KC", key="back", week=11, kickoff="2026-11-22T18:00:00+00:00", actual=40.0, books=None):
    return {"season": 2026, "week": week, "game_id": game_id, "player": "Back", "key": key, "kickoff": kickoff,
            "actual": actual, "books": books or {"draftkings": q(49.5, -115), "fanduel": q(51.5, -112)}}


def test_registration_constants_are_frozen():
    assert REG["study"] == "nfl-rushing-underdog-under-v1" and REG["status"] == "registered"
    assert REG["registered_at"] == "2026-11-15T05:30:00+00:00" and REG["weeks"] == [10, 18]
    assert REG["underdog_points"] == 3.0 and REG["side"] == "under"
    assert REG["books"] == ["draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics"]
    assert REG["floors"] == {"graded_bets": 200, "game_weeks": 7}
    assert REG["bootstrap"] == {"resamples": 2000, "seed": 20261115, "cluster": "game_week", "interval": 0.95}


def test_underdog_by_reads_the_home_spread_from_each_side():
    assert study.underdog_by("2026_11_ARI_KC", "ARI", 14.5) == 14.5   # home favoured by 14.5
    assert study.underdog_by("2026_11_ARI_KC", "KC", 14.5) == -14.5
    assert study.underdog_by("2026_11_ARI_KC", "ARI", None) is None
    assert study.underdog_by("2026_11_ARI_KC", "DEN", 14.5) is None


def test_pick_takes_the_highest_line_then_the_best_price_and_ignores_pinnacle():
    books = {"draftkings": q(49.5, -105), "fanduel": q(51.5, -120), "betmgm": q(51.5, -110), "pinnacle": q(55.5, 100)}
    assert study.pick({"books": books}, REG["books"]) == ("betmgm", 51.5, -110)
    assert study.pick({"books": {"draftkings": q(49.5, -105), "pinnacle": q(55.5, 100)}}, REG["books"]) is None


def test_only_underdogs_of_three_or_more_after_registration_are_bet():
    teams = {("2026_11_ARI_KC", "back"): "ARI", ("2026_11_ARI_KC", "fav"): "KC", ("2026_11_BAL_CAR", "back"): "BAL",
             ("2026_10_WAS_NYG", "back"): "WAS"}
    spreads = {"2026_11_ARI_KC": 14.5, "2026_11_BAL_CAR": 1.5, "2026_10_WAS_NYG": 6.0}
    rows = [row(), row(key="fav"), row(game_id="2026_11_BAL_CAR"),
            row(game_id="2026_10_WAS_NYG", week=10, kickoff="2026-11-13T01:15:00+00:00"),   # before registration
            row(key="nostat", actual=None)]
    bets = study.cohort_bets(rows, teams, spreads, REG)
    assert [(b["game_id"], b["team"], b["book"], b["line"]) for b in bets] == [("2026_11_ARI_KC", "ARI", "fanduel", 51.5)]
    assert bets[0]["result"] == "win" and round(bets[0]["pnl"], 4) == round(100 / 112, 4)


def test_no_verdict_before_the_final_look():
    out = study.summarize([], final=False, registration=REG)
    assert out["verdict_if_first"] is None and out["floors_met"] is False
    assert study.summarize([], final=True, registration=REG)["verdict_if_first"] == "INCONCLUSIVE"
