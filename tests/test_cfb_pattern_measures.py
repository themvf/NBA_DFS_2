"""Descriptive measurements for the CFB pattern watch (cfb-pattern-measures-v1)."""

from datetime import datetime, timedelta, timezone

import pytest

from model import cfb_pattern_measures as m

KICK = datetime(2026, 10, 17, 23, 30, tzinfo=timezone.utc)


def _g(week, home_score, away_score, spread=-7.0, hml=-300, aml=240, total=50.0, hclass="fbs", aclass="fbs"):
    return {"week": week, "season_type": "regular", "home_score": home_score, "away_score": away_score,
            "home_spread": spread, "home_ml": hml, "away_ml": aml, "vegas_total": total, "hclass": hclass, "aclass": aclass}


def test_weekly_base_rates_counts_only_fbs_games_with_lines_and_measures_excess():
    games = [_g(1, 30, 10), _g(1, 10, 30), _g(1, 28, 24), _g(2, 35, 0),
             _g(1, 40, 0, aclass="fcs"),            # excluded: not FBS vs FBS
             dict(_g(1, 20, 10), home_ml=None)]      # excluded: no moneyline
    rates = {r["week"]: r for r in m.weekly_base_rates(games)}
    w1 = rates[1]
    assert w1["games"] == 3
    assert w1["fav_su"] == pytest.approx(2 / 3)
    assert w1["excess_pp"] == pytest.approx((2 / 3 - w1["implied"]) * 100)
    assert w1["fav_ats"] == pytest.approx(1 / 3)         # vs -7: 30-10 covers, 10-30 and 28-24 do not
    assert w1["within_7"] == pytest.approx(1 / 3)        # only the 28-24 game
    assert rates[2]["games"] == 1


def test_game_counts_reports_games_behind_rows():
    rows = [{"matchup_id": 1, "trigger": "T1_g5_mid_fav_ml"}, {"matchup_id": 1, "trigger": "T6_mid_fav_spread"},
            {"matchup_id": 1, "trigger": "T5_sat_evening_g5_fav_ml"}, {"matchup_id": 2, "trigger": "T6_mid_fav_spread"}]
    gc = m.game_counts(rows)
    assert gc == {"rows": 4, "games": 2, "games_with_multiple_triggers": 1, "max_triggers_per_game": 3,
                  "per_trigger_games": {"T1_g5_mid_fav_ml": 1, "T6_mid_fav_spread": 2, "T5_sat_evening_g5_fav_ml": 1}}


def _row(trigger, home_spread, total, home_score, away_score, result, pnl, price=1.9091):
    return {"trigger": trigger, "matchup_id": 9, "home": "Home U", "away": "Away St", "home_spread": home_spread,
            "vegas_total": total, "home_ml": -400, "away_ml": 320, "home_score": home_score, "away_score": away_score,
            "result": result, "pnl_units": pnl, "price": price}


def _sel(trigger, home_spread, total, selection, best=None):
    return {"trigger": trigger, "matchup_id": 9, "home": "Home U", "away": "Away St", "home_spread": home_spread,
            "vegas_total": total, "home_ml": -400, "away_ml": 320, "selection": selection, "consensus_decimal": 1.9091,
            "best_retail": best, "commence_time": KICK.isoformat(), "frozen_at": (KICK - timedelta(hours=6)).isoformat()}


def test_freeze_vs_close_signs_points_from_the_picks_side_and_regrades_at_the_frozen_line():
    # favorite spread frozen at -9, closed -8.5, favorite won by 9: close-grade WIN, frozen-grade PUSH
    rows = [_row("T6_mid_fav_spread", -8.5, 50.0, 30, 21, "won", 0.9091)]
    sels = [_sel("T6_mid_fav_spread", -9.0, 50.0, "Home U -9", {"book": "fanduel", "line": -9.0, "price": -105, "decimal": 1.952})]
    out = m.freeze_vs_close(rows, sels)
    i = out["items"][0]
    assert i["points_clv"] == pytest.approx(-0.5)          # laid half a point more than the close
    assert (i["close_result"], i["freeze_result"]) == ("won", "push")
    assert out["summary"]["result_changed"] == 1 and out["summary"]["median_lead_minutes"] == 360
    assert i["best_retail_pnl"] == 0.0                      # push at the frozen line returns the stake

    # dog spread frozen +3.5, closed +3: took half a point more than the close
    dog = m.freeze_vs_close([_row("T3_small_fav_dog_spread", -3.0, 50.0, 24, 21, "push", 0.0)],
                            [_sel("T3_small_fav_dog_spread", -3.5, 50.0, "Away St +3.5")])
    assert dog["items"][0]["points_clv"] == pytest.approx(0.5) and dog["items"][0]["freeze_result"] == "won"

    # over frozen at 49.5, closed 50.5: bought the over a point low
    ov = m.freeze_vs_close([_row("T4_big_fav_over", -15.0, 50.5, 30, 20, "lost", -1.0)],
                           [_sel("T4_big_fav_over", -15.0, 49.5, "Over 49.5")])
    assert ov["items"][0]["points_clv"] == pytest.approx(1.0) and ov["items"][0]["freeze_result"] == "won"


def test_freeze_vs_close_skips_ungraded_selections():
    assert m.freeze_vs_close([], [_sel("T6_mid_fav_spread", -9.0, 50.0, "Home U -9")])["summary"]["n"] == 0


def _card(status, trigger="T4_big_fav_over", mid=5):
    return {"status": status, "trigger": trigger, "matchup_id": mid, "away": "A", "home": "H", "commence_time": KICK,
            "minutes_since_capture": 2749, "late_reasons": [], "frozen": None}


def test_events_are_written_once_per_trigger_game_status_and_runs_record_zero():
    now = KICK - timedelta(hours=3)
    keys: set = set()
    first = m.new_events([_card("stale"), _card("current"), _card("watch", mid=6)], keys, now)
    assert [(e["status"], e["matchup_id"]) for e in first] == [("stale", 5), ("watch", 6)]
    assert m.new_events([_card("stale")], keys, now) == []          # same event is not written twice
    run = m.run_record([_card("current")], 0, 0, now)
    assert run["status_counts"]["watch"] == 0 and run["status_counts"]["stale"] == 0   # zero is recorded, not omitted
    assert m.event_summary(first, [run]) == {"runs": 1, "last_run": now.isoformat(), "events": 2,
                                             "by_status": {"stale": 1, "watch": 1}}


def test_publish_module_imports():
    # CI runs Python 3.11; importing catches syntax newer Pythons accept
    # (a backslash inside an f-string expression broke the first live run).
    import model.cfb_pattern_publish as publish
    assert callable(publish.render)


def _split_game(home_ml, away_ml, hs, as_, spread=-7.0, total=50.0, week=1, cls=("fbs", "fbs")):
    return {"week": week, "season_type": "regular", "home_score": hs, "away_score": as_, "home_spread": spread,
            "home_ml": home_ml, "away_ml": away_ml, "vegas_total": total, "hclass": cls[0], "aclass": cls[1]}


def test_market_splits_grade_the_moneyline_favorite():
    from model import cfb_pattern_measures as m
    games = [
        _split_game(-200, 170, 30, 20, spread=-5.5, total=45.0),   # home fav wins: +0.5
        _split_game(150, -180, 28, 21, spread=2.5, total=44.5),    # away fav loses: -1
        _split_game(-110, -110, 24, 17),                           # level prices: no favorite, skipped
        _split_game(-300, 250, 35, 10, cls=("fbs", "fcs")),        # FCS game: excluded
    ]
    rows = {r["split"]: r for r in m.market_splits(games)}
    a = rows["All moneyline favorites"]
    assert (a["games"], a["wins"], a["losses"]) == (2, 1, 1)
    assert round(a["units"], 4) == -0.5 and round(a["roi"], 4) == -0.25
    # The underdog side of the same two games: the home dog lost (-1), the away-favored game's dog won at +150 (+1.5).
    assert round(a["dog_units"], 4) == 0.5 and round(a["dog_roi"], 4) == 0.25
    assert rows["Total 45 or higher"]["games"] == 1 and rows["Total 45 or higher"]["wins"] == 1
    assert rows["Spread 2.5 or less"]["games"] == 1 and rows["Spread 2.5 or less"]["losses"] == 1
    assert rows["Spread 7 to 13.5"]["games"] == 0
    assert rows["Spread 14 or more"]["games"] == 0
    seven = {r["split"]: r for r in m.market_splits([_split_game(-400, 300, 17, 20, spread=-10.0)])}
    assert seven["Spread 7 to 13.5"]["dog_units"] == 3.0 and seven["Spread 14 or more"]["games"] == 0


def _rows(b, key):
    return {r["group"] if r["column"] is None else (r["group"], r["column"]): r for r in b[key]}


def test_market_breakdown_bands_partition_the_games():
    from model import cfb_pattern_measures as m
    from model import cfb_pattern_csv as cc
    games = [
        _split_game(-120, 100, 21, 20, spread=-1.5, total=44.5),   # spread 0-2.5, total under 45
        _split_game(-250, 200, 31, 24, spread=-6.5, total=52.0),   # spread 3-6.5, 45-54.5
        _split_game(150, -180, 28, 21, spread=7.0, total=55.0),    # away fav loses; 7-13.5, 55+
        _split_game(-2000, 1100, 49, 7, spread=-24.0, total=None), # 14+, no total
    ]
    b = m.market_breakdown(games)
    assert b["overall"]["games"] == 4
    assert [r["games"] for r in b["by_spread"]] == [1, 1, 1, 1]
    assert [r["games"] for r in b["by_total"]] == [1, 1, 1]
    assert sum(r["games"] for r in b["x_spread_total"]) == 3 and len(b["x_spread_total"]) == 12
    cell = _rows(b, "x_spread_total")[("Spread 7 to 13.5", "Total 55 or more")]
    assert (cell["wins"], cell["losses"]) == (0, 1) and round(cell["dog_units"], 4) == 1.5
    # The bands add up to the whole.
    assert round(sum(r["units"] for r in b["by_spread"]), 6) == round(b["overall"]["units"], 6)
    table = cc.market_breakdown_table(b)
    assert table[0]["Split"] == "All games" and table[0]["Level"] == 1
    assert [r["Level name"] for r in table if r["Level"] == 2] == ["By venue"] * 3
    assert all(r["Grid"] for r in table if r["Level name"] == "Spread and total")
    assert "Fav ATS record" in table[0] and "Over-under record" in table[0]


def test_market_breakdown_venue_groups():
    from model import cfb_pattern_measures as m
    neutral = {**_split_game(-300, 250, 35, 10, spread=-9.5), "neutral_site": True}
    games = [
        _split_game(-200, 170, 30, 20, spread=-5.5),   # home favorite wins
        _split_game(150, -180, 28, 21, spread=3.5),    # away favorite loses
        neutral,
    ]
    b = m.market_breakdown(games)
    v = _rows(b, "by_venue")
    assert (v["Home favorite"]["games"], v["Home favorite"]["wins"]) == (1, 1)
    assert (v["Away favorite"]["games"], v["Away favorite"]["losses"]) == (1, 1)
    assert v["Neutral site"]["games"] == 1
    assert len(b["x_venue_spread"]) == 12 and sum(r["games"] for r in b["x_venue_spread"]) == 3


def test_market_breakdown_grades_spread_and_total():
    from model import cfb_pattern_measures as m
    games = [
        _split_game(-200, 170, 30, 20, spread=-7.0, total=50.0),   # fav by 10 covers -7; 50 total -> push
        _split_game(-200, 170, 27, 20, spread=-7.0, total=40.0),   # fav by 7: push; 47 > 40 over
        _split_game(150, -180, 20, 24, spread=6.5, total=50.0),    # away fav by 4 misses -6.5; 44 under
    ]
    r = m.market_breakdown(games)["overall"]
    assert (r["fav_covers"], r["dog_covers"], r["ats_pushes"]) == (1, 1, 1)
    assert r["cover_rate"] == 0.5
    assert round(r["fav_ats_units"], 4) == round(100 / 110 - 1, 4)
    assert (r["overs"], r["unders"], r["total_pushes"], r["total_games"]) == (1, 1, 1, 3)


def test_market_breakdown_context_cuts():
    from datetime import datetime, timezone
    from model import cfb_pattern_measures as m

    def g(mid, home, away, hs, as_, when, home_ml=-200, away_ml=170, **kw):
        return {**_split_game(home_ml, away_ml, hs, as_), "matchup_id": mid, "home_team_id": home,
                "away_team_id": away, "commence_time": when, **kw}

    sat = lambda d, h: datetime(2026, 9, d, h, 0, tzinfo=timezone.utc)
    games = [
        g(1, 10, 20, 14, 21, sat(5, 16)),                       # team 10 loses week 1 (noon ET)
        g(2, 30, 40, 28, 3, sat(5, 23)),                        # team 30 wins week 1 (7 pm ET)
        # Week 3: team 10 (off a loss, 14 days rest) hosts team 30 (7 days rest, off its week-2 game).
        g(3, 30, 50, 21, 20, sat(12, 20)),
        g(4, 10, 30, 31, 14, sat(19, 20), conference_game=True, hconf="Sun Belt", aconf="SEC",
          hname="Troy", aname="Alabama", home_line_scores=[0, 3, 14, 14], away_line_scores=[7, 7, 0, 0]),
    ]
    m.attach_context(games, {4: (-150, 130)})
    b = m.market_breakdown(games)
    r4 = [r for r in m._ml_eligible(games) if r["g"]["matchup_id"] == 4][0]
    assert m._last_result(r4) == "Favorite lost last game"
    assert m._rest(r4) == "Favorite had more rest"          # 14 days vs 7
    assert m._half(r4) == "Favorite trailed at half"         # 3 vs 14
    assert m._tier(r4) == "Power vs Group of Five"
    assert m._conference(r4) == "Conference game"
    assert m._kickoff(r4) == "Saturday 2 to 5 pm"            # 20:00 UTC = 4 pm ET
    assert m._move(r4) == "Moved toward favorite"            # -150 open to -200 close
    assert m._price_band(r4) == "-151 to -250"
    # The favorite's first game has no previous result: left out of that cut, not guessed.
    r1 = [r for r in m._ml_eligible(games) if r["g"]["matchup_id"] == 1][0]
    assert m._last_result(r1) is None and m._rest(r1) is None and m._move(r1) is None
    assert sum(r["games"] for r in b["by_last"]) == 2


def test_home_ladder_and_home_dog_prices():
    from model import cfb_pattern_measures as m
    games = [
        _split_game(-2000, 1100, 49, 7, spread=-24.0),               # home favored by 14+
        _split_game(250, -300, 21, 28, spread=8.5),                  # home underdog by 7-13.5 at +250
        _split_game(130, -150, 24, 20, spread=3.0),                  # home dog by 3-6.5 at +130, wins
        {**_split_game(-200, 170, 30, 20, spread=-5.5), "neutral_site": True},   # neutral: left out
    ]
    b = m.market_breakdown(games)
    ladder = _rows(b, "by_home_ladder")
    assert list(ladder) == list(m.HOME_LADDER)
    assert ladder["Home favored by 14 or more"]["games"] == 1
    assert ladder["Home underdog by 7 to 13.5"]["games"] == 1
    assert ladder["Home underdog by 3 to 6.5"]["losses"] == 1        # the favorite (away) lost: home dog won
    assert sum(r["games"] for r in b["by_home_ladder"]) == 3
    prices = _rows(b, "by_home_dog_price")
    assert prices["Home underdog +151 to +300"]["games"] == 1 and prices["Home underdog +100 to +150"]["games"] == 1
    assert sum(r["games"] for r in b["by_home_dog_price"]) == 2


def test_tier_counts_the_old_pac12_as_power_through_2023():
    from model import cfb_pattern_measures as m
    g = {**_split_game(-200, 170, 30, 20), "hconf": "Pac-12", "aconf": "Mountain West", "hname": "Oregon", "aname": "Boise State"}
    r23 = m._ml_eligible([{**g, "season": 2023}])[0]
    r24 = m._ml_eligible([{**g, "season": 2024}])[0]
    assert m._tier(r23) == "Power vs Group of Five"
    assert m._tier(r24) == "Group of Five vs Group of Five"
