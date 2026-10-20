from model import nfl_market_scenarios as ns


def _row(season=2026, week=2, home="KC", away="DEN", hs=27, as_=20, spread=6.5, hml=-280, aml=230,
         total=44.5, location="Home", weekday="Sunday", gametime="13:00", roof="outdoors", div=1,
         home_rest=7, away_rest=7, game_type="REG"):
    return {"season": season, "week": week, "game_id": f"{season}_{week}_{away}_{home}", "game_type": game_type,
            "home_team": home, "away_team": away, "home_score": hs, "away_score": as_, "spread_line": spread,
            "home_moneyline": hml, "away_moneyline": aml, "total_line": total, "location": location,
            "weekday": weekday, "gametime": gametime, "roof": roof, "div_game": div,
            "home_rest": home_rest, "away_rest": away_rest, "gameday": f"{season}-09-{10 + week:02d}"}


def test_every_breakdown_level_has_a_chart():
    names = {n for _, n, _, _ in ns.BREAKDOWN_ORDER if n != "All games"}
    assert names == set(ns.SINGLE_CUTS) | set(ns.GRIDS)


def test_spread_line_is_the_home_margin_and_grading_follows_it():
    # nflverse spread_line +6.5 = home favored by 6.5; home won by 7, so the favorite covered.
    g = ns.build_games([_row()])[0]
    assert g["home_spread"] == -6.5
    r = ns.eligible([g])[0]
    assert r["home_fav"] and r["won"] and r["cover"] == 1 and r["ou"] == 1
    # Home by 6 does not cover -6.5.
    r = ns.eligible(ns.build_games([_row(hs=26)]))[0]
    assert r["won"] and r["cover"] == -1


def test_skips_playoffs_unplayed_ties_and_level_prices():
    rows = [_row(game_type="WC"), _row(hs=None, as_=None), _row(hs=20, as_=20), _row(hml=-110, aml=-110)]
    games = ns.build_games(rows)
    assert len(games) == 2                     # playoff dropped; unplayed dropped
    assert ns.eligible(games) == []            # tie and level prices left out


def test_last_result_comes_from_the_same_season_only():
    rows = [_row(season=2025, week=18, hs=10, as_=30), _row(week=1), _row(week=2, hs=30, as_=10)]
    games = ns.build_games(rows)
    week1 = next(g for g in games if g["season"] == 2026 and g["week"] == 1)
    week2 = next(g for g in games if g["week"] == 2)
    assert week1["home_last_margin"] is None   # last season's game does not carry over
    assert week2["home_last_margin"] == 7


def test_cut_labels():
    tag = lambda **kw: {k: fn(ns.eligible(ns.build_games([_row(**kw)]))[0]) for k, (_, fn) in ns.CUTS.items()}
    t = tag()
    assert t["venue"] == "Home favorite" and t["division"] == "Division game" and t["roof"] == "Outdoors"
    assert t["spread"] == "Spread 4 to 6.5" and t["home_ladder"] == "Home favored by 4 to 6.5"
    assert t["kickoff"] == "Sunday before 4 pm" and t["phase"] == "Weeks 1 to 4"
    assert tag(gametime="16:25")["kickoff"] == "Sunday 4 pm window"
    assert tag(gametime="20:20")["kickoff"] == "Sunday night"
    assert tag(weekday="Thursday", home_rest=4, away_rest=4)["rest"] == "Short week"
    assert tag(home_rest=14)["rest"] == "Favorite had more rest"
    t = tag(spread=-3, hml=150, aml=-170, location="Neutral", roof="dome")
    assert t["venue"] == "Neutral site" and t["home_ladder"] is None and t["roof"] == "Indoors"
    t = tag(spread=-3, hml=150, aml=-170)
    assert t["home_ladder"] == "Home underdog by 3 to 3.5" and t["home_dog_price"] == "Home underdog +100 to +150"


def test_table_and_charts(tmp_path):
    rows = [_row(week=w, hs=20 + w, as_=17) for w in range(1, 8)] + [_row(season=2024, week=w) for w in range(1, 5)]
    games = ns.build_games(rows)
    current, now, prior = ns.split_seasons(games)
    assert current == 2026 and len(now) == 7 and len(prior) == 4
    table = ns.breakdown_table(now)
    assert table[0]["Level name"] == "All games" and table[0]["Games"] == 7
    made = ns.build_charts(games, tmp_path)
    assert made["nfl_scenario_spread.png"] and made["nfl_grid_spread_total.png"]
    assert (tmp_path / "nfl_scenario_spread.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
