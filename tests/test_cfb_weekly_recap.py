from datetime import datetime, timezone

from model import cfb_weekly_recap as wr

_ID = iter(range(1, 10_000))


def game(week=1, hs=24, as_=21, spread=-3.5, hml=-160, aml=140, total=50.5, hclass="fbs", aclass="fbs",
         hname="Home", aname="Away", neutral=False, season_type="regular", day=6):
    return {
        "matchup_id": next(_ID), "week": week, "season_type": season_type,
        "home_score": hs, "away_score": as_, "home_spread": spread, "home_ml": hml, "away_ml": aml,
        "vegas_total": total, "hclass": hclass, "aclass": aclass, "hname": hname, "aname": aname,
        "neutral_site": neutral, "commence_time": datetime(2026, 9, day, 20, 0, tzinfo=timezone.utc),
    }


def test_upset_detects_away_underdog_win_with_price_and_probability():
    g = game(hs=17, as_=24, spread=-10.5, hml=-400, aml=310, hname="Bama", aname="Vandy")
    [r] = wr.upsets([g])
    assert r["Winner"] == "Vandy" and r["Loser"] == "Bama"
    assert r["Winner moneyline"] == 310 and r["Winner price"] == "+310"
    assert r["Final"] == "24-17" and r["Winner spread"] == 10.5 and r["Winner venue"] == "away"
    assert 18 < r["Winner implied (%)"] < 25


def test_favorite_win_tie_and_level_price_are_not_upsets():
    assert wr.upsets([game(hs=30, as_=10)]) == []
    assert wr.upsets([game(hs=20, as_=20)]) == []
    assert wr.upsets([game(hs=10, as_=20, hml=-110, aml=-110)]) == []
    assert wr.upsets([game(hs=10, as_=20, hml=None)]) == []


def test_upsets_sorted_by_probability_within_week_and_fcs_flagged():
    a = game(week=2, hs=10, as_=14, hml=-200, aml=170)
    b = game(week=2, hs=10, as_=14, hml=-1000, aml=650, aclass="fcs")
    c = game(week=1, hs=10, as_=14, hml=-150, aml=130)
    rows = wr.upsets([a, b, c])
    assert [r["Week"] for r in rows] == [1, 2, 2]
    assert rows[1]["Winner moneyline"] == 650 and rows[1]["Matchup"] == "FBS vs FCS"
    assert rows[1]["Winner implied (%)"] < rows[2]["Winner implied (%)"]


def test_hook_spread_at_exactly_half_point_and_push():
    half = game(hs=24, as_=21, spread=-3.5, total=70.5)      # fav by 3 vs 3.5: dog covers by the hook
    push = game(hs=24, as_=21, spread=-3.0, total=70.5)      # push
    away_fav = game(hs=20, as_=24, spread=3.5, total=70.5)   # away fav by 4 vs 3.5: fav covers by the hook
    miss = game(hs=28, as_=21, spread=-3.5, total=70.5)      # covered by 3.5, not a hook
    rows = wr.hook_games([half, push, away_fav, miss])
    by_id = {r["matchup_id"]: r for r in rows}
    assert by_id[half["matchup_id"]]["Went to"] == "Away covered" and by_id[half["matchup_id"]]["Miss"] == -0.5
    assert by_id[push["matchup_id"]]["Went to"] == "push"
    assert by_id[away_fav["matchup_id"]]["Went to"] == "Away covered"
    assert by_id[away_fav["matchup_id"]]["Line"] == "Away -3.5"
    assert miss["matchup_id"] not in by_id
    assert all(r["Bet"] == "Spread" for r in rows)


def test_hook_total_half_point_push_and_both_bets():
    over = game(hs=26, as_=25, spread=-14.0, total=50.5)    # 51 vs 50.5: over by the hook
    push = game(hs=25, as_=25, spread=-14.0, total=50.0)    # tie final, 50 vs 50: push; spread miss 14 not a hook
    both = game(hs=24, as_=21, spread=-3.5, total=45.5)     # spread by the hook and total 45 vs 45.5: under
    rows = wr.hook_games([over, push, both])
    tot = {r["matchup_id"]: r for r in rows if r["Bet"] == "Total"}
    assert tot[over["matchup_id"]]["Went to"] == "Over"
    assert tot[push["matchup_id"]]["Went to"] == "push"
    assert tot[both["matchup_id"]]["Went to"] == "Under"
    assert sum(r["matchup_id"] == both["matchup_id"] for r in rows) == 2


def test_total_misses_top_five_by_absolute_miss_both_directions():
    gs = [game(week=3, hs=s, as_=0, total=40.0) for s in (40, 70, 10, 55, 41, 44, 21)]
    rows = wr.total_misses(gs)
    assert [abs(r["Miss"]) for r in rows] == [30.0, 30.0, 19.0, 15.0, 4.0]
    assert sorted(r["Miss"] for r in rows[:2]) == [-30.0, 30.0]
    assert [r["Rank"] for r in rows] == [1, 2, 3, 4, 5]
    assert {r["Direction"] for r in rows} == {"Over", "Under"}


def test_week_summary_groups_by_week_and_keeps_fcs_out_of_rates():
    gs = [
        game(week=1, hs=30, as_=10, spread=-7.5, total=39.5),          # fav SU win, covers, over
        game(week=1, hs=10, as_=14, spread=-3.5, total=30.5),          # upset, dog covers, under
        game(week=1, hs=7, as_=10, aclass="fcs", hml=-2000, aml=1000),  # FCS upset, not in rates
        game(week=2, hs=21, as_=20, spread=-3.0, total=41.0),          # fav win, ATS loss, push total
        game(week=1, season_type="postseason", hs=0, as_=50),          # ignored
    ]
    w1, w2 = wr.week_summary(gs)
    assert w1["Week"] == 1 and w1["Games"] == 2 and w1["Fav SU record"] == "1-1" and w1["Fav SU (%)"] == 50.0
    assert w1["Fav ATS record"] == "1-1" and w1["Overs record"] == "1-1"
    assert w1["Upsets"] == 1 and w1["FCS upsets"] == 1 and w1["FBS vs FCS games"] == 1
    assert "[FCS]" in w1["Biggest upset"]
    assert w2["Fav ATS record"] == "0-1" and w2["Overs record"] == "0-0-1" and w2["Overs (%)"] is None
    assert w2["Upsets"] == 0 and w2["Biggest upset"] is None


def test_charts_return_false_on_empty(tmp_path):
    assert wr.upset_chart([], 1, tmp_path / "u.png") is False
    assert wr.week_summary_chart([], tmp_path / "w.png") is False


def test_charts_write_files(tmp_path):
    gs = [game(week=1, hs=10, as_=14, hml=-300, aml=240), game(week=2, hs=30, as_=10)]
    assert wr.upset_chart(wr.upsets(gs), 1, tmp_path / "u.png")
    assert wr.upset_chart(wr.upsets(gs), 2, tmp_path / "u2.png") is False
    assert wr.week_summary_chart(wr.week_summary(gs), tmp_path / "w.png")
    assert (tmp_path / "u.png").exists() and (tmp_path / "w.png").exists()
