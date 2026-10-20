"""Offline tests for model/nfl_play_charts.py: tiny synthetic play-by-play frames, no downloads."""

import pandas as pd
import pytest

from model import nfl_play_charts as pc

BASE = {"season_type": "REG", "qtr": 1, "down": 1, "ydstogo": 10, "yardline_100": 75,
        "half_seconds_remaining": 1700, "game_seconds_remaining": 3500, "wp": 0.5, "home_wp": 0.5,
        "home_wp_post": 0.5, "fumble": 0, "interception": 0, "qb_scramble": 0, "two_point_attempt": 0,
        "fixed_drive": 1, "desc": "play"}


def frame(rows):
    out = []
    for i, r in enumerate(rows):
        row = {c: None for c in pc.PBP_COLUMNS} | BASE | {"play_id": i + 1} | r
        out.append(row)
    return pd.DataFrame(out, columns=pc.PBP_COLUMNS)


def game(gid="2026_01_A_B", week=1, home="B", away="A", hs=20, as_=10):
    return {"game_id": gid, "week": week, "home_team": home, "away_team": away, "home_score": hs, "away_score": as_}


def play(g, **kw):
    return g | kw


def test_game_results_and_epa_rows():
    g1 = game()
    g2 = game("2026_02_B_A", week=2, home="A", away="B", hs=17, as_=14)
    pbp = frame([
        play(g1, posteam="B", defteam="A", play_type="pass", epa=0.5),
        play(g1, posteam="B", defteam="A", play_type="run", epa=0.1),
        play(g1, posteam="A", defteam="B", play_type="pass", epa=-0.4),
        play(g1, posteam="A", defteam="B", play_type="punt", epa=3.0),       # not a scrimmage play
        play(g2, posteam="A", defteam="B", play_type="run", epa=0.2),
        play(g2, posteam="B", defteam="A", play_type="no_play", epa=-2.0),   # penalty only: ignored
    ])
    games = pc.game_results(pbp)
    assert [(x["home"], x["home_score"]) for x in games] == [("B", 20), ("A", 17)]
    rows = {r["team"]: r for r in pc.epa_rows(pbp, games)}
    assert rows["B"]["off_epa"] == pytest.approx(0.3)
    assert rows["B"]["def_epa"] == pytest.approx(-0.1)          # (-0.4 + 0.2) / 2
    assert rows["B"]["epa_diff"] == pytest.approx(0.4)
    assert rows["B"]["pd_pg"] == pytest.approx((10 - 3) / 2)
    assert rows["A"]["pd_pg"] == pytest.approx(-3.5)


def test_close_game_split_counts_ties_as_half():
    games = [
        {"game_id": "1", "week": 1, "home": "A", "away": "B", "home_score": 21, "away_score": 20},   # close
        {"game_id": "2", "week": 2, "home": "A", "away": "C", "home_score": 10, "away_score": 31},   # not close
        {"game_id": "3", "week": 3, "home": "B", "away": "A", "home_score": 17, "away_score": 17},   # tie
        {"game_id": "4", "week": 4, "home": "C", "away": "A", "home_score": 20, "away_score": 12},   # margin 8: close
    ]
    a = {r["team"]: r for r in pc.close_game_rows(games)}["A"]
    assert (a["close_n"], a["close_w"], a["close_l"], a["close_t"]) == (3, 1.5, 1, 1)
    assert a["close_pct"] == pytest.approx(0.5)
    assert (a["other_n"], a["other_w"]) == (1, 0.0)
    assert pc._record(a["close_w"], a["close_n"], a["close_t"]) == "1-1-1"


def test_turnover_rows_fumble_share_and_margin():
    g1 = game()
    pbp = frame([
        play(g1, posteam="B", defteam="A", play_type="pass", interception=1),
        play(g1, posteam="B", defteam="A", play_type="run", fumble=1, fumbled_1_team="B", fumble_recovery_1_team="B"),
        play(g1, posteam="A", defteam="B", play_type="run", fumble=1, fumbled_1_team="A", fumble_recovery_1_team="B"),
        play(g1, posteam="A", defteam="B", play_type="pass", fumble=1, fumbled_1_team="A", fumble_recovery_1_team="A"),
        play(g1, posteam="A", defteam="B", play_type="pass", fumble=1, fumbled_1_team="A"),   # out of bounds: no recovery
        play(g1, posteam="A", defteam="B", play_type="no_play", interception=1),            # nullified
    ])
    rows = {r["team"]: r for r in pc.turnover_rows(pbp, pc.game_results(pbp))}
    assert rows["B"]["fumbles"] == 3 and rows["B"]["recovered"] == 2
    assert rows["B"]["rec_share"] == pytest.approx(2 / 3)
    assert rows["A"]["rec_share"] == pytest.approx(1 / 3)
    # B: one interception thrown, one fumble taken away -> margin 0; A: one fumble lost, one interception -> 0
    assert (rows["B"]["giveaways"], rows["B"]["takeaways"]) == (1, 1)
    assert (rows["A"]["giveaways"], rows["A"]["takeaways"]) == (1, 1)


def test_fourth_down_filter_and_rows():
    g1 = game()
    fourth = {"down": 4, "ydstogo": 1, "yardline_100": 40, "half_seconds_remaining": 600, "qtr": 2,
              "posteam": "B", "defteam": "A"}
    pbp = frame([
        play(g1, **fourth, play_type="run", fourth_down_converted=1),
        play(g1, **fourth, play_type="punt"),
        play(g1, **(fourth | {"ydstogo": 2}), play_type="pass", fourth_down_converted=0),
        play(g1, **(fourth | {"ydstogo": 3}), play_type="run"),                     # 4th-and-3: out
        play(g1, **(fourth | {"yardline_100": 60}), play_type="punt"),              # own side: out
        play(g1, **(fourth | {"yardline_100": 3}), play_type="field_goal"),         # inside the 5: out
        play(g1, **(fourth | {"half_seconds_remaining": 100}), play_type="run"),    # last two minutes: out
        play(g1, **fourth, play_type="no_play"),                                    # penalty: out
        play(g1, **(fourth | {"posteam": "A", "defteam": "B"}), play_type="field_goal"),
    ])
    assert len(pc.fourth_down_chances(pbp)) == 4
    rows = {r["team"]: r for r in pc.fourth_down_rows(pbp, {"B": "Coach B"})}
    b = rows["B"]
    assert (b["chances"], b["go"], b["converted"]) == (3, 2, 1)
    assert b["go_rate"] == pytest.approx(2 / 3) and b["coach"] == "Coach B"
    assert rows["A"]["go_rate"] == 0 and rows["A"]["conv_rate"] is None


def test_proe_filter():
    g1 = game()
    ok = {"down": 2, "wp": 0.5, "half_seconds_remaining": 900, "qtr": 1, "posteam": "B", "play_type": "pass"}
    pbp = frame([
        play(g1, **ok, pass_oe=10.0),
        play(g1, **(ok | {"play_type": "run"}), pass_oe=-20.0),
        play(g1, **(ok | {"down": 4}), pass_oe=50.0),                    # 4th down: out
        play(g1, **(ok | {"wp": 0.9}), pass_oe=50.0),                    # not in doubt: out
        play(g1, **(ok | {"half_seconds_remaining": 60}), pass_oe=50.0),  # last two minutes: out
        play(g1, **ok, pass_oe=None),                                    # no model value: out
    ])
    assert len(pc.proe_filter(pbp)) == 2
    (row,) = pc.proe_rows(pbp)
    assert row["proe"] == pytest.approx(-5.0) and row["pass_rate"] == pytest.approx(0.5)


def test_red_zone_trip_points_and_two_minute_points():
    g1 = game()
    pbp = frame([
        # Drive 1 (B): reaches the 12, touchdown and extra point = 7.
        play(g1, fixed_drive=1, posteam="B", play_type="pass", yardline_100=12, posteam_score=0, posteam_score_post=0,
             total_home_score=0, total_away_score=0),
        play(g1, fixed_drive=1, posteam="B", play_type="run", yardline_100=2, posteam_score=0, posteam_score_post=6,
             total_home_score=6, total_away_score=0, qtr=2, half_seconds_remaining=110),
        play(g1, fixed_drive=1, posteam="B", play_type="extra_point", yardline_100=15, posteam_score=6,
             posteam_score_post=7, total_home_score=7, total_away_score=0, qtr=2, half_seconds_remaining=105),
        # Drive 2 (A): never inside the 20; the try-like spot of the 15 on a kickoff must not count.
        play(g1, fixed_drive=2, posteam="A", play_type="pass", yardline_100=45, posteam_score=0, posteam_score_post=0,
             total_home_score=7, total_away_score=0, qtr=3, half_seconds_remaining=1700),
        play(g1, fixed_drive=2, posteam="A", play_type="field_goal", yardline_100=30, posteam_score=0,
             posteam_score_post=3, total_home_score=7, total_away_score=3, qtr=4, half_seconds_remaining=90),
        # Drive 3 (A): field goal from the 8 = a 3-point trip.
        play(g1, fixed_drive=3, posteam="A", play_type="field_goal", yardline_100=8, posteam_score=3,
             posteam_score_post=6, total_home_score=7, total_away_score=6, qtr=4, half_seconds_remaining=500),
    ])
    trips = pc.red_zone_trips(pbp)
    assert sorted((t["team"], t["points"]) for t in trips) == [("A", 3), ("B", 7)]
    late = pc.two_minute_points(pbp)
    assert late == {"B": 7, "A": 3}
    rows = {r["team"]: r for r in pc.red_zone_rows(pbp, pc.game_results(pbp))}
    assert rows["B"]["rz_ppt"] == 7 and rows["B"]["td_trips"] == 1 and rows["A"]["td_trips"] == 0


def test_two_minute_credits_try_to_touchdown_time():
    g1 = game()
    pbp = frame([
        play(g1, posteam="B", play_type="pass", qtr=4, half_seconds_remaining=130, total_home_score=6,
             total_away_score=0),
        play(g1, posteam="B", play_type="extra_point", qtr=4, half_seconds_remaining=118, total_home_score=7,
             total_away_score=0),
    ])
    assert pc.two_minute_points(pbp) == {}


def _usage_pbp():
    rows = []
    for week in range(1, 7):
        g = game(f"2026_{week:02d}_A_B", week=week)
        # Team B throws 10 passes a week: X gets 2 early and 6 late; Y gets 4 a week but misses week 5 entirely.
        x = 2 if week <= 3 else 6
        y = 0 if week == 5 else 4
        other = 10 - x - y
        for pid, n in (("X", x), ("Y", y), ("Z", other)):
            rows += [play(g, posteam="B", play_type="pass", receiver_player_id=pid, receiver_player_name=pid)] * n
        rows += [play(g, posteam="B", play_type="run", rusher_player_id="R", rusher_player_name="R", qb_scramble=0)] * 5
        rows += [play(g, posteam="B", play_type="run", rusher_player_id="Q", rusher_player_name="Q", qb_scramble=1)] * 3
    return frame(rows)


def test_usage_split_uses_active_weeks_only():
    rows = {r["player"]: r for r in pc.usage_rows(_usage_pbp(), "targets", min_volume=1)}
    assert rows["X"]["early_share"] == pytest.approx(0.2) and rows["X"]["late_share"] == pytest.approx(0.6)
    assert rows["X"]["change"] == pytest.approx(0.4)
    # Y's absent week is ignored rather than counted as zero share.
    assert rows["Y"]["late_share"] == pytest.approx(0.4) and rows["Y"]["late_games"] == 2
    carries = {r["player"]: r for r in pc.usage_rows(_usage_pbp(), "carries", min_volume=1)}
    assert set(carries) == {"R"} and carries["R"]["early_share"] == pytest.approx(1.0)


def test_wp_swing_games_picks_lowest_winner_wp():
    g1 = game(hs=20, as_=17)          # home won
    g2 = game("2026_01_C_D", home="D", away="C", hs=10, as_=14)   # away won
    pbp = frame([
        play(g1, posteam="B", play_type="pass", home_wp=0.6, home_wp_post=0.3, game_seconds_remaining=3000),
        play(g1, posteam="B", play_type="run", home_wp=0.3, home_wp_post=0.9, game_seconds_remaining=100, qtr=4,
             half_seconds_remaining=100),
        play(g2, posteam="C", play_type="pass", home_wp=0.95, home_wp_post=0.5, game_seconds_remaining=200, qtr=4),
        play(g2, posteam="C", play_type="pass", home_wp=0.5, home_wp_post=0.1, game_seconds_remaining=50, qtr=4),
    ])
    out = pc.wp_swing_games(pbp, pc.game_results(pbp), n=2)
    assert [g["winner"] for g in out] == ["C", "B"]
    assert out[0]["low"] == pytest.approx(0.05) and out[1]["low"] == pytest.approx(0.3)
    top = out[1]["swings"][0]
    assert top["winner_delta"] == pytest.approx(0.6) and top["clock"] == "Q4 1:40"
    assert out[0]["swings"][0]["winner_delta"] == pytest.approx(0.45)


def test_short_desc_trims_play_text():
    d = "(3:01) (Shotgun) 5-T.Huntley pass short right to 89-M.Andrews to ATL 4 for 9 yards (51-J.Ivey). ** Injury"
    assert pc._short_desc(d) == "T.Huntley pass short right to M.Andrews to ATL 4 for 9 yards."


def test_every_chart_writes_a_png(tmp_path):
    pbp = _usage_pbp()
    games = pc.game_results(pbp)
    note = "6 games, weeks 1-6"
    epa = [{"team": t, "epa_diff": d, "pd_pg": p, "off_epa": 0, "def_epa": 0, "plays": 50, "games": 6}
           for t, d, p in (("A", 0.1, 5), ("B", -0.1, -4), ("C", 0.0, 2), ("D", 0.2, 3))]
    assert pc.luck_vs_skill_chart(epa, tmp_path / "a.png", note)
    swing = [{**game(), "home": "B", "away": "A", "week": 1, "low": 0.05, "winner": "B", "t": [0, 30, 60],
              "wp": [0.5, 0.2, 1.0], "swings": [{"t": 30, "wp": 0.2, "winner_delta": -0.3, "clock": "Q2 0:00",
                                                  "desc": "pass"}], "home_score": 20, "away_score": 10}]
    assert pc.wp_swings_chart(swing, tmp_path / "b.png", note)
    close = pc.close_game_rows([{"game_id": "1", "week": 1, "home": "A", "away": "B", "home_score": 21,
                                 "away_score": 20}, {"game_id": "2", "week": 2, "home": "C", "away": "D",
                                                     "home_score": 30, "away_score": 3}])
    assert pc.close_games_chart(close, tmp_path / "c.png", note)
    turn = [{"team": t, "margin_pg": m, "rec_share": s, "fumbles": 6, "recovered": 3, "games": 6,
             "giveaways": 1, "takeaways": 2} for t, m, s in (("A", 0.5, 0.7), ("B", -0.5, 0.3), ("C", 0, 0.5))]
    assert pc.turnover_luck_chart(turn, tmp_path / "d.png", note)
    fourth = [{"team": "A", "chances": 4, "go": 3, "converted": 2, "go_rate": 0.75, "conv_rate": 2 / 3,
               "coach": "Coach A"}, {"team": "B", "chances": 2, "go": 0, "converted": 0, "go_rate": 0.0,
                                     "conv_rate": None, "coach": None}]
    assert pc.fourth_down_chart(fourth, tmp_path / "e.png", note, teams=["A", "B", "C"])
    proe = [{"team": "A", "plays": 100, "proe": 4.0, "pass_rate": 0.6}, {"team": "B", "plays": 90, "proe": -3.0,
                                                                         "pass_rate": 0.5}]
    assert pc.pass_rate_oe_chart(proe, tmp_path / "f.png", note)
    rz = [{"team": t, "games": 6, "trips": 10, "rz_points": p, "rz_ppt": p / 10, "td_trips": 5, "two_min": q,
           "two_min_pg": q / 6} for t, p, q in (("A", 50, 30), ("B", 40, 12))]
    assert pc.red_zone_two_minute_chart(rz, tmp_path / "g.png", note)
    t = pc.usage_rows(pbp, "targets", min_volume=1)
    c = pc.usage_rows(pbp, "carries", min_volume=1)
    assert pc.usage_shifts_chart(t, c, tmp_path / "h.png", note)
    for name in "abcdefgh":
        assert (tmp_path / f"{name}.png").stat().st_size > 5000
    assert games and len(games) == 6


def test_charts_skip_without_data(tmp_path):
    assert not pc.luck_vs_skill_chart([], tmp_path / "x.png", "n")
    assert not pc.wp_swings_chart([], tmp_path / "x.png", "n")
    assert not pc.usage_shifts_chart([], [], tmp_path / "x.png", "n")
    assert not (tmp_path / "x.png").exists()
