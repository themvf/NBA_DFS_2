from datetime import datetime, timedelta, timezone

import pytest

from model import nfl_schedule_market_charts as m


def _row(week=1, home="KC", away="DEN", hs=27, as_=20, spread=6.5, hml=-280, aml=230, total=44.5, season=2026,
         location="Home", stadium=None, roof="outdoors", temp=60, wind=5, referee="Ref A", rest=(7, 7),
         played=True):
    return {"season": season, "week": week, "game_id": f"{season}_{week:02d}_{away}_{home}", "game_type": "REG",
            "home_team": home, "away_team": away, "home_score": hs if played else None,
            "away_score": as_ if played else None, "spread_line": spread, "home_moneyline": hml,
            "away_moneyline": aml, "total_line": total, "location": location, "stadium": stadium,
            "roof": roof, "temp": temp, "wind": wind, "referee": referee, "home_rest": rest[0],
            "away_rest": rest[1], "gameday": f"{season}-09-{7 + 7 * (week - 1) % 21 + 1:02d}"}


def test_haversine_and_zones():
    # New York to Los Angeles is about 2,450 miles.
    assert m.haversine_miles((40.7128, -74.0060), (34.0522, -118.2437)) == pytest.approx(2445, abs=15)
    assert m.haversine_miles((1, 2), (1, 2)) == 0
    day = datetime(2026, 9, 10).date()
    assert m.zones_between("America/New_York", "America/Los_Angeles", day) == 3
    assert m.zones_between("America/Los_Angeles", "Australia/Melbourne", day) == 7   # the short way round
    assert m.zones_between("America/Phoenix", "America/Los_Angeles", day) == 0       # Arizona keeps standard time


def test_team_travel_round_trips_and_abroad_double_header():
    rows = [_row(week=1, home="KC", away="DEN"), _row(week=2, home="DEN", away="KC"),
            _row(week=3, home="JAX", away="HOU", location="Neutral", stadium="Tottenham Hotspur Stadium"),
            _row(week=4, home="HOU", away="JAX", location="Neutral", stadium="Wembley Stadium", rest=(4, 4)),
            _row(week=5, home="LV", away="KC", location="Neutral", stadium="Mystery Bowl")]
    teams, unmatched = m.team_travel(rows, 2026)
    by = {t["team"]: t for t in teams}
    kc_den = m.haversine_miles(m.TEAM_SITES["KC"][:2], m.TEAM_SITES["DEN"][:2])
    kc_lv = m.haversine_miles(m.TEAM_SITES["KC"][:2], m.TEAM_SITES["LV"][:2])
    # KC: one round trip to Denver plus the unmatched neutral venue placed at LV's stadium.
    assert by["KC"]["miles"] == pytest.approx(2 * kc_den + 2 * kc_lv)
    assert by["KC"]["zones"] == 2 * 1 + 2 * 2
    assert unmatched == ["Mystery Bowl"]
    # JAX plays twice in London in a row: one trip there, a short hop, one trip back.
    jax = m.TEAM_SITES["JAX"][:2]
    tot, wem = m.NEUTRAL_SITES["Tottenham Hotspur Stadium"][:2], m.NEUTRAL_SITES["Wembley Stadium"][:2]
    assert by["JAX"]["miles"] == pytest.approx(m.haversine_miles(jax, tot) + m.haversine_miles(tot, wem)
                                               + m.haversine_miles(wem, jax))
    assert by["JAX"]["short_weeks"] == 1 and by["HOU"]["short_weeks"] == 1
    assert by["DEN"]["road"] == 1


def test_weather_filters_outdoor_games_with_weather():
    rows = [_row(roof="outdoors", hs=30, as_=20, total=44.5), _row(week=2, roof="dome"),
            _row(week=3, roof="open", temp=None), _row(week=4, roof="open", wind=20, hs=10, as_=10),
            _row(week=5, played=False), _row(season=2022)]
    games = m.weather_games(rows, (2026,))
    assert [g["week"] for g in games] == [1, 4]
    assert games[0]["diff"] == pytest.approx(5.5) and games[1]["diff"] == pytest.approx(-24.5)
    assert m.outdoor_coverage(rows, (2026,)) == (3, 2)
    bins = {b["label"]: b for b in m.bin_means(games, "wind", m.WIND_BINS)}
    assert bins["0-5 mph"]["n"] == 1 and bins["16+"]["mean"] == pytest.approx(-24.5)
    assert bins["16+"]["over"] == 0


def test_referee_aggregation():
    rows = [_row(week=1, referee="A", hs=30, as_=20, total=44.5), _row(week=2, referee="A", hs=10, as_=10),
            _row(week=3, referee="B", hs=20, as_=24, total=44), _row(week=4, referee=None)]
    pens = m.penalties_from_pbp([{"game_id": rows[0]["game_id"], "penalty": 1},
                                 {"game_id": rows[0]["game_id"], "penalty": 1},
                                 {"game_id": rows[0]["game_id"], "penalty": 0},
                                 {"game_id": rows[1]["game_id"], "penalty": 0}])
    assert pens == {rows[0]["game_id"]: 2, rows[1]["game_id"]: 0}
    refs = {r["referee"]: r for r in m.referee_rows(rows, pens, (2026,))}
    assert refs["A"]["games"] == 2 and refs["A"]["pen_pg"] == 1.0 and refs["A"]["ppg"] == 35
    assert refs["A"]["over_rate"] == 0.5
    assert refs["B"]["pen_pg"] is None and refs["B"]["decided"] == 0     # 44 on a total of 44 is a push
    assert m.missing_referees(rows, (2026,)) == 1


def test_rating_fit_recovers_known_ratings():
    true = {"A": 6.0, "B": 2.0, "C": -1.0, "D": -7.0}
    hfa = 1.5
    teams = list(true)
    games = []
    for week in range(1, 7):
        for i, h in enumerate(teams):
            for a in teams[i + 1:]:
                home, away = (h, a) if week % 2 else (a, h)
                games.append({"week": week, "home": home, "away": away, "neutral": False,
                              "margin": hfa + true[home] - true[away]})
    f = m.fit_week(games, 6, ridge=1e-6)
    mean = sum(f["ratings"].values()) / 4
    for t, v in true.items():
        assert f["ratings"][t] - mean == pytest.approx(v - sum(true.values()) / 4, abs=0.01)
    assert f["hfa"] == pytest.approx(hfa, abs=0.01)
    rows, fits = m.weekly_ratings(games)
    assert [x["week"] for x in fits] == list(range(1, 7)) and len(rows) == 24


def test_line_move_classification():
    rows = [_row(week=1, home="KC", away="DEN", hs=27, as_=20), _row(week=2, home="BUF", away="MIA", hs=17, as_=20),
            _row(week=3, home="NE", away="NYJ", hs=24, as_=21)]
    t = datetime(2026, 9, 1, tzinfo=timezone.utc)
    hist = [
        # Toward KC (home): -3 to -6.5; KC won by 7, covered.
        {"game_id": rows[0]["game_id"], "open_spread": -3.0, "close_spread": -6.5, "first_at": t,
         "last_at": t + timedelta(days=6), "kickoff": t + timedelta(days=7), "captures": 10},
        # Toward MIA (away): BUF -7 to -4; MIA won by 3, covered.
        {"game_id": rows[1]["game_id"], "open_spread": -7.0, "close_spread": -4.0},
        # Toward NE by exactly 1.0 point: below the 1.5 threshold.
        {"game_id": rows[2]["game_id"], "open_spread": -2.0, "close_spread": -3.0},
    ]
    moves = m.line_moves(hist, rows)
    assert [mv["team"] for mv in moves] == ["KC", "MIA"]   # biggest move first
    kc, mia = moves
    assert mia["open"] == 7.0 and mia["close"] == 4.0 and mia["cover"] == 1 and not mia["home"]
    assert kc["open"] == -3.0 and kc["close"] == -6.5 and kc["cover"] == 1 and kc["margin"] == 7
    push = m.line_moves([{"game_id": rows[2]["game_id"], "open_spread": -1.0, "close_spread": -3.0}], rows)
    assert push[0]["cover"] == 0
    miss = m.line_moves([{"game_id": rows[0]["game_id"], "open_spread": -6.0, "close_spread": -8.0}], rows)
    assert miss[0]["cover"] == -1


def test_upset_selection():
    rows = [_row(week=1, hml=-280, aml=230, hs=20, as_=27),        # away dog +230 wins
            _row(week=2, hml=150, aml=-170, hs=24, as_=21),        # home dog +150 wins
            _row(week=3, hml=-120, aml=100, hs=30, as_=10),        # favorite wins
            _row(week=4, hml=-110, aml=-110, hs=10, as_=20),       # level prices: skipped
            _row(week=5, hs=17, as_=17)]                           # tie: skipped
    ups, decided = m.upsets(rows, 2026)
    assert decided == 3 and len(ups) == 2
    assert ups[0]["dog"] == "DEN" and ups[0]["payout"] == 230 and ups[0]["score"] == "27-20"
    assert ups[1]["dog"] == "KC" and ups[1]["home"]
    assert m.payout(-200) == pytest.approx(50)


def test_charts_write_pngs(tmp_path):
    rows = []
    teams = ["KC", "DEN", "LV", "LAC", "BUF", "MIA", "NE", "NYJ"]
    for week in range(1, 6):
        for i in range(0, 8, 2):
            h, a = teams[(i + week) % 8], teams[(i + week + 1) % 8]
            hs, as_ = 17 + (week * 3 + i) % 14, 14 + (week * 5 + i) % 17
            rows.append(_row(week=week, home=h, away=a, hs=hs, as_=as_, spread=(i - 3) * 1.5,
                             hml=-150 if i < 4 else 140, aml=130 if i < 4 else -160, total=42 + i,
                             wind=(week * 4 + i) % 22, temp=30 + week * 9 + i, referee=f"Ref {i % 3}"))
    rows.append(_row(week=6, home="MIA", away="KC", location="Neutral", stadium="Estadio Banorte"))
    teams_t, unmatched = m.team_travel(rows, 2026)
    assert m.travel_chart(teams_t, tmp_path / "t.png", 2026, unmatched)
    assert m.weather_chart(m.weather_games(rows, (2026,)), tmp_path / "w.png", 2026, (len(rows), len(rows)))
    pens = {r["game_id"]: 10 + (r["week"] % 4) for r in rows}
    assert m.referee_chart(m.referee_rows(rows, pens, (2026,)), tmp_path / "r.png", (2026,), no_ref=1)
    ratings, fits = m.weekly_ratings(m.rating_games(rows, 2026))
    assert m.ratings_chart(ratings, tmp_path / "m.png", 2026, fits)
    t = datetime(2026, 9, 1, tzinfo=timezone.utc)
    hist = [{"game_id": r["game_id"], "open_spread": -r["spread_line"], "close_spread": -r["spread_line"] - 2,
             "first_at": t, "last_at": t + timedelta(days=6), "kickoff": t + timedelta(days=7)} for r in rows]
    moves = m.line_moves(hist, rows)
    assert m.line_moves_chart(moves, tmp_path / "l.png", 2026, hist, len(hist))
    ups, decided = m.upsets(rows, 2026)
    assert m.upsets_chart(ups, tmp_path / "u.png", 2026, decided)
    for name in ("t", "w", "r", "m", "l", "u"):
        assert (tmp_path / f"{name}.png").stat().st_size > 10_000
    assert not m.upsets_chart([], tmp_path / "x.png", 2026)
    assert not m.line_moves_chart([], tmp_path / "x.png", 2026)
    assert not m.weather_chart([], tmp_path / "x.png", 2026)
    assert not (tmp_path / "x.png").exists()


def test_latest_season():
    assert m.latest_season([_row(season=2025), _row(season=2026, played=False)]) == 2025
