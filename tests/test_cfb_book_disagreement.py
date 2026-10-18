from datetime import datetime, timedelta, timezone

from model import cfb_book_disagreement as bd

KICK = datetime(2026, 10, 17, 19, 30, tzinfo=timezone.utc)


def _game(mid=1, week=7, home_score=None, away_score=None, completed=False):
    return {"matchup_id": mid, "week": week, "commence_time": KICK, "home": "Home U", "away": "Road St",
            "home_score": home_score, "away_score": away_score, "completed": completed}


def _book(spread, total, mlh=-150, mla=130, title=None):
    return {"spread_home": spread, "spread_away": -spread, "total_line": total,
            "ml_home": mlh, "ml_away": mla, "title": title}


def _cap(hours_before, spread, total, books=None, mid=1, mlh=-150, mla=130):
    return {"matchup_id": mid, "captured_at": KICK - timedelta(hours=hours_before),
            "home_spread": spread, "vegas_total": total, "vegas_total_raw": None,
            "home_ml": mlh, "away_ml": mla, "books": books}


def test_exchanges_excluded_from_gaps():
    books = {"draftkings": _book(-3, 50, title="DraftKings"), "fanduel": _book(-3.5, 51, title="FanDuel"),
             "matchbook": _book(-10, 60, title="Matchbook"), "betfair_ex": _book(4, 40, title="Betfair")}
    rows = bd.book_gaps([_cap(1, -3, 50, books)], [_game()])
    assert len(rows) == 1
    r = rows[0]
    assert r["Books"] == 2
    assert r["Spread range (pts)"] == 0.5
    assert r["Total range (pts)"] == 1.0
    assert r["Low spread books"] == "FanDuel" and r["High spread books"] == "DraftKings"
    assert "Matchbook" not in (r["Low total books"] + r["High total books"])


def test_gaps_need_two_books_and_use_last_pre_kickoff_capture():
    early = {"a": _book(-3, 50), "b": _book(-6, 50)}
    late = {"a": _book(-3, 50), "b": _book(-4, 50)}
    after = {"a": _book(-3, 50), "b": _book(-20, 50)}
    caps = [_cap(5, -4, 50, early), _cap(1, -3.5, 50, late),
            {**_cap(0, -3, 50, after), "captured_at": KICK + timedelta(minutes=5)}]
    rows = bd.book_gaps(caps, [_game()])
    assert rows[0]["Spread range (pts)"] == 1.0
    assert bd.book_gaps([_cap(1, -3, 50, {"a": _book(-3, 50), "matchbook": _book(-4, 50)})], [_game()]) == []


def test_capture_at_kickoff_is_not_pre_kickoff():
    caps = [_cap(20, -3, 50), {**_cap(0, -9, 55), "captured_at": KICK}]
    r = bd.line_moves(caps, [_game()])[0]
    assert r["Home spread close"] == -3      # the at-kickoff capture is ignored
    assert r["Open hours before kickoff"] is None   # the only capture is the close


def test_open_is_earliest_capture_at_least_12h_out():
    caps = [_cap(30, -2, 48), _cap(13, -3, 49), _cap(11.9, -6, 52), _cap(1, -7, 53)]
    r = bd.line_moves(caps, [_game()])[0]
    assert r["Open hours before kickoff"] == 30.0
    assert r["Home spread open"] == -2 and r["Home spread close"] == -7
    assert r["Spread move (pts)"] == -5.0
    assert r["Moved toward"] == "Home U"
    assert r["Fav spread open"] == -2 and r["Fav spread close"] == -7

    no_open = bd.line_moves([_cap(11, -2, 48), _cap(1, -3, 49)], [_game()])[0]
    assert no_open["Open hours before kickoff"] is None and no_open["Spread move (pts)"] is None


def test_close_vs_open_accuracy_with_ties():
    games = [
        _game(1, home_score=31, away_score=24, completed=True),  # margin +7: close -7 exact, open -3 off by 4
        _game(2, home_score=20, away_score=17, completed=True),  # margin +3: open -3 exact, close -7 off by 4
        _game(3, home_score=24, away_score=20, completed=True),  # spread unchanged: tie
        _game(4),                                                # not played: no verdict
    ]
    caps = []
    for mid, (o, c) in {1: (-3, -7), 2: (-3, -7), 3: (-5, -5), 4: (-3, -7)}.items():
        caps += [_cap(20, o, 50, mid=mid), _cap(1, c, 48, mid=mid)]
    moves = {r["Matchup"] + str(i): r for i, r in enumerate(bd.line_moves(caps, games))}
    by_closer = sorted(str(r["Spread closer"]) for r in moves.values())
    assert by_closer == ["Close", "None", "Open", "Tie"]

    s = bd.move_summary(bd.line_moves(caps, games))[0]
    assert s["Scope"] == "Season" and s["Games"] == 4 and s["Games with open"] == 4
    assert s["Spread decided games"] == 2 and s["Spread ties"] == 1
    assert s["Close beat open, spread (%)"] == 50.0
    # totals: 55, 37, 44 points vs open 50 / close 48 -> close, open, close
    assert s["Total decided games"] == 3 and s["Close beat open, total (%)"] == 66.7
    assert s["Spread moved 2+ (%)"] == 75.0
    assert bd.move_summary(bd.line_moves(caps, games))[1]["Week"] == 7


def test_away_favorite_and_prob():
    caps = [_cap(20, 3, 50, mlh=150, mla=-170), _cap(1, 6, 50, mlh=200, mla=-240)]
    r = bd.line_moves(caps, [_game()])[0]
    assert r["Favorite (close)"] == "Road St"
    assert r["Fav spread open"] == -3 and r["Fav spread close"] == -6
    assert r["Moved toward"] == "Road St"
    assert r["Fav win prob close (%)"] > r["Fav win prob open (%)"] > 50


def test_charts_empty_inputs(tmp_path):
    assert bd.biggest_moves_chart([], tmp_path / "a.png") is False
    assert bd.close_vs_open_chart([], tmp_path / "b.png") is False
