"""cfb_line_charts: pure helpers and PNG output on synthetic data (no database)."""
import pytest

from model import cfb_line_charts as lc
from model.cfb_pattern_measures import _ml_eligible

PNG = b"\x89PNG\r\n\x1a\n"


def _game(week, hs, as_, spread, total=50.0, hml=-200, aml=170, h="Home U", a="Away St"):
    return {"week": week, "season_type": "regular", "hclass": "fbs", "aclass": "fbs", "hname": h, "aname": a,
            "home_score": hs, "away_score": as_, "home_spread": spread, "home_ml": hml, "away_ml": aml,
            "vegas_total": total, "neutral_site": False, "matchup_id": week * 100 + hs}


GAMES = [
    _game(1, 31, 10, -7.0),            # home fav covers
    _game(1, 17, 14, -6.5, total=40),  # home fav wins, does not cover
    _game(1, 20, 27, 3.5, hml=150, aml=-170),  # away fav covers
    _game(2, 24, 17, -7.0),            # push
    _game(2, 10, 35, -10.0, total=45),  # upset
    _game(2, 28, 21, -3.0),
]


def test_spread_points_orients_on_the_favorite():
    pts = lc.spread_points(GAMES, week=1)
    assert [(p["line"], p["margin"], p["result"]) for p in pts] == [(7.0, 21, 1), (6.5, 3, -1), (3.5, 7, 1)]
    assert pts[2]["fav"] == "Away St"
    week2 = lc.spread_points(GAMES, week=2)
    assert [p["result"] for p in week2] == [0, -1, 1]


def test_spread_points_skips_non_fbs_and_pickem_without_moneyline():
    fcs = dict(_game(1, 50, 0, -30.0), aclass="fcs")
    pk = dict(_game(1, 21, 20, 0.0), home_ml=None, away_ml=None)
    pk_ml = dict(_game(1, 21, 24, 0.0), home_ml=-120, away_ml=100)
    pts = lc.spread_points([fcs, pk, pk_ml])
    assert len(pts) == 1 and pts[0]["fav"] == "Home U" and pts[0]["margin"] == -3


def test_total_points_and_latest_week():
    tp = lc.total_points(GAMES, week=1)
    assert [(p["line"], p["points"], p["result"]) for p in tp] == [(50.0, 41, -1), (40.0, 31, -1), (50.0, 47, -1)]
    assert lc.latest_completed_week(GAMES) == 2
    # A thin latest week (Thursday games of the next week) falls back to the previous one.
    assert lc.latest_completed_week(GAMES + [_game(3, 30, 3, -14.0)]) == 2
    assert lc.latest_completed_week([]) is None


def _rec(imp, won, week=1):
    return {"imp": imp, "won": won, "g": {"week": week}}


def test_calibration_buckets_edges_and_counts():
    recs = [_rec(0.5, True), _rec(0.59, False), _rec(0.6, True), _rec(0.95, True), _rec(1.0, True), _rec(0.9, False)]
    b = {x["label"]: x for x in lc.calibration_buckets(recs)}
    assert b["50-60%"]["games"] == 2 and b["50-60%"]["won"] == 0.5
    assert b["60-70%"]["games"] == 1
    assert b["70-80%"]["games"] == 0 and b["70-80%"]["won"] is None and b["70-80%"]["ci"] is None
    assert b["90-100%"]["games"] == 3 and b["90-100%"]["wins"] == 2   # 1.0 lands in the top band
    lo, hi = b["90-100%"]["ci"]
    assert 0 < lo < 2 / 3 < hi <= 1


def test_upsets_by_week():
    rows = lc.upsets_by_week([_rec(0.8, True, 1), _rec(0.6, False, 1), _rec(0.7, True, 2)])
    assert [(r["week"], r["games"], r["upsets"]) for r in rows] == [(1, 2, 1), (2, 1, 0)]
    assert rows[0]["expected"] == pytest.approx(0.6)


def test_move_histogram():
    moves = [{"Spread move (pts)": v, "Open hours before kickoff": 20.0} for v in (0, 0, -0.5, 1.0, 2.5, -9.0)]
    moves.append({"Spread move (pts)": 3.0, "Open hours before kickoff": None})   # no open: ignored
    h = lc.move_histogram(moves)
    assert h["counts"][0] == 2 and h["counts"][1] == 1 and h["counts"][2] == 1 and h["counts"][5] == 1
    assert h["counts"][-1] == 1   # 9 points lands in 7+
    assert h["unmoved"] == 2 and h["share_2plus"] == pytest.approx(2 / 6) and h["median"] == 0.75


def _q(book, spread):
    return {"book": book, "spread": spread}


def test_book_outliers_ties_shared_and_exchanges_dropped():
    games = [
        # home favored (median -7): most points on fav = lowest home spread
        [_q("A", -7.5), _q("B", -7.0), _q("C", -6.5), _q("Matchbook", -9.0)],
        # tie at both ends: A and B share most, C and D share fewest
        [_q("A", -3.0), _q("B", -3.0), _q("C", -2.5), _q("D", -2.5)],
        # away favored (median +4): most points on fav = highest home spread
        [_q("A", 4.0), _q("B", 4.5), _q("C", 4.0)],
        # every book agrees: nobody credited
        [_q("A", -1.0), _q("B", -1.0)],
    ]
    out = lc.book_outliers(games, min_games=1)
    rows = {r["book"]: r for r in out["rows"]}
    assert "Matchbook" not in rows
    assert rows["A"]["games"] == 4 and rows["D"]["games"] == 1
    assert rows["A"]["most"] == pytest.approx(1.5)            # game 1 alone, game 2 shared
    assert rows["A"]["fewest"] == pytest.approx(0.5)          # game 3 shared with C
    assert rows["B"]["most"] == pytest.approx(1.5)            # game 2 shared, game 3 alone
    assert rows["C"]["fewest"] == pytest.approx(1.0 + 0.5 + 0.5)
    assert rows["D"]["fewest"] == pytest.approx(0.5)
    dropped = lc.book_outliers(games, min_games=2)
    assert dropped["dropped"] == ["D"] and "D" not in {r["book"] for r in dropped["rows"]}


def test_every_chart_writes_a_png(tmp_path):
    recs = _ml_eligible(GAMES)
    line_moves = [{"Spread move (pts)": v, "Open hours before kickoff": 20.0} for v in (0, 0.5, 2.0, -3.5)]
    quotes = [[_q("A", -7.5), _q("B", -7.0)], [_q("A", 3.0), _q("B", 3.5)]]
    made = {
        "s_w.png": lc.spread_vs_result_chart(GAMES, 1, tmp_path / "s_w.png"),
        "s_all.png": lc.spread_vs_result_chart(GAMES, None, tmp_path / "s_all.png"),
        "t_w.png": lc.total_vs_result_chart(GAMES, 2, tmp_path / "t_w.png"),
        "t_all.png": lc.total_vs_result_chart(GAMES, None, tmp_path / "t_all.png"),
        "u.png": lc.cumulative_upsets_chart(lc.upsets_by_week(recs), tmp_path / "u.png"),
        "c.png": lc.calibration_chart(GAMES, GAMES[:3], tmp_path / "c.png"),
        "h.png": lc.line_move_histogram_chart(line_moves, tmp_path / "h.png"),
        "b.png": lc.book_outlier_chart(lc.book_outliers(quotes, min_games=1), tmp_path / "b.png"),
    }
    assert all(made.values()), made
    for name in made:
        assert (tmp_path / name).read_bytes()[:8] == PNG


def test_charts_skip_without_data(tmp_path):
    assert not lc.spread_vs_result_chart([], 1, tmp_path / "a.png")
    assert not lc.spread_vs_result_chart(GAMES, 9, tmp_path / "a.png")
    assert not lc.total_vs_result_chart([], None, tmp_path / "a.png")
    assert not lc.cumulative_upsets_chart([], tmp_path / "a.png")
    assert not lc.calibration_chart([], [], tmp_path / "a.png")
    assert not lc.line_move_histogram_chart([], tmp_path / "a.png")
    assert not lc.book_outlier_chart({"rows": [], "dropped": []}, tmp_path / "a.png")
    assert not list(tmp_path.iterdir())
