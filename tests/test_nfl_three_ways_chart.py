from model import nfl_model_vs_market_chart as mv
from model import nfl_three_ways_chart as t


def test_classes_and_bloc_counts():
    assert [t.lean_class(p) for p in (0.7, 0.6, 0.52, 0.4, 0.3)] == [
        "home_strong", "home", "toss", "away", "away_strong"]
    assert [t.result_class(*s) for s in ((27, 10), (20, 17), (17, 17), (14, 17), (3, 24))] == [
        "home_strong", "home", "tie", "away", "away_strong"]
    assert t.bloc_counts(["home", "toss", "tie", "away_strong", "home_strong"]) == (2, 2, 1)


def test_seat_positions_count_and_left_to_right():
    seats = t.seat_positions(47)
    assert len(seats) == 47
    assert seats[0][0] < 0 < seats[-1][0]


def _g(hs, as_, m, k, total, spread):
    return {"week": 5, "home": "H", "away": "A", "home_score": hs, "away_score": as_,
            "model_home": m, "market_home": k, "total": total, "spread": spread}


def test_slices_and_chart(tmp_path):
    rows = mv.game_rows([_g(20, 10, 0.70, 0.60, 41.5, 3.5), _g(10, 20, 0.45, 0.50, 48.5, 7.5),
                         _g(17, 17, 0.55, 0.50, 44.5, 1.0)])
    names = {n: len(r) for n, r in t.slices(rows)}
    assert names["All games"] == 3 and names["Total under 43"] == 1 and names["Spread 7+"] == 1
    s = t.slice_stats("All games", rows)
    assert s["n"] == 2 and s["lean_n"] == 2 and s["lo"] <= s["diff"] <= s["hi"]
    assert t.chart(rows, tmp_path / "x.png", 2026, "week 5")
    assert not t.chart([], tmp_path / "y.png", 2026, "")
