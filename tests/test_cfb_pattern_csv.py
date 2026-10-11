import csv

from model import cfb_pattern_csv as cc


def _row(trigger, date, result, pnl, price=1.9091, away="A", home="B"):
    return {"trigger": trigger, "game_date": date, "commence_time": date + "T20:00:00+00:00", "result": result,
            "pnl_units": pnl, "price": price, "away": away, "home": home, "selection": f"{home} -7",
            "away_score": 10, "home_score": 24, "matchup_id": 1}


ROWS = [
    _row("T3_small_fav_dog_spread", "2026-10-09", "won", 0.9091),
    _row("T6_mid_fav_spread", "2026-10-10", "lost", -1.0),
    _row("T1_g5_mid_fav_ml", "2026-10-10", "won", 0.3333, price=1.3333),
]


def test_patterns_table_has_all_six_and_numeric_roi():
    t = cc.patterns_table(ROWS)
    assert [r["Pattern"] for r in t] == ["T1", "T2", "T3", "T4", "T5", "T6"]
    t3 = next(r for r in t if r["Pattern"] == "T3")
    assert t3["Record"] == "1-0" and t3["Units"] == 0.91 and t3["ROI (%)"] == 90.9
    assert next(r for r in t if r["Pattern"] == "T2")["ROI (%)"] is None


def test_results_prices_are_american():
    t = cc.results_table(ROWS)
    assert [r["Price"] for r in t] == ["-110", "-300", "-110"]
    assert t[0]["Date"] == "2026-10-09"


def test_cumulative_units_run_by_date():
    t = cc.cumulative_table(ROWS)
    assert [r["Date"] for r in t] == ["2026-10-09", "2026-10-10"]
    assert t[-1]["T3"] == 0.91 and t[-1]["T6"] == -1.0 and t[-1]["All patterns"] == 0.24


def test_write_csv_blanks_missing_values(tmp_path):
    p = tmp_path / "x.csv"
    assert cc.write_csv(p, [{"a": 1, "b": None}]) == 1
    assert list(csv.DictReader(p.open(encoding="utf-8"))) == [{"a": "1", "b": ""}]
