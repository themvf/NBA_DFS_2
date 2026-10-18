from model import cfb_pattern_charts as ch
from model import cfb_pattern_csv as cc
from model import cfb_pattern_store as store


def _row(trigger, date, result, pnl):
    return {"trigger": trigger, "game_date": date, "commence_time": date + "T20:00:00+00:00", "result": result,
            "pnl_units": pnl, "price": 1.9091, "away": "A", "home": "B", "selection": "B -7",
            "away_score": 10, "home_score": 24, "matchup_id": 1}


ROWS = [_row("T3_small_fav_dog_spread", "2026-10-09", "won", 0.9091),
        _row("T6_mid_fav_spread", "2026-10-10", "lost", -1.0)]
WEEKS = [{"week": 1, "games": 40, "fav_su": 0.875, "implied": 0.818, "excess_pp": 5.7, "fav_ats": 0.579, "over": 0.564, "within_7": 0.25},
         {"week": 2, "games": 46, "fav_su": 0.783, "implied": 0.758, "excess_pp": 2.4, "fav_ats": 0.444, "over": 0.457, "within_7": 0.391}]


def test_charts_write_pngs(tmp_path):
    assert ch.cumulative_chart(cc.cumulative_table(ROWS), tmp_path / "c.png")
    assert ch.pattern_chart(cc.patterns_table(ROWS), tmp_path / "p.png")
    assert ch.weekly_chart(cc.weekly_table(WEEKS), tmp_path / "w.png")
    for name in ("c.png", "p.png", "w.png"):
        assert (tmp_path / name).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_charts_skip_without_data(tmp_path):
    assert not ch.cumulative_chart([], tmp_path / "c.png")
    assert not ch.pattern_chart([], tmp_path / "p.png")
    assert not ch.weekly_chart([], tmp_path / "w.png")
    assert not list(tmp_path.iterdir())


def test_dataset_digest_ignores_key_order():
    a = {"study_version": "v2", "patterns": [{"Pattern": "T1", "Units": 1.0}]}
    b = {"patterns": [{"Units": 1.0, "Pattern": "T1"}], "study_version": "v2"}
    assert store.dataset_digest(a) == store.dataset_digest(b)
    assert store.dataset_digest(a) != store.dataset_digest({**a, "study_version": "v3"})


def test_roi_band_chart(tmp_path):
    from model import cfb_pattern_measures as m
    g = {"week": 1, "season_type": "regular", "home_score": 30, "away_score": 20, "home_spread": -5.5,
         "home_ml": -200, "away_ml": 170, "vegas_total": 50.0, "hclass": "fbs", "aclass": "fbs"}
    table = cc.market_breakdown_table(m.market_breakdown([g]))
    assert ch.roi_band_chart(table, "By spread", "t", tmp_path / "s.png")
    assert not ch.roi_band_chart([], "By spread", "t", tmp_path / "x.png")
