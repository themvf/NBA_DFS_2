from model import cfb_pattern_csv as cc
from model import cfb_pattern_measures as m
from model import cfb_scenario_charts as sc


def _game(home_ml, away_ml, hs, as_, spread, total=50.0, **kw):
    return {"week": 1, "season_type": "regular", "home_score": hs, "away_score": as_, "home_spread": spread,
            "home_ml": home_ml, "away_ml": away_ml, "vegas_total": total, "hclass": "fbs", "aclass": "fbs", **kw}


GAMES = [
    _game(-200, 170, 30, 20, -7.5),
    _game(150, -180, 28, 21, 4.5, total=44.0),
    _game(-1500, 900, 45, 10, -24.0, total=58.0),
    _game(-130, 110, 20, 24, -2.0, neutral_site=True),
]


def test_every_breakdown_level_has_a_chart():
    names = {n for _, n, _, _ in m.BREAKDOWN_ORDER if n != "All games"}
    assert names == set(sc.SINGLE_CUTS) | set(sc.GRIDS)


def test_home_lens_swaps_sides_for_home_underdogs():
    dog = {"Split": "Home underdog by 3 to 6.5", "ROI (%)": 10.0, "Underdog ROI (%)": -20.0,
           "Over ROI (%)": 5.0, "Under ROI (%)": -5.0, "Games": 30, "Games with total": 30}
    fav = {**dog, "Split": "Home favored by 3 to 6.5"}
    ml, total = sc.MARKETS[0], sc.MARKETS[2]
    assert sc.market_values(dog, ml, home_lens=True)[:2] == (-20.0, 10.0)
    assert sc.market_values(fav, ml, home_lens=True)[:2] == (10.0, -20.0)
    assert sc.market_values(dog, total, home_lens=True)[:2] == (5.0, -5.0)   # totals never swap


def test_charts_write_pngs_and_skip_without_data(tmp_path):
    rows = cc.market_breakdown_table(m.market_breakdown(GAMES))
    prior = {r["Split"]: r for r in rows if r["Level name"] == "By spread"}
    assert sc.scenario_chart(rows, prior, "By spread", tmp_path / "s.png")
    assert sc.scenario_chart(rows, {}, "Home team ladder", tmp_path / "h.png")
    assert sc.grid_chart(rows, "Spread and total", tmp_path / "g.png")
    for name in ("s.png", "h.png", "g.png"):
        assert (tmp_path / name).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not sc.scenario_chart([], {}, "By spread", tmp_path / "x.png")
    assert not sc.grid_chart([], "Spread and total", tmp_path / "y.png")
