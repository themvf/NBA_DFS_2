import pytest

from model import nfl_home_division_chart as c


def _g(gid, hs, as_, hd, ad, hm=-150, am=130):
    return {"game_id": gid, "week": 5, "home_score": hs, "away_score": as_, "home_ml": hm, "away_ml": am,
            "home_div": hd, "away_div": ad}


def test_market_home_removes_the_margin():
    assert c.market_home(-110, -110) == pytest.approx(0.5)
    assert c.market_home(None, 120) is None


def test_division_rows_count_home_road_ties_and_skip_neutral_sites():
    rows = c.division_rows([
        _g("a", 20, 10, "NFC South", "AFC East"),       # South home win
        _g("b", 10, 20, "NFC South", "AFC East"),       # South home loss, East road win
        _g("c", 17, 17, "NFC South", "AFC East"),       # tie: neither
        _g("d", 24, 3, "AFC East", "NFC South"),        # East home win, South road loss
        _g("2026_07_PIT_NO", 3, 30, "NFC South", "AFC North"),  # neutral site: dropped
    ])
    by = {r["division"]: r for r in rows}
    s = by["NFC South"]
    assert (s["w"], s["l"], s["t"], s["road_w"], s["road_l"]) == (1, 1, 1, 0, 1)
    assert s["share"] == pytest.approx(0.5)
    assert s["expected_wins"] == pytest.approx(2 * s["expected"])
    assert "AFC North" not in by
    assert rows[0]["division"] == "AFC East"           # 1-0 at home sorts first


def test_chart_renders(tmp_path):
    rows = c.division_rows([_g("a", 20, 10, "NFC South", "AFC East"), _g("b", 10, 20, "AFC East", "NFC South")])
    assert c.chart(rows, tmp_path / "d.png", 2026, "weeks 1-7")
    assert not c.chart([], tmp_path / "e.png", 2026, "")


def test_team_rows_split_home_and_road_for_chosen_divisions(tmp_path):
    def g(gid, home, away, hs, as_, hd, ad):
        return {**_g(gid, hs, as_, hd, ad, hm=-110, am=-110), "home": home, "away": away}
    rows = c.team_rows([g("a", "TB", "NYJ", 10, 20, "NFC South", "AFC East"),
                        g("b", "BUF", "TB", 10, 20, "AFC East", "NFC South"),
                        g("c", "TB", "NO", 17, 17, "NFC South", "NFC South")], ("AFC South", "NFC South"))
    by = {r["team"]: r for r in rows}
    assert set(by) == {"TB", "NO"}
    tb = by["TB"]
    assert (tb["home"]["w"], tb["home"]["l"], tb["home"]["t"]) == (0, 1, 1)
    assert (tb["road"]["w"], tb["road"]["l"]) == (1, 0)
    assert tb["road"]["expected"] == pytest.approx(0.5) and by["NO"]["road"]["share"] is None
    assert c.team_chart(rows, tmp_path / "s.png", 2026, "weeks 1-7")



def test_missing_wins_chart_renders(tmp_path):
    def g(gid, home, away, hs, as_):
        return {**_g(gid, hs, as_, "NFC South", "AFC East"), "home": home, "away": away}
    rows = c.team_rows([g("a", "TB", "NYJ", 10, 20), g("b", "NO", "BUF", 20, 10)], ("NFC South",))
    assert c.missing_wins_chart(rows, tmp_path / "m.png", 2026, "weeks 1-7")
    assert not c.missing_wins_chart([], tmp_path / "n.png", 2026, "")
