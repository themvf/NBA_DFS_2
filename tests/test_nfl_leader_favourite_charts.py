from datetime import datetime, timedelta, timezone

from model import nfl_leader_favourite_charts as f

K = datetime(2026, 10, 18, 17, tzinfo=timezone.utc)


def _row(gid, metric, ident, dk, ours, anch, origin="prospective", computed=-2, captured=-3):
    return {"game_id": gid, "metric": metric, "identity": ident, "dk_implied_share": dk, "our_share": ours,
            "anchored_share": anch, "origin": origin, "computed_at": K + timedelta(hours=computed),
            "prop_captured_at": K + timedelta(hours=captured)}


def test_latest_pregame_prefers_prospective_and_drops_post_kickoff():
    rows = [_row("g1", "receptions", "a", .5, .4, .45, origin="discovery", computed=5),
            _row("g1", "receptions", "a", .6, .4, .5, computed=-1),
            _row("g1", "receptions", "a", .7, .4, .5, computed=1),
            _row("g2", "receptions", "b", .3, .3, .3, origin="discovery", computed=9)]
    got = f.latest_pregame(rows, {"g1": K, "g2": K})
    assert got["g1"][0]["dk_implied_share"] == .6 and got["g2"][0]["dk_implied_share"] == .3


def test_favourites_and_summary():
    games = {"g1": [_row("g1", "receptions", "a", .6, .4, .5), _row("g1", "receptions", "b", .3, .5, .4),
                    _row("g1", "receptions", "OTHER:KC", .9, .1, .1)]}
    rows = f.favourites(games, lambda gid, metric, mrows: {"a": 0.5, "b": 0.5})
    assert len(rows) == 1 and rows[0]["identity"] == "a" and rows[0]["led"] == 0.5
    s = f.summary(rows)
    assert s["n"] == 1 and s["rate"] == 0.5 and s["dk"] == .6
    assert f.favourites(games, lambda *a: None) == []


def test_chart_writes_png(tmp_path):
    rows = [{"game_id": f"g{i}", "metric": m, "identity": "x", "dk": .2 + i / 20, "ours": .3, "anchored": .3,
             "led": float(i % 2)} for i in range(10) for m in ("rushing_yards", "receptions")]
    assert f.chart(rows, tmp_path / "a.png", "weeks 5-6")
    assert (tmp_path / "a.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not f.chart([], tmp_path / "x.png", "")
