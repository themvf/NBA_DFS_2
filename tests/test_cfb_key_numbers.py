"""Key-number tables and chart from synthetic games (no database)."""
from __future__ import annotations

from model import cfb_key_numbers as kn


def g(h, a, league="cfb", **kw):
    return {"home_score": h, "away_score": a, "league": league, "season": 2024, **kw}


def test_margin_distribution_counts_shares_and_cumulative():
    games = [g(24, 21), g(21, 24), g(14, 7), g(70, 0), g(10, 10, "nfl"), g(20, 17, "nfl")]
    rows = kn.margin_distribution(games)
    assert len(rows) == kn.MAX_MARGIN + 1
    by = {r["Margin"]: r for r in rows}
    assert by["3"]["CFB games"] == 2 and by["3"]["CFB share (%)"] == 50.0
    assert by["7"]["CFB cumulative (%)"] == 75.0
    assert by["41+"]["CFB games"] == 1 and by["41+"]["CFB cumulative (%)"] == 100.0
    # the NFL tie has no margin row and is not in the denominator
    assert by["3"]["NFL games"] == 1 and by["3"]["NFL share (%)"] == 100.0


def test_margin_distribution_without_nfl_leaves_nfl_columns_empty():
    rows = kn.margin_distribution([g(10, 7)])
    assert rows[0]["NFL games"] is None and rows[2]["CFB games"] == 1


def test_margin_distribution_empty():
    assert kn.margin_distribution([]) == []


def test_key_number_table_ranks_and_ratio():
    games = [g(10, 7)] * 6 + [g(14, 7)] * 4 + [g(3, 0, "nfl")] * 8 + [g(7, 0, "nfl")] * 2
    rows = kn.key_number_table(games, top=2)
    assert [r["Margin"] for r in rows] == [3, 7]
    three = rows[0]
    assert three["CFB rank"] == 1 and three["CFB share (%)"] == 60.0
    assert three["NFL share (%)"] == 80.0 and three["CFB vs NFL"] == 0.75


def test_key_number_table_unions_each_leagues_top():
    games = [g(10, 7)] * 5 + [g(10, 9)] * 3 + [g(6, 0, "nfl")] * 4
    margins = {r["Margin"] for r in kn.key_number_table(games, top=1)}
    assert margins == {3, 6}   # 6 matters only in the NFL sample, still shown
    six = next(r for r in kn.key_number_table(games, top=1) if r["Margin"] == 6)
    assert six["CFB games"] == 0 and six["CFB vs NFL"] == 0.0


def test_key_number_table_needs_cfb():
    assert kn.key_number_table([g(3, 0, "nfl")]) == []


def test_spread_landing():
    games = [
        g(24, 21, home_spread=-3.0),    # push at 3
        g(27, 21, home_spread=-3.0),    # home covers by 3
        g(20, 21, home_spread=-3.0),    # away covers by 4
        g(30, 10, home_spread=-7.5),    # covers by 12.5
        g(14, 21, home_spread=7.0),     # push at 7 (home dog loses by 7)
        g(10, 0, home_spread=None),     # no line: left out
    ]
    rows = kn.spread_landing(games)
    m = {(r["Measure"], r["Line"]): r for r in rows}
    within = m[("Final margin within 1 point of the spread", "All lines")]
    assert within["Games"] == 5 and within["Count"] == 2
    close = m[("Cover decided by 3 points or less", "All lines (pushes left out)")]
    assert close["Games"] == 3 and close["Count"] == 1
    assert m[("Push at the closing line", "3")]["Count"] == 1
    assert m[("Push at the closing line", "3")]["Games"] == 3
    assert m[("Push at the closing line", "7")]["Share (%)"] == 100.0
    assert m[("Push at the closing line", "10")]["Share (%)"] is None
    allint = m[("Push at the closing line", "All integer lines")]
    assert allint["Games"] == 4 and allint["Count"] == 2   # the 7.5 line is not an integer line


def test_spread_landing_empty():
    assert kn.spread_landing([g(10, 7)]) == []


def test_total_landing_bins():
    games = [g(30, 20, vegas_total=50.0), g(30, 22, vegas_total=50.0), g(10, 10, vegas_total=50.0),
             g(60, 40, vegas_total=50.0), g(1, 1, vegas_total=None)]
    rows = kn.total_landing(games)
    by = {r["Miss (points)"]: r for r in rows}
    assert by["0 to +3"]["Games"] == 2        # exactly on and +2
    assert by["Under by more than 24"]["Games"] == 1   # -30
    assert by["Over by 24 or more"]["Games"] == 1      # +50
    assert rows[-1]["Cumulative (%)"] == 100.0
    assert sum(r["Games"] for r in rows) == 4


def test_total_landing_empty():
    assert kn.total_landing([]) == []


def test_margin_chart(tmp_path):
    games = [g(10, 7)] * 3 + [g(14, 7)] * 2 + [g(3, 0, "nfl")] * 2
    rows = kn.margin_distribution(games)
    out = tmp_path / "m.png"
    assert kn.margin_chart(rows, out, kn.chart_subtitle(games)) is True
    assert out.stat().st_size > 0
    assert kn.margin_chart([], tmp_path / "e.png") is False


def test_chart_subtitle_mentions_spans():
    s = kn.chart_subtitle([g(10, 7), {**g(3, 0, "nfl"), "season": 2025}])
    assert "1 FBS-vs-FBS" in s and "2024-2024" in s and "NFL, 1 regular-season games, 2025-2025" in s
