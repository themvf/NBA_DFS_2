"""The digest must never invent a number the report card did not score."""
from model.nfl_dfs_review_digest import accuracy, delta, members_of, movers, render

def row(position="WR", d=None, name="P", actual_present=True, **extra):
    actual = None if d is None or not actual_present else 10 + d
    return {"variant": "production", "position": position, "name": name, "team": "GB",
            "forecast": {"mean": 10.0}, "actual": actual, "error": d,
            "interval_hit": None, "overdue": False, **extra}

def report(rows, **extra):
    return {"season": 2026, "week": 1, "evaluated_at": "t", "scheduled_games": 16,
            "completed_games": 15, "missing_policy": "policy", "rows": rows, **extra}

def test_unscored_rows_never_rank_or_count():
    pending = row(d=None)
    stale = row(d=-9, actual_present=False)          # error present, actual missing
    assert delta(pending) is None and delta(stale) is None
    exceeded, disappointed = movers([pending, stale, row(d=5)])
    assert [r["error"] for r in exceeded] == [5]
    assert disappointed == []
    assert accuracy([pending, stale, row(d=5)])["n"] == 1

def test_lists_split_on_sign_so_they_cannot_overlap():
    rows = [row(d=3, name="up"), row(d=-4, name="down"), row(d=0, name="flat")]
    exceeded, disappointed = movers(rows)
    assert [r["name"] for r in exceeded] == ["up"]
    assert [r["name"] for r in disappointed] == ["down"]
    assert "flat" not in {r["name"] for r in exceeded + disappointed}

def test_ordering_and_limit():
    rows = [row(d=1), row(d=9), row(d=5), row(d=-2), row(d=-8)]
    exceeded, disappointed = movers(rows, limit=2)
    assert [r["error"] for r in exceeded] == [9, 5]
    assert [r["error"] for r in disappointed] == [-8, -2]

def test_flex_is_an_eligibility_not_a_position():
    rows = [row("RB"), row("WR"), row("TE"), row("QB"), row("DST")]
    assert {r["position"] for r in members_of(rows, "FLEX")} == {"RB", "WR", "TE"}
    assert [r["position"] for r in members_of(rows, "QB")] == ["QB"]

def test_empty_and_unscored_reports_say_so_rather_than_printing_zero():
    assert "No `production` rows" in render(report([]))
    text = render(report([row(d=None)]))
    assert "Nothing is scored yet" in text
    assert "0.00" not in text                      # never a fake MAE
    assert accuracy([row(d=None)])["mae"] is None  # None, not 0

def test_render_includes_both_boards_and_position_split():
    rows = [row("QB", 8, "Q"), row("WR", -6, "W"), row("TE", 2, "T"), row("DST", -1, "D")]
    text = render(report(rows))
    assert "Top 10 — exceeded projection" in text and "Top 10 — disappointed" in text
    assert "| Q (QB · GB) |" in text and "+8.0" in text and "-6.0" in text
    assert "| FLEX |" in text                       # WR+TE roll up
    assert "policy" in text                         # missing-policy carried through

def test_variant_is_respected():
    rows = [row("QB", 5), {**row("QB", 99), "variant": "opportunity"}]
    assert accuracy([r for r in rows if r["variant"] == "production"])["n"] == 1
    assert "99" not in render(report(rows), variant="production")

def test_non_finite_values_do_not_reach_the_output():
    assert delta({"actual": 1, "error": float("nan")}) is None
    assert delta({"actual": 1, "error": float("inf")}) is None

def test_a_delta_that_rounds_to_zero_carries_no_sign():
    from model.nfl_dfs_review_digest import _signed
    assert _signed(-0.04) == "0.0" and _signed(0.04) == "0.0"   # symmetric
    assert _signed(0.0) == "0.0" and _signed(-0.0) == "0.0"     # never "-0.0"
    assert _signed(0.06) == "+0.1" and _signed(-0.06) == "-0.1"
    assert _signed(None) == "—"
