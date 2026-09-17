"""The anchors are the point: they decide whether the headline can be believed."""
from datetime import datetime, timedelta, timezone

from model.nfl_inactive_gap import proxy_is_credible, render, status_before_kickoff, summarize

KICK = datetime(2026, 9, 20, 17, 0, tzinfo=timezone.utc)

def rows(status, n, no_show, projected=10.0, week=1):
    return ([{"status": status, "played": False, "projected": projected, "week": week}] * no_show +
            [{"status": status, "played": True, "projected": projected, "week": week}] * (n - no_show))

def test_counts_and_rates():
    s = summarize(rows("QUESTIONABLE", 20, 5))
    q = s["buckets"][0]
    assert q["n"] == 20 and q["no_show"] == 5 and q["no_show_rate"] == 0.25

def test_only_no_shows_count_toward_wasted_points():
    s = summarize(rows("QUESTIONABLE", 10, 3, projected=12.0))
    assert s["buckets"][0]["projected_on_no_shows"] == 36.0

def test_anchors_are_ordered_first():
    s = summarize(rows("QUESTIONABLE", 5, 1) + rows("OUT", 5, 5) + rows("HEALTHY", 5, 0))
    assert [b["status"] for b in s["buckets"]] == ["OUT", "QUESTIONABLE", "HEALTHY"]

def test_per_week_divides_by_weeks_seen_not_rows():
    s = summarize(rows("QUESTIONABLE", 4, 2, projected=10.0, week=1) +
                  rows("QUESTIONABLE", 4, 2, projected=10.0, week=2))
    q = s["buckets"][0]
    assert q["weeks"] == 2 and q["projected_on_no_shows"] == 40.0 and q["points_per_week"] == 20.0

# ── the credibility guard ──────────────────────────────────────────────
def test_healthy_out_anchors_behaving_means_credible():
    ok, _ = proxy_is_credible(summarize(rows("OUT", 40, 40) + rows("HEALTHY", 200, 20))["buckets"])
    assert ok

def test_out_players_showing_stat_lines_invalidates_the_proxy():
    """If OUT players are recording stats, the join is wrong — say so loudly."""
    ok, why = proxy_is_credible(summarize(rows("OUT", 40, 20))["buckets"])
    assert not ok and "expected near 100%" in why

def test_healthy_players_mostly_missing_means_we_measured_bench_time():
    ok, why = proxy_is_credible(summarize(rows("OUT", 40, 40) + rows("HEALTHY", 100, 80))["buckets"])
    assert not ok and "bench time" in why

def test_too_few_out_players_to_anchor():
    ok, why = proxy_is_credible(summarize(rows("OUT", 3, 3))["buckets"])
    assert not ok and "too few OUT" in why

def test_render_withholds_the_headline_when_the_proxy_is_broken():
    text = render(summarize(rows("OUT", 40, 20) + rows("QUESTIONABLE", 30, 9)), 2026, [1])
    assert "Do not read the Questionable row yet" in text
    assert "never recorded a stat line" not in text

def test_render_states_the_headline_when_anchors_hold():
    text = render(summarize(rows("OUT", 40, 40) + rows("HEALTHY", 200, 10) +
                            rows("QUESTIONABLE", 40, 10, projected=9.0)), 2026, [1, 2])
    assert "10 of 40 Questionable players (25%) never recorded a stat line" in text
    assert "weeks 1–2" in text
    assert "a fraction of this" in text, "the reachable share must be qualified"

def test_render_says_so_when_there_is_nothing_to_measure():
    assert "Nothing to measure" in render(summarize([]), 2026, [])

# ── same pre-kickoff rule as the projection uses ───────────────────────
def test_status_is_the_newest_one_before_that_players_kickoff():
    caps = [{"status": "OUT", "captured_at": KICK - timedelta(days=2)},
            {"status": "QUESTIONABLE", "captured_at": KICK - timedelta(hours=3)},
            {"status": "HEALTHY", "captured_at": KICK + timedelta(hours=2)}]
    assert status_before_kickoff(caps, KICK) == "QUESTIONABLE"

def test_no_kickoff_or_no_captures_yields_nothing():
    assert status_before_kickoff([{"status": "OUT", "captured_at": KICK}], None) is None
    assert status_before_kickoff([], KICK) is None
