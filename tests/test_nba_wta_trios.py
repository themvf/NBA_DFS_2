from datetime import date

import numpy as np
import pytest

import research.nba_wta_trios as w


def _player(key, line, event="e1", team="A", books=6, curve=None):
    if curve is None:
        curve = [(line - 8.5, 0.9), (line, 0.5), (line + 7.5, 0.15), (line + 12.5, 0.05)]
    return w.PoolPlayer(key=key, player=key.title(), event_id=event, home_team="A", away_team="B", team=team,
                        line=line, p_over=0.5, books=books, ladder_books=4, curve=curve, floor=None, ceiling=None,
                        p30=None, p40=None)


def _pool(n=12):
    out = []
    for i in range(n):
        out.append(_player(f"p{i:02d}", 30.0 - i, event=f"e{i % 4}", team="A" if i % 2 else "B"))
    return out


def test_survival_knots_are_monotone_and_extend_both_ends():
    s, x = w.survival_knots([(10.5, 0.8), (19.5, 0.5), (29.5, 0.12)])
    assert np.all(np.diff(s) > 0) and np.all(np.diff(x) < 0)   # survival ascending maps to points descending
    assert x[-1] == 0.0 and s[-1] == pytest.approx(0.995)       # anchored at zero points
    assert x[0] > 29.5 and s[0] <= w.TAIL_FLOOR                 # tail runs past the last rung
    assert w.survival_knots([(20.5, 0.5)]) is None              # one point is not a curve


def test_sitting_rate_follows_book_count():
    assert _player("a", 20, books=1).dnp_prob == 0.35
    assert _player("a", 20, books=3).dnp_prob == 0.03
    assert _player("a", 20, books=6).dnp_prob == 0.002


def test_lead_time_sitting_rate_is_opt_in_and_rises_with_the_wait():
    # nba-points-sit-lead-v1: by books at the line and hours from the capture to the player's own tip.
    assert w.sit_prob(6, 0.5) == 0.002 and w.sit_prob(6, 1.5) == 0.021 and w.sit_prob(6, 5.99) == 0.021 and w.sit_prob(6, 6.0) == 0.039
    assert w.sit_prob(3, 0.0) == 0.010 and w.sit_prob(2, 3.0) == 0.024 and w.sit_prob(3, 9.0) == 0.045
    assert w.sit_prob(1, 0.2) == w.sit_prob(1, 8.0) == 0.29
    # No lead time: the registered book-count rate, so the frozen v1 entries are untouched.
    assert w.sit_prob(6, None) == 0.002 and w.sit_prob(1, None) == 0.35 and w.sit_prob(3, float("nan")) == 0.03
    pool = [_player("a", 20, books=6), _player("b", 20, books=6)]
    default = w.simulate_points(pool, 20000, np.random.default_rng(3))
    given = w.simulate_points(pool, 20000, np.random.default_rng(3), sit=[0.0, 0.10])
    assert (default == 0).mean(axis=0).max() < 0.02          # 0.2% each, plus the rare real zero
    assert (given[:, 0] == 0).mean() < 0.01 and 0.085 < (given[:, 1] == 0).mean() < 0.125


def test_simulated_points_are_whole_nonnegative_and_centered_on_the_line():
    rng = np.random.default_rng(1)
    pts = w.simulate_points([_player("a", 20.0), _player("b", 12.0, books=1)], 20000, rng)
    assert np.all(pts == np.rint(pts)) and pts.min() >= 0
    assert abs(np.median(pts[:, 0]) - 20.0) <= 1.0
    assert 0.32 < (pts[:, 1] == 0).mean() < 0.38                # one-book player sits about 35% of nights


def test_recalibration_is_opt_in_and_thins_only_the_top_end():
    pool = [_player("a", 20.0), _player("b", 12.0)]
    plain = w.simulate_points(pool, 20000, np.random.default_rng(3))
    assert np.array_equal(plain, w.simulate_points(pool, 20000, np.random.default_rng(3), recal=False))
    fixed = w.simulate_points(pool, 20000, np.random.default_rng(3), recal=True)
    # the books' curve says line+12.5 or more 5% of the time; the corrected draws say less
    assert (fixed[:, 0] >= 33).mean() < (plain[:, 0] >= 33).mean() * 0.8
    assert abs(np.median(fixed[:, 0]) - np.median(plain[:, 0])) <= 1.0   # the middle barely moves
    u = np.array([0.0, 0.003, 0.5, 1.0])
    out = w.recalibrate_survival(u)
    assert out[0] == 0.0 and out[-1] == 1.0 and np.all(np.diff(out) > 0)
    assert out[1] > 0.003       # a draw at the 0.3% extreme is read lower on the curve (fewer points)


def test_no_ladder_fallback_also_takes_the_correction():
    p = _player("a", 20.0, curve=[])
    plain = w.simulate_points([p], 20000, np.random.default_rng(4))
    fixed = w.simulate_points([p], 20000, np.random.default_rng(4), recal=True)
    assert (fixed[:, 0] >= 30).mean() < (plain[:, 0] >= 30).mean()


def test_correlation_uses_the_measured_same_game_values():
    pool = [_player("a", 20, event="e1", team="A"), _player("b", 18, event="e1", team="A"),
            _player("c", 17, event="e1", team="B"), _player("d", 16, event="e2", team="A"),
            _player("e", 15, event="e1", team=None)]
    c = w.correlation_matrix(pool)
    assert c[0, 1] == pytest.approx(w.TEAMMATE_RHO)
    assert c[0, 2] == pytest.approx(w.OPPONENT_RHO)
    assert c[0, 3] == 0 and c[0, 4] == 0                        # other game, unknown team


def test_prize_share_counts_ties_with_the_field_and_with_our_own_entries():
    # 4 simulated nights, 3 candidate trios; field best 50 every night, shared by 1 field entry
    totals = np.array([[60, 50, 40], [50, 50, 40], [40, 60, 40], [40, 40, 40]], dtype=float)
    fmax, fcount = np.full(4, 50.0), np.ones(4)
    picks = w.build_portfolio(totals, fmax, fcount, 2, order=[0, 1])
    # trio 0: wins night 1 outright, ties the field on night 2 (half) -> 1.5 / 4
    assert picks[0]["marginalShare"] == pytest.approx(1.5 / 4)
    # adding trio 1: night 2 now three-way tie, two of them ours (2/3 instead of 1/2); night 3 outright
    assert picks[1]["portfolioShare"] == pytest.approx((1 + 2 / 3 + 1) / 4)
    assert picks[1]["standaloneShare"] == pytest.approx((0.5 + 0.5 + 1) / 4)


def test_greedy_picks_distinct_trios_and_shares_accumulate():
    rng = np.random.default_rng(3)
    totals = rng.integers(40, 90, size=(500, 30)).astype(float)
    fmax, fcount = np.full(500, 80.0), np.ones(500)
    picks = w.build_portfolio(totals, fmax, fcount, 6)
    ks = [p["trio"] for p in picks]
    assert len(set(ks)) == 6
    shares = [p["portfolioShare"] for p in picks]
    assert shares == sorted(shares) and 0 <= shares[-1] <= 1
    assert sum(p["marginalShare"] for p in picks) == pytest.approx(shares[-1])


def test_baseline_takes_the_largest_line_sums():
    pool = _pool(6)
    trios = w.candidate_trios(6)
    order = w.baseline_order(pool, trios, 2)
    assert trios[order[0]] == (0, 1, 2)
    assert trios[order[1]] == (0, 1, 3)


def test_select_is_deterministic_and_returns_distinct_entries():
    pool = _pool(12)
    a = w.select(date(2026, 11, 6), pool, sims=1500)
    b = w.select(date(2026, 11, 6), pool, sims=1500)
    assert a == b
    entries = [tuple(sorted(e["players"])) for e in a["entries"]]
    assert len(entries) == w.N_ENTRIES and len(set(entries)) == w.N_ENTRIES
    assert all(0 <= e["standaloneShare"] <= 1 for e in a["entries"])
    assert a["baseline"][0]["players"] == ["p00", "p01", "p02"]
    # on this pool the greedy set beats the line-sum baseline in its own simulation (not a guarantee)
    assert a["entries"][-1]["portfolioShare"] >= a["baseline"][-1]["portfolioShare"] - 1e-9


def test_injury_report_sitting_rule():
    # nba-points-sit-injury-v1: Out and Doubtful are dropped, Questionable carries the measured rate.
    assert w.sit_prob_injury("Out", 5, 0.5, 0.2, True) is None and w.sit_prob_injury("doubtful", 5, 0.5, 0.2, True) is None
    assert w.sit_prob_injury("Questionable", 5, 0.5, 0.2, True) == 0.43 == w.sit_prob_injury("Questionable", 1, 9.0, None, False)
    # Cleared by a fresh report: listed Available/Probable, or not listed with both teams' reports in.
    assert w.sit_prob_injury("Probable", 5, 3.0, 4.0, False) == 0.003 == w.sit_prob_injury(None, 5, 3.0, 0.2, True)
    assert w.sit_prob_injury(None, 5, 3.0, -0.5, True) == 0.003                      # report read after the lock is fresh
    # Not cleared: a stale report, no report, or a team that has not filed. The lead-time rate stands.
    assert w.sit_prob_injury(None, 5, 3.0, 4.01, True) == w.sit_prob(5, 3.0) == 0.021
    assert w.sit_prob_injury(None, 5, 8.0, None, True) == 0.039
    assert w.sit_prob_injury(None, 5, 3.0, 0.2, False) == 0.021
    assert w.sit_prob_injury("unverified", 5, 0.5, 0.2, True) == 0.002               # an unknown status clears nobody
    assert w.sit_prob_injury(None, 1, 0.5, 0.2, True) == 0.29                         # one book stays a one-book player


def test_grade_waits_for_every_pool_player_and_scores_sitting_as_zero():
    pool = [{"key": k, "line": l} for k, l in zip("abcdef", (30, 28, 26, 24, 22, 20))]
    payload = {"pool": pool, "seed": 7, "assumptions": {"fieldSize": 50, "chalkShare": 0.1, "popularityTau": 3.0},
               "entries": [{"players": ["a", "b", "c"]}, {"players": ["d", "e", "f"]}] * 5,
               "baseline": [{"players": ["a", "b", "d"]}] * 10}
    actual = {"a": 30.0, "b": 25.0, "c": 0.0, "d": 40.0, "e": 35.0, "f": 20.0}
    assert w.grade_portfolio(payload, {**actual, "f": None}) is None
    g = w.grade_portfolio(payload, actual)
    assert g["bestPoolTrio"] == 105.0                  # d + e + a
    assert g["ours"][:2] == [55.0, 95.0]
    cell = g["byN"]["5"]
    assert cell["ours"]["best"] == 95.0 and cell["baseline"]["best"] == 95.0
    assert cell["ours"]["gapToBest"] == 10.0
    assert 0 <= cell["ours"]["realizedShare"] <= 1
    assert g == w.grade_portfolio(payload, actual)      # seeded: a re-grade gives the same shares


def test_realized_share_rewards_an_entry_the_field_cannot_match():
    # the field picks mostly the big lines (a, b, c); the night's best trio is d + e + f
    pool = [{"key": k, "line": l} for k, l in zip("abcdef", (30, 29, 28, 12, 11, 10))]
    payload = {"pool": pool, "seed": 3, "assumptions": {"fieldSize": 100, "chalkShare": 0.5, "popularityTau": 1.0}}
    pts = {"a": 10.0, "b": 10.0, "c": 10.0, "d": 40.0, "e": 40.0, "f": 40.0}
    contrarian = w.realized_share(payload, pts, [{"players": ["d", "e", "f"]}], 1)
    chalk = w.realized_share(payload, pts, [{"players": ["a", "b", "c"]}], 1)
    assert contrarian > 0.5 and chalk < 0.02   # chalk ties the whole field at best: about 1/101


def test_summary_pairs_nights_and_reports_an_interval():
    def night(ours, base):
        return {"byN": {"5": {"ours": {"percentile": ours, "inContention": ours > 0.99, "realizedShare": ours / 10},
                              "baseline": {"percentile": base, "inContention": base > 0.99, "realizedShare": base / 10}}}}
    s = w.summarize([night(0.95, 0.90), night(0.995, 0.80), night(0.70, 0.75)])
    assert s["nights"] == 3 and s["oursBetter"] == 2 and s["baselineBetter"] == 1
    assert s["meanDiff"] == pytest.approx((0.05 + 0.195 - 0.05) / 3)
    assert s["ci95"][0] <= s["meanDiff"] <= s["ci95"][1]
    assert s["oursContention"] == 1
    assert s["oursShare"] == pytest.approx((0.95 + 0.995 + 0.70) / 10)
    assert s["shareCi95"][0] <= s["shareMeanDiff"] <= s["shareCi95"][1]
    assert w.summarize([]) == {"nights": 0}


def test_web_fixture_is_current():
    # The browser optimizer is checked against this file; regenerate with
    # `python -m research.nba_wta_trios fixture` after any change to the rules.
    import json
    with open(w.FIXTURE_PATH, encoding="utf-8") as f:
        stored = json.load(f)
    assert stored == json.loads(json.dumps(w.fixture_payload()))
