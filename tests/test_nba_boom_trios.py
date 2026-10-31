"""nba-boom-trios-v1: the frozen rules pick what the registration says, and grading never scores a miss as 0."""
from research import nba_boom_trios as bt


def cand(key, line, shots=0.0, ft=0.0, ready=True, status="unverified"):
    return {"event_id": "e1", "player_key": key, "player": key.upper(), "history_status": "ready" if ready else "insufficient_history",
            "availability": {"status": status}, "predictions": {"market_line": line},
            "features": {"shots_growth": shots, "ft_growth": ft, "fga3": 15.0 + shots, "fta3": 4.0 + ft} if ready else None}


def snap(cands, created="2026-11-01T15:30:00+00:00", mode="prospective"):
    return {"game_date": "2026-11-01", "sha256": "x", "created_at": created, "mode": mode, "candidates": cands}


def test_constants_are_the_registration():
    assert (bt.VERSION, bt.BOOM_MIN, bt.FT_WEIGHT, bt.ONE_SWAP_MAX_DROP, bt.TWO_SWAP_MAX_DROP, bt.BASELINE_METHOD) == \
        ("nba-boom-trios-v1", 2.0, 0.44, 3.0, 6.0, "market_line")


def test_boom_replaces_third_within_limit_by_boom_not_points():
    s = snap([cand("a", 30), cand("b", 28), cand("c", 26), cand("d", 25.5, shots=2.5), cand("e", 23.5, shots=6.0), cand("f", 22.0, shots=9.0)])
    out = bt.select(s)
    assert [m["player_key"] for m in out["baseline"]["members"]] == ["a", "b", "c"]
    # f has the biggest boom but is 4 points below c; e (2.5 below) wins over d.
    assert [m["player_key"] for m in out["boom"]["members"]] == ["a", "b", "e"]
    assert out["boom"]["projected_drop"] == 2.5 and out["boom"]["replaced"]["player_key"] == "c"
    assert out["boom"]["why"].startswith("Projected total drops by 2.5 points, but E has taken 6.0 more shot attempts")


def test_aggressive_takes_best_boom_pair_inside_six_points():
    s = snap([cand("a", 30), cand("b", 28), cand("c", 26), cand("d", 25.5, shots=2.5), cand("e", 23.5, shots=6.0), cand("f", 22.0, shots=9.0)])
    agg = bt.select(s)["aggressive"]
    # e+f is 8.5 below b+c (too far); d+f is 6.5 (too far); d+e is 5.0 -> chosen.
    assert [m["player_key"] for m in agg["members"]] == ["a", "d", "e"] and agg["projected_drop"] == 5.0


def test_free_throws_count_and_unready_or_out_players_never_qualify():
    assert bt.boom_score(cand("x", 10, shots=1.0, ft=3.0)) == 1.0 + 0.44 * 3.0
    s = snap([cand("a", 30), cand("b", 28), cand("c", 26), cand("d", 25.9, ready=False),
              cand("e", 25.8, shots=9.0, status="Out"), cand("f", 25.0, shots=1.9)])
    out = bt.select(s)
    assert out["boom"] is None and out["aggressive"] is None


def test_counts_only_after_registration_and_prospective():
    assert bt.counts_toward_rule(snap([]))
    assert not bt.counts_toward_rule(snap([], created="2026-10-31T15:37:00+00:00"))
    assert not bt.counts_toward_rule(snap([], mode="retrospective_reconstruction"))


def test_grade_compares_totals_and_leaves_dnp_incomplete():
    s = snap([cand("a", 30), cand("b", 28), cand("c", 26), cand("e", 23.5, shots=6.0), cand("d", 25.5, shots=2.5)])
    sel = bt.select(s)
    rows = [{"event_id": "e1", "player_key": k, "status": "graded", "actual_points": v, "historical_p90": 30.0}
            for k, v in {"a": 31, "b": 20, "c": 10, "d": 33, "e": 18}.items()]
    g = bt.grade(sel, rows)
    assert g["baseline_actual"] == 61 and g["boom"]["delta"] == 8 and g["boom"]["outcome"] == "helped"
    assert g["boom"]["p90_added"] == [False] and g["boom"]["p90_replaced"] == [False]
    rows[2] = {**rows[2], "status": "dnp", "actual_points": None}
    assert bt.grade(sel, rows)["boom"]["status"] == "incomplete"
    summary = bt.summarize([bt.grade(sel, [{**r, "status": "graded", "actual_points": 5.0} for r in rows])])
    assert summary["boom"]["graded"] == 1 and summary["verdict"] is None
