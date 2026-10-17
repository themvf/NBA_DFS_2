"""nfl-leaders-market-anchored-v1: anchors, the quantile map, and the test's grading rule."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from model import nfl_game_leaders_anchored as A
from research import nfl_game_leaders_anchored as R


def test_anchors_devig_the_main_line_and_ladder_and_force_monotone():
    main = {"line": 45.5, "over": -110, "under": -110}
    ladder = {"ladder": [{"side": "over", "point": 14.5, "price": -1000},     # 0.909/1.0476 = 0.868
                         {"side": "over", "point": 29.5, "price": -250},
                         {"side": "over", "point": 45.5, "price": +100},      # same point as the main line: ignored
                         {"side": "over", "point": 60.5, "price": +250},
                         {"side": "over", "point": 70.5, "price": +240},      # not falling: forced down
                         {"side": "over", "point": 120.5, "price": +5000}]}  # under 3%: dropped
    pts = A.anchors(main, ladder)
    assert [x for x, _ in pts] == [14.5, 29.5, 45.5, 60.5, 70.5]
    assert pts[2][1] == pytest.approx(0.5)
    assert all(a[1] >= b[1] for a, b in zip(pts, pts[1:]))
    assert pts[4][1] == pts[3][1]
    assert A.anchors(None, ladder) == [] and A.anchors({"line": 4.5, "over": -110}, None) == []


def test_quantile_map_hits_every_anchor_and_keeps_order_and_zeros():
    rng = np.random.default_rng(1)
    d = np.concatenate([np.zeros(2000), rng.gamma(2.0, 15.0, 8000)])
    pts = [(14.5, 0.85), (29.5, 0.65), (45.5, 0.5), (70.5, 0.25)]
    t = A.quantile_map(d, pts)
    for x, p in pts:
        assert (t > x).mean() == pytest.approx(p, abs=0.02)
    strict = np.argsort(d)
    assert np.all(np.diff(t[strict][d[strict] > 0]) >= -1e-9)        # order kept among distinct draws
    # 20% of games are zeros; DK's lowest rung leaves room for 15%, so 15% stay at zero.
    assert (t == 0).mean() == pytest.approx(0.15, abs=0.01)


def test_receptions_stay_whole_and_no_anchor_means_no_change():
    d = np.random.default_rng(2).poisson(3.0, 5000).astype(float)
    t = A.quantile_map(d, [(4.5, 0.45)], integer=True)
    assert np.all(t == np.round(t)) and t.min() >= 0
    assert (t > 4.5).mean() == pytest.approx(0.45, abs=0.03)
    assert np.array_equal(A.quantile_map(d, []), d)


def test_unlisted_players_keep_our_draws():
    rng = np.random.default_rng(3)
    players = [{"identity": i, "name": n, "team": "AAA", "draws": {"rushYds": rng.gamma(2, 10, 500), "recYds": rng.gamma(2, 10, 500),
                                                                   "receptions": rng.poisson(3, 500).astype(float)}}
               for i, n in (("1", "Listed"), ("2", "Unlisted"))]
    arrays, diag = A.anchor_players(players, {("rushing_yards", "listed"): [(40.5, 0.7)]}, lambda s: s.lower())
    assert np.array_equal(arrays["rushing_yards"][1], players[1]["draws"]["rushYds"])
    assert (arrays["rushing_yards"][0] > 40.5).mean() == pytest.approx(0.7, abs=0.03)
    assert set(diag) == {"1"}


def test_leader_log_loss_splits_ties_and_floors_zero():
    assert A.leader_log_loss({"a": 0.5, "b": 0.25}, {"a": 1.0}) == pytest.approx(-np.log(0.5))
    assert A.leader_log_loss({"a": 0.5, "b": 0.25}, {"a": 0.5, "b": 0.5}) == pytest.approx(-(0.5 * np.log(0.5) + 0.5 * np.log(0.25)))
    assert A.leader_log_loss({"a": 1.0}, {"b": 1.0}) == pytest.approx(-np.log(1e-4))


def _graded_world(n_games, anchored_better):
    start = datetime.fromisoformat(R.REGISTRATION["registered_at"])
    stored, kicks, weeks, box = [], {}, {}, []
    for i in range(n_games):
        gid = f"2026_{7 + i // 14:02d}_AA{i:02d}_BB{i:02d}"[:7] + f"_A{i:02d}_B{i:02d}"
        kick = start + timedelta(days=1 + i)
        kicks[gid], weeks[gid] = kick, 7 + i // 14
        for metric in R.METRICS:
            for ident, ours, anch in (("p1", 0.4, 0.6 if anchored_better else 0.2), ("p2", 0.6, 0.4 if anchored_better else 0.8)):
                stored.append({"game_id": gid, "metric": metric, "identity": ident, "our_share": ours, "dk_implied_share": None,
                               "anchored_share": anch, "prop_captured_at": kick - timedelta(hours=1),
                               "computed_at": kick - timedelta(minutes=59), "origin": "prospective",
                               "version": R.REGISTRATION["model_version"]})
        a, b = gid.split("_")[2:4]
        box.append({"week": weeks[gid], "team": a, "player_id": "p1", "rushing_yards": 90, "receiving_yards": 50, "receptions": 8})
        box.append({"week": weeks[gid], "team": b, "player_id": "p2", "rushing_yards": 10, "receiving_yards": 5, "receptions": 1})
    return stored, kicks, weeks, pd.DataFrame(box)


def test_verdicts_only_at_the_floor_and_in_the_right_direction():
    stored, kicks, weeks, box = _graded_world(20, anchored_better=True)
    report = R.grade(stored, kicks, weeks, box, "prospective")
    assert report["markets"]["receptions"]["games"] == 20 and report["markets"]["receptions"]["verdict_if_first"] is None
    stored, kicks, weeks, box = _graded_world(60, anchored_better=True)
    report = R.grade(stored, kicks, weeks, box, "prospective")
    assert all(m["verdict_if_first"] == "PASS" for m in report["markets"].values())
    stored, kicks, weeks, box = _graded_world(60, anchored_better=False)
    report = R.grade(stored, kicks, weeks, box, "prospective")
    assert all(m["verdict_if_first"] == "FAIL" for m in report["markets"].values())


def test_rows_priced_after_kickoff_or_before_registration_are_not_graded():
    stored, kicks, weeks, box = _graded_world(3, anchored_better=True)
    gid = next(iter(kicks))
    for r in stored:
        if r["game_id"] == gid:
            r["computed_at"] = kicks[gid] + timedelta(minutes=1)
    report = R.grade(stored, kicks, weeks, box, "prospective")
    assert report["markets"]["rushing_yards"]["games"] == 2
