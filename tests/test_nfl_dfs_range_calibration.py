import numpy as np
import pytest

from model.nfl_dfs_historical import MODEL_CONFIG, HistoricalWeek, ProjectionContext, project_player
from research.nfl_dfs_range_calibration import (
    GATED,
    fit_scale,
    guards,
    projection_draws,
    range_loss,
    scale_tails,
    summarize_draws,
    verdict,
)


def _rows():
    rng = np.random.default_rng(7)
    rows = []
    for pid in range(1, 13):
        for season in (2023, 2024):
            for week in range(1, 18):
                if rng.random() < 0.2:
                    continue
                rows.append(HistoricalWeek(
                    player_id=pid, player_gsis_id=f"00-{pid:04d}", player_name=f"P{pid}", position="RB",
                    season=season, week=week, team="AAA", opponent="BBB",
                    stats={"carries": float(rng.integers(2, 25)), "rushing_yards": float(rng.integers(0, 140)),
                           "receptions": float(rng.integers(0, 7)), "receiving_yards": float(rng.integers(0, 70)),
                           "rushing_tds": float(rng.random() < 0.3)},
                ))
    return sorted(rows, key=lambda r: r.chronological_key)


@pytest.mark.parametrize("pid", [1, 5, 12])
def test_draws_match_project_player_at_every_half_life(pid):
    rows = _rows()
    prior = [r for r in rows if (r.season, r.week) < (2024, 9)]
    ctx = ProjectionContext(team_implied_total=24.0)
    draws = projection_draws(player_id=pid, player_gsis_id=f"00-{pid:04d}", player_name=f"P{pid}", position="RB",
                             prior=prior, cutoff_season=2024, cutoff_week=9, context=ctx, half_lives=(3.0, 8.0))
    for h in (3.0, 8.0):
        cfg = dict(MODEL_CONFIG, player_half_life_games=h)
        p = project_player(player_id=pid, player_gsis_id=f"00-{pid:04d}", player_name=f"P{pid}", position="RB",
                           historical_rows=rows, cutoff_season=2024, cutoff_week=9, context=ctx, config=cfg)
        s = summarize_draws(draws[h])
        assert (s["mean"], s["p10"], s["p50"], s["p90"]) == (p.model_proj_fpts, p.floor_fpts, p.median_fpts, p.ceiling_fpts)


def test_tail_scaling_keeps_the_mean_and_commutes_with_the_anchor():
    m, p10, p90, k = 12.0, 4.0, 22.0, 1.3
    lo, hi = scale_tails(m, p10, p90, 1.2, 0.9)
    assert lo == pytest.approx(12 - 1.2 * 8) and hi == pytest.approx(12 + 0.9 * 10)
    lo_k, hi_k = scale_tails(k * m, k * p10, k * p90, 1.2, 0.9)
    assert lo_k == pytest.approx(k * lo) and hi_k == pytest.approx(k * hi)
    assert scale_tails(m, p10, p90, 1.0, 1.0) == (p10, p90)


def test_fit_scale_widens_a_too_narrow_floor():
    rng = np.random.default_rng(3)
    actual = rng.normal(10, 5, 4000)
    rows = [{"actual": float(a), "base": {"mean": 10.0, "p10": 10 - 0.5 * 6.4, "p90": 10 + 0.5 * 6.4}} for a in actual]
    assert fit_scale(rows, 0.10)["scale"] == pytest.approx(2.0, abs=0.1)
    assert fit_scale(rows, 0.90)["scale"] == pytest.approx(2.0, abs=0.1)


def test_range_loss_is_the_two_pinballs():
    assert range_loss(5.0, 6.0, 20.0) == pytest.approx(0.9 * 1.0 + 0.1 * 15.0)


def _season(hi, pos_mean=-0.01, base_cov=(0.13, 0.13), cand_cov=(0.10, 0.11), mae_hi=0.0):
    return {
        "range_loss_delta": {"mean": hi - 0.01, "lo": hi - 0.02, "hi": hi},
        "mae_delta": {"mean": 0.0, "lo": 0.0, "hi": mae_hi},
        "coverage": {"baseline": {"below_p10": base_cov[0], "above_p90": base_cov[1]},
                     "candidate": {"below_p10": cand_cov[0], "above_p90": cand_cov[1]}},
        "by_position": {p: {"range_loss_delta": {"mean": pos_mean}} for p in GATED},
    }


def test_verdict_rules():
    assert verdict({2024: _season(-0.001), 2025: _season(0.002)})["verdict"] == "KILL"
    assert verdict({2024: _season(-0.001), 2025: _season(-0.003)})["verdict"] == "H_CLEARED"
    assert verdict({2024: _season(-0.001), 2025: _season(-0.003, pos_mean=0.02)})["verdict"] == "INCONCLUSIVE"
    assert verdict({2024: _season(-0.001, mae_hi=0.06), 2025: _season(-0.003)})["verdict"] == "INCONCLUSIVE"
    assert not guards(_season(-0.001, cand_cov=(0.16, 0.13)))["calibration_closer_to_target"]


def test_peer_index_matches_peer_rows():
    from model.nfl_dfs_historical import _peer_rows
    from research.nfl_dfs_range_calibration import PeerIndex

    rows = _rows()
    prior = [r for r in rows if (r.season, r.week) < (2024, 9)]
    index = PeerIndex("RB", prior)
    for pid in (1, 4, 9):
        own = [r for r in prior if r.player_id == pid]
        assert [id(r) for r in index.peers(own, 60)] == [id(r) for r in _peer_rows("RB", own, prior, 60)]
    assert [id(r) for r in index.peers([], 60)] == [id(r) for r in _peer_rows("RB", [], prior, 60)]
