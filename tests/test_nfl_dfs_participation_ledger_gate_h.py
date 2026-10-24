"""Gate H of nfl-dfs-participation-ledger-v1: the ledger builder, the loss, the rule."""

from __future__ import annotations

import math

import pytest

from model import nfl_dfs_participation_ledger_gate_h as gate
from model.nfl_dfs_historical import OFFENSE_FIELDS, HistoricalWeek


def _stat(gsis: str, season: int, week: int, team: str, yards: float = 50.0) -> HistoricalWeek:
    stats = {key: 0.0 for key in OFFENSE_FIELDS}
    stats["rushing_yards"] = yards
    stats["carries"] = 10.0
    return HistoricalWeek(player_id=1 if gsis == "A" else 2, player_gsis_id=gsis, player_name=gsis, position="RB",
                          season=season, week=week, team=team, opponent="OPP", stats=stats)


def _games(season: int, weeks, team: str = "KC"):
    return {(season, w, team): gate.TeamGameRef(f"{season}_{w:02d}_OPP_{team}", season, w, team, "OPP", True, 45.0) for w in weeks}


def test_pinned_model_hash_matches_the_registration():
    assert gate.model_file_matches_pin()


def test_pin_check_survives_line_endings_but_rejects_code_changes():
    raw = gate.MODEL_FILE.read_bytes()
    lf = raw.replace(b"\r\n", b"\n")
    crlf = lf.replace(b"\n", b"\r\n")
    assert gate.model_file_matches_pin(lf) and gate.model_file_matches_pin(crlf)
    assert not gate.model_file_matches_pin(lf + b"# changed\n")


def test_ledger_adds_zero_rows_for_act_and_dev_only_and_keeps_stat_lines():
    season = 2024
    stat_rows = [_stat("A", season, 1, "KC"), _stat("A", season, 2, "KC")]
    roster = {
        (season, 1): {"A": ("ACT", "KC", "RB")},
        (season, 2): {"A": ("ACT", "KC", "RB")},
        (season, 3): {"A": ("ACT", "KC", "RB")},   # dressed, no stat line -> zero row
        (season, 4): {"A": ("DEV", "KC", "RB")},   # practice squad -> zero row
        (season, 5): {"A": ("INA", "KC", "RB")},   # inactive -> nothing
        (season, 6): {"A": ("RES", "KC", "RB")},   # injured reserve -> nothing
        (season, 7): {"A": ("ACT", "KC", "RB")},   # bye week (no game) -> nothing
    }
    games = _games(season, [1, 2, 3, 4, 5, 6])
    ledger, counts = gate.build_ledger(stat_rows, roster, games, {"A": "RB"}, {"A": 1}, {"A": "A"})
    weeks = sorted((r.week, r.dk_points) for r in ledger)
    assert weeks == [(1, 5.0), (2, 5.0), (3, 0.0), (4, 0.0)]
    assert counts == {"zero_rows_ACT": 1, "zero_rows_DEV": 1, "zero_rows": 2, "ledger_bye_or_no_game": 1}
    zero = [r for r in ledger if r.week == 3][0]
    assert zero.opponent == "OPP" and zero.position == "RB" and all(zero.stats[k] == 0.0 for k in OFFENSE_FIELDS)


def test_ledger_keeps_a_stat_line_with_no_roster_week():
    season = 2024
    stat_rows = [_stat("A", season, 9, "KC")]
    ledger, counts = gate.build_ledger(stat_rows, {}, _games(season, [9]), {"A": "RB"}, {"A": 1}, {"A": "A"})
    assert len(ledger) == 1 and ledger[0].week == 9 and counts == {}


def test_ledger_ignores_positions_outside_the_requested_set():
    season = 2024
    roster = {(season, 1): {"A": ("ACT", "KC", "RB"), "B": ("ACT", "KC", "K")}}
    ledger, counts = gate.build_ledger([], roster, _games(season, [1]), {"A": "RB", "B": "K"}, {"A": 1, "B": 2}, {})
    assert [r.player_gsis_id for r in ledger] == ["A"] and counts["zero_rows"] == 1


def test_pinball_matches_the_registered_formula():
    assert gate.pinball(0.10, 5.0, 2.0) == pytest.approx(0.9 * 3.0)   # actual below the quantile
    assert gate.pinball(0.10, 5.0, 8.0) == pytest.approx(0.1 * 3.0)   # actual above
    assert gate.pinball(0.90, 20.0, 30.0) == pytest.approx(0.9 * 10.0)
    assert gate.pinball(0.10, 5.0, 5.0) == 0.0


def test_game_bootstrap_resamples_whole_games():
    values = [-1.0, -1.0, 1.0, 1.0]
    groups = ["g1", "g1", "g2", "g2"]
    reading = gate.game_bootstrap(values, groups, draws=500, seed=1)
    assert reading["mean"] == 0.0 and reading["n"] == 4 and reading["games"] == 2
    # With only two games the resample mean is -1, 0 or 1, never anything else.
    assert reading["lo"] in (-1.0, 0.0) and reading["hi"] in (0.0, 1.0)


def test_implied_total_uses_the_nflverse_sign_convention():
    assert gate._implied(45.0, 3.0, home=True) == 24.0    # home favoured by 3
    assert gate._implied(45.0, 3.0, home=False) == 21.0
    assert gate._implied(None, 3.0, home=True) is None
    assert gate._implied(float("nan"), 3.0, home=True) is None


def _rb_summary(hi: float, rate_b: float, rate_c: float, mae_hi: float = 0.0, mae_played_hi: float = 0.0, p90: float = 0.0):
    return {
        "n": 100,
        "p10_pinball_delta": {"mean": hi - 0.1, "lo": hi - 0.2, "hi": hi, "n": 100, "games": 30},
        "below_p10_rate": {"baseline": rate_b, "candidate": rate_c},
        "mae_delta": {"mean": 0.0, "lo": -0.1, "hi": mae_hi, "n": 100, "games": 30},
        "mae_delta_played": {"mean": 0.0, "lo": -0.1, "hi": mae_played_hi, "n": 80, "games": 30},
        "p90_pinball_delta_point": p90,
    }


def test_verdict_kills_when_either_season_reaches_zero():
    good = _rb_summary(hi=-0.01, rate_b=0.25, rate_c=0.15)
    bad = _rb_summary(hi=0.0, rate_b=0.25, rate_c=0.15)
    assert gate.gate_h_verdict({2024: good, 2025: bad})["verdict"] == "KILL"
    assert gate.gate_h_verdict({2024: bad, 2025: good})["verdict"] == "KILL"


def test_verdict_clears_only_when_every_guard_holds_in_both_seasons():
    good = _rb_summary(hi=-0.01, rate_b=0.25, rate_c=0.15)
    assert gate.gate_h_verdict({2024: good, 2025: good})["verdict"] == "H_CLEARED"
    worse_mae_on_played = _rb_summary(hi=-0.01, rate_b=0.25, rate_c=0.15, mae_played_hi=0.11)  # Amendment 1 guard
    assert gate.gate_h_verdict({2024: good, 2025: worse_mae_on_played})["verdict"] == "INCONCLUSIVE"
    overshoot = _rb_summary(hi=-0.01, rate_b=0.12, rate_c=0.05)  # moved past 10% by more than it was above it
    assert gate.gate_h_verdict({2024: good, 2025: overshoot})["verdict"] == "INCONCLUSIVE"
    wider_ceiling = _rb_summary(hi=-0.01, rate_b=0.25, rate_c=0.15, p90=0.02)
    assert gate.gate_h_verdict({2024: good, 2025: wider_ceiling})["verdict"] == "INCONCLUSIVE"


def test_verdict_is_incomplete_until_both_seasons_exist():
    good = _rb_summary(hi=-0.01, rate_b=0.25, rate_c=0.15)
    out = gate.gate_h_verdict({2024: good})
    assert out["verdict"] == "INCOMPLETE" and "2025" in out["reason"]


def test_summarize_reports_every_registered_metric_and_split():
    rows = []
    for i in range(6):
        rows.append({
            "game_id": f"g{i % 3}", "played": i % 2 == 0, "status": ("ACT", "INA", "DEV")[i % 3],
            "actual": float(i), "base_mean": 6.0 + i, "base_p10": 2.0, "base_p90": 15.0,
            "cand_mean": 5.0 + i, "cand_p10": 1.0, "cand_p90": 14.0,
        })
    s = gate.summarize(rows)
    assert s["n"] == 6 and s["games"] == 3
    for key in ("p10_pinball_delta", "p90_pinball_delta_point", "mae_delta", "mae_delta_played", "below_p10_rate", "moved_more_than_1pt"):
        assert key in s
    assert {"played", "did_not_play", "status_ACT", "status_INA", "status_DEV", "band_5-10", "band_10-15", "band_15+"} <= set(s["splits"])
    assert not math.isnan(s["p10_pinball_delta"]["mean"])
