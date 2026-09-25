from datetime import date

import pytest

from research.cfb_study_evaluation import _read_registered_config, clustered_interval, evaluate_rows


CONFIG = {
    "health_floors": {"mapped_event_rate": .99, "eligible_quote_freshness_rate": .95,
                      "settlement_completeness_rate": .99, "economic_conflict_rate_max": 0},
    "minimum_independent_game_dates": 2, "minimum_effect": .5,
}


def row(index, value=1.0, pnl=.1, *, state="settled", fresh=True):
    return {"alert_type": "steam", "result_state": state, "outcome": "won", "entry_decimal": 2.0,
            "roi_stake_units": 1, "pnl_units": pnl, "decimal_price_ratio_pct": value,
            "game_date": date(2026, 10, 20 + index), "matchup_id": index, "fresh_quote": fresh}


def test_clustered_interval_is_deterministic_and_cluster_aware():
    rows = [row(0, 1), row(0, 3), row(1, 2)]
    first = clustered_interval(rows, "decimal_price_ratio_pct", "game_date", draws=200)
    second = clustered_interval(rows, "decimal_price_ratio_pct", "game_date", draws=200)
    assert first == second
    assert first["clusters"] == 2 and first["n"] == 3


def test_confirmation_pass_requires_all_frozen_gates():
    rows = [row(index, 2.0, .2) for index in range(3)]
    result = evaluate_rows(rows, CONFIG, purpose="confirmation_1")
    assert result["result"] == "pass"
    assert result["health_pass"] is True


def test_bad_health_invalidates_and_negative_primary_fails():
    invalid = evaluate_rows([row(0, fresh=False)], CONFIG, purpose="confirmation_1")
    assert invalid["result"] == "invalid"
    failed = evaluate_rows([row(index, -1.0, .2) for index in range(3)], CONFIG, purpose="confirmation_1")
    assert failed["result"] == "fail"


def test_pilot_can_never_pass_qualification():
    result = evaluate_rows([row(index, 2.0, .2) for index in range(3)], CONFIG, purpose="pilot")
    assert result["result"] == "inconclusive"


def test_registered_study_uses_verified_portable_artifact_when_uri_is_on_another_os():
    study = {"configuration_digest": "364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c",
             "uri": r"C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\cfb_moneyline_study.json"}
    assert _read_registered_config(study)["study_version"] == 4
    with pytest.raises(ValueError, match="artifact unavailable"):
        _read_registered_config({**study, "configuration_digest": "0" * 64})
