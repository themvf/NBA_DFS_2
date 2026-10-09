from dataclasses import asdict
import pytest

from model.nfl_dfs_historical import HistoricalWeek, MODEL_CONFIG, project_player
from research.nfl_matchup_scenario_export import build_score_banks


def fixture():
    stats = {"passing_yards": 250, "passing_tds": 2, "passing_interceptions": 1,
             "rushing_yards": 20, "rushing_tds": 0, "receptions": 0,
             "receiving_yards": 0, "receiving_tds": 0, "fumbles_lost_total": 0}
    history = [HistoricalWeek(1, "qb-1", "Quarterback", "QB", 2025, week, "BUF", "NYJ", stats) for week in range(1, 6)]
    config = {**MODEL_CONFIG, "draws": 20}
    projection = asdict(project_player(player_id=1, player_gsis_id="qb-1", player_name="Quarterback", position="QB",
                                       historical_rows=history, cutoff_season=2026, cutoff_week=3, config=config))
    projection["team"] = "BUF"
    row = {"id": 1, "dk_player_id": 101, "ff_player_id": 1, "name": "Quarterback", "position": "QB", "team": "BUF", "opponent": "NYJ",
           "is_out": False, "captain_dk_player_id": None, "captain_salary": None, "roster_positions": ["QB"], "game_key": "NYJ@BUF", "game_info": None,
           "salary": 7000, "avg_fpts_dk": 20, "dk_status": None, "fantasypros_proj": None, "linestar_proj": None, "linestar_own_pct": None, "custom_proj": None}
    run = {"run_id": "run", "seed": 20260902, "model_config": config}
    upload = {"upload_id": "slate", "format": "classic", "games": ["NYJ@BUF"], "teams": ["NYJ", "BUF"]}
    return [row], [projection], history, run, upload


def test_exact_baseline_replay_separate_streams_and_no_production_claim():
    result = build_score_banks(*fixture(), captured_at="2026-09-27T14:00:00+00:00")
    assert result["audit"]["auditedPlayers"] == 1
    assert result["selection"]["provenance"]["maximumMeanDifference"] < .001
    assert result["audit"]["productionChanged"] is False
    assert result["audit"]["coherentGameBankAvailable"] is False
    assert result["selection"]["metadata"]["seed"] != result["evaluation"]["metadata"]["seed"]
    assert not set(result["selection"]["scenarioIds"]) & set(result["evaluation"]["scenarioIds"])
    assert result["selection"]["provenance"]["historyMode"] == "current_source_replay"


def test_missing_identity_stays_excluded_not_zero():
    rows, projections, history, run, upload = fixture()
    rows.append({**rows[0], "dk_player_id": 102, "ff_player_id": None})
    result = build_score_banks(rows, projections, history, run, upload, captured_at="2026-09-27T14:00:00+00:00")
    assert "102" not in result["selection"]["scores"]
    assert result["audit"]["excluded"][0]["reason"] == "unresolved_projection_identity"


def test_baseline_drift_fails_closed():
    rows, projections, history, run, upload = fixture()
    projections[0]["model_proj_fpts"] += 1
    with pytest.raises(ValueError, match="no eligible audited"):
        build_score_banks(rows, projections, history, run, upload, captured_at="2026-09-27T14:00:00+00:00")


def test_shadow_transforms_require_the_supported_frozen_ledger():
    args = fixture()
    comparison = {"baseline_run_id": "run", "players": [{"player_id": 1, "shadow": {"status": "under_evaluation", "ledger": [{"component": "passing_tds", "factor": 1.1, "status": "shadow_only"}]}}]}
    with pytest.raises(ValueError, match="unsupported or unqualified"):
        build_score_banks(*args, captured_at="2026-09-27T14:00:00+00:00", comparison=comparison)
    with pytest.raises(ValueError, match="baseline run differs"):
        build_score_banks(*args, captured_at="2026-09-27T14:00:00+00:00", comparison={"baseline_run_id": "other"})
