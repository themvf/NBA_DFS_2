from __future__ import annotations

import json

from model.mlb_ownership_model import predict_pool_ownership


def write_artifact(path, hitter_artifact, pitcher_artifact) -> None:
    path.write_text(
        json.dumps(
            {
                "modelVersion": "mlb_ownership_v1",
                "roles": {
                    "hitter": hitter_artifact,
                    "pitcher": pitcher_artifact,
                },
            }
        ),
        encoding="utf-8",
    )


def test_predict_pool_ownership_preserves_baseline_ratios(tmp_path) -> None:
    artifact_path = tmp_path / "mlb_ownership_v1.json"
    base_artifact = {
        "featureOrder": ["baseline_own"],
        "intercept": 0.0,
        "coefficients": [0.0],
        "means": [0.0],
        "scales": [1.0],
        "fillValues": {"baseline_own": 0.0},
        "budget": 800.0,
        "minScore": 0.05,
    }
    pitcher_artifact = {**base_artifact, "budget": 200.0}
    write_artifact(artifact_path, base_artifact, pitcher_artifact)

    players = [
        {
            "eligible_positions": "OF",
            "salary": 5000,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 60.0,
            "linestar_proj": 10.0,
            "our_proj": 11.0,
        },
        {
            "eligible_positions": "1B",
            "salary": 4200,
            "is_out": False,
            "linestar_own_pct": 40.0,
            "proj_own_pct": 40.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 9.0,
        },
        {
            "eligible_positions": "2B",
            "salary": 4100,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "3B",
            "salary": 4300,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "SS",
            "salary": 4500,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "OF",
            "salary": 4700,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "OF",
            "salary": 4800,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "C",
            "salary": 3900,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "OF",
            "salary": 3600,
            "is_out": False,
            "linestar_own_pct": 100.0,
            "proj_own_pct": 100.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "OF",
            "salary": 3500,
            "is_out": False,
            "linestar_own_pct": 100.0,
            "proj_own_pct": 100.0,
            "our_own_pct": 80.0,
            "linestar_proj": 8.0,
            "our_proj": 8.0,
        },
        {
            "eligible_positions": "SP",
            "salary": 9300,
            "is_out": False,
            "linestar_own_pct": 80.0,
            "proj_own_pct": 80.0,
            "our_own_pct": 60.0,
            "linestar_proj": 18.0,
            "our_proj": 19.0,
        },
        {
            "eligible_positions": "SP",
            "salary": 8100,
            "is_out": False,
            "linestar_own_pct": 60.0,
            "proj_own_pct": 60.0,
            "our_own_pct": 70.0,
            "linestar_proj": 15.0,
            "our_proj": 14.0,
        },
        {
            "eligible_positions": "SP",
            "salary": 7600,
            "is_out": False,
            "linestar_own_pct": 60.0,
            "proj_own_pct": 60.0,
            "our_own_pct": 70.0,
            "linestar_proj": 13.0,
            "our_proj": 13.0,
        },
    ]

    field_map = predict_pool_ownership(players, projection_mode="field", artifact_path=artifact_path)
    our_map = predict_pool_ownership(players, projection_mode="our", artifact_path=artifact_path)

    assert round(sum(field_map[i] for i in range(10)), 2) == 800.0
    assert round(sum(field_map[i] for i in range(10, 13)), 2) == 200.0
    assert field_map[0] > field_map[1]
    assert our_map[0] < our_map[1]
    assert round(sum(our_map[i] for i in range(10)), 2) == 800.0
    assert round(sum(our_map[i] for i in range(10, 13)), 2) == 200.0


def test_predict_pool_ownership_applies_lineup_context_correction(tmp_path) -> None:
    artifact_path = tmp_path / "mlb_ownership_v1.json"
    hitter_artifact = {
        "featureOrder": ["baseline_own", "is_leadoff"],
        "intercept": 0.0,
        "coefficients": [0.0, 0.7],
        "means": [0.0, 0.0],
        "scales": [1.0, 1.0],
        "fillValues": {"baseline_own": 0.0, "is_leadoff": 0.0},
        "budget": 800.0,
        "minScore": 0.05,
    }
    pitcher_artifact = {
        "featureOrder": ["baseline_own"],
        "intercept": 0.0,
        "coefficients": [0.0],
        "means": [0.0],
        "scales": [1.0],
        "fillValues": {"baseline_own": 0.0},
        "budget": 200.0,
        "minScore": 0.05,
    }
    write_artifact(artifact_path, hitter_artifact, pitcher_artifact)

    players = []
    for idx in range(8):
        players.append(
            {
                "eligible_positions": "OF" if idx else "SS",
                "salary": 5000 + idx,
                "is_out": False,
                "linestar_own_pct": 100.0,
                "proj_own_pct": 100.0,
                "our_own_pct": 100.0,
                "linestar_proj": 10.0,
                "our_proj": 10.0,
                "dk_starting_lineup_order": 1 if idx == 0 else 7,
                "dk_team_lineup_confirmed": True,
            }
        )

    field_map = predict_pool_ownership(players, projection_mode="field", artifact_path=artifact_path)

    assert field_map[0] > field_map[1]
    assert round(field_map[0], 2) == 100.0
    assert round(sum(field_map.values()), 2) < 800.0
