"""Reviewed depth-order overrides for Game Leaders requests.

The roles rule (model/nfl_game_leaders_roles.py, pinned) reads FantasyPros'
depth rank first and Sleeper second. When the providers disagree and a third
source settles it, a person records the reviewed order here, in
`artifacts/nfl-game-leaders/reviewed-depth.json`, keyed by game:

    {"2026_06_HOU_JAX": {"HOU": {"RB": {"Dare Ogunbowale": 2, "Jawhar Jordan": 4}},
                         "reason": "...", "sources": {...}, "reviewed_at": "..."}}

`apply_reviewed` builds the role scenario with those ranks and then restores
the captured provider data, so the request still shows what each feed said;
the override and its sources are recorded in `scenario_evidence`. An entry
applies only to its own game, so it lapses once that game is played.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Callable, Mapping

REVIEWED_PATH = Path("artifacts/nfl-game-leaders/reviewed-depth.json")


def load_reviewed(path: Path = REVIEWED_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def with_reviewed_ranks(request: Mapping[str, Any], review: Mapping[str, Any]) -> dict:
    """A copy of `request` whose depth comparisons carry the reviewed ranks."""
    adjusted = copy.deepcopy(dict(request))
    for team, positions in review.items():
        if team in ("reason", "sources", "reviewed_at") or team not in adjusted["dual_depth"]:
            continue
        for c in adjusted["dual_depth"][team].get("comparisons", []):
            rank = positions.get(c["position"], {}).get(c["name"])
            if rank is not None:
                c["fantasypros"] = {**(c.get("fantasypros") or {}), "rank": rank}
    return adjusted


def apply_reviewed(game_id: str, request: Mapping[str, Any], attach: Callable[[dict], dict],
                   reviewed: Mapping[str, Any]) -> dict:
    """Attach roles, honouring a reviewed override for this game when one exists.
    `attach(request)` is the default role attachment for one request."""
    review = reviewed.get(game_id)
    if not review:
        return attach(dict(request))
    out = attach(with_reviewed_ranks(request, review))
    out["dual_depth"] = copy.deepcopy(request["dual_depth"])
    evidence = dict(out["scenario_evidence"])
    evidence["description"] = f"{evidence['description']} Reviewed depth override: {review.get('reason', '')}".strip()
    evidence["reviewed_override"] = {k: v for k, v in review.items()}
    out["scenario_evidence"] = evidence
    return out
