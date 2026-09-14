"""Register, evaluate, snapshot, and settle CFB historical hypotheses."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
from datetime import date, datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.cfb_historical_signals import (
    DEFINITION_VERSION,
    MARKET_BREAK_EVEN_110,
    cohort_summary,
    grade_home,
    walk_forward_splits,
    wilson_interval,
)

DEFAULT_KEY = "CFB-H001"
DEFAULT_VERSION = "v1"
STATUS_ORDER = (
    "PROPOSED", "PREREGISTERED", "BACKTESTED", "HOLDOUT_PASSED",
    "PROSPECTIVE_SHADOW", "VALIDATED_SIGNAL", "RETIRED",
)


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _code_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, timeout=5,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return os.getenv("VERCEL_GIT_COMMIT_SHA", "unknown")


def allowed_transition(current: str, target: str) -> bool:
    if target == "RETIRED":
        return current != "RETIRED"
    try:
        return STATUS_ORDER.index(target) == STATUS_ORDER.index(current) + 1
    except ValueError:
        return False


# Every registered hypothesis, with the complete preregistration fields §10.1
# of the historical/roster spec requires.  Definitions are frozen at
# registration: a changed population, feature, threshold or test is a new
# version, never an edit to an existing row.
#
# ``evaluator`` names the implemented evaluation routine.  ``None`` means the
# definition is frozen but no routine can score it yet; ``data_readiness``
# then names exactly which input is missing and what would supply it.  A
# hypothesis registered this way is preregistration working as intended — the
# claim is fixed before the data exists to test it, so the definition cannot
# be shaped by the result.
HYPOTHESIS_DEFINITIONS: dict[tuple[str, str], dict] = {
    ("CFB-H001", "v1"): {
        "name": "Non-neutral home favorites 14.0-16.5",
        "claim": "The registered cohort covers above the -110 market break-even rate.",
        "evaluator": "spread_bucket_ats",
        # Frozen as originally registered. It predates the convention of
        # storing an explicit formula, and adding one now would alter a frozen
        # definition; that is a new version, not an edit.
        "outcome": {
            "market": "full_game_spread", "perspective": "home",
            "overtime": "included",
        },
        "population": {
            "home_spread_min": -16.5, "home_spread_max": -14.0,
            "neutral_site": False, "home_classification": "fbs",
            "away_classification": "fbs", "line_designation": "historical_reference",
        },
        "features": {},
        "buckets": {"version": DEFINITION_VERSION, "favorite_low": 14.0, "favorite_high": 16.5},
        "minimums": {"holdout_n": 40, "prospective_n": 100},
        "split": {"method": "expanding_walk_forward", "start": 2016, "holdout": 2025},
        "test": {"alpha": 0.05, "direction": "greater", "baseline": MARKET_BREAK_EVEN_110},
        "family": "spread-buckets-v1",
        "promotion": {"requires_positive_prospective_clv": True, "manual_review": True},
        "data_readiness": {"state": "READY", "missing_inputs": [], "enabler": None},
        "notes": "Frozen before evaluation; descriptive unless later promotion gates pass.",
    },
    ("CFB-H003", "v1"): {
        "name": "Large favorites with low returning defensive production",
        "claim": (
            "Among large favorites, teams in the bottom quartile of returning "
            "defensive production allow more points than the market's implied "
            "opponent team total."
        ),
        "evaluator": None,
        "outcome": {
            "market": "implied_opponent_team_total", "perspective": "favorite",
            "overtime": "included",
            "formula": (
                "opponent_total_error = opponent_points "
                "- (canonical_total / 2 - abs(canonical_home_spread) / 2)"
            ),
            "units": "points",
        },
        "population": {
            "favorite_size_min": 21.0, "neutral_site": False,
            "home_classification": "fbs", "away_classification": "fbs",
            "line_designation": "historical_reference",
            "requires_canonical_spread": True, "requires_canonical_total": True,
        },
        "features": {
            "returning_defensive_production_pct": {
                "definition": (
                    "Share of the team's prior-season defensive production "
                    "(havoc and tackle share by DL/LB/SECONDARY) accounted for "
                    "by players present on the point-in-time roster snapshot."
                ),
                "selection": "bottom_quartile_within_season",
                "availability": "roster snapshot available_at <= kickoff",
            },
            "controls": ["pace", "opponent_offense", "garbage_time"],
        },
        "buckets": {"version": DEFINITION_VERSION, "favorite_low": 21.0, "favorite_high": None},
        "minimums": {"holdout_n": 60, "prospective_n": 100},
        "split": {"method": "expanding_walk_forward", "start": 2016, "holdout": 2025},
        "test": {
            "alpha": 0.05, "direction": "greater", "baseline": 0.0,
            "statistic": "mean_opponent_total_error",
            "minimum_meaningful_effect_points": 1.0,
        },
        "family": "roster-continuity-v1",
        "promotion": {"requires_positive_prospective_clv": True, "manual_review": True},
        "data_readiness": {
            "state": "BLOCKED_MISSING_FEATURE",
            "available_inputs": [
                "canonical reference spread and total (cfb_historical_game_lines)",
                "final scores (cfb_matchups)",
                "point-in-time roster snapshots with position groups",
            ],
            "missing_inputs": ["returning_defensive_production_pct"],
            "reason": (
                "CFBD /player/returning carries only offensive measures "
                "(passing, receiving and rushing PPA and usage). It exposes no "
                "defensive returning production, so the feature cannot be read "
                "from the source the roster job already calls."
            ),
            "enabler": (
                "Ingest prior-season per-player defensive statistics and derive "
                "the returning share against the point-in-time roster. Roster "
                "headcount continuity is NOT a substitute: it is unweighted by "
                "production and would be a different hypothesis needing its own "
                "registration."
            ),
        },
        "notes": (
            "Frozen 2026-09-14 before its feature exists, so the definition "
            "cannot be shaped by the data. Not evaluable until the enabler ships."
        ),
    },
    ("CFB-H005", "v1"): {
        "name": "Year-one offensive coordinator with low returning QB continuity",
        "claim": (
            "Teams in year one under a new offensive coordinator, with no "
            "returning quarterback, cover below the -110 market break-even rate."
        ),
        "evaluator": None,
        "outcome": {
            "market": "full_game_spread", "perspective": "subject_team",
            "overtime": "included",
            "formula": "team_cover = (team_score - opponent_score) + team_spread > 0",
            "secondary": "verified_line_clv",
        },
        "population": {
            "neutral_site": None, "home_classification": "fbs",
            "away_classification": "fbs",
            "line_designation": "historical_reference",
            "requires_canonical_spread": True,
        },
        "features": {
            "offensive_coordinator_year_one": {
                "definition": (
                    "The team's OFFENSIVE_COORDINATOR regime has "
                    "start_season equal to the game season and start_week at or "
                    "before the game week."
                ),
                "availability": "cfb_staff_regimes.available_at <= kickoff",
            },
            "returning_quarterbacks": {
                "definition": (
                    "Count of quarterbacks on the point-in-time roster snapshot "
                    "who appeared on the most recent prior-season snapshot."
                ),
                "threshold": 0,
            },
            "controls": ["talent_composite", "transfers_in", "opponent", "week"],
        },
        "buckets": {},
        "minimums": {"holdout_n": 40, "prospective_n": 100},
        "split": {"method": "expanding_walk_forward", "start": 2016, "holdout": 2025},
        "test": {
            "alpha": 0.05, "direction": "less", "baseline": MARKET_BREAK_EVEN_110,
            "statistic": "ats_cover_rate",
            "minimum_meaningful_effect": 0.03,
        },
        "family": "coaching-regime-v1",
        "promotion": {"requires_positive_prospective_clv": True, "manual_review": True},
        "data_readiness": {
            "state": "BLOCKED_MISSING_FEATURE",
            "available_inputs": [
                "canonical reference spread (cfb_historical_game_lines)",
                "final scores (cfb_matchups)",
                "returning quarterback counts (cfb_roster_snapshots.summary_json)",
            ],
            "missing_inputs": ["offensive_coordinator_regime"],
            "reason": (
                "cfb_staff_regimes permits the OFFENSIVE_COORDINATOR role but no "
                "writer produces one: ingest.cfb_rosters records HEAD_COACH only, "
                "from CFBD /coaches, and CFBD exposes no coordinator identity. "
                "The table is structurally empty for this role."
            ),
            "enabler": (
                "A coordinator-identity source with per-team start and end "
                "seasons and a real available_at, ingested as "
                "OFFENSIVE_COORDINATOR staff regimes."
            ),
        },
        "notes": (
            "Direction is 'less': the claim is that these teams are OVERVALUED, "
            "so they are predicted to cover below break-even. Frozen 2026-09-14 "
            "before its feature exists. Not evaluable until the enabler ships."
        ),
    },
}

IMPLEMENTED_EVALUATORS = frozenset({"spread_bucket_ats"})


def definition_for(key: str, version: str) -> dict:
    try:
        return HYPOTHESIS_DEFINITIONS[(key, version)]
    except KeyError:
        raise ValueError(f"no registered definition for {key} {version}") from None


def is_evaluable(key: str, version: str) -> bool:
    return definition_for(key, version).get("evaluator") in IMPLEMENTED_EVALUATORS


def _require_evaluable(key: str, version: str) -> dict:
    """Refuse to score a hypothesis whose evaluator is not implemented.

    Without this the walk-forward routine would run its hardcoded H001 cohort
    and store those results under whichever hypothesis id was requested.
    """
    definition = definition_for(key, version)
    if definition.get("evaluator") not in IMPLEMENTED_EVALUATORS:
        readiness = definition.get("data_readiness") or {}
        raise ValueError(
            f"{key} {version} has no implemented evaluator "
            f"({readiness.get('state', 'UNKNOWN')}); "
            f"missing inputs: {', '.join(readiness.get('missing_inputs') or ['unknown'])}"
        )
    return definition


_DEFINITIONAL_COLUMNS = (
    ("name", "name"), ("claim", "claim"),
    ("outcome", "outcome_definition_json"), ("population", "population_filter_json"),
    ("buckets", "bucket_definition_json"), ("minimums", "min_sample_json"),
    ("split", "split_plan_json"), ("test", "test_plan_json"),
    ("promotion", "promotion_rules_json"), ("family", "multiple_test_family"),
)


def definition_drift(definition: dict, stored: dict) -> list[str]:
    """Name the frozen fields where the catalog and the registry disagree.

    ``data_readiness`` is excluded on purpose: it records whether our pipeline
    can supply an input today, which legitimately changes when an enabler
    ships. It is an operational fact, not part of the claim being tested.
    """
    drift = []
    for field, column in _DEFINITIONAL_COLUMNS:
        if column not in stored:
            continue
        wanted = definition.get(field) if field != "buckets" else definition.get("buckets", {})
        if _json(wanted) != _json(stored[column]):
            drift.append(field)
    features = dict(stored.get("feature_definition_json") or {})
    features.pop("data_readiness", None)
    features.pop("evaluator", None)
    if "feature_definition_json" in stored and _json(definition.get("features", {})) != _json(features):
        drift.append("features")
    return drift


def register(db: DatabaseManager, key: str = DEFAULT_KEY, version: str = DEFAULT_VERSION) -> int:
    """Freeze one catalog definition into the registry.

    Re-registration is deliberately inert on every definitional column: the
    conflict clause touches nothing, so a frozen population, feature, threshold
    or test can never be rewritten by a later run. Changing any of them means
    registering a new version.
    """
    definition = definition_for(key, version)
    existing = db.execute_one(
        "SELECT * FROM cfb_hypotheses WHERE hypothesis_key=%s AND version=%s",
        (key, version),
    )
    if existing:
        drift = definition_drift(definition, dict(existing))
        if drift:
            raise ValueError(
                f"{key} {version} is already frozen and the catalog now differs on: "
                f"{', '.join(drift)}. A changed claim, population, feature, "
                f"threshold or test is a NEW version, never an edit to a frozen row."
            )
    row = db.execute_one(
        """
        INSERT INTO cfb_hypotheses (
            hypothesis_key, version, name, claim, status,
            outcome_definition_json, population_filter_json,
            feature_definition_json, bucket_definition_json, min_sample_json,
            split_plan_json, test_plan_json, promotion_rules_json,
            multiple_test_family, frozen_at, notes
        ) VALUES (%s, %s, %s, %s, 'PREREGISTERED', %s::jsonb, %s::jsonb,
                  %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                  %s::jsonb, %s, NOW(), %s)
        ON CONFLICT (hypothesis_key, version) DO UPDATE SET
            -- Refresh only the operational keys. Readiness and the evaluator
            -- describe what this pipeline can do today, not the claim under
            -- test, so they may change; every definitional column is left
            -- untouched and is instead guarded by definition_drift above.
            feature_definition_json=cfb_hypotheses.feature_definition_json
                || jsonb_build_object(
                    'data_readiness', EXCLUDED.feature_definition_json->'data_readiness',
                    'evaluator', EXCLUDED.feature_definition_json->'evaluator'
                )
        RETURNING id
        """,
        (
            key, version, definition["name"], definition["claim"],
            _json(definition["outcome"]), _json(definition["population"]),
            _json({
                **definition.get("features", {}),
                "data_readiness": definition.get("data_readiness", {}),
                "evaluator": definition.get("evaluator"),
            }),
            _json(definition.get("buckets", {})), _json(definition["minimums"]),
            _json(definition["split"]), _json(definition["test"]),
            _json(definition["promotion"]), definition["family"],
            definition["notes"],
        ),
    )
    return int(row["id"])


def register_default(db: DatabaseManager) -> int:
    """Register CFB-H001 only; retained so existing callers keep working."""
    return register(db, DEFAULT_KEY, DEFAULT_VERSION)


def register_all(db: DatabaseManager) -> dict[str, int]:
    return {
        f"{key}-{version}": register(db, key, version)
        for key, version in sorted(HYPOTHESIS_DEFINITIONS)
    }


def readiness_report() -> list[dict]:
    """Describe every registered definition and whether it can be scored."""
    return [
        {
            "hypothesis": f"{key}-{version}",
            "name": definition["name"],
            "family": definition["family"],
            "evaluator": definition.get("evaluator"),
            "evaluable": definition.get("evaluator") in IMPLEMENTED_EVALUATORS,
            "readiness": (definition.get("data_readiness") or {}).get("state"),
            "missing_inputs": (definition.get("data_readiness") or {}).get("missing_inputs") or [],
        }
        for key, version in sorted(HYPOTHESIS_DEFINITIONS)
        for definition in [HYPOTHESIS_DEFINITIONS[(key, version)]]
    ]


def _hypothesis(db: DatabaseManager, key: str, version: str) -> dict:
    row = db.execute_one(
        "SELECT * FROM cfb_hypotheses WHERE hypothesis_key=%s AND version=%s",
        (key, version),
    )
    if not row:
        raise ValueError(f"unknown hypothesis {key} {version}")
    return dict(row)


def _cohort_rows(db: DatabaseManager) -> list[dict]:
    return db.execute(
        """
        SELECT m.id, m.season, m.week, m.game_date, m.commence_time,
               m.home_team_id, m.away_team_id, m.home_score, m.away_score,
               hl.home_value AS home_spread, hl.provider
        FROM cfb_matchups m
        JOIN cfb_historical_game_lines hl ON hl.game_id=m.id
        WHERE m.completed=TRUE AND m.neutral_site=FALSE
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND hl.market_type='spread'
          AND hl.line_designation='historical_reference'
          AND hl.is_canonical_reference=TRUE
          AND hl.home_classification='fbs' AND hl.away_classification='fbs'
          AND hl.home_value BETWEEN -16.5 AND -14.0
        ORDER BY m.commence_time, m.id
        """
    )


def evaluate(db: DatabaseManager, key: str = DEFAULT_KEY, version: str = DEFAULT_VERSION) -> list[dict]:
    _require_evaluable(key, version)
    hypothesis = _hypothesis(db, key, version)
    if not hypothesis.get("frozen_at"):
        raise ValueError("hypothesis must be frozen before evaluation")
    rows = _cohort_rows(db)
    seasons = sorted({int(row["season"]) for row in rows})
    results = []
    for train, test in walk_forward_splits(seasons):
        test_rows = [row for row in rows if int(row["season"]) == test]
        summary = cohort_summary(test_rows, favorite_low=14.0, favorite_high=16.5)
        decisions = summary.ats.wins + summary.ats.losses
        effect = summary.ats.rate - MARKET_BREAK_EVEN_110 if summary.ats.rate is not None else None
        standard_error = (
            math.sqrt(summary.ats.rate * (1 - summary.ats.rate) / decisions)
            if summary.ats.rate is not None and decisions else None
        )
        payload = {
            "train_seasons": list(train), "test_season": test,
            "summary": summary.to_dict(), "effect_vs_minus_110": effect,
        }
        data_version = _hash([
            {key: str(row.get(key)) for key in ("id", "season", "home_score", "away_score", "home_spread", "provider")}
            for row in test_rows
        ])
        result_hash = _hash(payload)
        db.execute(
            """
            INSERT INTO cfb_hypothesis_results (
                hypothesis_id, evaluation_type, train_start, train_end,
                test_start, test_end, n, wins, losses, pushes, effect,
                standard_error, ci_low, ci_high, calibration_json,
                data_version, code_version, result_payload_hash
            ) VALUES (%s, 'walk_forward', %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (hypothesis_id, evaluation_type, test_start, test_end, result_payload_hash)
            DO NOTHING
            """,
            (
                hypothesis["id"], date(min(train), 1, 1), date(max(train), 12, 31),
                date(test, 1, 1), date(test, 12, 31), summary.ats.n,
                summary.ats.wins, summary.ats.losses, summary.ats.pushes,
                effect, standard_error, summary.ats.ci_low, summary.ats.ci_high,
                _json(payload), data_version, _code_version(), result_hash,
            ),
        )
        results.append(payload)
    if hypothesis["status"] == "PREREGISTERED" and results:
        db.execute("UPDATE cfb_hypotheses SET status='BACKTESTED' WHERE id=%s", (hypothesis["id"],))
    return results


def advance(db: DatabaseManager, key: str, version: str, target: str) -> None:
    hypothesis = _hypothesis(db, key, version)
    current = str(hypothesis["status"])
    if not allowed_transition(current, target):
        raise ValueError(f"invalid hypothesis transition {current} -> {target}")
    if target == "HOLDOUT_PASSED":
        holdout = db.execute_one(
            """SELECT n, ci_low FROM cfb_hypothesis_results
               WHERE hypothesis_id=%s AND evaluation_type='walk_forward'
                 AND EXTRACT(YEAR FROM test_start)=2025
               ORDER BY evaluated_at DESC LIMIT 1""",
            (hypothesis["id"],),
        )
        minimums = hypothesis.get("min_sample_json") or {}
        if not holdout or int(holdout["n"]) < int(minimums.get("holdout_n", 40)):
            raise ValueError("holdout sample gate not met")
        if holdout.get("ci_low") is None or float(holdout["ci_low"]) <= MARKET_BREAK_EVEN_110:
            raise ValueError("holdout uncertainty gate not met")
    if target == "VALIDATED_SIGNAL":
        prospective = db.execute_one(
            """SELECT n, ci_low, avg_clv FROM cfb_hypothesis_results
               WHERE hypothesis_id=%s AND evaluation_type='prospective'
               ORDER BY test_end DESC NULLS LAST, evaluated_at DESC LIMIT 1""",
            (hypothesis["id"],),
        )
        minimums = hypothesis.get("min_sample_json") or {}
        if not prospective or int(prospective["n"]) < int(minimums.get("prospective_n", 100)):
            raise ValueError("prospective sample gate not met")
        if prospective.get("ci_low") is None or float(prospective["ci_low"]) <= MARKET_BREAK_EVEN_110:
            raise ValueError("prospective uncertainty gate not met")
        if prospective.get("avg_clv") is None or float(prospective["avg_clv"]) <= 0:
            raise ValueError("positive verified prospective CLV gate not met")
    db.execute(
        "UPDATE cfb_hypotheses SET status=%s, retired_at=CASE WHEN %s='RETIRED' THEN NOW() ELSE retired_at END WHERE id=%s",
        (target, target, hypothesis["id"]),
    )


def snapshot_qualified(db: DatabaseManager, through_date: date) -> int:
    hypotheses = db.execute(
        "SELECT * FROM cfb_hypotheses WHERE status IN ('PROSPECTIVE_SHADOW','VALIDATED_SIGNAL') AND frozen_at IS NOT NULL"
    )
    games = db.execute(
        """
        SELECT id, home_team_id, home_spread, commence_time
        FROM cfb_matchups
        WHERE completed=FALSE AND commence_time > NOW()
          AND game_date <= %s AND neutral_site=FALSE
          AND home_spread BETWEEN -16.5 AND -14.0
        """,
        (through_date,),
    )
    written = 0
    for hypothesis in hypotheses:
        # Only a hypothesis with an implemented evaluator may freeze a pregame
        # snapshot; the qualifying query below is H001's cohort definition.
        try:
            if not is_evaluable(str(hypothesis["hypothesis_key"]), str(hypothesis["version"])):
                continue
        except ValueError:
            continue
        for game in games:
            context = db.execute_one(
                """
                SELECT COUNT(*)::int AS n,
                       AVG(((m.home_score-m.away_score)+hl.home_value > 0)::int)::float AS cover_rate
                FROM cfb_matchups m
                JOIN cfb_historical_game_lines hl ON hl.game_id=m.id
                WHERE m.completed=TRUE AND m.neutral_site=FALSE
                  AND hl.market_type='spread' AND hl.line_designation='historical_reference'
                  AND hl.is_canonical_reference=TRUE
                  AND hl.home_classification='fbs' AND hl.away_classification='fbs'
                  AND hl.home_value BETWEEN -16.5 AND -14.0
                """
            )
            value = float(context["cover_rate"]) if context and context.get("cover_rate") is not None else None
            confidence = min(1.0, int(context.get("n") or 0) / 200) if context else 0
            db.execute(
                """
                INSERT INTO cfb_game_signal_snapshots (
                    game_id, team_id, hypothesis_id, signal_status, signal_value,
                    confidence, evidence_level, inputs_json, model_version,
                    captured_at, qualified_for_tracking
                ) VALUES (%s, %s, %s, %s, %s, %s, 'national_bucket', %s::jsonb,
                          %s, NOW(), TRUE)
                ON CONFLICT (game_id, team_id, hypothesis_id, model_version) DO NOTHING
                """,
                (
                    game["id"], game["home_team_id"], hypothesis["id"],
                    hypothesis["status"], value, confidence,
                    _json({"historical_n": int(context.get("n") or 0), "rate": value, "spread": game["home_spread"]}),
                    f"{DEFAULT_KEY}-{DEFAULT_VERSION}",
                ),
            )
            written += 1
    return written


def settle_prospective(
    db: DatabaseManager, key: str = DEFAULT_KEY, version: str = DEFAULT_VERSION,
    through_date: date | None = None,
) -> dict:
    """Aggregate immutable pregame snapshots after results and verified closes exist.

    A missing verified close never receives a latest-row proxy. Such games can
    contribute to ATS outcomes, but not to the CLV estimate or promotion gate.
    """
    _require_evaluable(key, version)
    hypothesis = _hypothesis(db, key, version)
    rows = db.execute(
        """
        SELECT s.id, s.inputs_json, m.id AS game_id, m.game_date,
               m.home_score, m.away_score,
               close_history.home_spread AS close_home_spread,
               vc.quality AS close_quality
        FROM cfb_game_signal_snapshots s
        JOIN cfb_matchups m ON m.id=s.game_id
        LEFT JOIN verified_clv_closes vc
          ON vc.sport='cfb' AND vc.matchup_id=m.id
        LEFT JOIN game_odds_history close_history ON close_history.id=vc.history_id
        WHERE s.hypothesis_id=%s AND s.qualified_for_tracking=TRUE
          AND m.completed=TRUE AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND (%s IS NULL OR m.game_date <= %s)
        ORDER BY m.game_date, s.id
        """,
        (hypothesis["id"], through_date, through_date),
    )
    wins = losses = pushes = 0
    clv_values: list[float] = []
    observations = []
    for row in rows:
        inputs = row.get("inputs_json") or {}
        entry_spread = inputs.get("spread")
        if entry_spread is None:
            continue
        _, ats = grade_home(int(row["home_score"]), int(row["away_score"]), float(entry_spread))
        if ats == "win":
            wins += 1
        elif ats == "loss":
            losses += 1
        else:
            pushes += 1
        close_spread = row.get("close_home_spread")
        line_clv = None
        if close_spread is not None:
            # Positive means the frozen home-favorite entry beat the verified close.
            line_clv = float(entry_spread) - float(close_spread)
            clv_values.append(line_clv)
        observations.append({
            "snapshot_id": int(row["id"]), "game_id": int(row["game_id"]),
            "game_date": str(row["game_date"]), "entry_spread": float(entry_spread),
            "outcome": ats, "verified_line_clv": line_clv,
            "close_quality": row.get("close_quality"),
        })
    decisions = wins + losses
    n = decisions + pushes
    rate = wins / decisions if decisions else None
    ci_low, ci_high = wilson_interval(wins, decisions)
    effect = rate - MARKET_BREAK_EVEN_110 if rate is not None else None
    standard_error = math.sqrt(rate * (1 - rate) / decisions) if rate is not None and decisions else None
    roi = ((wins * (100 / 110)) - losses) / decisions if decisions else None
    avg_clv = sum(clv_values) / len(clv_values) if clv_values else None
    payload = {
        "summary": {"n": n, "wins": wins, "losses": losses, "pushes": pushes,
                    "decision_rate": rate, "ci_low": ci_low, "ci_high": ci_high},
        "verified_clv": {"n": len(clv_values), "average_points": avg_clv},
        "observations": observations,
    }
    if rows:
        result_hash = _hash(payload)
        db.execute(
            """
            INSERT INTO cfb_hypothesis_results (
                hypothesis_id, evaluation_type, test_start, test_end, n, wins,
                losses, pushes, effect, standard_error, ci_low, ci_high, roi,
                avg_clv, calibration_json, data_version, code_version,
                result_payload_hash
            ) VALUES (%s, 'prospective', %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (hypothesis_id, evaluation_type, test_start, test_end, result_payload_hash)
            DO NOTHING
            """,
            (
                hypothesis["id"], min(row["game_date"] for row in rows),
                max(row["game_date"] for row in rows), n, wins, losses, pushes,
                effect, standard_error, ci_low, ci_high, roi, avg_clv,
                _json(payload), _hash(observations), _code_version(), result_hash,
            ),
        )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("register-default")
    sub.add_parser("register-all")
    sub.add_parser("status")
    evaluate_parser = sub.add_parser("evaluate")
    evaluate_parser.add_argument("key", nargs="?", default=DEFAULT_KEY)
    evaluate_parser.add_argument("--version", default=DEFAULT_VERSION)
    evaluate_parser.add_argument("--walk-forward", action="store_true", help="Explicitly select the only supported evaluation method")
    advance_parser = sub.add_parser("advance")
    advance_parser.add_argument("key")
    advance_parser.add_argument("target", choices=STATUS_ORDER)
    advance_parser.add_argument("--version", default=DEFAULT_VERSION)
    snapshot_parser = sub.add_parser("snapshot-qualified")
    snapshot_parser.add_argument("--date", type=date.fromisoformat, required=True)
    settle_parser = sub.add_parser("settle")
    settle_parser.add_argument("key", nargs="?", default=DEFAULT_KEY)
    settle_parser.add_argument("--version", default=DEFAULT_VERSION)
    settle_parser.add_argument("--through-date", type=date.fromisoformat)
    args = parser.parse_args()
    if args.command == "status":
        # A pure read of the frozen catalog; deliberately needs no database so
        # readiness can be inspected anywhere, including before deployment.
        print(json.dumps(readiness_report(), indent=2))
        return
    db = DatabaseManager(load_config().database_url or "")
    if args.command == "register-default":
        print(json.dumps({"hypothesis_id": register_default(db)}))
    elif args.command == "register-all":
        print(json.dumps(register_all(db), indent=2, sort_keys=True))
    elif args.command == "evaluate":
        print(json.dumps(evaluate(db, args.key, args.version), indent=2, default=str))
    elif args.command == "advance":
        advance(db, args.key, args.version, args.target)
        print(json.dumps({"status": args.target}))
    elif args.command == "snapshot-qualified":
        print(json.dumps({"snapshots_written": snapshot_qualified(db, args.date)}))
    elif args.command == "settle":
        print(json.dumps(settle_prospective(db, args.key, args.version, args.through_date), indent=2, default=str))


if __name__ == "__main__":
    main()
