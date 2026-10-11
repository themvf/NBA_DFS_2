"""Read-only CFB moneyline audit and frozen prospective study registration."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path

from config import PROJECT_DIR, load_config
from db.database import DatabaseManager
from model.cfb_context_economics import resolve_legacy_economics, summarize_resolutions


MONEYLINE_TYPES = {"dk_value", "steam", "walking", "late_move", "pinnacle_divergence"}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def _decimal(american: object) -> float | None:
    try:
        price = float(american)
    except (TypeError, ValueError):
        return None
    if price == 0:
        return None
    return 1 + (100 / abs(price) if price < 0 else price / 100)


def _dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _entry_band(decimal_price: float | None) -> str:
    if decimal_price is None:
        return "unknown"
    implied = 1 / decimal_price
    if implied >= 0.60:
        return "favorite_60_plus"
    if implied >= 0.50:
        return "favorite_50_60"
    if implied >= 0.35:
        return "underdog_35_50"
    if implied >= 0.20:
        return "longshot_20_35"
    return "longshot_under_20"


def _load(db: DatabaseManager) -> list[tuple[dict, list[dict]]]:
    alerts = db.execute("""SELECT a.*, h.books AS trigger_books, h.captured_at AS trigger_captured_at,
          ht.classification AS home_classification, at.classification AS away_classification,
          ht.conference AS home_conference, at.conference AS away_conference
        FROM line_alerts a JOIN cfb_matchups m ON m.id=a.matchup_id
        JOIN cfb_teams ht ON ht.team_id=m.home_team_id JOIN cfb_teams at ON at.team_id=m.away_team_id
        LEFT JOIN game_odds_history h ON h.id=a.trigger_history_id
        WHERE a.sport='cfb' AND a.origin='prospective'
          AND (a.alert_type=ANY(%s) OR a.details_json->>'market'='moneyline') ORDER BY a.id""", (list(MONEYLINE_TYPES),))
    grades = db.execute("""SELECT g.* FROM alert_grades g JOIN line_alerts a ON a.id=g.alert_id
        WHERE g.is_current AND a.sport='cfb' ORDER BY g.alert_id,g.id""")
    grouped: dict[int, list[dict]] = defaultdict(list)
    for grade in grades:
        grouped[int(grade["alert_id"])].append(dict(grade))
    return [(dict(alert), grouped[int(alert["id"])]) for alert in alerts]


def _study_registration(frozen_at: datetime) -> dict:
    tomorrow = (frozen_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    registration = {
        "study_id": "cfb-moneyline-observation-study-v1", "study_version": 4,
        "supersedes_study_version": 3,
        "frozen_at": frozen_at.isoformat(), "research_state": "prospective-paper",
        "consumer_permission": "decision-denied",
        "candidate_versions": sorted(MONEYLINE_TYPES),
        "candidate_definitions": [
            {"alert_type": "dk_value", "signal_version": "cfb-lines-v1"},
            {"alert_type": "steam", "signal_version": "cfb-lines-v1"},
            {"alert_type": "walking", "signal_version": "cfb-lines-v1"},
            {"alert_type": "pinnacle_divergence", "signal_version": "cfb-lines-v1"},
            {"alert_type": "late_move", "signal_version": "market-structure-v1"},
        ],
        "population": "prospective CFB pregame full-game moneyline observations; all matchup classes retained",
        "paper_decision_policy": "at most one observation per event/selection/family; earliest qualifying trigger; no real stake",
        "primary_metric": "decimal_price_ratio_pct", "primary_unit": "percent",
        "secondary_metrics": ["pnl_units_per_roi_stake_unit", "probability_clv_pp", "beat_close_rate"],
        "minimum_effect": 0.5,
        "minimum_independent_game_dates": 20,
        "clustering_method": "game-date clustered percentile interval; game-clustered sensitivity",
        "precision_plan": "95% interval half-width <= 1.0 percentage point for primary metric",
        "window_decision_rule": {
            "pass": "all health floors pass; at least 20 independent game dates; primary mean >= 0.5%; game-date clustered 95% lower bound > 0; interval half-width <= 1.0%; and the 1% adverse-price sensitivity remains positive",
            "fail": "primary mean <= 0 or the 95% upper bound <= 0 after settlement completion",
            "inconclusive": "all other valid completed-window results",
            "invalid": "temporal leakage, economic conflicts, or a failed mandatory health floor",
        },
        "overall_qualification_rule": "confirmation_1 and confirmation_2 must each pass independently; pilot is excluded",
        "multiple_testing_family": "five frozen moneyline families; Holm correction for family selection",
        "multiplicity_application": "The combined frozen cohort is primary. Family diagnostics use Holm-adjusted one-sided cluster-bootstrap probabilities and cannot independently qualify a consumer.",
        "review_schedule": {"settlement_grace_hours": 72, "revisions": "append only when pinned resolution heads or invalidations change"},
        "comparison_plan": "No matched comparison in v1; omission is preregistered before enrollment.",
        "health_floors": {"mapped_event_rate": 0.99, "eligible_quote_freshness_rate": 0.95,
                          "settlement_completeness_rate": 0.99, "economic_conflict_rate_max": 0.0},
        "segments": {"matchup_class": ["fbs_fbs", "fbs_fcs", "other_unknown"],
                     "entry_probability": ["60%+", "50-60%", "35-50%", "20-35%", "<20%"],
                     "horizon_minutes": ["0-15", "15-60", "60-360", "360+", "unknown"],
                     "season_regime": ["weeks_0_4", "weeks_5_9", "weeks_10_plus"]},
        "windows": [
            {"window_key": "pilot", "purpose": "pilot", "start_at": tomorrow.isoformat(),
             "end_at": (tomorrow + timedelta(days=19)).isoformat(), "eligible_for_confirmation": False},
            {"window_key": "confirmation_1", "purpose": "confirmation_1", "start_at": (tomorrow + timedelta(days=19)).isoformat(),
             "end_at": (tomorrow + timedelta(days=61)).isoformat(), "eligible_for_confirmation": True},
            {"window_key": "confirmation_2", "purpose": "confirmation_2", "start_at": "2027-08-20T00:00:00+00:00",
             "end_at": "2027-12-01T00:00:00+00:00", "eligible_for_confirmation": True},
        ],
        "activation_gate": "Requires both untouched confirmations, health floors, scoped qualification, and a separate consumer activation record.",
    }
    registration["configuration_digest"] = sha256(_canonical(registration)).hexdigest()
    return registration


def build_audit(db: DatabaseManager, *, generated_at: datetime | None = None) -> dict:
    generated_at = generated_at or datetime.now(timezone.utc)
    loaded = _load(db)
    resolutions = [(alert, resolve_legacy_economics(alert, grades)) for alert, grades in loaded]
    checks = Counter()
    bands: dict[str, Counter] = defaultdict(Counter)
    families: dict[str, Counter] = defaultdict(Counter)
    calibration: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for alert, resolution in resolutions:
        details = alert.get("details_json") or {}
        price = details.get("exec_decimal") or details.get("dk_decimal")
        price = float(price) if price is not None else None
        american = details.get("exec_odds") if details.get("exec_odds") is not None else details.get("dk_odds")
        converted = _decimal(american)
        checks["rows"] += 1
        checks["selection_identity_present"] += int(alert.get("side") in {"home", "away"})
        checks["decimal_american_consistent"] += int(price is not None and converted is not None and abs(price-converted) <= 0.00011)
        checks["entry_price_present"] += int(price is not None)
        books = alert.get("trigger_books") or {}
        exec_book = details.get("exec_book") or "draftkings"
        quote = books.get(exec_book) or {}
        updated = _dt(quote.get("last_update"))
        captured = _dt(alert.get("trigger_captured_at"))
        fresh = updated is not None and captured is not None and timedelta(0) <= captured-updated <= timedelta(seconds=300)
        checks["fresh_execution_quote"] += int(fresh)
        checks["freshness_unknown"] += int(updated is None or captured is None)
        pin = books.get("pinnacle") or {}
        paired = pin.get("ml_home") is not None and pin.get("ml_away") is not None
        checks["paired_reference_quote"] += int(paired)
        band = _entry_band(price)
        bands[band][resolution.result_state] += 1
        bands[band][resolution.outcome or "no_outcome"] += 1
        family = str(alert["alert_type"])
        families[family][resolution.result_state] += 1
        families[family][resolution.outcome or "no_outcome"] += 1
        if alert.get("sharp_prob") is not None and resolution.outcome in {"won", "lost"}:
            calibration[family].append((float(alert["sharp_prob"]), int(resolution.outcome == "won")))
    cal_summary = {}
    for family, values in calibration.items():
        cal_summary[family] = {"n": len(values), "mean_probability": sum(x for x, _ in values)/len(values),
                               "observed_win_rate": sum(y for _, y in values)/len(values),
                               "brier_score": sum((x-y)**2 for x, y in values)/len(values),
                               "label": "reference_probability_diagnostic_not_detector_calibration"}
    return {
        "generated_at": generated_at.isoformat(), "mode": "read_only_descriptive_audit",
        "economic_reconciliation": summarize_resolutions(resolutions),
        "identity_price_freshness_checks": dict(checks),
        "entry_probability_bands": {key: dict(value) for key, value in sorted(bands.items())},
        "signal_families": {key: dict(value) for key, value in sorted(families.items())},
        "reference_probability_diagnostics": cal_summary,
        "study_registration": _study_registration(generated_at),
        "conclusions": [
            "The 29-observation dk_value cohort includes one non-W/L row and is not silently reduced to 28.",
            "Legacy quote freshness is unknown where the execution book timestamp was not retained; unknown is not treated as fresh.",
            "Movement observations are not probability forecasts; reference probabilities are reported separately.",
            "Observed favorite/longshot differences remain hypotheses and cannot activate a decision consumer.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROJECT_DIR / "artifacts" / "cfb_moneyline_audit_rev3.json")
    args = parser.parse_args()
    audit = build_audit(DatabaseManager(load_config().database_url or "", initialize_schema=False))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "rows": audit["identity_price_freshness_checks"]["rows"],
                      "study_digest": audit["study_registration"]["configuration_digest"]}, indent=2))


if __name__ == "__main__":
    main()
