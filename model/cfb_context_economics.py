"""Canonical legacy CFB economics adapter for market-context Phase 1.

This module is deliberately pure at its core.  It resolves the existing
``line_alerts``/``alert_grades`` representation without changing either table,
so the corrected baseline can be reproduced before the revision-3 engine
schema is migrated.  Persisting ``cfb_economic_resolutions`` is a later,
explicit migration step.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping


RESOLVER_VERSION = "cfb-economics-resolver-v1"
TOLERANCE = Decimal("0.0001")


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


@dataclass(frozen=True)
class EconomicResolution:
    alert_id: int
    resolver_version: str
    result_state: str
    outcome: str | None
    entry_decimal: Decimal | None
    stake_units: Decimal
    roi_stake_units: Decimal | None
    pnl_units: Decimal | None
    pnl_source: str | None
    grade_id: int | None
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict:
        payload = asdict(self)
        for key in ("entry_decimal", "stake_units", "roi_stake_units", "pnl_units"):
            value = payload[key]
            payload[key] = str(value) if value is not None else None
        payload["reason_codes"] = list(self.reason_codes)
        return payload


def _entry_price(alert: Mapping[str, object]) -> tuple[Decimal | None, list[str]]:
    details = _mapping(alert.get("details_json"))
    preferred = _decimal(details.get("exec_decimal"))
    legacy = _decimal(details.get("dk_decimal"))
    reasons: list[str] = []
    for label, value in (("exec_decimal", preferred), ("dk_decimal", legacy)):
        if value is not None and value <= 1:
            reasons.append(f"invalid_{label}")
    if reasons:
        return None, reasons
    if preferred is not None and legacy is not None and abs(preferred - legacy) > TOLERANCE:
        return None, ["conflicting_entry_prices"]
    return preferred if preferred is not None else legacy, []


def _expected_pnl(outcome: str, entry_decimal: Decimal | None) -> tuple[Decimal | None, Decimal | None]:
    if outcome == "void":
        return Decimal("0"), Decimal("0")
    if outcome == "push":
        return Decimal("0"), Decimal("1")
    if outcome == "lost":
        return Decimal("-1"), Decimal("1")
    if outcome == "won" and entry_decimal is not None:
        return entry_decimal - Decimal("1"), Decimal("1")
    return None, None


def _normalized_outcome(
    raw_outcome: str | None, alert: Mapping[str, object], grade_json: Mapping[str, object],
) -> str | None:
    """Recover legacy football pushes that were historically stored as voids."""
    if raw_outcome != "void":
        return raw_outcome
    details = _mapping(alert.get("details_json"))
    market = str(grade_json.get("market") or details.get("market") or "")
    home_score = _decimal(grade_json.get("home_score"))
    away_score = _decimal(grade_json.get("away_score"))
    if home_score is None or away_score is None:
        return raw_outcome
    if market == "spread":
        entry = _decimal(grade_json.get("entry_home_line"))
        if entry is None:
            entry = _decimal(details.get("entry_home_line"))
        if entry is not None and home_score - away_score + entry == 0:
            return "push"
    if market == "total":
        entry = _decimal(grade_json.get("entry_line"))
        if entry is None:
            entry = _decimal(details.get("exec_line") or details.get("trigger_line"))
        if entry is not None and home_score + away_score == entry:
            return "push"
    return raw_outcome


def resolve_legacy_economics(
    alert: Mapping[str, object], current_grades: Iterable[Mapping[str, object]] = (),
) -> EconomicResolution:
    """Resolve one alert under Appendix D without mutating legacy evidence."""
    alert_id = int(alert["id"])
    grades = [dict(grade) for grade in current_grades]
    if len(grades) > 1:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "conflict", None, None, Decimal("1"),
            None, None, None, None, ("multiple_current_grades",),
        )

    grade = grades[0] if grades else None
    grade_json = _mapping(grade.get("grading_json")) if grade else {}
    grade_outcome = str(grade["outcome"]) if grade and grade.get("outcome") is not None else None
    alert_outcome = str(alert["outcome"]) if alert.get("outcome") is not None else None
    if grade_outcome and alert_outcome and grade_outcome != alert_outcome:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "conflict", None, None, Decimal("1"),
            None, None, None, int(grade["id"]), ("conflicting_outcomes",),
        )
    outcome = _normalized_outcome(grade_outcome or alert_outcome, alert, grade_json)
    if outcome not in (None, "won", "lost", "push", "void"):
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "conflict", outcome, None, Decimal("1"),
            None, None, None, int(grade["id"]) if grade else None,
            ("unsupported_outcome",),
        )

    entry_decimal, entry_reasons = _entry_price(alert)
    if entry_reasons:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "conflict", outcome, None, Decimal("1"),
            None, None, None, int(grade["id"]) if grade else None,
            tuple(entry_reasons),
        )
    if outcome is None:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "pending", None, entry_decimal, Decimal("1"),
            None, None, None, int(grade["id"]) if grade else None, (),
        )
    if outcome == "won" and entry_decimal is None:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "missing_entry", outcome, None, Decimal("1"),
            None, None, None, int(grade["id"]) if grade else None,
            ("winning_price_unavailable",),
        )

    stored_candidates = [
        ("alert_grades.pnl_units", _decimal(grade.get("pnl_units")) if grade else None),
        ("alert_grades.grading_json.pnl_units", _decimal(grade_json.get("pnl_units"))),
        ("line_alerts.pnl_units", _decimal(alert.get("pnl_units"))),
    ]
    stored_candidates = [(name, value) for name, value in stored_candidates if value is not None]
    computed, roi_stake = _expected_pnl(outcome, entry_decimal)
    if computed is None:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "missing_entry", outcome, entry_decimal,
            Decimal("1"), None, None, None, int(grade["id"]) if grade else None,
            ("economics_unavailable",),
        )
    conflicts = [name for name, value in stored_candidates if abs(value - computed) > TOLERANCE]
    if conflicts:
        return EconomicResolution(
            alert_id, RESOLVER_VERSION, "conflict", outcome, entry_decimal,
            Decimal("1"), None, None, None, int(grade["id"]) if grade else None,
            tuple(f"pnl_mismatch:{name}" for name in conflicts),
        )

    pnl_source, pnl = stored_candidates[0] if stored_candidates else ("recomputed", computed)
    state = "void" if outcome == "void" else "settled"
    return EconomicResolution(
        alert_id, RESOLVER_VERSION, state, outcome, entry_decimal, Decimal("1"),
        roi_stake, pnl, pnl_source, int(grade["id"]) if grade else None, (),
    )


def summarize_resolutions(rows: Iterable[tuple[Mapping[str, object], EconomicResolution]]) -> list[dict]:
    """Aggregate canonical economics with explicit state and stake denominators."""
    groups: dict[tuple[str, str], list[EconomicResolution]] = {}
    for alert, resolution in rows:
        details = _mapping(alert.get("details_json"))
        version = str(alert.get("signal_version") or details.get("signal_version") or "unstamped")
        groups.setdefault((str(alert["alert_type"]), version), []).append(resolution)
    output = []
    for (alert_type, version), resolutions in sorted(groups.items()):
        stake = sum((r.roi_stake_units or Decimal("0")) for r in resolutions)
        pnl = sum((r.pnl_units or Decimal("0")) for r in resolutions if r.pnl_units is not None)
        output.append({
            "alert_type": alert_type,
            "signal_version": version,
            "observations": len(resolutions),
            "settled": sum(r.result_state == "settled" for r in resolutions),
            "void": sum(r.result_state == "void" for r in resolutions),
            "pending": sum(r.result_state == "pending" for r in resolutions),
            "missing_entry": sum(r.result_state == "missing_entry" for r in resolutions),
            "conflict": sum(r.result_state == "conflict" for r in resolutions),
            "wins": sum(r.outcome == "won" for r in resolutions),
            "losses": sum(r.outcome == "lost" for r in resolutions),
            "pushes": sum(r.outcome == "push" for r in resolutions),
            "roi_stake_units": str(stake),
            "pnl_units": str(pnl),
            "roi": str(pnl / stake) if stake else None,
        })
    return output
