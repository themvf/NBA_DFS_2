"""Atomic, auditable publication of Tennis result observations.

Providers append immutable evidence here. PostgreSQL row locks serialize the
legacy match projection and its dependent moneyline ledger; ambiguous or
conflicting observations never mutate the published result.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any

from psycopg2.extras import Json

from db.database import DatabaseManager
from model.tennis_book_rules import settle_tennis_selection

POLICY_VERSION = "tennis-settlement-v1"
_KNOWN_STATUSES = {"completed", "retired", "walkover", "awarded", "cancelled"}
_GAME_SIDE_ALERT_TYPES = {
    "pinnacle_divergence", "pinnacle_favorite_forward", "pinnacle_polymarket_delta",
    "steam", "dk_value", "walking", "reversal", "reference_led", "price_pressure",
    "book_disagreement", "market_convergence", "late_move", "favorite_flip",
}
_TENNIS_TOTAL_ALERT_TYPES = {"dk_prop_value", "prop_line_gap"}


@dataclass(frozen=True)
class ResultObservation:
    match_id: int
    provider: str
    winner_side: str | None
    completion_status: str = "unknown"
    status_evidence: bool = False
    observed_match_date: date | None = None
    home_sets: int | None = None
    away_sets: int | None = None
    home_games: int | None = None
    away_games: int | None = None
    provider_event_id: str | None = None
    source_url: str | None = None
    source_available_at: datetime | None = None
    parser_version: str = "unknown"
    raw_payload: dict[str, Any] = field(default_factory=dict)
    match_method: str = "exact"
    match_confidence: float = 1.0
    actor: str | None = None
    reason: str | None = None
    evidence_url: str | None = None


def _checksum(observation: ResultObservation) -> str:
    payload = asdict(observation)
    payload["observed_match_date"] = (
        observation.observed_match_date.isoformat() if observation.observed_match_date else None
    )
    payload["source_available_at"] = (
        observation.source_available_at.isoformat() if observation.source_available_at else None
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def start_provider_run(db: DatabaseManager, provider: str, tour: str | None, parser_version: str) -> int:
    return db.execute_insert(
        """INSERT INTO tennis_provider_runs (provider, tour, parser_version)
           VALUES (%s, %s, %s) RETURNING id""",
        (provider, tour, parser_version),
    )


def finish_provider_run(
    db: DatabaseManager, run_id: int, *, status: str, fetched: int = 0,
    parsed: int = 0, matched: int = 0, ambiguous: int = 0,
    http_status: int | None = None, error: str | None = None,
) -> None:
    db.execute(
        """UPDATE tennis_provider_runs
           SET finished_at=NOW(), status=%s, fetched_count=%s, parsed_count=%s,
               matched_count=%s, ambiguous_count=%s, http_status=%s,
               error_message=%s WHERE id=%s""",
        (status, fetched, parsed, matched, ambiguous, http_status, error, run_id),
    )


def fail_provider_run_if_open(
    db: DatabaseManager, run_id: int, exc: Exception, *, status: str = "parse_error",
    fetched: int = 0, parsed: int = 0, matched: int = 0, ambiguous: int = 0,
) -> None:
    """Close a run only when an earlier stage has not already finalized it."""
    http_status = getattr(getattr(exc, "response", None), "status_code", None)
    if http_status is None:
        http_status = getattr(getattr(getattr(exc, "__cause__", None), "response", None),
                              "status_code", None)
    db.execute(
        """UPDATE tennis_provider_runs
           SET finished_at=NOW(), status=%s, fetched_count=%s, parsed_count=%s,
               matched_count=%s, ambiguous_count=%s, http_status=%s,
               error_message=%s
           WHERE id=%s AND finished_at IS NULL""",
        (status, fetched, parsed, matched, ambiguous, http_status, str(exc), run_id),
    )


def _audit(cur, resolution_id: int, target_type: str, target_id: int,
           action: str, prior: dict, new: dict) -> None:
    cur.execute(
        """INSERT INTO tennis_settlement_audit
             (resolution_id, target_type, target_id, action, prior_state,
              new_state, policy_version)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (resolution_id, target_type, target_id, action) DO NOTHING""",
        (resolution_id, target_type, target_id, action,
         Json(json.loads(json.dumps(prior, default=str))),
         Json(json.loads(json.dumps(new, default=str))), POLICY_VERSION),
    )


def _append_alert_grade_cur(
    cur, alert: dict, outcome: str, resolution_id: int, *, fallback_version: str,
) -> dict:
    """Append a current grade revision while preserving prior price-grade metadata."""
    grading_json = dict(alert.get("grading_json") or {})
    grading_json["result_resolution_id"] = resolution_id
    grading_version = alert.get("grading_version") or fallback_version
    cur.execute(
        """SELECT grading_version, outcome, grading_json FROM alert_grades
           WHERE alert_id=%s AND is_current ORDER BY id DESC LIMIT 1""",
        (alert["id"],),
    )
    current = cur.fetchone()
    if (current and current["grading_version"] == grading_version
            and current["outcome"] == outcome
            and (current["grading_json"] or {}).get("result_resolution_id") == resolution_id):
        return {"grading_version": grading_version, "grading_json": grading_json}
    cur.execute(
        "UPDATE alert_grades SET is_current=FALSE WHERE alert_id=%s AND is_current",
        (alert["id"],),
    )
    cur.execute(
        """INSERT INTO alert_grades
             (alert_id, grading_version, comparison_status, convergence, outcome,
              dk_clv_pct, grading_json, is_current)
           VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE)""",
        (alert["id"], grading_version, alert.get("comparison_status"),
         alert.get("convergence"), outcome, alert.get("dk_clv_pct"), Json(grading_json)),
    )
    return {"grading_version": grading_version, "grading_json": grading_json}


def _expected_alert_outcome(alert: dict) -> str | None:
    details = alert.get("details_json") or {}
    book = details.get("exec_book") or details.get("clv_book")
    if book is None and details.get("dk_odds") is not None:
        book = "draftkings"
    if alert.get("resolution_state") == "void":
        return "void"
    if alert["alert_type"] in _GAME_SIDE_ALERT_TYPES:
        return settle_tennis_selection(
            book=book, market="moneyline", selection_side=alert["side"],
            winner_side=alert.get("winner"),
            completion_status=alert.get("completion_status"),
        )
    if alert["alert_type"] in _TENNIS_TOTAL_ALERT_TYPES:
        try:
            line = float(details["line"])
            bet = details["bet"]
        except (KeyError, TypeError, ValueError):
            return None
        return settle_tennis_selection(
            book=book, market="total", selection_side=alert["side"],
            winner_side=alert.get("winner"),
            completion_status=alert.get("completion_status"),
            home_games=alert.get("home_games"), away_games=alert.get("away_games"),
            line=line, total_bet=bet,
        )
    return None


def _resolution_state(
    match: dict, observation: ResultObservation, *, allow_correction: bool = False,
) -> str:
    match_void = (
        observation.winner_side is None
        and observation.completion_status in {"walkover", "cancelled"}
    )
    conflicts_with_published = bool(
        match["winner"]
        and (match_void or observation.winner_side != match["winner"])
    )
    if conflicts_with_published and not allow_correction:
        return "disputed"
    if match_void:
        return "void"
    if observation.winner_side in ("home", "away"):
        return "resolved"
    return "unresolved"


def record_observation_and_settle(
    db: DatabaseManager, observation: ResultObservation, *, require_prestart: bool = False,
    allow_correction: bool = False,
) -> dict[str, int | str]:
    """Append evidence and atomically publish/settle it when unambiguous."""
    if observation.winner_side not in (None, "home", "away"):
        raise ValueError("winner_side must be home, away, or None")
    if observation.completion_status not in _KNOWN_STATUSES | {"unknown"}:
        raise ValueError(f"unsupported completion status: {observation.completion_status}")
    if observation.status_evidence is False and observation.completion_status != "unknown":
        raise ValueError("a non-unknown completion status requires explicit status evidence")

    checksum = _checksum(observation)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM tennis_matches WHERE id=%s FOR UPDATE", (observation.match_id,))
        match = cur.fetchone()
        if not match:
            raise ValueError(f"Tennis match {observation.match_id} was not found")
        if require_prestart and match["winner"] is not None:
            raise ValueError(f"Tennis match {observation.match_id} already has a published result")
        if require_prestart and match["commence_time"] is not None:
            from datetime import timezone
            if match["commence_time"] <= datetime.now(timezone.utc):
                raise ValueError("Refusing to resolve a withdrawal at or after first serve")

        cur.execute(
            """INSERT INTO tennis_result_observations
                 (match_id, provider, provider_event_id, observed_match_date,
                  winner_side, home_sets, away_sets, home_games, away_games,
                  completion_status, status_evidence, source_url,
                  source_available_at, parser_version, raw_checksum, raw_payload,
                  match_method, match_confidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (provider, match_id, raw_checksum, parser_version) DO NOTHING
               RETURNING id""",
            (observation.match_id, observation.provider, observation.provider_event_id,
             observation.observed_match_date, observation.winner_side,
             observation.home_sets, observation.away_sets, observation.home_games,
             observation.away_games, observation.completion_status,
             observation.status_evidence, observation.source_url,
             observation.source_available_at, observation.parser_version, checksum,
             Json(json.loads(json.dumps(observation.raw_payload, default=str))),
             observation.match_method,
             observation.match_confidence),
        )
        row = cur.fetchone()
        if row:
            observation_id = row["id"]
        else:
            cur.execute(
                """SELECT id FROM tennis_result_observations
                   WHERE provider=%s AND match_id=%s AND raw_checksum=%s
                     AND parser_version=%s""",
                (observation.provider, observation.match_id, checksum, observation.parser_version),
            )
            observation_id = cur.fetchone()["id"]

        state = _resolution_state(match, observation, allow_correction=allow_correction)
        cur.execute(
            """SELECT id FROM tennis_result_resolutions
               WHERE observation_id=%s AND policy_version=%s""",
            (observation_id, POLICY_VERSION),
        )
        existing = cur.fetchone()
        if existing:
            resolution_id = existing["id"]
        else:
            reason = observation.reason or (
                "Conflicts with the currently published winner" if state == "disputed"
                else "Provider observation accepted by settlement policy" if state == "resolved"
                else "Verified pre-match void" if state == "void"
                else "Provider evidence is insufficient for automatic resolution"
            )
            cur.execute(
                """INSERT INTO tennis_result_resolutions
                     (match_id, observation_id, state, winner_side, completion_status,
                      home_sets, away_sets, home_games, away_games, policy_version,
                      actor, reason, evidence_url, correction_of_id)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   RETURNING id""",
                (observation.match_id, observation_id, state, observation.winner_side,
                 observation.completion_status, observation.home_sets, observation.away_sets,
                 observation.home_games, observation.away_games, POLICY_VERSION,
                 observation.actor or observation.provider, reason,
                 observation.evidence_url or observation.source_url,
                 match.get("result_resolution_id")),
            )
            resolution_id = cur.fetchone()["id"]

        if state in {"unresolved", "disputed"}:
            return {"state": state, "resolution_id": resolution_id, "bets": 0, "alerts": 0}

        prior_match = {key: match.get(key) for key in (
            "winner", "home_sets", "away_sets", "home_games", "away_games",
            "completion_status", "retired", "walkover", "result_source",
            "result_comment", "result_resolution_id",
        )}
        final_status = observation.completion_status
        if final_status == "unknown" and match["completion_status"] not in (None, "scheduled", "unknown"):
            final_status = match["completion_status"]
        replace_projection = allow_correction or state == "void"
        published_winner = None if state == "void" else observation.winner_side
        home_sets = (observation.home_sets if replace_projection
                     else observation.home_sets if observation.home_sets is not None else match["home_sets"])
        away_sets = (observation.away_sets if replace_projection
                     else observation.away_sets if observation.away_sets is not None else match["away_sets"])
        home_games = (observation.home_games if replace_projection
                      else observation.home_games if observation.home_games is not None else match["home_games"])
        away_games = (observation.away_games if replace_projection
                      else observation.away_games if observation.away_games is not None else match["away_games"])
        cur.execute(
            """UPDATE tennis_matches SET
                 winner=%s, home_sets=%s, away_sets=%s, home_games=%s, away_games=%s,
                 completion_status=%s, retired=%s, walkover=%s,
                 result_source=%s, result_comment=%s, result_resolution_id=%s
               WHERE id=%s""",
            (published_winner, home_sets, away_sets, home_games, away_games, final_status,
             final_status == "retired", final_status == "walkover", observation.provider,
             observation.reason, resolution_id, observation.match_id),
        )
        new_match = {
            **prior_match, "winner": published_winner,
            "home_sets": home_sets, "away_sets": away_sets,
            "home_games": home_games, "away_games": away_games,
            "completion_status": final_status, "retired": final_status == "retired",
            "walkover": final_status == "walkover", "result_source": observation.provider,
            "result_comment": observation.reason, "result_resolution_id": resolution_id,
        }
        _audit(cur, resolution_id, "match", observation.match_id, "publish", prior_match, new_match)

        bet_status_filter = "status IN ('pending','won','lost')" if allow_correction else "status='pending'"
        cur.execute(
            f"""SELECT id, side, status, result_detail, settled_at FROM tennis_bets
                WHERE match_id=%s AND bet_type='moneyline' AND {bet_status_filter} FOR UPDATE""",
            (observation.match_id,),
        )
        bets = cur.fetchall()
        bets_count = 0
        for bet in bets:
            status = "void" if state == "void" else (
                "won" if bet["side"] == observation.winner_side else "lost"
            )
            if bet["status"] == status:
                continue
            detail = observation.reason or (
                f"{match['home_player']} {observation.home_sets}-{observation.away_sets} "
                f"{match['away_player']} ({observation.provider})"
            )
            cur.execute(
                """UPDATE tennis_bets SET status=%s, result_detail=%s,
                     settled_at=NOW() WHERE id=%s""",
                (status, detail, bet["id"]),
            )
            _audit(cur, resolution_id, "bet", bet["id"], "settle", dict(bet),
                   {**dict(bet), "status": status, "result_detail": detail})
            bets_count += 1

        alerts_count = 0
        if state == "void" or allow_correction:
            settled_filter = "" if allow_correction else "AND settled_at IS NULL"
            cur.execute(
                f"""SELECT * FROM line_alerts
                    WHERE sport='tennis' AND matchup_id=%s {settled_filter} FOR UPDATE""",
                (observation.match_id,),
            )
            for alert in cur.fetchall():
                enriched_alert = {
                    **dict(alert), "winner": published_winner,
                    "completion_status": final_status, "home_games": home_games,
                    "away_games": away_games, "resolution_state": state,
                }
                target = "void" if state == "void" else _expected_alert_outcome(enriched_alert)
                if target is None or alert["outcome"] == target:
                    continue
                prior_alert = dict(alert)
                grade = _append_alert_grade_cur(
                    cur, alert, target, resolution_id,
                    fallback_version=("tennis-void-v1" if target == "void"
                                      else "tennis-result-correction-v1"),
                )
                cur.execute(
                    """UPDATE line_alerts SET outcome=%s, settled_at=NOW(),
                         grading_version=%s, grading_json=%s,
                         details_json=COALESCE(details_json, '{}'::jsonb) ||
                           jsonb_build_object('resolution_reason', %s, 'resolution_id', %s)
                       WHERE id=%s""",
                    (target, grade["grading_version"], Json(grade["grading_json"]),
                     observation.reason, resolution_id, alert["id"]),
                )
                new_alert = {
                    **prior_alert, "outcome": target, "settled_at": "now",
                    "grading_version": grade["grading_version"],
                    "grading_json": grade["grading_json"],
                }
                action = "void" if target == "void" else "correct"
                _audit(cur, resolution_id, "alert", alert["id"], action, prior_alert, new_alert)
                alerts_count += 1

        return {"state": state, "resolution_id": resolution_id,
                "bets": bets_count, "alerts": alerts_count}


def replay_pending_moneylines(db: DatabaseManager) -> int:
    """Repair deterministic moneyline drift from the current published resolution."""
    rows = db.execute(
        """SELECT DISTINCT tm.id, tm.winner, tm.result_resolution_id, tr.state
           FROM tennis_matches tm
           JOIN tennis_bets tb ON tb.match_id=tm.id
           JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
           WHERE tb.bet_type='moneyline' AND (
             (tr.state='resolved' AND tm.winner IN ('home','away')
               AND tb.status IN ('pending','won','lost')
               AND tb.status IS DISTINCT FROM
                   CASE WHEN tb.side=tm.winner THEN 'won' ELSE 'lost' END)
             OR (tr.state='void' AND tb.status IN ('pending','won','lost'))
           )"""
    )
    repaired = 0
    for row in rows:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT tm.*, tr.state AS resolution_state
                   FROM tennis_matches tm
                   JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
                   WHERE tm.id=%s FOR UPDATE OF tm""",
                (row["id"],),
            )
            match = cur.fetchone()
            cur.execute(
                """SELECT id, side, status, result_detail, settled_at FROM tennis_bets
                   WHERE match_id=%s AND bet_type='moneyline'
                     AND status IN ('pending','won','lost') FOR UPDATE""",
                (row["id"],),
            )
            for bet in cur.fetchall():
                status = ("void" if match["resolution_state"] == "void"
                          else "won" if bet["side"] == match["winner"] else "lost")
                if bet["status"] == status:
                    continue
                detail = f"Reconciled from resolution {match['result_resolution_id']}"
                cur.execute(
                    "UPDATE tennis_bets SET status=%s, result_detail=%s, settled_at=NOW() WHERE id=%s",
                    (status, detail, bet["id"]),
                )
                _audit(cur, match["result_resolution_id"], "bet", bet["id"],
                       "reconcile", dict(bet), {**dict(bet), "status": status})
                repaired += 1
    return repaired


def replay_tennis_alert_outcomes(db: DatabaseManager) -> int:
    """Reconcile Tennis alert outcomes and append an auditable grade revision."""
    rows = db.execute(
        """SELECT la.*, tm.winner, tm.completion_status, tm.home_games, tm.away_games,
                  tm.result_resolution_id, tr.state AS resolution_state,
                  EXISTS (
                    SELECT 1 FROM alert_grades ag WHERE ag.alert_id=la.id AND ag.is_current
                      AND ag.outcome IS NOT DISTINCT FROM la.outcome
                  ) AS grade_current
           FROM line_alerts la
           JOIN tennis_matches tm ON tm.id=la.matchup_id
           JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
           WHERE la.sport='tennis'"""
    )
    repaired = 0
    for row in rows:
        target = _expected_alert_outcome(row)
        if target is None or (row["outcome"] == target and row["grade_current"]):
            continue
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT la.*, tm.winner, tm.completion_status, tm.home_games, tm.away_games,
                          tm.result_resolution_id, tr.state AS resolution_state,
                          EXISTS (
                            SELECT 1 FROM alert_grades ag
                            WHERE ag.alert_id=la.id AND ag.is_current
                              AND ag.outcome IS NOT DISTINCT FROM la.outcome
                          ) AS grade_current
                   FROM line_alerts la
                   JOIN tennis_matches tm ON tm.id=la.matchup_id
                   JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
                   WHERE la.id=%s FOR UPDATE OF la""",
                (row["id"],),
            )
            alert = cur.fetchone()
            target = _expected_alert_outcome(alert)
            if target is None or (alert["outcome"] == target and alert["grade_current"]):
                continue
            prior_alert = dict(alert)
            grade = _append_alert_grade_cur(
                cur, alert, target, alert["result_resolution_id"],
                fallback_version="tennis-result-reconcile-v1",
            )
            cur.execute(
                """UPDATE line_alerts SET outcome=%s, settled_at=NOW(),
                     grading_version=%s, grading_json=%s,
                     details_json=COALESCE(details_json, '{}'::jsonb) ||
                       jsonb_build_object('resolution_id', %s, 'outcome_reconciled', TRUE)
                   WHERE id=%s""",
                (target, grade["grading_version"], Json(grade["grading_json"]),
                 alert["result_resolution_id"], alert["id"]),
            )
            new_alert = {
                **prior_alert, "outcome": target, "settled_at": "now",
                "grading_version": grade["grading_version"],
                "grading_json": grade["grading_json"],
            }
            _audit(cur, alert["result_resolution_id"], "alert", alert["id"],
                   "reconcile", prior_alert, new_alert)
            repaired += 1
    return repaired
