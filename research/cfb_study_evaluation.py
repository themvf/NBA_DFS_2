"""Phase 5 status and immutable evaluation runner for the CFB moneyline study."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import random
from statistics import mean
from uuid import UUID, uuid5

from config import PROJECT_DIR, load_config
from ingest.cfb_economics_migrate import migrate as migrate_economics


NAMESPACE = UUID("48f255fe-0d78-49ce-bd4e-905762253acf")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * fraction)]


def clustered_interval(rows: list[dict], value_key: str, cluster_key: str, *, draws: int = 2000) -> dict:
    usable = [row for row in rows if row.get(value_key) is not None and row.get(cluster_key) is not None]
    if not usable:
        return {"mean": None, "lower": None, "upper": None, "half_width": None, "n": 0, "clusters": 0}
    groups: dict[str, list[float]] = defaultdict(list)
    for row in usable:
        groups[str(row[cluster_key])].append(float(row[value_key]))
    keys = sorted(groups)
    seed = int(sha256(_canonical({"values": groups, "key": value_key})).hexdigest()[:16], 16)
    generator = random.Random(seed)
    samples = []
    for _ in range(draws):
        picked = [generator.choice(keys) for _ in keys]
        samples.append(mean(value for key in picked for value in groups[key]))
    lower, upper = _percentile(samples, 0.025), _percentile(samples, 0.975)
    estimate = mean(row[value_key] for row in usable)
    return {"mean": estimate, "lower": lower, "upper": upper,
            "half_width": (upper - lower) / 2 if lower is not None and upper is not None else None,
            "n": len(usable), "clusters": len(keys)}


def _adverse_roi(rows: list[dict], haircut: float) -> float | None:
    pnl = stake = 0.0
    for row in rows:
        outcome, entry = row.get("outcome"), row.get("entry_decimal")
        if outcome not in {"won", "lost", "push"} or entry is None:
            continue
        stake += 1
        if outcome == "won":
            pnl += max(1.0001, float(entry) * (1 - haircut)) - 1
        elif outcome == "lost":
            pnl -= 1
    return pnl / stake if stake else None


def evaluate_rows(rows: list[dict], config: dict, *, purpose: str) -> dict:
    settled = [row for row in rows if row["result_state"] == "settled"]
    conflicts = [row for row in rows if row["result_state"] == "conflict"]
    stake = sum(float(row["roi_stake_units"] or 0) for row in rows)
    pnl = sum(float(row["pnl_units"] or 0) for row in rows if row["pnl_units"] is not None)
    primary = clustered_interval(rows, "decimal_price_ratio_pct", "game_date")
    game_sensitivity = clustered_interval(rows, "decimal_price_ratio_pct", "matchup_id")
    dates = sorted({str(row["game_date"]) for row in rows})
    leave_one_date_out = []
    for game_date in dates:
        values = [float(row["decimal_price_ratio_pct"]) for row in rows
                  if str(row["game_date"]) != game_date and row.get("decimal_price_ratio_pct") is not None]
        leave_one_date_out.append({"omitted_date": game_date, "mean": mean(values) if values else None})
    by_date: dict[str, float] = defaultdict(float)
    by_game: dict[str, float] = defaultdict(float)
    for row in rows:
        if row.get("pnl_units") is not None:
            by_date[str(row["game_date"])] += float(row["pnl_units"])
            by_game[str(row["matchup_id"])] += float(row["pnl_units"])
    abs_pnl = sum(abs(value) for value in by_game.values())
    health = {
        "mapped_event_rate": sum(row.get("matchup_id") is not None for row in rows) / len(rows) if rows else 0,
        "eligible_quote_freshness_rate": sum(bool(row.get("fresh_quote")) for row in rows) / len(rows) if rows else 0,
        "settlement_completeness_rate": len(settled) / len(rows) if rows else 0,
        "economic_conflict_rate": len(conflicts) / len(rows) if rows else 0,
    }
    floors = config["health_floors"]
    health_pass = (health["mapped_event_rate"] >= floors["mapped_event_rate"]
                   and health["eligible_quote_freshness_rate"] >= floors["eligible_quote_freshness_rate"]
                   and health["settlement_completeness_rate"] >= floors["settlement_completeness_rate"]
                   and health["economic_conflict_rate"] <= floors["economic_conflict_rate_max"])
    adverse_1 = _adverse_roi(rows, 0.01)
    if not health_pass:
        result = "invalid"
    elif primary["mean"] is not None and primary["mean"] <= 0:
        result = "fail"
    elif primary["upper"] is not None and primary["upper"] <= 0:
        result = "fail"
    elif (purpose.startswith("confirmation") and len(dates) >= config["minimum_independent_game_dates"]
          and primary["mean"] is not None and primary["mean"] >= config["minimum_effect"]
          and primary["lower"] is not None and primary["lower"] > 0
          and primary["half_width"] is not None and primary["half_width"] <= 1.0
          and adverse_1 is not None and adverse_1 > 0):
        result = "pass"
    else:
        result = "inconclusive"
    family = {}
    for alert_type in sorted({row["alert_type"] for row in rows}):
        subset = [row for row in rows if row["alert_type"] == alert_type]
        family[alert_type] = {"primary": clustered_interval(subset, "decimal_price_ratio_pct", "game_date"),
                              "n": len(subset), "dates": len({str(row["game_date"]) for row in subset})}
    return {
        "result": result, "purpose": purpose, "observations": len(rows), "settled": len(settled),
        "games": len({row["matchup_id"] for row in rows}), "game_dates": len(dates),
        "pnl_units": pnl, "roi": pnl / stake if stake else None, "roi_stake_units": stake,
        "primary": primary, "game_clustered_sensitivity": game_sensitivity,
        "adverse_price_roi": {"one_percent": adverse_1, "two_percent": _adverse_roi(rows, 0.02)},
        "health": health, "health_pass": health_pass, "family_diagnostics": family,
        "leave_one_date_out": leave_one_date_out,
        "concentration": {"pnl_by_date": dict(sorted(by_date.items())), "pnl_by_game": dict(sorted(by_game.items())),
                          "largest_game_share_of_absolute_pnl": max((abs(value) for value in by_game.values()), default=0) / abs_pnl if abs_pnl else None},
        "missingness": {"primary_metric": sum(row.get("decimal_price_ratio_pct") is None for row in rows),
                        "entry_price": sum(row.get("entry_decimal") is None for row in rows)},
        "limitations": ["Family diagnostics are multiplicity-controlled research outputs, not standalone grants.",
                        "Pilot results cannot qualify a consumer.", "Multiple observations on one game are not treated as independent dates."],
    }


def _read_registered_config(study: dict) -> dict:
    """Resolve a frozen artifact across runner OSes and verify its contents."""
    digest = study["configuration_digest"]
    portable = PROJECT_DIR / "artifacts" / f"cfb_moneyline_study_{digest}.json"
    stored = Path(study["uri"]) if study.get("uri") else None
    path = stored if stored is not None and stored.is_file() else portable
    if not path.is_file():
        raise ValueError(f"registered study artifact unavailable for digest {digest}")
    config = json.loads(path.read_text(encoding="utf-8"))
    computed = sha256(_canonical({key: value for key, value in config.items()
                                  if key != "configuration_digest"})).hexdigest()
    if config.get("configuration_digest") != digest or computed != digest:
        raise ValueError("registered study configuration digest mismatch")
    return config


def _load_registration(cursor, study_version: int | None) -> tuple[dict, dict, list[dict]]:
    version_clause = "AND s.study_version=%s" if study_version is not None else ""
    params = (study_version,) if study_version is not None else ()
    cursor.execute(f"""SELECT s.*,a.uri FROM cfb_engine_studies s JOIN cfb_engine_artifacts a
      ON a.artifact_id=s.configuration_artifact_id WHERE 1=1 {version_clause}
      ORDER BY s.study_version DESC LIMIT 1""", params)
    study = dict(cursor.fetchone())
    config = _read_registered_config(study)
    cursor.execute("""SELECT * FROM cfb_engine_study_windows WHERE study_id=%s AND study_version=%s ORDER BY start_at""",
                   (study["study_id"], study["study_version"]))
    return study, config, [dict(row) for row in cursor.fetchall()]


def _window_rows(cursor, config: dict, window: dict) -> list[dict]:
    definitions = {(item["alert_type"], item["signal_version"]) for item in config["candidate_definitions"]}
    cursor.execute("""SELECT a.id AS alert_id,a.alert_type,COALESCE(a.signal_version,a.details_json->>'signal_version') signal_version,
        a.matchup_id,a.game_date,a.side,a.details_json,h.books,h.captured_at,r.resolution_id,r.result_state,r.outcome,
        r.entry_decimal,r.pnl_units,r.roi_stake_units,r.metrics
      FROM line_alerts a JOIN cfb_economic_resolutions r ON r.alert_id=a.id
      LEFT JOIN game_odds_history h ON h.id=a.trigger_history_id
      WHERE a.sport='cfb' AND a.origin='prospective' AND a.created_at>=%s AND a.created_at<%s
        AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)
      ORDER BY a.created_at,a.id""", (window["start_at"], window["end_at"]))
    output = []
    for raw in cursor.fetchall():
        row = dict(raw)
        if (row["alert_type"], row["signal_version"]) not in definitions:
            continue
        details, books = row["details_json"] or {}, row["books"] or {}
        quote = books.get(details.get("exec_book") or "draftkings") or {}
        updated = quote.get("last_update")
        fresh = False
        if updated and row.get("captured_at"):
            parsed = datetime.fromisoformat(str(updated).replace("Z", "+00:00"))
            age = (row["captured_at"] - parsed).total_seconds()
            fresh = 0 <= age <= 300
        metrics = row["metrics"] or {}
        row["decimal_price_ratio_pct"] = metrics.get("decimal_price_ratio_pct")
        row["fresh_quote"] = fresh
        output.append(row)
    return output


def status(database_url: str, *, study_version: int | None = None, now: datetime | None = None) -> dict:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    now = now or datetime.now(timezone.utc)
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        study, config, windows = _load_registration(cursor, study_version)
        result = {"study_id": str(study["study_id"]), "study_version": study["study_version"],
                  "configuration_digest": study["configuration_digest"], "as_of": now.isoformat(), "windows": []}
        for window in windows:
            rows = _window_rows(cursor, config, window)
            review_at = window["end_at"] + timedelta(hours=config["review_schedule"]["settlement_grace_hours"])
            state = ("scheduled" if now < window["start_at"] else "collecting" if now < window["end_at"]
                     else "awaiting_settlement_grace" if now < review_at else "ready_to_finalize")
            result["windows"].append({"window_key": window["window_key"], "purpose": window["purpose"],
                                      "start_at": window["start_at"], "end_at": window["end_at"],
                                      "review_at": review_at,
                                      "state": state, "observations": len(rows),
                                      "settled": sum(row["result_state"] == "settled" for row in rows)})
        return result


def finalize(database_url: str, window_key: str, *, study_version: int | None = None,
             now: datetime | None = None) -> dict:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor, register_uuid

    register_uuid()
    now = now or datetime.now(timezone.utc)
    migrate_economics(database_url)
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"cfb-study-finalize:{window_key}",))
        study, config, windows = _load_registration(cursor, study_version)
        window = next((item for item in windows if item["window_key"] == window_key), None)
        if window is None:
            raise ValueError(f"unknown study window: {window_key}")
        review_at = window["end_at"] + timedelta(hours=config["review_schedule"]["settlement_grace_hours"])
        if now < review_at:
            raise RuntimeError(f"window {window_key} cannot finalize before {review_at.isoformat()}")
        rows = _window_rows(cursor, config, window)
        report = evaluate_rows(rows, config, purpose=window["purpose"])
        report.update({"study_id": str(study["study_id"]), "study_version": study["study_version"],
                       "window_key": window_key, "window_start": window["start_at"].isoformat(),
                       "window_end": window["end_at"].isoformat(), "evaluated_through": now.isoformat(),
                       "resolution_ids": [str(row["resolution_id"]) for row in rows]})
        encoded = json.dumps(report, indent=2, sort_keys=True, default=str).encode()
        digest = sha256(encoded).hexdigest()
        path = PROJECT_DIR / "artifacts" / f"cfb_study_{study['study_version']}_{window_key}_{digest}.json"
        path.write_bytes(encoded)
        report_artifact = uuid5(NAMESPACE, f"report:{digest}")
        method_digest = sha256(Path(__file__).read_bytes()).hexdigest()
        method_artifact = uuid5(NAMESPACE, f"method:{method_digest}")
        manifest_digest = sha256(_canonical({"resolutions": sorted(report["resolution_ids"]),
                                              "study": str(study["study_id"]), "version": study["study_version"],
                                              "window": window_key})).hexdigest()
        manifest_id = uuid5(NAMESPACE, f"manifest:{manifest_digest}")
        idempotency = sha256(_canonical({"report": digest, "manifest": manifest_digest})).hexdigest()
        cursor.execute("SELECT evaluation_id,report_revision,result FROM cfb_engine_evaluations WHERE idempotency_key=%s", (idempotency,))
        existing = cursor.fetchone()
        if existing:
            return {"status": "already_finalized", **dict(existing), "report": str(path)}
        cursor.execute("""INSERT INTO cfb_engine_artifacts(artifact_id,kind,digest,uri,representation,byte_count,metadata)
          VALUES (%s,'cfb-study-evaluation-report',%s,%s,'report',%s,%s),
                 (%s,'cfb-study-evaluation-method',%s,%s,'code',%s,%s) ON CONFLICT DO NOTHING""",
          (report_artifact, digest, str(path), len(encoded), Json({"window": window_key}),
           method_artifact, method_digest, str(Path(__file__)), Path(__file__).stat().st_size, Json({"method": "cluster-bootstrap-v1"})))
        cursor.execute("""INSERT INTO cfb_context_manifests(manifest_id,kind,scope_key,as_of_at,manifest_digest)
          VALUES (%s,'evaluation',%s,%s,%s) ON CONFLICT DO NOTHING""",
          (manifest_id, f"study:{study['study_id']}:{study['study_version']}:{window_key}", window["end_at"], manifest_digest))
        for ordinal, row in enumerate(rows):
            cursor.execute("""INSERT INTO cfb_context_manifest_items(manifest_id,slot,ordinal,economic_resolution_id)
              VALUES (%s,'economic_resolution',%s,%s) ON CONFLICT DO NOTHING""",
              (manifest_id, ordinal, row["resolution_id"]))
        cursor.execute("""SELECT evaluation_id,report_revision FROM cfb_engine_evaluations
          WHERE study_id=%s AND study_version=%s AND window_key=%s ORDER BY report_revision DESC LIMIT 1""",
          (study["study_id"], study["study_version"], window_key))
        prior = cursor.fetchone()
        revision = int(prior["report_revision"]) + 1 if prior else 1
        evaluation_id = uuid5(NAMESPACE, f"evaluation:{idempotency}")
        cursor.execute("""INSERT INTO cfb_engine_evaluations
          (evaluation_id,study_id,study_version,window_key,report_revision,input_manifest_id,report_artifact_id,
           evaluated_through,result,supersedes_evaluation_id,idempotency_key)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
          (evaluation_id, study["study_id"], study["study_version"], window_key, revision, manifest_id,
           report_artifact, now, report["result"], prior["evaluation_id"] if prior else None, idempotency))
        metrics = [("primary_mean", report["primary"]["mean"]), ("primary_lower", report["primary"]["lower"]),
                   ("primary_upper", report["primary"]["upper"]), ("roi", report["roi"]),
                   ("adverse_price_roi_1pct", report["adverse_price_roi"]["one_percent"])]
        for metric, value in metrics:
            cursor.execute("""INSERT INTO cfb_engine_evaluation_metrics
              (evaluation_id,metric_key,cohort_key,unit,value,n_observations,n_games,n_dates,missing_count,method_artifact_id)
              VALUES (%s,%s,'all',%s,%s,%s,%s,%s,%s,%s)""",
              (evaluation_id, metric, "percent" if metric.startswith("primary") else "unit_per_stake", value,
               report["observations"], report["games"], report["game_dates"], report["missingness"]["primary_metric"], method_artifact))
    return {"status": "finalized", "evaluation_id": str(evaluation_id), "result": report["result"], "report": str(path)}


def finalize_due(database_url: str, *, study_version: int | None = None,
                 now: datetime | None = None) -> dict:
    """Finalize only windows whose frozen review boundary has elapsed."""
    snapshot = status(database_url, study_version=study_version, now=now)
    completed = []
    for window in snapshot["windows"]:
        if window["state"] == "ready_to_finalize":
            completed.append(finalize(database_url, window["window_key"],
                                      study_version=snapshot["study_version"], now=now))
    return {"study_version": snapshot["study_version"], "finalized": completed,
            "waiting": [window["window_key"] for window in snapshot["windows"]
                        if window["state"] != "ready_to_finalize"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study-version", type=int)
    parser.add_argument("--finalize", choices=("pilot", "confirmation_1", "confirmation_2"))
    parser.add_argument("--finalize-due", action="store_true")
    args = parser.parse_args()
    database_url = load_config().database_url or ""
    if args.finalize and args.finalize_due:
        parser.error("choose --finalize or --finalize-due, not both")
    output = (finalize(database_url, args.finalize, study_version=args.study_version) if args.finalize
              else finalize_due(database_url, study_version=args.study_version) if args.finalize_due
              else status(database_url, study_version=args.study_version))
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
