"""Workload-conflict review for frozen NBA ghost forecasts (`nba-workload-conflict-v1`).

Implements section A of docs/nba-points-opportunity-reliability-spec.md. A
REVIEW flag, not a probability, an injury bonus or a recommendation: "the model
expects 13 shots, this player has recently taken 18; review the role
assumption." It never changes a forecast, snapshot or experiment.

Per pinned model, for every candidate in an original frozen snapshot:

- Recent window: the latest five current-season appearances in the frozen
  history (three or four if that is all), ordered by game date then source game
  id. History rows are already regular-season-classified and DNP-free by the
  ghost v2 merge; zero-point, zero-make and low-minute games stay in. A missing
  FGA or invalid minutes in the window makes it incomplete (no older row is
  substituted).
- R = mean FGA in the window; F = the model's FGA forecast.
  review_required  when  R - F >= 3.0  and  R > 0  and  F / R <= 0.85.
- recent_workload_rising (explanatory tag only): R exceeds the mean FGA of the
  latest 20 appearances strictly before the window by at least 2.0 (10-19 such
  appearances allowed; fewer is unknown). Only the frozen history is used.
- Team continuity is reported from the window's own team fields; the
  decision-time roster is UNVERIFIED (no timestamped roster source yet).

Every candidate gets a status: review_required, no_conflict, insufficient_history,
missing_forecast, incomplete_window or ambiguous_identity. Conflicts sort by R - F,
then frozen line, then player key; all are kept.

    python -m research.nba_points_workload_conflicts --date 2026-10-30           # print
    python -m research.nba_points_workload_conflicts --date 2026-10-30 --apply   # also store the record
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
from typing import Any

VERSION = "nba-workload-conflict-v1"
GAP_MIN = 3.0
RATIO_MAX = 0.85
WINDOW_MAX, WINDOW_MIN = 5, 3
RISING_GAP = 2.0
BASELINE_MAX, BASELINE_MIN = 20, 10
RECORD_TABLE = "nba_role_reliability_records"
DDL = (
    f"""CREATE TABLE IF NOT EXISTS {RECORD_TABLE} (
        id BIGSERIAL PRIMARY KEY, game_date DATE NOT NULL, kind TEXT NOT NULL, version TEXT NOT NULL,
        snapshot_sha256 TEXT, payload_sha256 TEXT NOT NULL, payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (game_date, kind, version, snapshot_sha256))""",
    """CREATE OR REPLACE FUNCTION reject_nba_role_reliability_mutation() RETURNS trigger AS $$
       BEGIN RAISE EXCEPTION 'nba_role_reliability_records is append-only'; END $$ LANGUAGE plpgsql""",
    f"""DO $$ BEGIN
         IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nba_role_reliability_append_only') THEN
           CREATE TRIGGER nba_role_reliability_append_only BEFORE UPDATE OR DELETE ON {RECORD_TABLE}
             FOR EACH ROW EXECUTE FUNCTION reject_nba_role_reliability_mutation();
         END IF; END $$""",
)


def _finite(x: Any) -> bool:
    try:
        return x is not None and not isinstance(x, bool) and math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def season_of(day: Any) -> int:
    d = date.fromisoformat(str(day)[:10])
    return d.year if d.month >= 10 else d.year - 1


def _order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: (str(r.get("game_date")), str(r.get("source_game_id") or "")))


def evaluate_candidate(c: dict[str, Any], slate_date: str, model: str = "v2", forecast: dict[str, Any] | None = None) -> dict[str, Any]:
    """One candidate's conflict result. `forecast` overrides the snapshot's own (for a v3 forecast file)."""
    fc = forecast if forecast is not None else (c.get("opportunity_forecasts") or {})
    base = {"model": model, "event_id": c.get("event_id"), "player_key": c.get("player_key"), "player": c.get("player"),
            "line": (c.get("predictions") or {}).get("market_line", c.get("line")),
            "expected_points": (c.get("predictions") or {}).get("opportunity_points")}
    if c.get("history_status") == "blocked":
        return {**base, "status": "ambiguous_identity"}
    F, M0 = fc.get("fga"), fc.get("minutes")
    if not _finite(F) or float(F) < 0:
        return {**base, "status": "missing_forecast"}
    history = _order([h for h in (c.get("history") or []) if h.get("played", True)])
    keys = [(str(h.get("game_date")), str(h.get("source_game_id"))) for h in history]
    if len(keys) != len(set(keys)):
        return {**base, "status": "ambiguous_identity", "reason": "duplicate game rows in the frozen history"}
    season = season_of(slate_date)
    current = [h for h in history if season_of(h["game_date"]) == season and str(h["game_date"]) < slate_date]
    if len(current) < WINDOW_MIN:
        return {**base, "status": "insufficient_history", "current_season_appearances": len(current)}
    window = current[-WINDOW_MAX:]
    if any(not _finite(h.get("fga")) or float(h["fga"]) < 0 or not _finite(h.get("minutes")) or float(h["minutes"]) < 0 for h in window):
        return {**base, "status": "incomplete_window", "window_games": [h.get("source_game_id") for h in window]}
    R = sum(float(h["fga"]) for h in window) / len(window)
    mins = sum(float(h["minutes"]) for h in window)
    F = float(F)
    conflict = (R - F >= GAP_MIN) and R > 0 and (F / R <= RATIO_MAX)
    earlier = [h for h in history if str(h["game_date"]) < str(window[0]["game_date"])][-BASELINE_MAX:]
    if len(earlier) >= BASELINE_MIN and all(_finite(h.get("fga")) for h in earlier):
        ref = sum(float(h["fga"]) for h in earlier) / len(earlier)
        rising: bool | None = R - ref >= RISING_GAP
    else:
        ref, rising = None, None
    teams = sorted({str(h.get("team")) for h in window if h.get("team")})
    return {**base, "status": "review_required" if conflict else "no_conflict",
            "forecast_fga": F, "forecast_minutes": float(M0) if _finite(M0) else None,
            "recent_fga": R, "recent_minutes": mins / len(window), "window_size": len(window),
            "window_games": [h.get("source_game_id") for h in window], "window_dates": [str(h["game_date"]) for h in window],
            "gap": R - F, "ratio": F / R if R > 0 else None,
            "recent_fga_per_min": (sum(float(h["fga"]) for h in window) / mins) if mins > 0 else None,
            "forecast_fga_per_min": (F / float(M0)) if _finite(M0) and float(M0) > 0 else None,
            "reference_fga": ref, "reference_size": len(earlier), "recent_workload_rising": rising,
            "existing_flags": list(c.get("observed_workload_flags") or []),
            "window_teams": teams, "team_continuity": "same_team_in_window" if len(teams) == 1 else ("team_change_in_window" if teams else "unknown"),
            "decision_time_team": "unverified",
            "availability": (c.get("availability") or {}).get("status", "unverified")}


def evaluate_snapshot(snapshot: dict[str, Any], model: str = "v2", forecasts: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """All candidates of one original frozen snapshot. `forecasts` maps player_key -> FGA/minutes forecast for another model."""
    slate = str(snapshot["game_date"])
    results = []
    for c in snapshot.get("candidates") or []:
        fc = None if forecasts is None else forecasts.get(c.get("player_key"), {})
        results.append(evaluate_candidate(c, slate, model, fc))
    conflicts = sorted((r for r in results if r["status"] == "review_required"),
                       key=lambda r: (-r["gap"], -(r["line"] or 0), r["player_key"]))
    counts: dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"version": VERSION, "model": model, "game_date": slate, "snapshot_sha256": snapshot.get("sha256"),
            "snapshot_mode": snapshot.get("mode"), "snapshot_created_at": snapshot.get("created_at"),
            "rules": {"gap_min": GAP_MIN, "ratio_max": RATIO_MAX, "window": [WINDOW_MIN, WINDOW_MAX], "rising_gap": RISING_GAP,
                      "baseline": [BASELINE_MIN, BASELINE_MAX]},
            "counts": counts, "conflicts": conflicts, "all": results}


def headline(r: dict[str, Any]) -> str:
    return (f"Workload conflict: model {r['forecast_fga']:.1f} shots; recent {r['recent_fga']:.1f} across "
            f"{r['window_size']} games. Review role assumptions.")


def grade(record: dict[str, Any], box: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Descriptive next-day outcome per conflict: actual FGA vs the forecast (ties separate). `box` maps player_key -> {fga, played}."""
    out = []
    for r in record["conflicts"]:
        b = box.get(r["player_key"])
        if not b or not b.get("played") or not _finite(b.get("fga")):
            out.append({"player_key": r["player_key"], "outcome": "unresolved"})
            continue
        a = float(b["fga"])
        out.append({"player_key": r["player_key"], "actual_fga": a, "forecast_fga": r["forecast_fga"],
                    "outcome": "above_forecast" if a > r["forecast_fga"] else ("tie" if a == r["forecast_fga"] else "at_or_below_forecast")})
    return {"version": VERSION, "game_date": record["game_date"], "outcomes": out}


# ── database ──

def _db():
    from config import load_config
    from db.database import DatabaseManager

    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_snapshot(db, day: str) -> dict[str, Any] | None:
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT snapshot FROM nba_ghost_snapshots WHERE game_date = %s AND version = 'nba-points-ghost-v2'
            ORDER BY (mode = 'prospective') DESC, created_at LIMIT 1""", (day,))
        rows = fetch_rows(cur)
    return rows[0]["snapshot"] if rows else None


def store(db, record: dict[str, Any]) -> bool:
    """Append once per (date, kind, version, snapshot). Never replaces an earlier record."""
    from psycopg2.extras import Json

    payload = json.dumps(record, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    with db.connect() as conn:
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        cur.execute(f"""INSERT INTO {RECORD_TABLE} (game_date, kind, version, snapshot_sha256, payload_sha256, payload)
            VALUES (%s, 'workload_conflicts', %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING id""",
                    (record["game_date"], VERSION, record["snapshot_sha256"], digest, Json(json.loads(payload))))
        wrote = cur.fetchone() is not None
        conn.commit()
    return wrote


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default=None, help="Eastern slate date (default: today ET)")
    ap.add_argument("--apply", action="store_true", help="store the record (append-only)")
    args = ap.parse_args()
    from ingest.nba_box_scores import eastern_date

    day = args.date or eastern_date(datetime.now(timezone.utc)).isoformat()
    db = _db()
    snapshot = load_snapshot(db, day)
    if snapshot is None:
        print(f"no ghost v2 snapshot for {day}; nothing to review")
        return 0
    record = evaluate_snapshot(snapshot)
    print(f"{VERSION} {day} ({record['snapshot_mode']}): {record['counts']}")
    for r in record["conflicts"]:
        print(f"  {r['player']:<26} line {r['line']}: {headline(r)} rising={r['recent_workload_rising']} team={r['team_continuity']}")
    if args.apply:
        print("stored" if store(db, record) else "already stored for this snapshot; left unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
