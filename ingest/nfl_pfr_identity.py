"""Append official current-season PFR/GSIS claims before freezing PFR evidence.

Only identifiers present together in a provider roster row are accepted.
Conflicting claims remain quarantined by the existing identity registry.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

from db.nfl_identity_registry import DDL
from model.nfl_context_engine import stable_digest


def roster_claims(content: bytes, season: int, source: dict) -> list[dict]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    required = {"season", "gsis_id", "pfr_id", "full_name", "position", "team", "week"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("roster provider-ID columns missing")
    claims = {}
    for row in reader:
        if row["season"] != str(season):
            continue
        if not re.fullmatch(r"\d{2}-\d{7}", row["gsis_id"] or "") or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]+", row["pfr_id"] or ""):
            continue
        if not row["full_name"] or not row["position"] or not row["team"]:
            continue
        # A provider identifier is authoritative only for identity, never role.
        claim = {"namespace": "pfr", "external_id": row["pfr_id"], "gsis_id": row["gsis_id"],
                 "player_name": row["full_name"], "season": season, "team": row["team"], "position": row["position"],
                 "method": "nflverse_current_roster_provider_ids", "source_digest": source["sha256"],
                 "evidence": {"source": source, "row": {key: row[key] for key in sorted(required)},
                              "identity_only": True, "name_match_used": False}}
        key = (claim["external_id"], claim["gsis_id"], claim["team"], claim["position"])
        # Retain one deterministic supporting row per exact claim. Disagreeing
        # GSIS claims deliberately use different keys and cannot be overwritten.
        if key not in claims:
            claim["claim_digest"] = stable_digest(claim)
            claims[key] = claim
    if not claims:
        raise ValueError("no source-backed PFR/GSIS roster claims")
    return [claims[key] for key in sorted(claims)]


def refresh_identity(db, season: int, output_dir: Path) -> dict:
    from psycopg2.extras import Json, execute_values
    url = f"https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{season}.csv"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    source = {"url": url, "sha256": hashlib.sha256(response.content).hexdigest(),
              "captured_at": datetime.now(timezone.utc).isoformat(), "provider": "nflverse_weekly_rosters"}
    claims = roster_claims(response.content, season, source)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"roster_weekly_{season}.csv").write_bytes(response.content)
    (output_dir / f"roster_weekly_{season}.manifest.json").write_text(json.dumps(source, indent=2), encoding="utf-8")
    fields = ("claim_digest", "namespace", "external_id", "gsis_id", "player_name", "season", "team", "position", "method", "source_digest", "evidence")
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(DDL)
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('nfl_player_identity_registry'))")
        execute_values(cur, "INSERT INTO nfl_player_identity_claims (" + ",".join(fields) + ") VALUES %s ON CONFLICT DO NOTHING",
                       [tuple(Json(claim[field]) if field == "evidence" else claim[field] for field in fields) for claim in claims])
        cur.execute("SELECT status,COUNT(*) FROM nfl_player_identity_crosswalk WHERE namespace='pfr' GROUP BY status")
        counts = {row["status"]: row["count"] for row in cur.fetchall()}
    return {"status": "captured", "claims": len(claims), "crosswalk_counts": counts, "source": source}
