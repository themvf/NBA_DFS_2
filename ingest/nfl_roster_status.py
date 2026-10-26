"""Point-in-time nflverse roster status (practice squad or not) for the DFS pool.

Owner's decision 2026-10-25: NFL DFS projections exclude practice-squad
players unless the depth chart confirms them. Sleeper cannot say who is on a
practice squad -- it lists them ``status: Active`` with no depth number (Salvon
Ahmed, Sincere McCormick, Jawhar Jordan in every 2026 week) -- so the DFS page
kept projecting them at 6-7 points off stat lines years old. nflverse's weekly
roster does say: ``status = 'DEV'`` is the practice squad. A practice-squad
player elevated for a game is ``ACT`` for that week, so DEV in the final file
means he could not play: 0 of 1,144 offensive DEV player-weeks in 2026 weeks
1-7 have a stat line.

The file's newest week is rewritten in place as the week goes on (about once a
day), and ``ff_players.metadata.nflverse_roster`` keeps only the latest copy,
so neither can say what was known at a past decision time. This capture keeps
that history: each run reads the file and appends a row for every player whose
(roster_week, team, status, description) differs from his newest stored row.
The DFS read takes the newest row at or before the slate's decision time
(``web/src/db/nfl-dfs-availability.ts``); the rule itself lives in
``practiceSquadBlock`` (``web/src/lib/nfl-dfs/availability.ts``).

Runs in ``refresh_nfl_availability_context.yml`` beside the Sleeper capture,
as an optional step: when it fails the pool falls back to no practice-squad
exclusion and the page says the status is unavailable; it never blocks Sleeper.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
import sys
from typing import Any, Iterable, Mapping

import pandas as pd
import requests

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot
from ingest.nfl_dfs_weekly import target_season

VERSION = "nfl-roster-status-v1"
ROSTER_URL = "https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{season}.csv"
DATASET_PREFIX = "roster-status-live-"
# DraftKings positions (FB is listed as RB); DST has no roster row.
POSITIONS = frozenset({"QB", "RB", "FB", "WR", "TE", "K"})
# A healthy file has ~2,500 offensive rows in its newest week; far fewer means a
# truncated or mid-rewrite download, and writing it would "move" hundreds of
# players to whatever the partial file says.
MIN_CURRENT_ROWS = 1000

KEY_FIELDS = ("roster_week", "team", "status", "status_description")


def current_week_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """One row per player: his row in the file's newest week.

    A player missing from the newest week (released, or the week not yet
    rolled for his team) is not listed: absence is not a status, so his newest
    stored row stays in force rather than being overwritten by a guess.
    """
    if frame.empty:
        return []
    newest = int(frame["week"].max())
    rows = frame[(frame["week"] == newest) & frame["position"].isin(POSITIONS) & frame["gsis_id"].notna()]
    out: dict[str, dict[str, Any]] = {}
    for record in rows.to_dict("records"):
        gsis = str(record["gsis_id"]).strip()
        if not gsis:
            continue
        description = record.get("status_description_abbr")
        out[gsis] = {
            "gsis_id": gsis,
            "roster_week": newest,
            "team": None if pd.isna(record.get("team")) else str(record["team"]),
            "status": str(record.get("status") or "UNKNOWN").strip().upper(),
            "status_description": None if description is None or pd.isna(description) else str(description),
        }
    return list(out.values())


def changed_rows(current: Iterable[Mapping[str, Any]], latest: Mapping[str, Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The rows whose key fields differ from that player's newest stored row."""
    return [row for row in current
            if (prior := latest.get(row["gsis_id"])) is None
            or any(prior.get(field) != row.get(field) for field in KEY_FIELDS)]


def _provider_modified(response: requests.Response) -> datetime | None:
    value = response.headers.get("Last-Modified")
    try:
        parsed = parsedate_to_datetime(value) if value else None
    except (TypeError, ValueError):
        return None
    return parsed if parsed is not None and parsed.tzinfo is not None else None


def capture_roster_status(db: Any, *, season: int, now: datetime) -> dict[str, Any]:
    response = requests.get(ROSTER_URL.format(season=season), timeout=60, headers={"User-Agent": "DFS-Vegas/1.0"})
    response.raise_for_status()
    frame = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    current = current_week_rows(frame)
    if len(current) < MIN_CURRENT_ROWS:
        raise ValueError(f"nflverse roster newest week has {len(current)} offensive rows (< {MIN_CURRENT_ROWS}); no writes performed")
    players = db.execute("SELECT id, gsis_id FROM ff_players WHERE season=%s AND gsis_id IS NOT NULL", (season,))
    by_gsis: dict[str, int] = {}
    for row in players:
        by_gsis.setdefault(str(row["gsis_id"]), int(row["id"]))
    matched = [row | {"player_id": by_gsis[row["gsis_id"]]} for row in current if row["gsis_id"] in by_gsis]
    provider_modified = _provider_modified(response)
    snapshot_id = persist_source_snapshot(db, SnapshotProvenance(
        source="nflverse", dataset=f"{DATASET_PREFIX}{season}-{now:%Y%m%d%H}", season=season,
        week=current[0]["roster_week"],
        request_params={"url": ROSTER_URL.format(season=season), "version": VERSION},
        source_published_at=provider_modified,
        fetched_at=now, response_hash=hashlib.sha256(response.content).hexdigest(), row_count=len(frame),
        matched_count=len(matched), unmatched_count=len(current) - len(matched), missingness={},
        fallback_tier="A", model_eligible=True,
        eligibility_reason="nflverse weekly roster status for the practice-squad rule",
    ))
    latest = {str(row["gsis_id"]): dict(row) for row in db.execute(
        """SELECT DISTINCT ON (player_id) gsis_id, roster_week, team, status, status_description
           FROM nfl_roster_status_observations WHERE season=%s
           ORDER BY player_id, observed_at DESC, id DESC""", (season,))}
    new_rows = changed_rows(matched, latest)
    if new_rows:
        from psycopg2.extras import execute_values
        execute_values(db.conn.cursor(),
            """INSERT INTO nfl_roster_status_observations
                 (season,player_id,gsis_id,roster_week,team,status,status_description,
                  source_snapshot_id,provider_modified_at,observed_at,version) VALUES %s""",
            [(season, row["player_id"], row["gsis_id"], row["roster_week"], row["team"], row["status"],
              row["status_description"], snapshot_id, provider_modified, now, VERSION) for row in new_rows],
            page_size=500)
    return {"snapshotId": snapshot_id, "rosterWeek": current[0]["roster_week"], "currentRows": len(current),
            "matched": len(matched), "unmatched": len(current) - len(matched), "written": len(new_rows),
            "practiceSquad": sum(1 for row in matched if row["status"] == "DEV"),
            "providerModifiedAt": provider_modified.isoformat() if provider_modified else None,
            "capturedAt": now.isoformat()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    db = RefreshDatabase(load_config().database_url)
    failed = False
    try:
        result = capture_roster_status(db, season=season, now=now)
        print(json.dumps({"season": season, **result}, indent=2, default=str))
        return 0
    except Exception:
        failed = True
        raise
    finally:
        db.close(error=failed)


if __name__ == "__main__":
    sys.exit(main())
