"""Manual capture of DraftKings' own Best Ball rank and ADP.

This is a distinct signal from Fantasy Football Calculator's general-market
ADP (`ingest/ff_adp_snapshot.py`) -- it's DK's own site-wide average draft
position and draft-percentage for a specific Best Ball draft group/tournament
template, i.e. the exact market our Best Ball board is competing against.

Why this isn't automated like the FFC capture: DK's
`GET /rankings/v1/draftgroups/{draftGroupId}/playerpool?format=json` endpoint
requires an authenticated DK session cookie, protected by Akamai bot
detection (`_abck`/`bm_sz`/`bm_sv`/`ak_bmsc`) tied to the browser/TLS/IP that
issued it -- the same class of constraint this project already documents for
LineStar's `DNN_COOKIE`. Unlike DNN_COOKIE, this session belongs to a
real-money gambling account, so it is deliberately never stored in this repo,
committed to GitHub Secrets, or replayed from CI/a sandbox. Each run of this
script is therefore a manual, point-in-time capture: open a live DK Best Ball
draft room in a browser, copy the network request for that endpoint as a
"Copy response" (body only -- never the request/cookie headers), save the
JSON body to a file, and run this script against it.

Because there's no guaranteed cadence, `captured_at` records the actual wall-
clock capture time (or `--captured-at` if you know it more precisely) rather
than being floored to a fixed window the way ff_adp_snapshots.py's 12-hour
FFC capture is.

Matching: the DK payload carries DK's own numeric playerId and a display
name, but no position -- so a player is only matched to our ff_players
universe when their normalized name is UNIQUE within the season (an
ambiguous shared name is reported, not guessed). `ff_players.draftkings_id`
is deliberately left untouched here: it's already populated (when present)
from Sleeper's cross-platform ID map, a different provenance we haven't
verified shares the same ID namespace as this endpoint's playerId, and
silently mixing two ID sources into one column would be worse than not
storing it. The FK on `ff_dk_bestball_adp.player_id` is the join; re-running
this script re-resolves names every time rather than trusting a cached ID.

Usage:
    python -m ingest.ff_dk_bestball_adp --file dk_playerpool.json --draft-group 146136 --season 2026
    python -m ingest.ff_dk_bestball_adp --file DkPreDraftRankings.csv --season 2026
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase, as_float, as_int, normalize_name
from ingest.ff_independent import _snapshot, normalize_team

MIN_PLAYER_UNIVERSE = 100
MIN_DK_ROWS = 200


def parse_dk_predraft_csv(text: str) -> list[dict[str, Any]]:
    """Normalize DK's downloadable pre-draft ranking CSV.

    The CSV has no explicit rank column: DraftKings documents that row order is
    the ranking, so rank is deliberately derived from the 1-based row number.
    """
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    required = {"ID", "Name", "Position", "ADP", "Team"}
    if not reader.fieldnames or not required.issubset(reader.fieldnames):
        raise ValueError(f"DraftKings CSV is missing required columns: {sorted(required)}")
    players: list[dict[str, Any]] = []
    for row_number, row in enumerate(reader, start=1):
        if not (row.get("ID") and row.get("Name")):
            continue
        players.append({
            "playerId": as_int(row.get("ID")),
            "displayName": str(row.get("Name") or "").strip(),
            "position": str(row.get("Position") or "").strip().upper(),
            "team": normalize_team(row.get("Team")) or None,
            "averageDraftPosition": as_float(row.get("ADP")),
            "draftPercentage": None,
            "rank": row_number,
            "isAvailable": True,
            "teamId": None,
        })
    return players


def _player_lookup(db: RefreshDatabase, season: int) -> dict[str, list[dict[str, Any]]]:
    rows = db.execute(
        """WITH latest_board AS (
             SELECT id FROM ff_ranking_sets
             WHERE season=%s AND COALESCE(scoring_profile->>'preset','PPR')='PPR'
             ORDER BY created_at DESC,id DESC LIMIT 1
           )
           SELECT p.id,p.normalized_name,p.position,p.team_abbrev,
             EXISTS(SELECT 1 FROM ff_player_rankings r,latest_board lb
                    WHERE r.ranking_set_id=lb.id AND r.player_id=p.id) AS on_current_board
           FROM ff_players p WHERE p.season=%s""",
        (season, season),
    )
    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        lookup.setdefault(row["normalized_name"], []).append(row)
    return lookup


def _match_player(row: dict[str, Any], lookup: dict[str, list[dict[str, Any]]]) -> tuple[int | None, str]:
    def choose(candidates: list[dict[str, Any]], method: str) -> tuple[int | None, str]:
        current = [candidate for candidate in candidates if candidate.get("on_current_board")]
        if len(current) == 1:
            return int(current[0]["id"]), f"{method}_current_board"
        if len(candidates) == 1:
            return int(candidates[0]["id"]), method
        return None, "ambiguous" if candidates else "unmatched"

    candidates = lookup.get(normalize_name(str(row.get("displayName") or "")), [])
    position = str(row.get("position") or "").upper()
    team = normalize_team(row.get("team"))
    if position:
        position_matches = [candidate for candidate in candidates if candidate.get("position") == position]
        if position_matches:
            candidates = position_matches
    if team:
        team_matches = [candidate for candidate in candidates if normalize_team(candidate.get("team_abbrev")) == team]
        matched = choose(team_matches, "normalized_name_position_team")
        if matched[0] is not None:
            return matched
    return choose(candidates, "normalized_name_position")


def _run(
    season: int,
    draft_group_id: int,
    file_path: Path,
    captured_at: datetime,
    db: RefreshDatabase,
) -> dict[str, Any]:
    player_lookup = _player_lookup(db, season)
    if len(player_lookup) < MIN_PLAYER_UNIVERSE:
        raise RuntimeError(
            f"ff_players has too few {season} rows ({len(player_lookup)}) to match against -- "
            "run `python -m ingest.ff_independent` first to build the player universe."
        )

    raw_bytes = file_path.read_bytes()
    source_format = "predraft-csv" if file_path.suffix.lower() == ".csv" else "playerpool-json"
    if source_format == "predraft-csv":
        players = parse_dk_predraft_csv(raw_bytes.decode("utf-8-sig"))
    else:
        payload = json.loads(raw_bytes)
        players = ((payload.get("playerPool") or {}).get("draftablePlayers")) or []
    if not isinstance(players, list) or len(players) < MIN_DK_ROWS:
        raise RuntimeError(
            f"{file_path} has suspiciously few players ({len(players) if isinstance(players, list) else 'n/a'}) "
            f"-- expected a full draftgroup/{draft_group_id}/playerpool response body"
        )
    digest = hashlib.sha256(raw_bytes).hexdigest()

    snapshot_id = _snapshot(
        db, source="draftkings", dataset="bestball-adp", season=season,
        digest=digest, row_count=len(players), ranking_type="ADP",
        params={
            "draft_group_id": draft_group_id,
            "captured_at": captured_at.isoformat(),
            "file": str(file_path),
            "source_format": source_format,
            "rank_basis": "csv-row-order" if source_format == "predraft-csv" else "payload-rank",
        },
        model_eligible=False,
        eligibility_reason="market context is excluded from football-performance features",
    )

    stored = 0
    matched = 0
    ambiguous: list[str] = []
    unmatched: list[str] = []
    for row in players:
        dk_player_id = as_int(row.get("playerId"))
        display_name = row.get("displayName") or f"{row.get('firstName', '')} {row.get('lastName', '')}".strip()
        if dk_player_id is None or not display_name:
            continue
        ff_player_id, match_method = _match_player(row, player_lookup)
        if ff_player_id is not None:
            matched += 1
        elif match_method == "ambiguous":
            ambiguous.append(display_name)
        else:
            unmatched.append(display_name)

        db.execute(
            """INSERT INTO ff_dk_bestball_adp
               (draft_group_id, season, dk_player_id, player_id, display_name, dk_team_id,
                average_draft_position, draft_percentage, rank, is_available,
                captured_at, source_snapshot_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(draft_group_id, dk_player_id, captured_at) DO UPDATE SET
                 player_id=EXCLUDED.player_id, display_name=EXCLUDED.display_name,
                 dk_team_id=EXCLUDED.dk_team_id,
                 average_draft_position=EXCLUDED.average_draft_position,
                 draft_percentage=EXCLUDED.draft_percentage, rank=EXCLUDED.rank,
                 is_available=EXCLUDED.is_available, source_snapshot_id=EXCLUDED.source_snapshot_id""",
            (
                draft_group_id, season, dk_player_id, ff_player_id, display_name,
                as_int(row.get("teamId")), as_float(row.get("averageDraftPosition")),
                as_float(row.get("draftPercentage")), as_int(row.get("rank")),
                bool(row.get("isAvailable", True)), captured_at, snapshot_id,
            ),
        )
        stored += 1

    db.execute(
        "UPDATE ff_source_snapshots SET matched_count=%s,unmatched_count=%s WHERE id=%s",
        (matched, len(unmatched) + len(ambiguous), snapshot_id),
    )

    return {
        "season": season,
        "draft_group_id": draft_group_id,
        "captured_at": captured_at.isoformat(),
        "players": len(players),
        "stored": stored,
        "matched": matched,
        "ambiguous_names": sorted(set(ambiguous)),
        "unmatched_count": len(unmatched),
        "unmatched_sample": unmatched[:20],
        "source_snapshot_id": snapshot_id,
        "source_format": source_format,
        "adp_rows": sum(as_float(row.get("averageDraftPosition")) is not None for row in players),
    }


def run(season: int, draft_group_id: int, file_path: Path, captured_at: datetime | None) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        result = _run(season, draft_group_id, file_path, captured_at or datetime.now(timezone.utc), db)
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, type=Path, help="Path to the saved playerpool JSON response body")
    parser.add_argument(
        "--draft-group", type=int, dest="draft_group_id", default=0,
        help="DK draft group for JSON captures; omit for the global pre-draft CSV (stored as group 0)",
    )
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument(
        "--captured-at", type=str, default=None,
        help="ISO 8601 timestamp of the actual capture (defaults to now)",
    )
    args = parser.parse_args()
    captured = datetime.fromisoformat(args.captured_at) if args.captured_at else None
    if captured and captured.tzinfo is None:
        captured = captured.replace(tzinfo=timezone.utc)
    print(json.dumps(run(args.season, args.draft_group_id, args.file, captured), indent=2))
