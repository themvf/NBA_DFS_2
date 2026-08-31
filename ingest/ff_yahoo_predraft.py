"""Import a manually pasted Yahoo Fantasy pre-draft rankings snapshot.

Yahoo's lobby exposes two useful market signals:

* XRank -- Yahoo's default expert/pre-draft ordering;
* ADP -- where Yahoo users actually select the player when Yahoo has enough
  draft history. Deep players often have XRank but no ADP; that is valid and
  remains NULL rather than being inferred.

The exact pasted text is preserved in ``ff_yahoo_predraft_captures`` and every
parsed row is stored in ``ff_yahoo_predraft_rankings``. This data is market
context only. It never changes the independent projection or V1.6 rank.

Usage:
    python -m ingest.ff_yahoo_predraft --file yahoo-predraft.txt --season 2026
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase, normalize_name
from ingest.ff_independent import _snapshot, normalize_team

POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF", "DST"}
MIN_YAHOO_ROWS = 100
# Names Yahoo and our roster source spell differently. Declared as unordered
# PAIRS, not one-way rewrites, because the direction is not stable: this map
# used to send "kennygainwell" -> "kennethgainwell" since nflverse listed him
# as Kenneth, and when nflverse started calling him Kenny the rewrite pointed
# at a name that no longer existed and silently dropped a player the market
# drafts inside pick 120. A pair matches whichever spelling each side happens
# to be using today, so a future flip cannot break it again.
YAHOO_NAME_ALIAS_PAIRS = (
    ("kennygainwell", "kennethgainwell"),
    ("hollywoodbrown", "marquisebrown"),
    ("gabedavis", "gabrieldavis"),
)


def _alias_equivalents(pairs: tuple[tuple[str, str], ...]) -> dict[str, tuple[str, ...]]:
    """Expand the pairs into every spelling reachable from a given name."""
    groups: dict[str, set[str]] = {}
    for left, right in pairs:
        merged = groups.get(left, {left}) | groups.get(right, {right})
        for name in merged:
            groups[name] = merged
    return {name: tuple(sorted(names)) for name, names in groups.items()}


YAHOO_NAME_ALIASES = _alias_equivalents(YAHOO_NAME_ALIAS_PAIRS)


@dataclass(frozen=True)
class YahooPredraftRow:
    source_order: int
    display_name: str
    position: str
    team_abbrev: str | None
    bye_week: int | None
    xrank: float
    adp: float | None


def _number(line: str, prefix: str) -> float | None:
    match = re.fullmatch(rf"{re.escape(prefix)}\s*#?([0-9]+(?:\.[0-9]+)?)", line, flags=re.I)
    return float(match.group(1)) if match else None


def parse_yahoo_predraft_text(text: str) -> list[YahooPredraftRow]:
    """Parse Yahoo's copied pre-draft list without assuming ADP is present."""
    blocks = re.split(r"(?:\r?\n){2,}", text.strip())
    rows: list[YahooPredraftRow] = []
    errors: list[str] = []
    for block_number, block in enumerate(blocks, start=1):
        lines = [line.strip() for line in block.splitlines() if line.strip() and line.strip() != "·"]
        position_index = next((
            index for index, line in enumerate(lines)
            if all(part.strip().upper() in POSITIONS for part in line.split(","))
        ), None)
        xrank_index = next((index for index, line in enumerate(lines) if line.lower().startswith("xrank #")), None)
        # Yahoo occasionally leaves a final, partially rendered player card at
        # the end of a copied list. With no XRank it is not a ranking row.
        if xrank_index is None:
            continue
        if position_index is None:
            if lines:
                errors.append(f"block {block_number}: {lines[:4]}")
            continue
        if position_index < 1 or position_index + 1 >= len(lines):
            errors.append(f"block {block_number}: incomplete player identity")
            continue

        display_name = lines[position_index - 1]
        yahoo_position = lines[position_index].split(",", maxsplit=1)[0].strip().upper()
        position = "DST" if yahoo_position == "DEF" else yahoo_position
        team_abbrev = normalize_team(lines[position_index + 1]) or None
        bye_line = next((line for line in lines[position_index + 2:xrank_index] if line.lower().startswith("bye ")), "")
        bye_match = re.fullmatch(r"Bye\s+([0-9]{1,2})", bye_line, flags=re.I)
        xrank = _number(lines[xrank_index], "XRank")
        adp_line = next((line for line in lines[xrank_index + 1:] if line.lower().startswith("adp ")), "")
        adp = _number(adp_line, "ADP") if adp_line else None
        if not display_name or xrank is None:
            errors.append(f"block {block_number}: missing name/XRank")
            continue
        rows.append(YahooPredraftRow(
            source_order=len(rows) + 1,
            display_name=display_name,
            position=position,
            team_abbrev=team_abbrev,
            bye_week=int(bye_match.group(1)) if bye_match else None,
            xrank=xrank,
            adp=adp,
        ))

    if errors:
        raise ValueError(f"Could not parse {len(errors)} Yahoo blocks; sample: {errors[:5]}")
    return rows


def _player_lookups(db: RefreshDatabase, season: int) -> tuple[
    dict[tuple[str, str], list[dict[str, Any]]],
    dict[tuple[str, str], list[dict[str, Any]]],
]:
    players = db.execute(
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
    by_name_position: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_position_team: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for player in players:
        position = str(player["position"])
        team = normalize_team(player.get("team_abbrev"))
        by_name_position.setdefault((str(player["normalized_name"]), position), []).append(player)
        if team:
            by_position_team.setdefault((position, team), []).append(player)
    return by_name_position, by_position_team


def _match_player(
    row: YahooPredraftRow,
    by_name_position: dict[tuple[str, str], list[dict[str, Any]]],
    by_position_team: dict[tuple[str, str], list[dict[str, Any]]],
) -> tuple[int | None, str]:
    def choose(candidates: list[dict[str, Any]], method: str) -> tuple[int | None, str]:
        current = [candidate for candidate in candidates if candidate.get("on_current_board")]
        if len(current) == 1:
            return int(current[0]["id"]), f"{method}_current_board"
        if len(candidates) == 1:
            return int(candidates[0]["id"]), method
        return None, "ambiguous" if candidates else "unmatched"

    if row.position == "DST" and row.team_abbrev:
        defenses = by_position_team.get(("DST", row.team_abbrev), [])
        return choose(defenses, "position_team")

    normalized_name = normalize_name(row.display_name)
    candidates: list[dict[str, Any]] = []
    seen: set[int] = set()
    for name in YAHOO_NAME_ALIASES.get(normalized_name, (normalized_name,)):
        for candidate in by_name_position.get((name, row.position), []):
            if int(candidate["id"]) not in seen:
                seen.add(int(candidate["id"]))
                candidates.append(candidate)
    if row.team_abbrev:
        same_team = [candidate for candidate in candidates if normalize_team(candidate.get("team_abbrev")) == row.team_abbrev]
        matched = choose(same_team, "normalized_name_position_team")
        if matched[0] is not None:
            return matched
    return choose(candidates, "normalized_name_position")


def _run(
    *, season: int, file_path: Path, captured_at: datetime, db: RefreshDatabase,
) -> dict[str, Any]:
    raw_bytes = file_path.read_bytes()
    raw_text = raw_bytes.decode("utf-8-sig")
    rows = parse_yahoo_predraft_text(raw_text)
    if len(rows) < MIN_YAHOO_ROWS:
        raise ValueError(f"Yahoo paste has suspiciously few parsed rows ({len(rows)}; expected at least {MIN_YAHOO_ROWS})")
    digest = hashlib.sha256(raw_bytes).hexdigest()
    snapshot_id = _snapshot(
        db,
        source="yahoo",
        dataset="predraft-rankings",
        season=season,
        digest=digest,
        row_count=len(rows),
        scoring="PPR",
        ranking_type="DRAFT",
        params={
            "captured_at": captured_at.isoformat(),
            "format": "yahoo-paste-v1",
            "source_file_name": file_path.name,
            "xrank_rows": len(rows),
            "adp_rows": sum(row.adp is not None for row in rows),
            "used_for_projection": False,
        },
        model_eligible=False,
        eligibility_reason="market context is excluded from football-performance features",
    )
    db.execute(
        """INSERT INTO ff_yahoo_predraft_captures
           (source_snapshot_id,season,captured_at,raw_text,format_version,source_label)
           VALUES (%s,%s,%s,%s,'yahoo-paste-v1','Yahoo Fantasy Pre-Draft Rankings')
           ON CONFLICT(source_snapshot_id) DO UPDATE SET
             season=EXCLUDED.season,captured_at=EXCLUDED.captured_at,raw_text=EXCLUDED.raw_text""",
        (snapshot_id, season, captured_at, raw_text),
    )
    db.execute("DELETE FROM ff_yahoo_predraft_rankings WHERE source_snapshot_id=%s", (snapshot_id,))

    by_name_position, by_position_team = _player_lookups(db, season)
    matched = 0
    unmatched: list[str] = []
    match_methods: dict[str, int] = {}
    for row in rows:
        player_id, match_method = _match_player(row, by_name_position, by_position_team)
        match_methods[match_method] = match_methods.get(match_method, 0) + 1
        if player_id is None:
            unmatched.append(f"{row.display_name} ({row.position}, {row.team_abbrev or 'FA'})")
        else:
            matched += 1
        db.execute(
            """INSERT INTO ff_yahoo_predraft_rankings
               (source_snapshot_id,season,player_id,source_order,display_name,position,
                team_abbrev,bye_week,xrank,adp,captured_at,match_method,raw_row)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                snapshot_id, season, player_id, row.source_order, row.display_name,
                row.position, row.team_abbrev, row.bye_week, row.xrank, row.adp,
                captured_at, match_method, Json(asdict(row)),
            ),
        )

    db.execute(
        """UPDATE ff_source_snapshots SET matched_count=%s,unmatched_count=%s,
             source_updated_at=%s,status=%s WHERE id=%s""",
        (matched, len(unmatched), captured_at, "success" if matched == len(rows) else "partial", snapshot_id),
    )
    return {
        "season": season,
        "captured_at": captured_at.isoformat(),
        "source_snapshot_id": snapshot_id,
        "xrank_rows": len(rows),
        "adp_rows": sum(row.adp is not None for row in rows),
        "matched": matched,
        "unmatched_count": len(unmatched),
        "unmatched_sample": unmatched[:30],
        "match_methods": match_methods,
        "raw_text_saved": True,
        "used_for_projection": False,
    }


def run(season: int, file_path: Path, captured_at: datetime | None = None) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        result = _run(
            season=season,
            file_path=file_path,
            captured_at=captured_at or datetime.now(timezone.utc),
            db=db,
        )
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, type=Path, help="Path to the copied Yahoo rankings text")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--captured-at", type=str, default=None, help="ISO timestamp; defaults to now")
    args = parser.parse_args()
    captured = datetime.fromisoformat(args.captured_at) if args.captured_at else None
    if captured and captured.tzinfo is None:
        captured = captured.replace(tzinfo=timezone.utc)
    print(json.dumps(run(args.season, args.file, captured), indent=2))
