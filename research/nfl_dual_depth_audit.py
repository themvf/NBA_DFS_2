"""Compare a team's stored Sleeper depth with FantasyPros' published chart.

This is a read-only, point-in-time research capture. It does not promote a
backup, alter a projection, or change optimizer eligibility. Run before making
a depth-based NFL role claim; retain the JSON output with the decision record.

    python -m research.nfl_dual_depth_audit --season 2026 --team WSH --output audit.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import requests
from bs4 import BeautifulSoup

from config import load_config

POSITIONS = {"QB", "RB", "WR", "TE", "K"}
ALIASES = {"WAS": "WSH", "LA": "LAR", "AZ": "ARI", "JAC": "JAX"}


def normalize_name(value: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    ascii_name = re.sub(r"\b(jr|sr|ii|iii|iv)\.?\b", "", ascii_name, flags=re.I)
    return re.sub(r"[^a-z0-9]", "", ascii_name.lower())


def parse_fantasypros_depth(html: str, expected_team: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1")
    if not title or title.get_text(" ", strip=True).casefold() != expected_team.casefold():
        raise ValueError("FantasyPros depth page does not match the requested team")
    rows: list[dict[str, Any]] = []
    for table in soup.select("table.position-table"):
        caption = table.find("caption")
        if not caption or expected_team.casefold() not in caption.get_text(" ", strip=True).casefold():
            continue
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td", recursive=False)
            if len(cells) < 2:
                continue
            rank_label = cells[0].get_text(" ", strip=True).upper()
            match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", rank_label)
            player_link = cells[1].select_one("a.player-name")
            if not match or match.group(1) not in POSITIONS or not player_link:
                continue
            fp_class = next((value for value in player_link.get("class", []) if re.fullmatch(r"fp-id-[0-9]+", value)), None)
            rows.append({
                "name": player_link.get_text(" ", strip=True),
                "position": match.group(1),
                "rank": int(match.group(2)),
                "fantasypros_player_id": int(fp_class[6:]) if fp_class else None,
            })
    if not rows or len({(row["position"], row["rank"]) for row in rows}) != len(rows):
        raise ValueError("FantasyPros depth chart is empty or has duplicate position ranks")
    return rows


def compare_depth(sleeper: list[dict[str, Any]], fantasypros: list[dict[str, Any]]) -> dict[str, Any]:
    by_fp_id: dict[int, list[dict[str, Any]]] = {}
    by_identity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in sleeper:
        if row.get("fantasypros_player_id") is not None:
            by_fp_id.setdefault(int(row["fantasypros_player_id"]), []).append(row)
        by_identity.setdefault((normalize_name(row["name"]), row["position"]), []).append(row)
    matched: set[int] = set()
    comparisons: list[dict[str, Any]] = []
    unmatched_fp: list[dict[str, Any]] = []
    for fp in fantasypros:
        candidates = by_fp_id.get(fp["fantasypros_player_id"], []) if fp["fantasypros_player_id"] is not None else []
        method = "fantasypros_player_id"
        if not candidates:
            candidates = [row for row in by_identity.get((normalize_name(fp["name"]), fp["position"]), [])
                          if row.get("fantasypros_player_id") is None or fp["fantasypros_player_id"] is None]
            method = "unique_name_position"
        if len(candidates) != 1 or candidates[0]["position"] != fp["position"] or candidates[0]["id"] in matched:
            unmatched_fp.append({**fp, "reason": "identity_unresolved"})
            continue
        player = candidates[0]
        matched.add(player["id"])
        sleeper_rank = player.get("depth_order")
        comparisons.append({
            "player_id": player["id"], "name": player["name"], "position": player["position"],
            "identity_method": method,
            "sleeper": {"rank": sleeper_rank, "alignment": player.get("depth_chart_position"),
                        "status": player.get("status"), "fetched_at": player.get("fetched_at")},
            "fantasypros": {"rank": fp["rank"], "player_id": fp["fantasypros_player_id"]},
            "decision": "agree" if sleeper_rank == fp["rank"] else "conflict" if sleeper_rank is not None else "sleeper_depth_missing",
        })
    return {
        "comparisons": sorted(comparisons, key=lambda row: (row["position"], row["fantasypros"]["rank"])),
        "sleeper_only": [row for row in sleeper if row["id"] not in matched],
        "fantasypros_only": unmatched_fp,
        "counts": {"matched": len(comparisons), "conflicts": sum(row["decision"] == "conflict" for row in comparisons),
                   "sleeper_only": len(sleeper) - len(matched), "fantasypros_only": len(unmatched_fp)},
    }


def capture(season: int, team: str, timeout: int = 15) -> dict[str, Any]:
    team = ALIASES.get(team.upper(), team.upper())
    captured_at = datetime.now(timezone.utc).isoformat()
    with psycopg2.connect(load_config().database_url, connect_timeout=timeout) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("""SELECT id, fetched_at, response_hash, status FROM ff_source_snapshots
                WHERE source='sleeper' AND dataset='players' AND season=%s
                ORDER BY fetched_at DESC, id DESC LIMIT 1""", (season,))
            snapshot = cursor.fetchone()
            if not snapshot or snapshot[3] != "success":
                raise ValueError(f"No successful Sleeper player snapshot for {season}")
            cursor.execute("SELECT name FROM nfl_teams WHERE abbreviation=%s", (team,))
            team_row = cursor.fetchone()
            if not team_row:
                raise ValueError(f"Unknown NFL team abbreviation: {team}")
            team_name = str(team_row[0])
            cursor.execute("""SELECT id, canonical_name, position, fantasypros_player_id,
                metadata->'sleeper'->>'depth_chart_position', metadata->'sleeper'->>'depth_chart_order',
                COALESCE(metadata->'sleeper'->>'injury_status', metadata->'sleeper'->>'status'), fetched_at
                FROM ff_players WHERE season=%s AND team_abbrev=%s AND active
                  AND position IN ('QB','RB','WR','TE','K') ORDER BY position, canonical_name""", (season, team))
            sleeper = [{"id": int(row[0]), "name": row[1], "position": row[2],
                        "fantasypros_player_id": row[3], "depth_chart_position": row[4],
                        "depth_order": int(row[5]) if row[5] and str(row[5]).isdigit() else None,
                        "status": row[6], "fetched_at": row[7].isoformat()} for row in cursor.fetchall()]
    if not sleeper:
        raise ValueError(f"No stored Sleeper roster for {team} in {season}")
    slug = re.sub(r"[^a-z0-9]+", "-", team_name.lower()).strip("-")
    url = f"https://www.fantasypros.com/nfl/depth-chart/{slug}.php"
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "NBADFS-v2-depth-evidence/1.0"})
    response.raise_for_status()
    fp = parse_fantasypros_depth(response.text, team_name)
    return {
        "schema_version": "nfl-dual-depth-audit-v1", "season": season, "team": team,
        "captured_at": captured_at,
        "sources": {"sleeper": {"basis": "ff_players.metadata.sleeper", "snapshot_id": snapshot[0],
                                "snapshot_fetched_at": snapshot[1].isoformat(), "response_hash": snapshot[2],
                                "latest_player_row_updated_at": max(row["fetched_at"] for row in sleeper)},
                    "fantasypros": {"url": url, "retrieved_at": datetime.now(timezone.utc).isoformat(),
                                    "provider_updated_at": None,
                                    "response_sha256": hashlib.sha256(response.content).hexdigest()}},
        "role_policy": "A depth rank is evidence, not a snap or target forecast. Conflicts require review; neither source silently wins.",
        **compare_depth(sleeper, fp),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--team", required=True, help="NFL abbreviation, for example WSH or WAS")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = capture(args.season, args.team)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "counts": result["counts"]}))


if __name__ == "__main__":
    main()
