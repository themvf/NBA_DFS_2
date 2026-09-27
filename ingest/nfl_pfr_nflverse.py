"""Import public nflverse-distributed PFR stats into the game supplement store."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from config import load_config
from db.database import DatabaseManager
from db.nfl_pfr_schema import DDL, save_snapshot
from ingest.nfl_pfr_supplement import select_games
from ingest.nfl_season_schedule import fetch_schedule
from model.nfl_pfr_supplement import SECTIONS, boxscore_id, team_code

KINDS = {"pass": "passing_advanced", "rush": "rushing_advanced",
         "rec": "receiving_advanced", "def": "defense_advanced"}
IDENTITY = {"game_id", "pfr_game_id", "season", "week", "game_type", "team", "opponent",
            "pfr_player_name", "pfr_player_id"}
VERSION = "nflverse-pfr-v1"


class NotPublished(ValueError):
    """A completed game has no charting yet; other games may still be imported."""


def source_url(kind: str, season: int) -> str:
    return f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_week_{kind}_{season}.csv"


def build_supplement(game: dict, frames: dict, sources: dict, captured_at: str) -> dict:
    """Verify identities; normalize fraction-valued percentages to percentage points."""
    pfr_id = boxscore_id(str(game["pfr"]))
    teams = {team_code(game["home_team"]), team_code(game["away_team"])}
    rows = []
    coverage = {section: {"status": "missing", "rows": 0} for section in SECTIONS}
    for kind, section in KINDS.items():
        frame = frames[kind]
        if not IDENTITY.issubset(frame.columns):
            raise ValueError(f"Missing identity columns in {kind}")
        selected = frame[frame.game_id == game["game_id"]]
        seen = set()
        for row in selected.to_dict("records"):
            if (str(row["pfr_game_id"]) != pfr_id or row["season"] != game["season"]
                    or row["week"] != game["week"]):
                raise ValueError(f"Schedule/source mismatch for {game['game_id']}")
            team, opponent = team_code(str(row["team"])), team_code(str(row["opponent"]))
            if {team, opponent} != teams:
                raise ValueError("Team/opponent identity mismatch")
            player = row["pfr_player_id"]
            if pd.isna(player) or not str(player).strip():
                raise ValueError("Missing PFR player ID")
            key = (team, str(player))
            if key in seen:
                raise ValueError(f"Duplicate player in {section}")
            seen.add(key)
            raw = {k: None if pd.isna(v) else v for k, v in row.items()}
            stats = {k: v for k, v in raw.items() if k not in IDENTITY}
            for name, value in stats.items():
                if name.endswith("_pct") and value is not None:
                    if not isinstance(value, (int, float)) or not 0 <= value <= 1:
                        raise ValueError(f"Unexpected fraction units for {name}")
                    stats[name] = round(value * 100, 6)
            rows.append({"section": section, "pfr_player_id": str(player),
                         "player_name": row["pfr_player_name"], "team": team,
                         "stats": stats, "raw": raw})
        coverage[section] = {"status": "available" if len(selected) else "missing", "rows": len(selected)}
    if not rows:
        raise NotPublished("No PFR advanced stats published for this game")
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return {"game_id": game["game_id"], "season": int(game["season"]), "week": int(game["week"]),
            "home_team": team_code(game["home_team"]), "away_team": team_code(game["away_team"]),
            "pfr_game_id": pfr_id, "source_url": "https://github.com/nflverse/nflverse-data/releases/tag/pfr_advstats",
            "source_provider": "nflverse_pfr", "source_files": sources,
            "stats_schema": "nflverse_pfr_fields_percentage_points", "raw_percentage_unit": "fraction",
            "captured_at": captured_at, "parser_version": VERSION, "source_sha256": digest,
            "status": "partial", "coverage": coverage, "rows": rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int)
    parser.add_argument("--game", action="append")
    parser.add_argument("--input-dir", type=Path, help="Previously downloaded CSV files; records current import time")
    parser.add_argument("--output-dir", type=Path, default=Path("data/pfr/nflverse-output"))
    parser.add_argument("--write-db", action="store_true")
    args = parser.parse_args(argv)
    config = load_config()
    games = select_games(fetch_schedule(), args.season, args.week, args.game)
    frames, sources = {}, {}
    for kind in KINDS:
        url = source_url(kind, args.season)
        if args.input_dir:
            content = (args.input_dir / f"advstats_week_{kind}_{args.season}.csv").read_bytes()
        else:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            content = response.content
        frames[kind] = pd.read_csv(io.BytesIO(content))
        sources[kind] = {"url": url, "sha256": hashlib.sha256(content).hexdigest()}
    captured_at = datetime.now(timezone.utc).isoformat()
    # Validate every selected game before any database writes.
    payloads, pending = [], []
    for game in games:
        try:
            payloads.append(build_supplement(game, frames, sources, captured_at))
        except NotPublished:
            pending.append({"game_id": game["game_id"], "status": "awaiting_publication", "rows": 0})
    db = DatabaseManager(config.database_url, initialize_schema=False) if args.write_db else None
    if db:
        db.execute(DDL)
    identity_report = {"status": "not_requested"}
    if db and not args.input_dir:
        from ingest.nfl_pfr_identity import refresh_identity
        try:
            identity_report = refresh_identity(db, args.season, args.output_dir / "identity")
        except (requests.RequestException, ValueError) as exc:
            # Existing frozen mappings remain usable; absent identifiers remain
            # unresolved rather than being guessed from a name.
            identity_report = {"status": "refresh_unavailable", "reason": type(exc).__name__}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = list(pending)
    for payload in payloads:
        if identity_report.get("source"):
            payload["source_files"]["identity_roster"] = identity_report["source"]
        if db:
            payload = save_snapshot(db, payload)
        (args.output_dir / f"{payload['game_id']}.json").write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
        report.append({"game_id": payload["game_id"], "status": payload["status"],
                       "rows": len(payload["rows"]), "coverage": payload["coverage"]})
    (args.output_dir / "run-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (args.output_dir / "identity-report.json").write_text(json.dumps(identity_report, indent=2), encoding="utf-8")
    incomplete = [p['game_id'] for p in payloads if any(p['coverage'][s]['status'] != 'available' for s in KINDS.values())]
    print(json.dumps({"games": len(payloads), "awaiting_publication": len(pending), "incomplete_advanced_games": incomplete,
                      "player_section_rows": sum(len(p["rows"]) for p in payloads),
                      "written_to_db": bool(db), "identity_refresh": identity_report,
                      "missing_sections": [s for s in SECTIONS if s not in KINDS.values()]}))
    return 2 if pending or incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
