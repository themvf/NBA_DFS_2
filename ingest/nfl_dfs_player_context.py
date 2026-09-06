"""Export a read-only NFL player-context artifact from verified local source bytes.

No network or database writes. Run with --source-root pointing at the checkout
containing data/ff_v2_sources. Participation counts are recorded scrimmage
plays, not official snap counts or routes. All records are retrospective.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import pandas as pd

STAT_MAP = {
    "passYds": "passing_yards", "passTds": "passing_tds",
    "interceptions": "passing_interceptions", "rushYds": "rushing_yards",
    "rushTds": "rushing_tds", "recYds": "receiving_yards",
    "recTds": "receiving_tds", "receptions": "receptions",
    "fumblesLost": "fumbles_lost_total", "returnTds": "special_teams_tds",
    "offensiveFumbleRecoveryTds": "fumble_recovery_tds",
}


def recorded_participation(plays: pd.DataFrame, participation: pd.DataFrame):
    """Count only matched pass/run plays, including kneels and spikes.

    Missing personnel stays uncovered rather than implying nobody played.
    """
    base = plays[plays.play_type.isin(["pass", "run"]) & plays.posteam.notna()]
    personnel = participation.rename(columns={"nflverse_game_id": "game_id"})
    if base.duplicated(["game_id", "play_id"]).any() or personnel.duplicated(["game_id", "play_id"]).any():
        raise ValueError("Duplicate game/play identity")
    merged = base.merge(personnel[["game_id", "play_id", "offense_players"]], on=["game_id", "play_id"], how="left", validate="one_to_one")
    result = {}
    for (game, team), rows in merged.groupby(["game_id", "posteam"]):
        counts = Counter()
        covered = 0
        for value in rows.offense_players:
            ids = set(value.split(";")) if isinstance(value, str) and value.strip() else set()
            ids.discard("")
            if ids:
                covered += 1
                counts.update(ids)
        result[(game, team)] = {"plays": len(rows), "covered": covered, "counts": counts}
    return result


def build(root: Path, season: int):
    manifest = json.loads((root / "artifacts/ff_v2_historical_context_2020_2025.json").read_text())
    provenance = {}
    def read(key):
        source = manifest["sources"][key]
        path = root / Path(source["cachePath"].replace("\\", "/"))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != source["responseHash"]:
            raise ValueError(f"Source digest mismatch: {key}")
        provenance[key] = {k: source[k] for k in ["responseHash", "fetchedAt", "sourcePublishedAt", "url"]}
        return pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)

    stats = read(f"weekly-stats:{season}")
    rosters = read(f"weekly-rosters:{season}")
    plays = read(f"play-by-play:{season}")
    participation = read(f"participation:{season}")
    schedule = read("schedule:all")
    stats = stats[(stats.season == season) & (stats.season_type == "REG")]
    schedule = schedule[(schedule.season == season) & (schedule.game_type == "REG")]
    rosters = rosters[(rosters.season == season) & (rosters.game_type == "REG") & rosters.gsis_id.notna()]
    if stats.duplicated(["game_id", "player_id"]).any():
        raise ValueError("Duplicate player game stats")
    counts = recorded_participation(plays[plays.game_id.isin(schedule.game_id)], participation)
    stat_lookup = {(r["game_id"], r["player_id"]): r for r in stats.to_dict("records")}
    players, games, rows = {}, {}, []
    unmatched_participation = 0
    for game in schedule.sort_values(["week", "game_id"]).to_dict("records"):
        for side, other in [("home", "away"), ("away", "home")]:
            team, opponent = game[f"{side}_team"], game[f"{other}_team"]
            key = f'{game["game_id"]}:{team}'
            count = counts.get((game["game_id"], team), {"plays": 0, "covered": 0, "counts": Counter()})
            roster = rosters[(rosters.week == game["week"]) & (rosters.team == team)]
            members = {}
            for r in roster.to_dict("records"):
                pid = r["gsis_id"]
                member = {"id": pid, "name": str(r["full_name"] or pid), "position": str(r["position"] or "unknown"), "status": str(r["status"] or "unknown"), "recordedPlays": count["counts"].get(pid, 0) if count["covered"] else None}
                if pid in members and members[pid] != member:
                    raise ValueError(f"Conflicting roster identity: {key}:{pid}")
                members[pid] = member
            # Preserve stat-producing players even if their weekly roster is absent.
            for r in stats[(stats.game_id == game["game_id"]) & (stats.team == team)].to_dict("records"):
                pid = r["player_id"]
                members.setdefault(pid, {"id": pid, "name": str(r["player_display_name"] or pid), "position": str(r["position"] or "unknown"), "status": "unknown", "recordedPlays": count["counts"].get(pid, 0) if count["covered"] else None})
            for pid, n in count["counts"].items():
                if pid not in members:
                    unmatched_participation += 1
                    members[pid] = {"id": pid, "name": pid, "position": "unknown", "status": "unknown", "recordedPlays": n}
            games[key] = {"week": int(game["week"]), "date": game["gameday"], "team": team, "opponent": opponent, "plays": count["plays"], "covered": count["covered"], "roster": sorted(members.values(), key=lambda m: (m["position"], m["name"]))}
            for pid, member in members.items():
                if member["position"] not in ["QB", "WR", "TE"]:
                    continue
                players[pid] = {"id": pid, "name": member["name"], "position": member["position"]}
                stat = stat_lookup.get((game["game_id"], pid))
                scored = None
                if stat is not None:
                    required = list(STAT_MAP.values()) + ["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"]
                    if all(pd.notna(stat.get(k)) for k in required):
                        scored = {k: float(stat[v]) for k, v in STAT_MAP.items()}
                        scored["twoPointConversions"] = sum(float(stat[k]) for k in required[-3:])
                rows.append({"playerId": pid, "gameKey": key, "stats": scored,
                             "targets": float(stat["targets"]) if stat is not None and pd.notna(stat["targets"]) else None,
                             "attempts": float(stat["attempts"]) if stat is not None and pd.notna(stat["attempts"]) else None})
    return {"version": 1, "season": season, "sources": provenance, "players": sorted(players.values(), key=lambda p: p["name"]), "games": games, "rows": rows,
            "audit": {"scheduledGames": len(schedule), "teamGames": len(games), "playerRows": len(rows), "scoredRows": sum(r["stats"] is not None for r in rows), "recordedPlays": sum(g["covered"] for g in games.values()), "scrimmagePlays": sum(g["plays"] for g in games.values()), "unknownRosterStatuses": sum(m["status"] == "unknown" for g in games.values() for m in g["roster"]), "unmatchedParticipationIdentities": unmatched_participation, "routesAvailable": False, "pregameInjuryReasonsAvailable": False}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = build(args.source_root, args.season)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, separators=(",", ":"), allow_nan=False), encoding="utf-8")
    print(json.dumps(artifact["audit"], indent=2))
