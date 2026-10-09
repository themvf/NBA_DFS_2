"""Build a shadow Showdown scenario bank conditioned on pre-decision PBP drives.

Usage: python -m research.nfl_pbp_script_export --coherent INPUT.json --output OUTPUT.json
The coherent input must be a saved model-source Showdown export with aligned
selection/evaluation event-ledger JSON files. No production run is modified.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_pbp_script_bank import VERSION, drive_catalog, sample_scripts, transport_bank


def pbp_rows(db, decision_at: str, *, retrospective: bool) -> list[dict]:
    """Canonical game join; every accepted play precedes the forecast cutoff."""
    query = """SELECT p.game_id,p.play_id,p.season,p.week,
                      CASE p.posteam WHEN 'LA' THEN 'LAR' ELSE p.posteam END posteam,p.drive,
                      p.drive_archetype,p.score_differential,p.game_seconds_remaining,
                      p.play_type,p.qb_dropback,p.had_sack,
                      p.play_labeller_version,p.drive_labeller_version,p.labelled_at
               FROM nfl_pbp_archetypes p
               JOIN nfl_season_games g ON g.nflverse_game_id=p.game_id
               JOIN nfl_teams home ON home.team_id=g.home_team_id
               JOIN nfl_teams away ON away.team_id=g.away_team_id
               WHERE p.season_type='REG' AND g.game_type='REG'
                 AND p.season=g.season AND p.week=g.week
                 AND (CASE p.home_team WHEN 'LA' THEN 'LAR' WHEN 'WAS' THEN 'WSH' ELSE p.home_team END)=home.abbreviation
                 AND (CASE p.away_team WHEN 'LA' THEN 'LAR' WHEN 'WAS' THEN 'WSH' ELSE p.away_team END)=away.abbreviation
                 AND g.kickoff < %s
                 AND (%s OR p.labelled_at <= %s)
               ORDER BY p.game_id,p.play_id"""
    return [dict(row) for row in db.execute(query, (decision_at, retrospective, decision_at))]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coherent", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--retrospective", action="store_true", help="Allow later PBP labels for mechanics only; no historical pregame claim")
    args = parser.parse_args()
    source_bytes = args.coherent.read_bytes()
    source = json.loads(source_bytes)
    implementation_digest = sha256((Path(__file__).resolve().parents[1] / "model/nfl_pbp_script_bank.py").read_bytes()
                                   + Path(__file__).read_bytes()).hexdigest()
    if source.get("slate", {}).get("format") != "showdown" or len(source["slate"].get("games", [])) != 1:
        raise ValueError("Exactly one Showdown game is required")
    teams = tuple(source["slate"]["games"][0].split("@"))
    if len(teams) != 2 or len(set(teams)) != 2:
        raise ValueError("Invalid Showdown teams")
    selection, evaluation = source["selection"], source["evaluation"]
    if selection["decisionAt"] != evaluation["decisionAt"] or selection["snapshotId"] != evaluation["snapshotId"]:
        raise ValueError("Source streams are not paired")
    cutoff = selection["decisionAt"]
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        rows = pbp_rows(db, cutoff, retrospective=args.retrospective)
    if len({(row["game_id"], row["play_id"]) for row in rows}) != len(rows):
        raise ValueError("Canonical game join duplicated PBP plays")
    catalog = drive_catalog(rows)
    if len(catalog) < 100 or any(sum(d.team == team for d in catalog) < 20 for team in teams):
        raise ValueError("Insufficient qualified drives for target teams")
    output = {"version": VERSION, "authority": "shadow_only", "productionChanged": False,
              "status": "retrospective_mechanics_only" if args.retrospective else "pregame_shadow_unqualified",
              "slate": source["slate"], "optimizerPlayers": source.get("optimizerPlayers", []),
              "audit": source.get("audit", {}), "source_manifest": {"coherent_sha256": sha256(source_bytes).hexdigest(),
              "coherent_version": source["version"], "decision_at": cutoff, "pbp_rows": len(rows), "qualified_drives": len(catalog),
              "pbp_sha256": sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
              "implementation_sha256": implementation_digest,
              "play_label_versions": sorted({str(row["play_labeller_version"]) for row in rows}),
              "drive_label_versions": sorted({str(row["drive_labeller_version"]) for row in rows}),
              "pbp_label_time_policy": "retrospective_later_labels_allowed" if args.retrospective else "labelled_at_lte_decision",
              "pbp_query_join": "nfl_season_games.nflverse_game_id=game_id; verified season/week/home/away"},
              "diagnostics": {}, "limitations": [
                  "Drive paths use empirical outcomes and score state; matching to a whole coherent draw is approximate transport, not possession-level player-stat generation.",
                  "Touchdowns and SCORE_AGAINST are approximated as seven points; clock includes an explicit 25-second terminal residual.",
                  "Team history uses the most recent eight games when a score-state cell has at least eight drives, otherwise league state history.",
                  "No opponent, weather, or market conditioning in the drive sampler. No field or payout model. No production authority."]}
    for stream, bank in (("selection", selection), ("evaluation", evaluation)):
        ledger_path = args.coherent.with_name(args.coherent.stem + f"-{stream}-ledger.json")
        ledger = json.loads(ledger_path.read_text())
        scripts, fallback = sample_scripts(catalog, teams, len(bank["scenarios"]), int(bank["seed"]) ^ 0x50504250)
        transported, matching = transport_bank(bank, ledger, scripts, teams, int(bank["seed"]) ^ 0x53435250,
                                               provenance_digest=sha256((output["source_manifest"]["pbp_sha256"]
                                                                         + implementation_digest).encode()).hexdigest())
        output[stream] = transported
        output["diagnostics"][stream] = {**matching, "drive_source_counts": fallback,
                                          "mean_drives": sum(len(s["drives"]) for s in scripts) / len(scripts),
                                          "source_ledger_sha256": sha256(ledger_path.read_bytes()).hexdigest()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, separators=(",", ":"), allow_nan=False), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "status": output["status"], "diagnostics": output["diagnostics"]}))


if __name__ == "__main__":
    main()
