"""Repeatable descriptive matchup reports: PBP, PFR, market and availability."""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_pfr_supplement import team_code


def mean(values):
    values = [float(v) for v in values if v is not None]
    return sum(values) / len(values) if values else None


def no_vig(home, away):
    if home is None or away is None or abs(home) < 100 or abs(away) < 100:
        return None
    def implied(v):
        return -v / (100 - v) if v < 0 else 100 / (100 + v)
    h, a = implied(home), implied(away)
    return h / (h + a)


def play_summary(plays):
    eligible = [p for p in plays if p["play_type"] in {"run", "pass"}]
    out = {}
    for name, group in [("all", eligible), ("pass", [p for p in eligible if p["play_type"] == "pass"]),
                        ("run", [p for p in eligible if p["play_type"] == "run"])]:
        clean = [p for p in group if p["turnover_type"] not in {"interception", "fumble_lost"}]
        out[name] = {"plays": len(group), "epa": mean(p["epa"] for p in group),
                     "success": mean(p["success"] for p in group),
                     "sacks": sum(bool(p["had_sack"]) for p in group),
                     "turnovers": len(group) - len(clean),
                     "epa_excluding_turnover_plays": mean(p["epa"] for p in clean)}
    drives = {(p["game_id"], p["posteam"], p["drive"]): p["drive_result"]
              for p in plays if p["drive"] is not None and p["drive_result"]}
    out["drive_results"] = dict(Counter(drives.values()))
    return out


def build_report(db, game_id, *, as_of=None, lookback=4):
    now = datetime.now(timezone.utc)
    as_of = as_of or now
    if as_of.tzinfo is None or as_of > now:
        raise ValueError("as_of must be a timezone-aware present/past timestamp")
    game = db.execute_one("""SELECT g.*,h.abbreviation home,a.abbreviation away
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id WHERE nflverse_game_id=%s""", (game_id,))
    if not game:
        raise ValueError("Unknown schedule game")
    cutoff = min(as_of, game["kickoff"])
    warnings = ["Descriptive, opponent-unadjusted small samples; no calibrated probability adjustment.",
                "PBP uses current stored revisions; this is not a point-in-time historical backtest.",
                "Excluding turnover plays is a sensitivity check, not an expected-performance forecast.",
                "PFR counts are game-level charting; do not label individual PBP plays from them."]
    market = db.execute_one("""SELECT home_ml,away_ml,home_spread,captured_at
        FROM game_odds_history WHERE sport='nfl' AND matchup_id=%s
        AND captured_at<%s AND home_ml IS NOT NULL AND away_ml IS NOT NULL
        ORDER BY captured_at DESC,id DESC LIMIT 1""", (game["matchup_id"], cutoff))
    candidates = [dict(market, source="Captured sportsbook consensus")] if market else []
    if game["market_captured_at"] and game["market_captured_at"] < cutoff:
        candidates.append(dict(home_ml=game["market_home_ml"], away_ml=game["market_away_ml"],
                               home_spread=-float(game["market_spread_line"]) if game["market_spread_line"] is not None else None,
                               captured_at=game["market_captured_at"], source="Season market capture"))
    candidates = [q for q in candidates if no_vig(q["home_ml"], q["away_ml"]) is not None]
    market = max(candidates, key=lambda q: q["captured_at"]) if candidates else None
    if market:
        market["home_probability"] = no_vig(market["home_ml"], market["away_ml"])
        market["age_hours"] = (cutoff - market["captured_at"]).total_seconds() / 3600
        market["fresh"] = market["age_hours"] <= (2 if game["kickoff"] - cutoff <= timedelta(hours=24) else 24)
        if not market["fresh"]:
            warnings.append("Market quote is stale for the kickoff horizon; refresh before selecting a pick.")
    else:
        warnings.append("No eligible two-sided pregame moneyline; no market probability available.")
    teams = {}
    for side in ("away", "home"):
        team = team_code(game[side])
        prior = db.execute("""SELECT nflverse_game_id AS game_id,week,kickoff FROM nfl_season_games
            WHERE season=%s AND completed=TRUE AND kickoff<%s
            AND (home_team_id=%s OR away_team_id=%s)
            ORDER BY kickoff DESC LIMIT %s""", (game["season"], cutoff, game[f"{side}_team_id"], game[f"{side}_team_id"], lookback))
        ids = [g["game_id"] for g in prior]
        plays = db.execute("""SELECT game_id,posteam,defteam,play_type,epa,success,had_sack,
            turnover_type,passer,drive,drive_result FROM nfl_pbp_archetypes WHERE game_id=ANY(%s)""", (ids,)) if ids else []
        snapshots = db.execute("""SELECT DISTINCT ON(game_id) game_id,captured_at,payload
            FROM nfl_pfr_game_snapshots WHERE game_id=ANY(%s) AND captured_at<=%s
            ORDER BY game_id,captured_at DESC,snapshot_id DESC""", (ids, cutoff)) if ids else []
        pfr_rows, opponents, coverage = [], [], []
        for s in snapshots:
            payload = s["payload"]
            if payload.get("stats_schema") != "nflverse_pfr_fields_percentage_points":
                warnings.append(f"{s['game_id']}: unsupported PFR stats schema omitted from charting tables.")
                continue
            coverage.append({"game_id": s["game_id"], "captured_at": s["captured_at"], "coverage": payload["coverage"]})
            for row in payload["rows"]:
                annotated = dict(row, game_id=s["game_id"])
                if team_code(row["team"]) == team:
                    pfr_rows.append(annotated)
                elif row["section"] == "passing_advanced":
                    opponents.append(annotated)
        injuries = db.execute("""SELECT DISTINCT ON(p.id) p.canonical_name,p.position,p.team_abbrev,
            o.normalized_status,o.practice_status,o.body_part,o.description,o.source,
            o.observed_at,o.provider_updated_at,o.raw_payload->>'url' AS source_url
            FROM ff_players p JOIN ff_player_injury_observations o ON o.player_id=p.id AND o.season=p.season
            WHERE p.season=%s AND p.team_abbrev=%s AND o.observed_at<%s
            AND o.observed_at>=%s AND (o.provider_updated_at IS NULL OR o.provider_updated_at<%s)
            ORDER BY p.id,o.observed_at DESC,o.id DESC""",
            (game["season"], game[side], cutoff, cutoff-timedelta(days=7), cutoff))
        offense = [p for p in plays if team_code(p["posteam"] or "") == team]
        defense = [p for p in plays if team_code(p["defteam"] or "") == team]
        qbs = defaultdict(list)
        for p in offense:
            if p["play_type"] == "pass" and p["passer"]:
                qbs[p["passer"]].append(p)
        missing_pbp = sorted(set(ids) - {p["game_id"] for p in plays})
        missing_pfr = sorted(set(ids) - {s["game_id"] for s in coverage})
        if missing_pbp or missing_pfr:
            warnings.append(f"{team}: missing PBP {missing_pbp}; missing PFR {missing_pfr}.")
        if not injuries:
            warnings.append(f"{team}: no recent availability observations; this does not mean healthy.")
        elif (cutoff-max(r['observed_at'] for r in injuries)).total_seconds() > 86400:
            warnings.append(f"{team}: availability observations are older than 24 hours.")
        teams[team] = {"prior_games": prior, "offense": play_summary(offense), "defense": play_summary(defense),
                       "qbs": {q: play_summary(p)["pass"] for q, p in qbs.items()},
                       "pfr": pfr_rows, "opponent_passers": opponents, "coverage": coverage, "injuries": injuries,
                       "missing_pbp": missing_pbp, "missing_pfr": missing_pfr}
    return {"game_id": game_id, "home": team_code(game["home"]), "away": team_code(game["away"]),
            "kickoff": game["kickoff"], "generated_at": now, "evidence_cutoff": cutoff,
            "market": market, "teams": teams, "warnings": warnings}


def fmt(value, digits=2):
    return "missing" if value is None else f"{value:.{digits}f}"


def render(report):
    lines = [f"# {report['away']} at {report['home']} matchup evidence", "",
             f"Game: {report['game_id']} · Kickoff: {report['kickoff']}", "",
             f"Generated: {report['generated_at']} · Evidence cutoff: {report['evidence_cutoff']}", ""]
    m = report["market"]
    if m:
        lines += [f"Market baseline: {report['home']} {100*m['home_probability']:.1f}%, "
                  f"{report['away']} {100*(1-m['home_probability']):.1f}% (no vig, conditional on no tie). "
                  f"Home/away moneylines {m['home_ml']}/{m['away_ml']}; captured {m['captured_at']}; "
                  f"{'fresh' if m['fresh'] else 'STALE'}.", ""]
    for team, data in report["teams"].items():
        lines += [f"## {team}", "", "Prior games: " + ", ".join(g["game_id"] for g in data["prior_games"]), "",
                  "| PBP sample | Plays | EPA/play | Success | Sacks | Turnovers | EPA excluding turnover plays |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for unit in ("offense", "defense"):
            for kind in ("all", "pass", "run"):
                v = data[unit][kind]
                lines.append(f"| {unit} {kind} | {v['plays']} | {fmt(v['epa'],3)} | {fmt(100*v['success'] if v['success'] is not None else None,1)}% | {v['sacks']} | {v['turnovers']} | {fmt(v['epa_excluding_turnover_plays'],3)} |")
        lines += ["", "Defensive EPA is opponent EPA: lower is better. Run/pass sample includes sacks and run-coded scrambles/kneels.", "",
                  "Quarterback PBP (passing plays, including sacks):", ""]
        for qb, stats in data["qbs"].items():
            lines.append(f"- {qb}: {stats['plays']} plays; {fmt(stats['epa'],3)} EPA/play.")
        lines += ["", "### PFR passing and opposing quarterbacks", "",
                  "Opponent-QB rows describe pressure created by this team's defense. Rates remain per game; denominators are not reconstructed from rounded rates.", "",
                  "| Role | Game | QB | Pressures | Pressure % | Sacks | Blitzes faced | Drops | Bad throws |",
                  "|---|---|---|---:|---:|---:|---:|---:|---:|"]
        for role, rows in [("Own QB", [r for r in data["pfr"] if r["section"] == "passing_advanced"]), ("Opposing QB", data["opponent_passers"])]:
            for row in rows:
                s = row["stats"]
                vals = [s.get(k) for k in ("times_pressured", "times_pressured_pct", "times_sacked", "times_blitzed", "passing_drops", "passing_bad_throws")]
                lines.append(f"| {role} | {row['game_id']} | {row['player_name']} | " + " | ".join(fmt(v,1) for v in vals) + " |")
        lines += ["", "### Rushing contact", "", "| Game | Player | Carries | Yards before contact | Yards after contact | Broken tackles |", "|---|---|---:|---:|---:|---:|"]
        for row in data["pfr"]:
            if row["section"] == "rushing_advanced":
                s = row["stats"]
                lines.append(f"| {row['game_id']} | {row['player_name']} | " + " | ".join(fmt(s.get(k),0) for k in ("carries", "rushing_yards_before_contact", "rushing_yards_after_contact", "rushing_broken_tackles")) + " |")
        lines += ["", "### Availability observations", "", "Latest stored observation per player; feed reports are not a confirmed starting lineup. Missing defenders or linemen are not assumed healthy.", "",
                  "| Player | Position | Status | Practice | Source | Observed |", "|---|---|---|---|---|---|"]
        for r in data["injuries"]:
            if r['normalized_status'] in {'HEALTHY', 'UNKNOWN'} and r['position'] != 'QB':
                continue
            lines.append("| " + " | ".join(str(r.get(k) or "unknown").replace("|", "/") for k in ("canonical_name", "position", "normalized_status", "practice_status", "source", "observed_at")) + " |")
        lines += ["", "PFR captures: " + "; ".join(f"{c['game_id']} at {c['captured_at']}" for c in data["coverage"]), ""]
    lines += ["## Interpretation limits", ""] + [f"- {w}" for w in report["warnings"]]
    lines += ["- Starters and snap counts are not in this PFR supplement. Availability must be confirmed separately.",
              "- Coverage, missed tackles and receiving details remain in the companion JSON. Defender pressure credits are not summed into unique team pressures.",
              "- These observations do not automatically override the market or produce a betting edge.", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--game")
    group.add_argument("--upcoming", action="store_true", help="All scheduled games in the next seven days")
    parser.add_argument("--lookback", type=int, default=4)
    parser.add_argument("--as-of", help="Aware ISO timestamp; historical PBP revisions are not frozen")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/nfl-matchups"))
    args = parser.parse_args(argv)
    if args.lookback < 1:
        parser.error("lookback must be positive")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    now = datetime.now(timezone.utc)
    cutoff = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else now
    ids = [args.game] if args.game else [r["nflverse_game_id"] for r in db.execute(
        "SELECT nflverse_game_id FROM nfl_season_games WHERE kickoff>%s AND kickoff<=%s AND completed=FALSE ORDER BY kickoff",
        (cutoff, cutoff+timedelta(days=7)))]
    dest = args.output_dir / now.strftime("%Y%m%dT%H%M%SZ")
    dest.mkdir(parents=True, exist_ok=True)
    for game_id in ids:
        report = build_report(db, game_id, as_of=cutoff, lookback=args.lookback)
        (dest / f"{game_id}.json").write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
        (dest / f"{game_id}.md").write_text(render(report), encoding="utf-8")
    print(json.dumps({"reports": len(ids), "directory": str(dest)}))


if __name__ == "__main__":
    main()
