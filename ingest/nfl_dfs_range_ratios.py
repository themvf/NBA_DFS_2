"""Long-memory range shape for each RB/WR/TE (``nfl-dfs-range-split-v1``).

Owner's decision 2026-10-26, after Gate Q passed (docs/nfl-dfs-range-split-study.md,
Amendment 1): the DFS page keeps every projection and rescales each RB/WR/TE's
floor and ceiling by the shape of his half-life-17 distribution,

    P10' = m * (P10_17 / m17)      P90' = m * (P90_17 / m17)

where ``m`` is the page's final projection (after the absence commit and the
market anchor). This job writes the two ratios per player for the upcoming
week, computed exactly as the study's forward gate does (``projection_draws``
on nflverse history from three seasons back, before the target week, the
player's team implied total). The web read applies them
(``web/src/lib/nfl-dfs/range-split.ts``); a player without a row keeps the
model's range and the page counts him.

Writes are change-only: a ratio row is inserted when its inputs digest is new
for that player and week, and every run stamps one row in
``nfl_dfs_range_ratio_runs`` (the health clock), so a run that found nothing
new still shows it ran.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import sys
import uuid

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_weekly import target_season
from model.nfl_absence_reallocation import DEFAULT_CACHE
from model.nfl_dfs_historical import ProjectionContext, before_cutoff
from model.nfl_dfs_participation_ledger_gate_h import load_sources
from model.nfl_team_aliases import normalize_team
from research.nfl_dfs_range_calibration import PeerIndex, projection_draws, summarize_draws
from research.nfl_dfs_range_split import FORWARD_HISTORY_BACK, GATED, SPREAD_HALF_LIFE, refresh_2026_cache, split_range

VERSION = "nfl-dfs-range-split-v1"
# A healthy week has several hundred eligible players; far fewer means the
# nflverse files did not load properly and writing would blank most ratios.
MIN_PLAYERS = 200


def ratios(m17: float, p10_17: float, p90_17: float) -> tuple[float, float] | None:
    """(P10/m17, P90/m17), or None when the long-memory mean is not positive."""
    shape = split_range(1.0, m17, p10_17, p90_17)
    return None if shape is None else (round(shape[0], 6), round(shape[1], 6))


def compute(db: DatabaseManager, season: int, week: int, cache) -> tuple[list[dict], dict, dict[str, str]]:
    players = db.execute("""SELECT id, gsis_id, position, team_abbrev FROM ff_players
        WHERE season = %s AND position = ANY(%s) AND gsis_id IS NOT NULL AND COALESCE(active, TRUE)""",
                         (season, list(GATED)))
    if season == 2026:
        refresh_2026_cache(cache)
    sources = load_sources(season, cache, FORWARD_HISTORY_BACK)
    by_pos: dict[str, list] = defaultdict(list)
    for row in sorted(sources.stat_rows, key=lambda r: r.chronological_key):
        by_pos[row.position].append(row)
    prior_by_pos, index_by_pos = {}, {}
    for pos in GATED:
        prior_by_pos[pos] = [x for x in by_pos[pos] if before_cutoff(x, season, week)]
        index_by_pos[pos] = PeerIndex(pos, prior_by_pos[pos])
    counts: dict[str, int] = defaultdict(int)
    out: list[dict] = []
    for p in players:
        gsis, pos = str(p["gsis_id"]), str(p["position"])
        if gsis not in sources.player_ids:
            counts["no_nflverse_identity"] += 1
            continue
        game = sources.games.get((season, week, normalize_team(p["team_abbrev"] or "")))
        draws = projection_draws(
            player_id=sources.player_ids[gsis], player_gsis_id=gsis,
            player_name=sources.roster_names.get(gsis, gsis), position=pos, prior=prior_by_pos[pos],
            cutoff_season=season, cutoff_week=week,
            context=ProjectionContext(team_implied_total=game.implied_total if game else None),
            half_lives=(SPREAD_HALF_LIFE,), peer_index=index_by_pos[pos])
        if draws is None:
            counts["no_history"] += 1
            continue
        s = summarize_draws(draws[SPREAD_HALF_LIFE])
        shape = ratios(s["mean"], s["p10"], s["p90"])
        if shape is None:
            counts["m17_not_positive"] += 1
            continue
        history_games = len([r for r in prior_by_pos[pos] if r.player_gsis_id == gsis])
        inputs = {"m17": s["mean"], "p10_17": s["p10"], "p90_17": s["p90"], "history_games": history_games,
                  "implied_total": game.implied_total if game else None, "digests": sources.digests}
        out.append({"ff_player_id": int(p["id"]), "gsis_id": gsis, "position": pos,
                    "m17": s["mean"], "p10_17": s["p10"], "p90_17": s["p90"],
                    "ratio_p10": shape[0], "ratio_p90": shape[1], "history_games": history_games,
                    "inputs_digest": hashlib.sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest()})
        counts["computed"] += 1
    return out, dict(counts), sources.digests


def persist(db: DatabaseManager, season: int, week: int, rows: list[dict], counts: dict, digests: dict) -> dict:
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    written = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for r in rows:
            cur.execute("""INSERT INTO nfl_dfs_range_ratios (season, week, ff_player_id, gsis_id, position, m17, p10_17,
                p90_17, ratio_p10, ratio_p90, history_games, inputs_digest, run_id, computed_at, version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (season, week, ff_player_id, inputs_digest) DO NOTHING""",
                        (season, week, r["ff_player_id"], r["gsis_id"], r["position"], r["m17"], r["p10_17"], r["p90_17"],
                         r["ratio_p10"], r["ratio_p90"], r["history_games"], r["inputs_digest"], run_id, now, VERSION))
            written += cur.rowcount
        cur.execute("""INSERT INTO nfl_dfs_range_ratio_runs (run_id, season, week, computed_at, players, written, counts,
            source_digests, version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, season, week, now, len(rows), written, json.dumps(counts), json.dumps(digests), VERSION))
    return {"run_id": run_id, "season": season, "week": week, "players": len(rows), "written": written, "counts": counts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--cache", default=DEFAULT_CACHE)
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    db.require_tables(("nfl_dfs_range_ratios", "nfl_dfs_range_ratio_runs"))
    season = target_season(args.season, datetime.now(timezone.utc))
    week = args.week
    if week is None:
        from ingest.nfl_dfs_projections import infer_target_week
        upcoming = db.execute_one("""SELECT COUNT(*)::int n FROM nfl_season_games
            WHERE season=%s AND game_type='REG' AND kickoff > NOW()""", (season,))
        if not upcoming["n"]:
            print(json.dumps({"status": "no_upcoming_regular_season_games", "season": season}))
            return 0
        week = infer_target_week(db, season)
    rows, counts, digests = compute(db, season, week, args.cache)
    if len(rows) < MIN_PLAYERS:
        print(f"::error title=Range ratios not written::only {len(rows)} players computed for {season} week {week} "
              f"(floor {MIN_PLAYERS}); counts {counts}")
        return 1
    print(json.dumps(persist(db, season, week, rows, counts, digests)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
