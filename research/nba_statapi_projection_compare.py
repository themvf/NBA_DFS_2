"""Our NBA points forecasts against stat-api's, on the same player-games.

    python -m research.nba_statapi_projection_compare
    python -m research.nba_statapi_projection_compare --since 2026-10-26 --json out.json

Population: every player in a prospective ``nba-points-ghost-v2`` snapshot
(the latest grade attempt of each, status ``graded``: he played and has an
ESPN box score) who also has a stat-api projection for that Eastern date. That
is the market pool: players the books quoted a points line for that morning.

Forecasts compared, all against the same actual points:

- ``statapi``: stat-api's projected ``pts``, frozen at tip (inputs as late as
  a minute before the game, with confirmed lineups).
- ``history20`` / ``blend90_10`` / ``opportunity_points`` / ``market_line``:
  our ghost snapshot, frozen ~11:38 AM Eastern, hours before any tip.
- ``market_late``: the consensus line from the books' LAST pre-tip capture
  (``nba_points_calibration``), the closest thing we have to stat-api's
  timing. Only where that row is graded too.

This is NOT a like-for-like timing test: stat-api sees news (scratches,
starting lineups, minutes limits) that our morning freeze cannot. The late
market line is the fairer reference for "who forecasts better at tip".
A line is a median, a projection is a mean, so a line sits slightly below the
average scored (bias is reported, not corrected).

Uncertainty: paired differences with a bootstrap that resamples whole dates
(players on one night share news and pace). Descriptive, not pre-registered;
nothing here changes a forecast.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import json
import math
import random

from config import load_config
from db.database import DatabaseManager

GHOST_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (g.snapshot_sha256) s.game_date, g.grade
    FROM nba_ghost_grades g JOIN nba_ghost_snapshots s USING (snapshot_sha256)
    WHERE s.mode = 'prospective' AND s.game_date >= %(since)s
    ORDER BY g.snapshot_sha256, g.graded_at DESC
)
SELECT l.game_date, x->>'player' AS player, x->>'player_key' AS player_key,
       (x->>'actual_points')::float AS actual, (x->>'actual_minutes')::float AS minutes,
       (x->'predictions'->>'history20')::float AS history20,
       (x->'predictions'->>'blend90_10')::float AS blend90_10,
       (x->'predictions'->>'opportunity_points')::float AS opportunity_points,
       (x->'predictions'->>'market_line')::float AS market_line,
       (x->>'historical_p90')::float AS historical_p90,
       (x->>'experimental_p90')::float AS experimental_p90,
       (x->>'market_ceiling_approximation')::float AS market_ceiling
FROM latest l, jsonb_array_elements(l.grade->'rows') x
WHERE x->>'status' = 'graded'
"""

STATAPI_SQL = """
SELECT game_date, player_key, player, team, status, play_probability, start_probability,
       lineup_confirmed, minutes_mean, pts, pts_p10, pts_p50, pts_p90, inputs_as_of, game_time
FROM statapi_nba_player_projections
WHERE game_date >= %(since)s AND player_key IS NOT NULL
"""

LATE_SQL = """
SELECT game_date, player_key, line AS market_late, ceiling AS market_late_p90,
       minutes_before_tip, actual_points
FROM nba_points_calibration
WHERE grade_version = 'nba-points-grade-v1' AND status = 'graded' AND game_date >= %(since)s
"""

MEAN_FORECASTS = ["statapi", "market_late", "market_line", "blend90_10", "history20", "opportunity_points"]
P90_FORECASTS = ["statapi_p90", "market_late_p90", "historical_p90", "experimental_p90", "market_ceiling"]


def load(db: DatabaseManager, since: date) -> tuple[list[dict], dict]:
    params = {"since": since}
    ghost = db.execute(GHOST_SQL, params)
    statapi: dict[tuple, list[dict]] = defaultdict(list)
    for row in db.execute(STATAPI_SQL, params):
        statapi[(row["game_date"], row["player_key"])].append(row)
    late = {(r["game_date"], r["player_key"]): r for r in db.execute(LATE_SQL, params)}

    counts = {"ghost_graded": len(ghost), "no_statapi": 0, "statapi_ambiguous": 0, "late_actual_mismatch": 0}
    joined = []
    for g in ghost:
        key = (g["game_date"], g["player_key"])
        matches = statapi.get(key, [])
        if not matches:
            counts["no_statapi"] += 1
            continue
        if len(matches) > 1:
            counts["statapi_ambiguous"] += 1
            continue
        s = matches[0]
        row = dict(g)
        row.update({"statapi": s["pts"], "statapi_p90": s["pts_p90"], "statapi_status": s["status"],
                    "play_probability": s["play_probability"], "lineup_confirmed": s["lineup_confirmed"],
                    "statapi_minutes": s["minutes_mean"]})
        lt = late.get(key)
        if lt is not None:
            if lt["actual_points"] is not None and abs(lt["actual_points"] - g["actual"]) > 0.01:
                counts["late_actual_mismatch"] += 1
            else:
                row.update({"market_late": lt["market_late"], "market_late_p90": lt["market_late_p90"],
                            "minutes_before_tip": lt["minutes_before_tip"]})
        joined.append(row)
    counts["matched"] = len(joined)
    return joined, counts


def _dates_bootstrap(rows: list[dict], stat, n: int = 2000, seed: int = 7) -> tuple[float, float]:
    by_date: dict = defaultdict(list)
    for r in rows:
        by_date[r["game_date"]].append(r)
    dates = list(by_date)
    rng = random.Random(seed)
    draws = []
    for _ in range(n):
        sample = [r for d in (rng.choice(dates) for _ in dates) for r in by_date[d]]
        value = stat(sample)
        if value is not None:
            draws.append(value)
    draws.sort()
    return draws[int(0.025 * len(draws))], draws[int(0.975 * len(draws)) - 1]


def mean_table(rows: list[dict], forecasts: list[str]) -> list[dict]:
    """Every forecast scored on the rows where ALL listed forecasts exist."""
    common = [r for r in rows if all(r.get(f) is not None for f in forecasts)]
    out = []
    for f in forecasts:
        errors = [r[f] - r["actual"] for r in common]
        out.append({"forecast": f, "n": len(common),
                    "mae": sum(abs(e) for e in errors) / len(errors) if errors else None,
                    "rmse": math.sqrt(sum(e * e for e in errors) / len(errors)) if errors else None,
                    "bias": sum(errors) / len(errors) if errors else None})
    return out


def paired(rows: list[dict], a: str, b: str) -> dict:
    """MAE(a) - MAE(b) on rows with both; negative = a closer."""
    both = [r for r in rows if r.get(a) is not None and r.get(b) is not None]

    def stat(sample):
        if not sample:
            return None
        return sum(abs(r[a] - r["actual"]) - abs(r[b] - r["actual"]) for r in sample) / len(sample)

    point = stat(both)
    lo, hi = _dates_bootstrap(both, stat) if both else (None, None)
    closer = sum(1 for r in both if abs(r[a] - r["actual"]) < abs(r[b] - r["actual"]))
    return {"a": a, "b": b, "n": len(both), "dates": len({r["game_date"] for r in both}),
            "mae_diff": point, "ci95": [lo, hi], "a_closer_share": closer / len(both) if both else None}


def p90_table(rows: list[dict]) -> list[dict]:
    common = [r for r in rows if all(r.get(f) is not None for f in P90_FORECASTS)]
    out = []
    for f in P90_FORECASTS:
        above = sum(1 for r in common if r["actual"] > r[f])
        pinball = sum(max(0.9 * (r["actual"] - r[f]), 0.1 * (r[f] - r["actual"])) for r in common)
        out.append({"forecast": f, "n": len(common), "share_above": above / len(common) if common else None,
                    "pinball90": pinball / len(common) if common else None})
    return out


def report(rows: list[dict], counts: dict) -> dict:
    cohorts = {
        "all": rows,
        "line_20_plus": [r for r in rows if (r.get("market_line") or 0) >= 20],
        "statapi_sure_to_play": [r for r in rows if r.get("statapi_status") == "available"
                                 and (r.get("play_probability") or 0) >= 0.99],
    }
    out = {"counts": counts, "dates": sorted({str(r["game_date"]) for r in rows}),
           "statapi_status_of_players_who_played": dict(sorted(
               _count(r.get("statapi_status") for r in rows).items())),
           "cohorts": {}}
    for name, cohort in cohorts.items():
        out["cohorts"][name] = {
            "means_all_present": mean_table(cohort, MEAN_FORECASTS),
            "means_without_late": mean_table(cohort, [f for f in MEAN_FORECASTS if f != "market_late"]),
            "paired_vs_statapi": [paired(cohort, f, "statapi") for f in MEAN_FORECASTS if f != "statapi"],
        }
    out["p90"] = p90_table(rows)
    return out


def _count(values) -> dict:
    c: dict = defaultdict(int)
    for v in values:
        c[str(v)] += 1
    return c


def print_report(out: dict) -> None:
    c = out["counts"]
    print(f"Ghost graded rows {c['ghost_graded']}; matched to stat-api {c['matched']} "
          f"(no stat-api row {c['no_statapi']}, ambiguous {c['statapi_ambiguous']}, "
          f"late-grade actual mismatch {c['late_actual_mismatch']}); "
          f"{len(out['dates'])} dates {out['dates'][0]}..{out['dates'][-1]}")
    print(f"stat-api status of players who played: {out['statapi_status_of_players_who_played']}")
    for name, block in out["cohorts"].items():
        print(f"\n== {name}")
        for label in ("means_all_present", "means_without_late"):
            rows = block[label]
            print(f"  {label} (n={rows[0]['n']})")
            for r in rows:
                if r["mae"] is not None:
                    print(f"    {r['forecast']:<20} MAE {r['mae']:.2f}  RMSE {r['rmse']:.2f}  bias {r['bias']:+.2f}")
        print("  paired MAE difference vs stat-api (negative = closer than stat-api), date-clustered 95% CI")
        for p in block["paired_vs_statapi"]:
            if p["mae_diff"] is not None:
                print(f"    {p['a']:<20} {p['mae_diff']:+.2f}  [{p['ci95'][0]:+.2f}, {p['ci95'][1]:+.2f}]  "
                      f"n={p['n']} dates={p['dates']}  closer {p['a_closer_share']:.0%}")
    print(f"\n== P90 (share of actuals above; 10% is right). n={out['p90'][0]['n']}")
    for r in out["p90"]:
        print(f"    {r['forecast']:<20} above {r['share_above']:.1%}  pinball {r['pinball90']:.3f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--since", type=date.fromisoformat, default=date(2026, 10, 26),
                        help="first Eastern date (default: first prospective ghost snapshot)")
    parser.add_argument("--json", help="also write the report here")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows, counts = load(db, args.since)
    if not rows:
        raise SystemExit("no matched rows: run python -m ingest.statapi_nba_projections first")
    out = report(rows, counts)
    print_report(out)
    if args.json:
        with open(args.json, "x", encoding="utf-8") as fh:
            json.dump(out, fh, indent=1, default=str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
