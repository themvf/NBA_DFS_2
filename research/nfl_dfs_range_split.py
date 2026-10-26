"""``nfl-dfs-range-split-v1``: centre from half-life 3, spread from half-life 17.

Registered in ``docs/nfl-dfs-range-split-study.md`` on 2026-10-26, before week 8
of 2026 and before any 2020 or 2021 row was replayed. Follows
``nfl-dfs-range-calibration-v1``, where half-life 17 gave better P10/P90 but a
worse projection. Research only: production projections are not touched.

The candidate keeps the projection ``m`` and rescales the range by the
half-life-17 distribution's own shape::

    P10' = m * (P10_17 / m17)      P90' = m * (P90_17 / m17)

Historical gate: ``m`` is the v5 (half-life 3) mean. Forward gate: ``m`` is the
saved build's projection (after every overlay and the market anchor), so the
candidate only replaces the width; the anchor multiplies all quantiles by the
same k, so the ratio form is unaffected by it.

Usage::

    python -m research.nfl_dfs_range_split replay --season 2020
    python -m research.nfl_dfs_range_split replay --season 2021
    python -m research.nfl_dfs_range_split blind            # Gate B verdict, once
    python -m research.nfl_dfs_range_split forward --weeks 8-15   # Gate F, once, after week 15
    python -m research.nfl_dfs_range_split forward --weeks 1-7 --smoke   # plumbing check: counts only
    python -m research.nfl_dfs_range_split quick            # Gate Q (Amendment 1), weeks 1-7, once
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from model.nfl_absence_reallocation import DEFAULT_CACHE
from model.nfl_dfs_historical import HistoricalWeek, ProjectionContext, before_cutoff
from model.nfl_dfs_participation_ledger_gate_h import (
    MODEL_FILE_SHA256,
    game_bootstrap,
    load_sources,
    model_file_matches_pin,
    model_file_sha256,
    pinball,
)
from model.nfl_team_aliases import normalize_team
from research.nfl_dfs_range_calibration import (
    PeerIndex,
    calibration_error,
    projection_draws,
    range_loss,
    replay_season,
    summarize_draws,
)

STUDY_ID = "nfl-dfs-range-split-v1"
SPREAD_HALF_LIFE = 17.0          # frozen by nfl-dfs-range-calibration-v1's tuning (2022-23)
CENTRE_HALF_LIFE = 3.0
GATED = ("RB", "TE", "WR")
BLIND_SEASONS = (2020, 2021)
FORWARD_WEEKS = tuple(range(8, 16))
FORWARD_FALLBACK_WEEKS = tuple(range(8, 19))
QUICK_WEEKS = tuple(range(1, 8))          # Gate Q, Amendment 1 (2026-10-26)
FORWARD_MIN_ROWS = 400
FORWARD_MIN_WEEKS = 6
FORWARD_HISTORY_BACK = 3         # production's table holds 2023 onward
MIN_PROJECTION = 5.0
POSITION_GUARD_MAX_POINT = 0.01
BOOTSTRAP_SEED = 20261026
BOOTSTRAP_DRAWS = 10_000
ARTIFACT_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "nfl-dfs-range-split"


def split_range(m: float, m17: float, p10_17: float, p90_17: float) -> tuple[float, float] | None:
    """The candidate's (P10, P90), or None when the half-life-17 mean is not positive."""
    if m17 is None or m17 <= 0:
        return None
    return m * p10_17 / m17, m * p90_17 / m17


# ---------------------------------------------------------------------------
# Grading (shared by both gates)
# ---------------------------------------------------------------------------

def grade_rows(rows: Sequence[Mapping[str, Any]], cluster_key: str) -> dict[str, Any]:
    """Rows carry actual, base_p10, base_p90, cand_p10, cand_p90, position and a cluster id."""
    gated = [r for r in rows if r["position"] in GATED]
    clusters = [str(r[cluster_key]) for r in gated]
    weeks = [f"{r['season']}-{r['week']}" for r in gated]
    d = [range_loss(r["actual"], r["cand_p10"], r["cand_p90"]) - range_loss(r["actual"], r["base_p10"], r["base_p90"]) for r in gated]
    d10 = [pinball(0.10, r["cand_p10"], r["actual"]) - pinball(0.10, r["base_p10"], r["actual"]) for r in gated]
    d90 = [pinball(0.90, r["cand_p90"], r["actual"]) - pinball(0.90, r["base_p90"], r["actual"]) for r in gated]

    def cov(sub, arm):
        a = np.array([r["actual"] for r in sub])
        lo = np.array([r[f"{arm}_p10"] for r in sub]); hi = np.array([r[f"{arm}_p90"] for r in sub])
        return {"n": len(sub), "below_p10": float(np.mean(a < lo)), "above_p90": float(np.mean(a > hi)),
                "inside": float(np.mean((a >= lo) & (a <= hi)))}

    out: dict[str, Any] = {
        "n": len(gated), "clusters": len(set(clusters)), "weeks": len(set(weeks)),
        "range_loss_delta": game_bootstrap(d, clusters, draws=BOOTSTRAP_DRAWS, seed=BOOTSTRAP_SEED),
        "range_loss_delta_week_clustered": game_bootstrap(d, weeks, draws=BOOTSTRAP_DRAWS, seed=BOOTSTRAP_SEED),
        "p10_pinball_delta_point": float(np.mean(d10)), "p90_pinball_delta_point": float(np.mean(d90)),
        "baseline_range_loss": float(np.mean([range_loss(r["actual"], r["base_p10"], r["base_p90"]) for r in gated])),
        "coverage": {"baseline": cov(gated, "base"), "candidate": cov(gated, "cand")},
        "by_position": {},
    }
    for pos in GATED:
        idx = [i for i, r in enumerate(gated) if r["position"] == pos]
        sub = [gated[i] for i in idx]
        if not idx:
            out["by_position"][pos] = {"n": 0}
            continue
        out["by_position"][pos] = {
            "n": len(idx),
            "range_loss_delta": game_bootstrap([d[i] for i in idx], [clusters[i] for i in idx], draws=2000, seed=BOOTSTRAP_SEED),
            "coverage": {"baseline": cov(sub, "base"), "candidate": cov(sub, "cand")},
        }
    return out


def guards(g: Mapping[str, Any]) -> dict[str, Any]:
    pos_ok = {p: v.get("n", 0) == 0 or v["range_loss_delta"]["mean"] <= POSITION_GUARD_MAX_POINT for p, v in g["by_position"].items()}
    cov_ok = calibration_error(g["coverage"]["candidate"]) < calibration_error(g["coverage"]["baseline"])
    return {"interval_wholly_below_zero": g["range_loss_delta"]["hi"] < 0.0, "no_position_worse": all(pos_ok.values()),
            "position_readings": pos_ok, "calibration_closer_to_target": cov_ok,
            "all_guards_hold": all(pos_ok.values()) and cov_ok}


def blind_verdict(per_season: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    readings = {s: guards(per_season[s]) for s in BLIND_SEASONS}
    if any(not r["interval_wholly_below_zero"] for r in readings.values()):
        return {"verdict": "KILL", "reason": "range-loss interval reaches zero in 2020 or 2021", "readings": readings}
    if all(r["all_guards_hold"] for r in readings.values()):
        return {"verdict": "B_CLEARED", "reason": "interval below zero and every guard holds in 2020 and 2021; Gate F still runs", "readings": readings}
    return {"verdict": "INCONCLUSIVE", "reason": "interval below zero but a guard failed", "readings": readings}


def forward_verdict(blind: str, g: Mapping[str, Any], weeks_with_rows: int, final_window: bool) -> dict[str, Any]:
    if blind != "B_CLEARED":
        return {"verdict": "NOT_RUN", "reason": f"Gate B is {blind}"}
    if g["n"] < FORWARD_MIN_ROWS or weeks_with_rows < FORWARD_MIN_WEEKS:
        if not final_window:
            return {"verdict": "WAIT", "reason": f"{g['n']} rows over {weeks_with_rows} weeks; floor {FORWARD_MIN_ROWS} over {FORWARD_MIN_WEEKS}; grade once after week 18"}
    point_ok = g["range_loss_delta"]["mean"] < 0.0
    cov_ok = calibration_error(g["coverage"]["candidate"]) < calibration_error(g["coverage"]["baseline"])
    if point_ok and cov_ok:
        return {"verdict": "PASS", "reason": "Gate B cleared; forward point estimate below zero and calibration closer to target"}
    return {"verdict": "INCONCLUSIVE", "reason": "Gate B cleared but the forward gate did not hold", "point_below_zero": point_ok, "calibration_closer": cov_ok}


def quick_verdict(g: Mapping[str, Any]) -> dict[str, Any]:
    """Gate Q (Amendment 1): ship only if the point estimate is below 0 and calibration is closer."""
    point_ok = g["range_loss_delta"]["mean"] < 0.0
    cov_ok = calibration_error(g["coverage"]["candidate"]) < calibration_error(g["coverage"]["baseline"])
    if point_ok and cov_ok:
        return {"verdict": "SHIP_OK", "reason": "weeks 1-7: point estimate below zero and calibration closer to target",
                "point_below_zero": True, "calibration_closer": True}
    return {"verdict": "DO_NOT_SHIP", "reason": "weeks 1-7 did not hold", "point_below_zero": point_ok,
            "calibration_closer": cov_ok}


# ---------------------------------------------------------------------------
# Gate B: blind historical replays
# ---------------------------------------------------------------------------

def blind_rows(replay: Mapping[str, Any]) -> tuple[list[dict[str, Any]], Counter]:
    rows, counts = [], Counter()
    for r in replay["rows"]:
        h17 = r["half_life"][str(SPREAD_HALF_LIFE)]
        base = r["base"]
        cand = split_range(base["mean"], h17["mean"], h17["p10"], h17["p90"])
        if cand is None:
            counts["m17_not_positive"] += 1
            cand = (base["p10"], base["p90"])
        rows.append({"season": r["season"], "week": r["week"], "game_id": r["game_id"], "position": r["position"],
                     "actual": r["actual"], "base_p10": base["p10"], "base_p90": base["p90"],
                     "cand_p10": cand[0], "cand_p90": cand[1]})
    return rows, counts


# ---------------------------------------------------------------------------
# Gate F: forward, 2026 saved builds
# ---------------------------------------------------------------------------

def refresh_2026_cache(cache: Path) -> None:
    for name in ("games.csv", "stats_player_week_2026.csv", "roster_weekly_2026.csv"):
        (cache / name).unlink(missing_ok=True)


def forward_rows(weeks: Sequence[int], cache: Path) -> tuple[list[dict[str, Any]], Counter, dict[str, str]]:
    from config import load_config
    from db.database import DatabaseManager
    from research.nfl_dfs_range_coverage_2026 import collect

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    saved = [r for r in collect(db, weeks)
             if r["pos"] in GATED and not r["dnp"] and r["proj"] >= MIN_PROJECTION
             and r["p10"] is not None and r["p90"] is not None]
    gsis_of = {int(r["id"]): r["gsis_id"] for r in db.execute(
        "SELECT id, gsis_id FROM ff_players WHERE id = ANY(%s)", ([r["pid"] for r in saved],))}
    sources = load_sources(2026, cache, FORWARD_HISTORY_BACK)
    by_pos: dict[str, list[HistoricalWeek]] = defaultdict(list)
    for row in sorted(sources.stat_rows, key=lambda r: r.chronological_key):
        by_pos[row.position].append(row)
    counts: Counter = Counter()
    out: list[dict[str, Any]] = []
    cache_key: dict[tuple[int, str], tuple[list, PeerIndex]] = {}
    for r in saved:
        week, pos = r["week"], r["pos"]
        gsis = gsis_of.get(int(r["pid"]))
        cand = None
        if not gsis or gsis not in sources.player_ids:
            counts["no_nflverse_identity"] += 1
        else:
            if (week, pos) not in cache_key:
                prior = [x for x in by_pos[pos] if before_cutoff(x, 2026, week)]
                cache_key[(week, pos)] = (prior, PeerIndex(pos, prior))
            prior, index = cache_key[(week, pos)]
            game = sources.games.get((2026, week, normalize_team(r["team"])))
            draws = projection_draws(
                player_id=sources.player_ids[gsis], player_gsis_id=gsis,
                player_name=sources.roster_names.get(gsis, gsis), position=pos, prior=prior,
                cutoff_season=2026, cutoff_week=week,
                context=ProjectionContext(team_implied_total=game.implied_total if game else None),
                half_lives=(SPREAD_HALF_LIFE,), peer_index=index,
            )
            if draws is None:
                counts["no_history"] += 1
            else:
                s = summarize_draws(draws[SPREAD_HALF_LIFE])
                cand = split_range(r["proj"], s["mean"], s["p10"], s["p90"])
                if cand is None:
                    counts["m17_not_positive"] += 1
        if cand is None:
            cand = (r["p10"], r["p90"])  # candidate falls back to the saved range
            counts["candidate_equals_baseline"] += 1
        out.append({"season": 2026, "week": week, "game": f"{week}:{r['game']}", "position": pos, "name": r["name"],
                    "actual": r["actual"], "proj": r["proj"], "anchored": r["anchored"],
                    "base_p10": r["p10"], "base_p90": r["p90"], "cand_p10": cand[0], "cand_p90": cand[1]})
    return out, counts, sources.digests


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _ci(x: Mapping[str, Any]) -> str:
    return f"{x['mean']:+.4f} [{x['lo']:+.4f}, {x['hi']:+.4f}]"


def _print_grade(label: str, g: Mapping[str, Any]) -> None:
    b, c = g["coverage"]["baseline"], g["coverage"]["candidate"]
    print(f"  {label}: n={g['n']} clusters={g['clusters']} baseline loss {g['baseline_range_loss']:.4f}  delta {_ci(g['range_loss_delta'])}"
          f"  week-clustered {_ci(g['range_loss_delta_week_clustered'])}")
    print(f"     below/above P10/P90: base {b['below_p10']:.3f}/{b['above_p90']:.3f}  cand {c['below_p10']:.3f}/{c['above_p90']:.3f}"
          f"  (p10 {g['p10_pinball_delta_point']:+.4f}, p90 {g['p90_pinball_delta_point']:+.4f})")
    for pos, v in g["by_position"].items():
        if v.get("n"):
            pb, pc = v["coverage"]["baseline"], v["coverage"]["candidate"]
            print(f"     {pos}: n={v['n']} delta {_ci(v['range_loss_delta'])}  base {pb['below_p10']:.3f}/{pb['above_p90']:.3f} cand {pc['below_p10']:.3f}/{pc['above_p90']:.3f}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("replay"); p.add_argument("--season", type=int, choices=BLIND_SEASONS, required=True)
    p.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    sub.add_parser("blind")
    f = sub.add_parser("forward"); f.add_argument("--weeks", default="8-15"); f.add_argument("--smoke", action="store_true")
    f.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    q = sub.add_parser("quick"); q.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    args = parser.parse_args(argv)
    if not model_file_matches_pin():
        raise SystemExit(f"model/nfl_dfs_historical.py is {model_file_sha256()}, not the pinned {MODEL_FILE_SHA256}")
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    if args.cmd == "replay":
        art = replay_season(args.season, cache=args.cache, half_lives=(CENTRE_HALF_LIFE, SPREAD_HALF_LIFE),
                            log=lambda m: print(m, flush=True))
        art["study_id"] = STUDY_ID
        path = ARTIFACT_DIR / f"replay-{args.season}.json"
        path.write_text(json.dumps(art, default=str), encoding="utf-8")
        print(f"{len(art['rows'])} rows, excluded {art['excluded']} -> {path}")
        return 0

    if args.cmd == "blind":
        out_path = ARTIFACT_DIR / "blind-verdict.json"
        if out_path.exists():
            raise SystemExit("Gate B verdict already written; it is computed once")
        per, counts = {}, {}
        for s in BLIND_SEASONS:
            rows, c = blind_rows(json.loads((ARTIFACT_DIR / f"replay-{s}.json").read_text(encoding="utf-8")))
            per[s] = grade_rows(rows, "game_id"); counts[s] = dict(c)
        res = {"study_id": STUDY_ID, "gate": "B", "decided_at": datetime.now(timezone.utc).isoformat(),
               "per_season": per, "fallback_counts": counts, **blind_verdict(per)}
        out_path.write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
        print(f"Gate B: {res['verdict']} -- {res['reason']}")
        for s, g in per.items():
            _print_grade(str(s), g)
        print("  fallbacks:", counts)
        return 0

    if args.cmd == "quick":
        out_path = ARTIFACT_DIR / "quick-verdict.json"
        if out_path.exists():
            raise SystemExit("Gate Q verdict already written; it is computed once")
        refresh_2026_cache(args.cache)
        rows, counts, digests = forward_rows(QUICK_WEEKS, args.cache)
        g = grade_rows(rows, "game")
        res = {"study_id": STUDY_ID, "gate": "Q", "decided_at": datetime.now(timezone.utc).isoformat(),
               "weeks": list(QUICK_WEEKS), "counts": dict(counts), "source_digests": digests, "grade": g,
               **quick_verdict(g), "rows": rows}
        out_path.write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
        print(f"Gate Q: {res['verdict']} -- {res['reason']} ({len(rows)} rows, counts {dict(counts)})")
        _print_grade("2026 w1-7", g)
        return 0

    lo, hi = (int(x) for x in args.weeks.split("-"))
    weeks = tuple(range(lo, hi + 1))
    if not args.smoke:
        if weeks not in (FORWARD_WEEKS, FORWARD_FALLBACK_WEEKS):
            raise SystemExit("the forward gate is graded on weeks 8-15 (or 8-18 if the floor is not met)")
        if (ARTIFACT_DIR / "forward-verdict.json").exists():
            raise SystemExit("Gate F verdict already written; it is computed once")
    refresh_2026_cache(args.cache)
    rows, counts, digests = forward_rows(weeks, args.cache)
    weeks_with_rows = len({r["week"] for r in rows})
    print(f"forward rows {len(rows)} over {weeks_with_rows} weeks; counts {dict(counts)}; anchored {sum(r['anchored'] for r in rows)}")
    if args.smoke:
        (ARTIFACT_DIR / "forward-smoke-counts.json").write_text(json.dumps({"weeks": list(weeks), "rows": len(rows), "counts": dict(counts)}, indent=1), encoding="utf-8")
        print("smoke run: plumbing only, no losses computed")
        return 0
    blind = json.loads((ARTIFACT_DIR / "blind-verdict.json").read_text(encoding="utf-8"))["verdict"]
    g = grade_rows(rows, "game")
    res = {"study_id": STUDY_ID, "gate": "F", "decided_at": datetime.now(timezone.utc).isoformat(), "weeks": list(weeks),
           "counts": dict(counts), "source_digests": digests, "grade": g,
           **forward_verdict(blind, g, weeks_with_rows, weeks == FORWARD_FALLBACK_WEEKS), "rows": rows}
    if res["verdict"] != "WAIT":
        (ARTIFACT_DIR / "forward-verdict.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
    print(f"Gate F: {res['verdict']} -- {res['reason']}")
    _print_grade("2026", g)
    return 0


if __name__ == "__main__":
    sys.exit(main())
