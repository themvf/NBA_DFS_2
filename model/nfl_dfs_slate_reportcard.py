"""Slate-scoped forward accuracy under DraftKings' own convention.

The weekly report card (`model/nfl_dfs_reportcard.py`) grades the canonical
roster and, correctly for its purpose, refuses to treat "no stat row" as
evidence of zero. The cost is that it never scores the population the model
is most wrong about: 689 of 1,078 week-1 forecasts went unscored, and every
one of the backups projected off the starter-derived position prior sat in
that unscored pile.

This stream asks a narrower, fully answerable question instead:

    For the players DraftKings actually listed on a completed slate, how far
    were our projections from what DraftKings paid?

DraftKings' rule is simple: a listed player whose game is final and who has
no stat line scored 0. So here `actual = 0` when a completed, results-bearing
game has no row for the player. That is not a claim the player was inactive;
it is what the slate paid, which is what a projection on that slate was for.

Guards, so a lag in the results feed cannot masquerade as a slate of zeros:
- a game is scorable only when it is completed AND at least one exact result
  exists for it (the source has run for that game);
- players the slate itself marked OUT are reported as their own cohort and
  excluded from the accuracy cohorts (their projection was zeroed by policy);
- every row carries its projection_status / history bucket so the hist-0
  population can be read on its own.

Pure: no database access. `pooled_summary` gives weeks-clustered bootstrap
intervals across several slates for the v4-prior study (WP1b).
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from datetime import datetime
from math import isfinite

VERSION = "nfl-dfs-slate-report-v1"
POSITIONS = ("QB", "RB", "WR", "TE", "DST")
COHORTS = ("hist_0", "hist_1_5", "hist_6_plus", "out")


def _ts(value):
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Report timestamps must include a timezone")
    return parsed


def _num(value):
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if isfinite(f) else None


def cohort_for(player: dict) -> str:
    if player.get("is_out") or player.get("projection_status") == "out":
        return "out"
    if player.get("projection_status") == "position_prior":
        return "hist_0"
    games = int(player.get("history_games") or 0)
    if games == 0:
        return "hist_0"
    return "hist_1_5" if games <= 5 else "hist_6_plus"


def build_slate_report(*, upload: dict, players: list[dict], games: list[dict],
                       results: list[dict], now, forecasts: dict | None = None) -> dict:
    """Grade one completed slate.

    upload   : {upload_id, slate_signature, format, season, week, model_version,
                projection_run_id, created_at}
    players  : slate rows: {ff_player_id, name, position, team, game_key,
                projection_status, history_games, is_out, our_proj,
                floor_fpts, ceiling_fpts}
    games    : {id, game_key ('AWAY@HOME'), kickoff, completed}
    results  : {player_id, game_id, actual_dk_fpts, scoring_status, computed_at}
    forecasts: optional {ff_player_id: {mean, p10, p90}} overriding the slate's
               own projection, so an alternative model stream can be graded on
               the identical population (the v3-vs-v4 pairing in WP1b).
    """
    now = _ts(now)
    by_key = {g["game_key"]: g for g in games}
    exact_by_game: dict[int, dict[int, dict]] = defaultdict(dict)
    for r in results:
        if r.get("scoring_status") != "exact" or _num(r.get("actual_dk_fpts")) is None:
            continue
        if r.get("computed_at") is not None and _ts(r["computed_at"]) > now:
            continue
        prev = exact_by_game[r["game_id"]].get(r["player_id"])
        if prev is None or (_ts(r.get("computed_at")) or now, r.get("id", 0)) > (_ts(prev.get("computed_at")) or now, prev.get("id", 0)):
            exact_by_game[r["game_id"]][r["player_id"]] = r
    # Only the slate's own games count as scorable; results for other games
    # that week say nothing about whether THIS slate's source has run.
    slate_keys = {p.get("game_key") for p in players}
    slate_game_ids = {by_key[k]["id"] for k in slate_keys if k in by_key}
    scorable = {gid for gid, rows in exact_by_game.items() if rows and gid in slate_game_ids}

    rows = []
    for p in players:
        game = by_key.get(p.get("game_key"))
        kickoff = _ts(game.get("kickoff")) if game else None
        if forecasts is None:
            mean, p10, p90 = _num(p.get("our_proj")), _num(p.get("floor_fpts")), _num(p.get("ceiling_fpts"))
        else:
            # An alternative stream is graded ONLY where it projected; a player
            # it skipped must not silently inherit the slate's own number.
            proj_src = forecasts.get(p.get("ff_player_id")) or {}
            mean, p10, p90 = _num(proj_src.get("mean")), _num(proj_src.get("p10")), _num(proj_src.get("p90"))
        cohort = cohort_for(p)
        if game is None:
            status = "game_not_on_schedule"
        elif p.get("ff_player_id") is None:
            status = "unlinked_identity"
        elif mean is None:
            status = "no_projection"
        elif not game.get("completed") or kickoff is None or kickoff >= now:
            status = "pending_game"
        elif game["id"] not in scorable:
            status = "awaiting_source"
        else:
            status = "scored"
        actual = None
        if status == "scored":
            hit = exact_by_game[game["id"]].get(p["ff_player_id"])
            actual = _num(hit["actual_dk_fpts"]) if hit else 0.0
        error = actual - mean if actual is not None else None
        interval_hit = (p10 <= actual <= p90) if actual is not None and p10 is not None and p90 is not None else None
        rows.append({
            "ff_player_id": p.get("ff_player_id"), "name": p.get("name"), "position": p.get("position"),
            "team": p.get("team"), "game_key": p.get("game_key"), "cohort": cohort,
            "projection_status": p.get("projection_status"), "history_games": p.get("history_games"),
            "projected": mean, "p10": p10, "p90": p90, "actual": actual,
            "stat_row_present": bool(actual is not None and status == "scored"
                                     and p["ff_player_id"] in exact_by_game[game["id"]]),
            "error": error, "absolute_error": abs(error) if error is not None else None,
            "interval_hit": interval_hit, "status": status,
        })
    summary = {pos: {c: summarize([r for r in rows if r["position"] == pos and r["cohort"] == c])
                     for c in COHORTS} for pos in POSITIONS}
    summary["all"] = {c: summarize([r for r in rows if r["cohort"] == c]) for c in COHORTS}
    return {
        "version": VERSION, "upload_id": upload["upload_id"], "slate_signature": upload.get("slate_signature"),
        "format": upload.get("format"), "season": upload.get("season"), "week": upload.get("week"),
        "model_version": upload.get("model_version"), "projection_run_id": upload.get("projection_run_id"),
        "forecast_stream": "alternative" if forecasts is not None else "slate_production",
        "evaluated_at": now.isoformat(),
        "population": "players DraftKings listed on this slate; actual = 0 when a completed, results-bearing game has no stat line (DK convention)",
        "scorable_games": len(scorable), "slate_games": len(by_key),
        "statuses": dict(Counter(r["status"] for r in rows)),
        "summary": summary, "rows": rows,
    }


def summarize(rows: list[dict]) -> dict:
    scored = [r for r in rows if r["error"] is not None]
    intervals = [r for r in scored if r["interval_hit"] is not None]
    return {
        "players": len(rows), "scored": len(scored),
        "no_stat_row": sum(1 for r in scored if not r["stat_row_present"]),
        "mae": sum(r["absolute_error"] for r in scored) / len(scored) if scored else None,
        "bias_actual_minus_projected": sum(r["error"] for r in scored) / len(scored) if scored else None,
        "mean_projected": sum(r["projected"] for r in scored) / len(scored) if scored else None,
        "mean_actual": sum(r["actual"] for r in scored) / len(scored) if scored else None,
        "interval_coverage": sum(r["interval_hit"] for r in intervals) / len(intervals) if intervals else None,
        "interval_n": len(intervals),
    }


def latest_per_slate(reports: list[dict]) -> list[dict]:
    """One report per (season, week, format, slate_signature): the latest
    evaluated upload. Re-uploads of the same slate are not new evidence."""
    chosen: dict[tuple, dict] = {}
    for rep in reports:
        key = (rep["season"], rep["week"], rep["format"], rep["slate_signature"])
        prev = chosen.get(key)
        if prev is None or str(rep.get("upload_created_at") or "") > str(prev.get("upload_created_at") or ""):
            chosen[key] = rep
    return list(chosen.values())


def pooled_summary(reports: list[dict], *, positions=POSITIONS, cohorts=COHORTS,
                   iters: int = 2000, seed: int = 20260922) -> dict:
    """Weeks-clustered bootstrap of MAE and bias per position x cohort.

    Games in one week share a slate, weather and news cycle, so the week is
    the independent unit. Never resample rows.
    """
    reports = latest_per_slate(reports)
    by_week: dict[int, list[dict]] = defaultdict(list)
    for rep in reports:
        by_week[rep["week"]].extend(r for r in rep["rows"] if r["error"] is not None)
    weeks = sorted(by_week)
    rng = random.Random(seed)
    out: dict = {"weeks": weeks, "iters": iters, "cells": {}}
    for pos in list(positions) + ["all"]:
        for cohort in cohorts:
            def cell_rows(wk):
                return [r for r in by_week[wk] if (pos == "all" or r["position"] == pos) and r["cohort"] == cohort]
            base = [r for wk in weeks for r in cell_rows(wk)]
            if not base:
                out["cells"][f"{pos}:{cohort}"] = {"n": 0}
                continue
            def stats(rows):
                return (sum(r["absolute_error"] for r in rows) / len(rows),
                        sum(r["error"] for r in rows) / len(rows))
            mae, bias = stats(base)
            maes, biases = [], []
            if len(weeks) >= 2:
                for _ in range(iters):
                    sample = [r for wk in (rng.choice(weeks) for _ in weeks) for r in cell_rows(wk)]
                    if sample:
                        m, b = stats(sample)
                        maes.append(m); biases.append(b)
            def ci(values):
                if len(values) < 100:
                    return None
                values = sorted(values)
                return [values[int(0.025 * len(values))], values[int(0.975 * len(values)) - 1]]
            out["cells"][f"{pos}:{cohort}"] = {
                "n": len(base), "weeks": len({wk for wk in weeks if cell_rows(wk)}),
                "mae": mae, "mae_ci": ci(maes), "bias": bias, "bias_ci": ci(biases),
            }
    return out
