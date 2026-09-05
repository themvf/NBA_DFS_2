"""Pure weekly grading. Forecasts, revised outcomes and missingness stay separate."""
from collections import Counter
from datetime import datetime, timezone
from math import isfinite

VERSION = "nfl-dfs-weekly-report-v1"
VARIANTS = ("production", "shadow_baseline", "opportunity", "efficiency_research")


def timestamp(value):
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Report timestamps must include a timezone")
    return parsed


def number(value):
    return float(value) if value is not None and isfinite(float(value)) else None


def grade(forecast, result, game, now, grace_hours=48):
    kickoff = timestamp(game.get("kickoff"))
    captured = timestamp(forecast.get("captured_at")) if forecast else None
    valid = bool(forecast and captured and kickoff and captured < kickoff and forecast.get("mean") is not None)
    actual = number(result.get("actual_dk_fpts")) if result else None
    if not game.get("completed") or not kickoff or kickoff >= now:
        actual = None
    if result and result.get("scoring_status") != "exact":
        actual = None
    if not valid:
        status = "forecast_unavailable"
    elif not game.get("completed") or kickoff >= now:
        status = "pending_game" if kickoff > now else "in_progress"
    elif not result:
        status = "awaiting_source"
    elif actual is None:
        status = "excluded_scoring"
    else:
        status = "corrected" if result.get("revision_count", 1) > 1 else "scored"
    error = actual - forecast["mean"] if valid and actual is not None else None
    lo, hi = (number(forecast.get(k)) if valid else None for k in ("p10", "p90"))
    interval_hit = lo <= actual <= hi if valid and actual is not None and lo is not None and hi is not None else None
    overdue = bool(game.get("completed") and kickoff and (now-kickoff).total_seconds() > grace_hours*3600 and actual is None)
    components = []
    evidence = (result or {}).get("scoring_evidence", {})
    actual_stats = evidence.get("scoring_input") or evidence.get("scoring_components") or {}
    for key, predicted in (forecast or {}).get("stat_means", {}).items():
        observed = number(actual_stats.get(key)) if actual is not None else None
        components.append({"stat": key, "projected": number(predicted), "actual": observed,
                           "error": observed-float(predicted) if observed is not None and predicted is not None else None})
    return {"status": status, "actual": actual, "error": error,
            "absolute_error": abs(error) if error is not None else None,
            "interval_hit": interval_hit, "overdue": overdue, "components": components,
            "valid_forecast": valid, "result_id": (result or {}).get("id"),
            "scoring_version": (result or {}).get("scoring_version"),
            "result_digest": (result or {}).get("input_digest"),
            "result_revision_count": (result or {}).get("revision_count", 0),
            "exclusion_reason": (result or {}).get("exclusion_reason")}


def summarize(rows):
    scored = [r for r in rows if r["error"] is not None]
    intervals = [r for r in scored if r["interval_hit"] is not None]
    return {"players": len(rows), "forecasted": sum(r["valid_forecast"] for r in rows),
            "scored": len(scored), "unscored": len(rows)-len(scored),
            "overdue": sum(r["overdue"] for r in rows), "statuses": dict(Counter(r["status"] for r in rows)),
            "mae": sum(r["absolute_error"] for r in scored)/len(scored) if scored else None,
            "bias_actual_minus_projected": sum(r["error"] for r in scored)/len(scored) if scored else None,
            "interval_coverage": sum(r["interval_hit"] for r in intervals)/len(intervals) if intervals else None,
            "interval_n": len(intervals)}


def build_report(*, season, week, games, players, forecasts, results, now):
    now = timestamp(now)
    by_game = {g["id"]: g for g in games}
    by_team = {team: g for g in games for team in (g["home_team"], g["away_team"])}
    selected = {}
    rejected = 0
    for f in forecasts:
        game = by_game.get(f.get("game_id")) or by_team.get(f.get("team"))
        captured, kickoff = timestamp(f.get("captured_at")), timestamp((game or {}).get("kickoff"))
        if not game or not captured or not kickoff or captured >= kickoff or captured > now:
            rejected += 1
            continue
        key = (f["player_id"], game["id"], f["variant"])
        previous = selected.get(key)
        if previous is None or (captured, str(f["forecast_id"])) > (timestamp(previous["captured_at"]), str(previous["forecast_id"])):
            selected[key] = f
    # Preserve forecasted players even if later roster refreshes mark them inactive.
    universe = {(p["player_id"], by_team[p["team"]]["id"]): p for p in players if p.get("team") in by_team}
    for (player_id, game_id, _), f in selected.items():
        universe[(player_id, game_id)] = {"player_id": player_id, "name": f["name"], "position": f["position"], "team": f["team"]}
    latest = {}
    revisions = Counter()
    for result in results:
        if timestamp(result["computed_at"]) > now:
            continue
        key = (result["player_id"], result["game_id"])
        revisions[key] += 1
        if key not in latest or (timestamp(result["computed_at"]), result["id"]) > (timestamp(latest[key]["computed_at"]), latest[key]["id"]):
            latest[key] = result
    rows = []
    for (player_id, game_id), player in sorted(universe.items()):
        game = by_game[game_id]
        result = latest.get((player_id, game_id))
        if result:
            result = {**result, "revision_count": revisions[(player_id, game_id)]}
        for variant in VARIANTS:
            f = selected.get((player_id, game_id, variant))
            rows.append({**player, "variant": variant, "game_id": game_id, "week": week, "season": season,
                         "opponent": game["away_team"] if player["team"] == game["home_team"] else game["home_team"],
                         "kickoff": game["kickoff"], "completed": game["completed"], "forecast": f,
                         **grade(f, result, game, now)})
    return {"version": VERSION, "season": season, "week": week,
            "evaluated_at": now.isoformat(), "scheduled_games": len(games),
            "completed_games": sum(g["completed"] for g in games),
            "rejected_non_pregame_snapshots": rejected,
            "checkpoint": "last accepted pregame forecast per player/game/model stream",
            "population": "canonical weekly roster plus preserved forecast identities; not a DK salary slate",
            "missing_policy": "No stat row is not evidence of DNP or zero. No verified-inactive feed is integrated.",
            "grace_hours_after_kickoff": 48,
            "summary": {v: summarize([r for r in rows if r["variant"] == v]) for v in VARIANTS},
            "rows": rows}
