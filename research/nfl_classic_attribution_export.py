"""Export the frozen inputs of the Classic attribution study (read-only).

Companion to `research/nfl_showdown_attribution_export.py` (same eligibility
classes and DraftKings-points rules), for Classic main slates with a stored
contest. One file per slate:

* contest: entries, winning/median score, rank->score curve, verified top-100
  entries;
* pool: the frozen pregame run (`input_snapshot` of the largest optimizer run
  created before the slate's first kickoff, the latest on ties): the selected
  saved build; without an entry export it is not established that it was entered, with each player's pregame
  eligibility class and actual DraftKings base points (DraftKings' own contest
  numbers from every stored contest of the slate first, then the results
  table; no recorded score stays null);
* prior opportunity for the Winners-shape pool rules: the stored
  `priorOpportunity` when the snapshot has it (runs from 2026-10-10 on);
  otherwise RECONSTRUCTED with the workspace's own query (play-by-play weeks
  w-3..w-1, `labelled_at` no later than the run, team targets for the share)
  and flagged `priorOpportunitySource = "reconstructed"`. Where both exist the
  reconstruction is compared with the stored value, so the reconstruction's
  fidelity is measured, not assumed;
* the original saved portfolio.

    python -m research.nfl_classic_attribution_export --season 2026 --out artifacts/nfl-classic-attribution-2026-10-13
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_dk_pool_match import normalize_name
from research.nfl_showdown_attribution_export import _json, classify, contest_points, kickoff_for, results_points

TEAM = {"JAC": "JAX", "AZ": "ARI", "LA": "LAR", "WSH": "WAS", "OAK": "LV", "SD": "LAC", "STL": "LAR"}


def team(t):
    t = (t or "").strip().upper()
    return TEAM.get(t, t)


def prior_opportunity(db, season: int, week: int, as_of) -> tuple[dict, dict]:
    """(gsis -> {games, targets, carries}, team -> targets): the workspace's three-week window."""
    players, teams = {}, {}
    if week <= 1:
        return players, teams
    lo = max(1, week - 3)
    for r in db.execute(
            """WITH role_plays AS (
                 SELECT DISTINCT pp.player_id, pp.role, p.game_id, p.play_id, p.play_type, p.qb_dropback
                 FROM nfl_pbp_play_participants pp JOIN nfl_pbp_archetypes p ON p.game_id = pp.game_id AND p.play_id = pp.play_id
                 WHERE p.season = %s AND p.week >= %s AND p.week < %s AND p.season_type = 'REG' AND p.labelled_at <= %s
                   AND pp.player_id IS NOT NULL AND pp.role IN ('receiver', 'rusher'))
               SELECT player_id, COUNT(DISTINCT game_id)::int AS games,
                 COUNT(*) FILTER (WHERE role = 'receiver' AND play_type = 'pass')::int AS targets,
                 COUNT(*) FILTER (WHERE role = 'rusher' AND play_type = 'run' AND qb_dropback IS NOT TRUE)::int AS carries
               FROM role_plays GROUP BY player_id""", (season, lo, week, as_of)):
        players[str(r["player_id"])] = {"games": r["games"], "targets": r["targets"], "carries": r["carries"]}
    for r in db.execute(
            """WITH targets AS (
                 SELECT DISTINCT p.posteam, p.game_id, p.play_id FROM nfl_pbp_archetypes p
                 JOIN nfl_pbp_play_participants pp ON pp.game_id = p.game_id AND pp.play_id = p.play_id
                 WHERE p.season = %s AND p.week >= %s AND p.week < %s AND p.season_type = 'REG' AND p.labelled_at <= %s
                   AND p.play_type = 'pass' AND p.posteam IS NOT NULL AND pp.role = 'receiver')
               SELECT posteam, COUNT(*)::int AS targets FROM targets GROUP BY posteam""", (season, lo, week, as_of)):
        teams[team(r["posteam"])] = r["targets"]
    return players, teams


def export(db, season: int, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    contests = db.execute(
        """SELECT c.contest_id, c.contest_name, c.week, c.entry_count, c.winning_score, c.median_score, c.score_curve,
                  c.slate_upload_id, u.games, u.slate_signature,
                  (SELECT count(*) FROM nfl_dfs_field_top_entries t WHERE t.contest_id = c.contest_id) AS tops
           FROM nfl_dfs_field_contests c JOIN nfl_dfs_slate_uploads u ON u.upload_id = c.slate_upload_id
           WHERE c.format = 'classic' AND c.season = %s AND c.contest_id NOT LIKE 'dfsdb-%%'""", (season,))
    by_slate: dict[str, list[dict]] = {}
    for c in contests:
        by_slate.setdefault(c["slate_signature"], []).append(dict(c))
    gsis = {r["id"]: r["gsis_id"] for r in db.execute("SELECT id, gsis_id FROM ff_players WHERE season = %s AND gsis_id IS NOT NULL", (season,))}
    coverage = []
    for sig, cs in sorted(by_slate.items(), key=lambda kv: kv[1][0]["week"]):
        cs.sort(key=lambda c: (bool(_json(c["score_curve"])) and c["tops"] > 0, bool(_json(c["score_curve"])), c["tops"] > 0,
                               c["entry_count"] or 0), reverse=True)
        c = cs[0]
        week, games = c["week"], _json(c["games"]) or []
        kicks = [k for k in (kickoff_for(db, season, week, g) for g in games) if k]
        first = min(kicks) if kicks else None
        runs = db.execute(
            """SELECT o.run_id, o.created_at, o.generated_lineups, o.optimizer_version, o.projection_run_id, o.settings, o.input_snapshot
               FROM nfl_dfs_optimizer_runs o JOIN nfl_dfs_slate_uploads u ON u.upload_id = o.upload_id
               WHERE u.slate_signature = %s AND o.generated_lineups > 0 AND (%s::timestamptz IS NULL OR o.created_at < %s)
                 AND o.status IN ('complete','partial') AND NOT (o.settings ? 'researchStudy')
               ORDER BY o.generated_lineups DESC, o.created_at DESC LIMIT 1""", (sig, first, first))
        if not runs:
            coverage.append({"week": week, "games": len(games), "contest": c["contest_id"], "run": False})
            continue
        run = dict(runs[0])
        pool = list(_json(run["input_snapshot"]))
        # Early snapshots (week 1) carry no game key or opponent; take them from the same
        # upload's salary file (pregame), by DraftKings id, and flag each filled player.
        if any(not p.get("gameKey") for p in pool):
            file_rows = {int(r["dk_player_id"]): r for r in db.execute(
                """SELECT sp.dk_player_id, sp.game_key, sp.opponent, sp.team FROM nfl_dfs_slate_players sp
                   JOIN nfl_dfs_optimizer_runs o ON o.upload_id = sp.upload_id WHERE o.run_id = %s""", (run["run_id"],))}
            for p in pool:
                f = file_rows.get(int(p["dkPlayerId"]))
                if not p.get("gameKey") and f:
                    p.update({"gameKey": f["game_key"], "opponent": p.get("opponent") or f["opponent"], "gameKeyFromSalaryFile": True})
        el = (run["settings"].get("runEvidence") or {}).get("eligibility")
        eligibility = {e["dkPlayerId"]: e for e in el} if el else None
        mv = db.execute("SELECT model_version FROM nfl_dfs_projection_runs WHERE run_id = %s", (run["projection_run_id"],))
        cpts = contest_points(db, [x["contest_id"] for x in cs])
        rpts = results_points(db, season, week)
        # Name + team fallback for snapshots without player ids (week 1): the
        # results table's own player, by normalized name and team; DST by team.
        by_name, by_dst = {}, {}
        for r in db.execute(
                """SELECT DISTINCT ON (w.player_id) f.canonical_name, w.team, w.position, w.actual_dk_fpts
                   FROM nfl_dfs_player_week_results w JOIN ff_players f ON f.id = w.player_id
                   WHERE w.season = %s AND w.week = %s AND w.actual_dk_fpts IS NOT NULL
                   ORDER BY w.player_id, w.scoring_version DESC, w.computed_at DESC""", (season, week)):
            if r["position"] == "DST":
                by_dst[team(r["team"])] = float(r["actual_dk_fpts"])
            else:
                by_name.setdefault((normalize_name(r["canonical_name"]), team(r["team"])), float(r["actual_dk_fpts"]))
        prior_p, prior_t = prior_opportunity(db, season, week, run["created_at"])
        window = db.execute(
            """SELECT count(*) FILTER (WHERE labelled_at <= %s) AS gated, count(*) AS total FROM nfl_pbp_archetypes
               WHERE season = %s AND week >= %s AND week < %s AND season_type = 'REG'""",
            (run["created_at"], season, max(1, week - 3), week))[0] if week > 1 else {"gated": 0, "total": 0}
        missing, disagreements, prior_checks = [], [], []
        players = []
        for p in pool:
            key = normalize_name(p["name"])
            fc = cpts.get(key)
            fr = rpts.get(int(p["ffPlayerId"])) if p.get("ffPlayerId") is not None else None
            if fr is None:
                fr = by_dst.get(team(p.get("team"))) if p["position"] == "DST" else by_name.get((key, team(p.get("team"))))
            actual = fc if fc is not None else fr
            if fc is not None and fr is not None and abs(fc - fr) > 0.05:
                disagreements.append({"name": p["name"], "contest": fc, "results": fr})
            cls = classify(p, eligibility)
            if actual is None and cls != "availability":
                missing.append(p["name"])
            q = dict(p)
            q["eligibilityCode"] = (eligibility or {}).get(p["dkPlayerId"], {}).get("reasonCode") if eligibility else None
            q.update({"actual": actual, "actualSource": "contest" if fc is not None else ("results" if fr is not None else None),
                      "eligibilityClass": cls})
            if p["position"] in ("RB", "WR", "TE"):
                g = gsis.get(p.get("ffPlayerId"))
                rec = prior_p.get(g) if g else None
                tt = prior_t.get(team(p.get("team")))
                recon = None if rec is None else {**rec, "targetShare": (rec["targets"] / tt) if tt else None}
                stored = p.get("priorOpportunity")
                if stored is not None or "priorOpportunity" in p:
                    q["priorOpportunitySource"] = "stored"
                    if recon is not None and stored is not None:
                        prior_checks.append(all(abs((stored.get(k) or 0) - (recon.get(k) or 0)) < 1e-6 for k in ("games", "targets", "carries")))
                else:
                    q["priorOpportunity"] = recon
                    q["priorOpportunitySource"] = "reconstructed" if week > 1 else "none (week 1)"
            players.append(q)
        recorded = {}
        for wr in db.execute(
                """SELECT o.run_id, o.created_at, o.settings->'runEvidence'->'eligibility' AS el FROM nfl_dfs_optimizer_runs o
                   JOIN nfl_dfs_slate_uploads u ON u.upload_id = o.upload_id
                   WHERE u.slate_signature = %s AND o.settings->>'buildShape' = 'winners' AND o.created_at < %s
                     AND o.status IN ('complete','partial') AND NOT (o.settings ? 'researchStudy')
                   ORDER BY o.created_at DESC LIMIT 1""", (sig, first)):
            recorded = {"runId": str(wr["run_id"]), "createdAt": wr["created_at"].isoformat(),
                        "excluded": [e["dkPlayerId"] for e in (_json(wr["el"]) or []) if e.get("reasonCode") == "WINNERS_SHAPE"]}
        tops = [{"rank": r["rank"], "points": r["points"], "username": r["username"], "players": _json(r["players"])}
                for r in db.execute("SELECT rank, points, username, players FROM nfl_dfs_field_top_entries WHERE contest_id = %s ORDER BY rank",
                                    (c["contest_id"],))]
        portfolio = [[{"slot": s.get("slot"), "dkPlayerId": s.get("dkPlayerId"), "name": s.get("name")} for s in _json(l["slots"])]
                     for l in db.execute("SELECT lineup_number, slots FROM nfl_dfs_lineups WHERE run_id = %s ORDER BY lineup_number", (run["run_id"],))]
        record = {
            "slate": f"{len(games)}-game Classic", "games": games, "season": season, "week": week,
            "firstKickoff": first.isoformat() if first else None,
            "contest": {"id": c["contest_id"], "name": c["contest_name"], "entries": c["entry_count"], "winning": c["winning_score"],
                        "median": c["median_score"], "curve": _json(c["score_curve"]) or [], "topEntries": tops},
            "run": {"runId": str(run["run_id"]), "createdAt": run["created_at"].isoformat(), "lineups": run["generated_lineups"],
                    "optimizerVersion": run["optimizer_version"], "modelVersion": mv[0]["model_version"] if mv else None,
                    "settings": {k: v for k, v in run["settings"].items() if k != "runEvidence"}, "eligibilityReport": eligibility is not None},
            "players": players, "portfolio": portfolio, "recordedWinnersExclusions": recorded,
            "audit": {"pool": len(players), "missingActual": missing, "sourceDisagreements": disagreements,
                      "classes": {k: sum(1 for p in players if p["eligibilityClass"] == k) for k in sorted({p["eligibilityClass"] for p in players})},
                      "priorOpportunity": {s: sum(1 for p in players if p.get("priorOpportunitySource") == s)
                                           for s in sorted({p.get("priorOpportunitySource") for p in players if p.get("priorOpportunitySource")})},
                      "priorReconstructionMatchesStored": [sum(prior_checks), len(prior_checks)],
                      "priorWindowPlaysLabelledBeforeRun": [window["gated"], window["total"]]},
        }
        (out / f"{season}-w{week}-classic.json").write_text(json.dumps(record, default=str), encoding="utf-8")
        coverage.append({"week": week, "games": len(games), "contest": c["contest_id"], "entries": c["entry_count"],
                         "curve": bool(record["contest"]["curve"]), "topEntries": len(tops), "run": True,
                         "runLineups": run["generated_lineups"], "shape": run["settings"].get("buildShape"),
                         "modelVersion": record["run"]["modelVersion"], "optimizerVersion": run["optimizer_version"],
                         "eligibilityReport": eligibility is not None, "pool": len(players), "missingActual": len(missing),
                         "sourceDisagreements": len(disagreements), "prior": record["audit"]["priorOpportunity"],
                         "priorCheck": record["audit"]["priorReconstructionMatchesStored"]})
    (out / "coverage.json").write_text(json.dumps(coverage, indent=1, default=str), encoding="utf-8")
    return {"slates": coverage}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out", default="artifacts/nfl-classic-attribution-2026-10-13")
    a = ap.parse_args(argv)
    for s in export(DatabaseManager(load_config().database_url, initialize_schema=False), a.season, Path(a.out))["slates"]:
        print(s)


if __name__ == "__main__":
    main()
