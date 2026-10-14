"""Export the frozen inputs of the Showdown attribution study (read-only).

Study: where do our Showdown losses come from -- construction rules, the
selection objective, or ownership leverage? The analysis itself runs in
`web/scripts/research-nfl-showdown-attribution.ts` against the files written
here, so both solvers read exactly the same declared inputs.

One file per Showdown game with a stored contest (`nfl_dfs_field_contests`):

* contest: entries, winning/median score, the rank->score curve, and the
  verified top-100 ENTRIES (`nfl_dfs_field_top_entries`, ties kept whole);
* pool: the players of the frozen pregame run (`input_snapshot` of the last
  optimizer run created before kickoff), or, for a game we uploaded but never
  built, the uploaded salary file (`nfl_dfs_slate_players`);
* each player's pregame eligibility class from the run's own eligibility
  report: available, punt-only exclusion (relaxed in the punts-allowed
  variant), availability exclusion (never relaxed), or a rule/choice the study
  switches on and off itself (kicker ban, winners-shape pool rule, manual);
* actual DK points, DraftKings' own contest numbers first (base points, no
  captain multiplier), then `nfl_dfs_player_week_results`. A player with no
  recorded score is null, never 0, and is counted.
* the original saved portfolio (that run's lineups, with captains).

    python -m research.nfl_showdown_attribution_export --season 2026 --out artifacts/nfl-showdown-attribution-2026-10-13
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_dk_pool_match import normalize_name

PUNT_CODES = {"ABSOLUTE_SALARY_BLOCK", "ROLE_UNRESOLVED", "ROLE_UNKNOWN", "NO_PROJECTED_OPPORTUNITY"}
AVAILABILITY_CODES = {"INACTIVE", "DOUBTFUL", "KICKER_ROLE", "OUT"}
STUDY_SWITCHED = {"KICKER_EXCLUDED", "WINNERS_SHAPE", "MANUAL_EXCLUSION"}
ABSOLUTE_PUNT_SALARY = 1000  # the No punts absolute block, applied when a run kept no eligibility report


def _json(v):
    return v if isinstance(v, (list, dict)) else json.loads(v or "null")


def kickoff_for(db, season: int, week: int | None, game: str) -> datetime | None:
    away, home = game.split("@")
    alias = {"LAR": ("LAR", "LA"), "WAS": ("WAS", "WSH")}
    rows = db.execute(
        """SELECT g.kickoff, g.week, h.abbreviation home, a.abbreviation away FROM nfl_season_games g
           JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
           WHERE g.season = %s""", (season,))
    for r in rows:
        if r["home"] in alias.get(home, (home,)) and r["away"] in alias.get(away, (away,)) and (week is None or r["week"] == week):
            return r["kickoff"]
    return None


def contest_points(db, contest_ids: list[str]) -> dict[str, float]:
    """DraftKings' own base points, from every stored contest of the game.

    A DraftKings export lists every player anyone drafted, including a player
    who did not play (DraftKings scored him 0); the stat-api import lists only
    the players its fetched users used. Reading all contests of the game turns
    "no stat row" into DraftKings' recorded 0 where DraftKings recorded one.
    """
    pts = {}
    for r in db.execute("SELECT normalized_name, fpts FROM nfl_dfs_field_ownership WHERE contest_id = ANY(%s) AND fpts IS NOT NULL",
                        (contest_ids,)):
        pts.setdefault(r["normalized_name"], float(r["fpts"]))
    return pts


def results_points(db, season: int, week: int) -> dict[int, float]:
    out = {}
    for r in db.execute(
            """SELECT DISTINCT ON (player_id) player_id, actual_dk_fpts FROM nfl_dfs_player_week_results
               WHERE season = %s AND week = %s AND actual_dk_fpts IS NOT NULL
               ORDER BY player_id, scoring_version DESC, computed_at DESC""", (season, week)):
        out[int(r["player_id"])] = float(r["actual_dk_fpts"])
    return out


def classify(player: dict, eligibility: dict | None) -> str:
    if player.get("isOut"):
        return "availability"
    if eligibility is None:  # older run without an eligibility report
        return "punt" if (player.get("salary") or 0) <= ABSOLUTE_PUNT_SALARY else "available"
    e = eligibility.get(player["dkPlayerId"])
    if not e or e.get("eligible"):
        return "available"
    code = e.get("reasonCode")
    if code in PUNT_CODES:
        return "punt"
    if code in STUDY_SWITCHED:
        return "available"
    if code in AVAILABILITY_CODES:
        return "availability"
    return f"other:{code}"


def export(db, season: int, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    contests = db.execute(
        """SELECT c.contest_id, c.contest_name, c.week, c.entry_count, c.winning_score, c.median_score, c.score_curve,
                  c.slate_upload_id, u.games, u.slate_signature,
                  (SELECT count(*) FROM nfl_dfs_field_top_entries t WHERE t.contest_id = c.contest_id) AS tops
           FROM nfl_dfs_field_contests c JOIN nfl_dfs_slate_uploads u ON u.upload_id = c.slate_upload_id
           WHERE c.format = 'showdown' AND c.season = %s""", (season,))
    by_game: dict[str, list[dict]] = {}
    for c in contests:
        games = _json(c["games"]) or []
        if len(games) == 1:
            by_game.setdefault(games[0], []).append(dict(c))
    coverage = []
    for game, cs in sorted(by_game.items(), key=lambda kv: kv[1][0]["week"]):
        # One contest per game: score curve and top entries > curve > top entries; then the biggest field.
        cs.sort(key=lambda c: (bool(_json(c["score_curve"])) and c["tops"] > 0, bool(_json(c["score_curve"])), c["tops"] > 0,
                               c["entry_count"] or 0), reverse=True)
        c = cs[0]
        week = c["week"]
        kickoff = kickoff_for(db, season, week, game)
        runs = db.execute(
            """SELECT o.run_id, o.created_at, o.generated_lineups, o.optimizer_version, o.projection_run_id, o.settings, o.input_snapshot
               FROM nfl_dfs_optimizer_runs o JOIN nfl_dfs_slate_uploads u ON u.upload_id = o.upload_id
               WHERE u.slate_signature = %s AND o.generated_lineups > 0 AND (%s::timestamptz IS NULL OR o.created_at < %s)
                 AND o.status IN ('complete','partial') AND NOT (o.settings ? 'researchStudy')
               ORDER BY o.created_at DESC LIMIT 1""", (c["slate_signature"], kickoff, kickoff))
        run = dict(runs[0]) if runs else None
        if run:
            pool = list(_json(run["input_snapshot"]))
            el = (run["settings"].get("runEvidence") or {}).get("eligibility")
            eligibility = {e["dkPlayerId"]: e for e in el} if el else None
            model_version = db.execute("SELECT model_version FROM nfl_dfs_projection_runs WHERE run_id = %s",
                                       (run["projection_run_id"],))
            model_version = model_version[0]["model_version"] if model_version else None
        else:
            pool = [{"dkPlayerId": int(r["dk_player_id"]), "captainDkPlayerId": r["captain_dk_player_id"], "name": r["name"],
                     "position": r["position"], "team": r["team"], "opponent": r["opponent"], "gameKey": r["game_key"],
                     "salary": r["salary"], "captainSalary": r["captain_salary"], "isOut": bool(r["is_out"]),
                     "ffPlayerId": r["ff_player_id"]}
                    for r in db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id = %s", (c["slate_upload_id"],))]
            eligibility, model_version = None, None
        cpts = contest_points(db, [x["contest_id"] for x in cs])
        rpts = results_points(db, season, week)
        disagreements, missing = [], []
        players = []
        for p in pool:
            key = normalize_name(p["name"])
            from_contest = cpts.get(key)
            from_results = rpts.get(int(p["ffPlayerId"])) if p.get("ffPlayerId") is not None else None
            actual = from_contest if from_contest is not None else from_results
            source = "contest" if from_contest is not None else ("results" if from_results is not None else None)
            if from_contest is not None and from_results is not None and abs(from_contest - from_results) > 0.05:
                disagreements.append({"name": p["name"], "contest": from_contest, "results": from_results})
            cls = classify(p, eligibility)
            if actual is None and cls != "availability":
                missing.append(p["name"])
            q = dict(p)
            q.update({"actual": actual, "actualSource": source, "eligibilityClass": cls})
            players.append(q)
        tops = [{"rank": r["rank"], "points": r["points"], "username": r["username"], "players": _json(r["players"])}
                for r in db.execute("SELECT rank, points, username, players FROM nfl_dfs_field_top_entries WHERE contest_id = %s ORDER BY rank",
                                    (c["contest_id"],))]
        portfolio = []
        if run:
            for l in db.execute("SELECT lineup_number, slots FROM nfl_dfs_lineups WHERE run_id = %s ORDER BY lineup_number", (run["run_id"],)):
                portfolio.append([{"slot": s.get("slot"), "dkPlayerId": s.get("dkPlayerId"), "name": s.get("name")} for s in _json(l["slots"])])
        record = {
            "game": game, "season": season, "week": week, "kickoff": kickoff.isoformat() if kickoff else None,
            "contest": {"id": c["contest_id"], "name": c["contest_name"], "entries": c["entry_count"],
                        "winning": c["winning_score"], "median": c["median_score"], "curve": _json(c["score_curve"]) or [],
                        "topEntries": tops},
            "run": None if not run else {"runId": str(run["run_id"]), "createdAt": run["created_at"].isoformat(),
                                         "lineups": run["generated_lineups"], "optimizerVersion": run["optimizer_version"],
                                         "modelVersion": model_version, "settings": {k: v for k, v in run["settings"].items() if k != "runEvidence"},
                                         "eligibilityReport": eligibility is not None},
            "players": players, "portfolio": portfolio,
            "audit": {"pool": len(players), "missingActual": missing, "sourceDisagreements": disagreements,
                      "classes": {k: sum(1 for p in players if p["eligibilityClass"] == k) for k in sorted({p["eligibilityClass"] for p in players})}},
        }
        (out / f"{season}-w{week}-{game.replace('@', '_at_')}.json").write_text(json.dumps(record, default=str), encoding="utf-8")
        coverage.append({"game": game, "week": week, "contest": c["contest_id"], "entries": c["entry_count"],
                         "curve": bool(record["contest"]["curve"]), "topEntries": len(tops), "run": bool(run),
                         "runLineups": run["generated_lineups"] if run else None, "modelVersion": model_version,
                         "optimizerVersion": run["optimizer_version"] if run else None,
                         "eligibilityReport": eligibility is not None if run else None,
                         "pool": len(players), "missingActual": len(missing), "sourceDisagreements": len(disagreements)})
    (out / "coverage.json").write_text(json.dumps(coverage, indent=1, default=str), encoding="utf-8")
    return {"games": coverage}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out", default="artifacts/nfl-showdown-attribution-2026-10-13")
    a = ap.parse_args(argv)
    res = export(DatabaseManager(load_config().database_url, initialize_schema=False), a.season, Path(a.out))
    for g in res["games"]:
        print(g)


if __name__ == "__main__":
    main()
