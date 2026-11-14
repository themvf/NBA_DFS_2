"""Persist auditable weekly production/shadow report cards, without reforecasting."""
import argparse
import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone

from psycopg2.extras import Json
from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase, target_season
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_reportcard import GRACE_HOURS, VERSION, build_report
from model.nfl_dfs_context_variant_study import context_forecasts

# Files hashed into every card (`implementation`), and every module that
# shapes one. A change to any of them is a change of inputs.
IMPLEMENTATION_FILES = ("model/nfl_dfs_reportcard.py", "ingest/nfl_dfs_reportcard.py")
BUILD_FILES = (*IMPLEMENTATION_FILES, "model/nfl_dfs_context_variant_study.py")
EVIDENCE_VERSION = "nfl-dfs-report-card-evidence-v1"
# A settled week is rebuilt at least this often even when nothing it reads has
# changed. If that rebuild gives a different card, the evidence digest missed
# an input, and the job fails instead of serving a stale card indefinitely.
RECHECK_AFTER = timedelta(days=7)

# Only the forecast `build_report` keeps: the last capture before the player's
# kickoff (ties broken by forecast id, as there). Loading every run killed the
# runner from 2026-09-30: the 15-minute pre-kickoff cadence put 88 runs
# (323 MB) in week 3 and 52 runs (1.5 GB) in week 4 by Wednesday. Rows this
# drops are counted, so `rejected_non_pregame_snapshots` and the report digest
# are unchanged; superseded pregame rows were never counted.
PRODUCTION_SQL = """WITH g AS (SELECT g.id game_id,g.kickoff,h.abbreviation home_team,a.abbreviation away_team
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%(season)s AND g.week=%(week)s AND g.game_type='REG'),
    f AS (SELECT p.id,p.player_id,p.team,GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
        FROM nfl_dfs_player_projections p JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id
        WHERE r.season=%(season)s AND r.week=%(week)s AND p.player_id IS NOT NULL
          AND p.position IN ('QB','RB','WR','TE','DST')),
    m AS (SELECT f.*,g.game_id,g.kickoff FROM f LEFT JOIN LATERAL (
        SELECT * FROM g WHERE f.team IN (g.home_team,g.away_team) ORDER BY g.kickoff,g.game_id LIMIT 1) g ON TRUE)"""
REJECTED_SQL = """SELECT count(*) FROM m WHERE game_id IS NULL OR captured_at IS NULL OR kickoff IS NULL
        OR captured_at >= kickoff OR captured_at > %(now)s"""
PICK_SQL = """pick AS (SELECT DISTINCT ON (player_id,game_id) id FROM m
        WHERE game_id IS NOT NULL AND captured_at < kickoff AND captured_at <= %(now)s
        ORDER BY player_id,game_id,captured_at DESC,id::text COLLATE "C" DESC)"""


def week_games(db, season, week):
    return db.execute("""SELECT g.id,g.kickoff,g.completed,h.abbreviation home_team,a.abbreviation away_team
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG'""", (season, week))


def roster(db, season):
    """The live roster. Read once a run: it is the same for every week."""
    return db.execute("""SELECT id player_id,gsis_id,canonical_name name,position,team_abbrev team
        FROM ff_players WHERE season=%s AND active AND position IN ('QB','RB','WR','TE','DST')""", (season,))


def identity_map(players):
    identities = {str(player["gsis_id"]): player["player_id"] for player in players if player.get("gsis_id")}
    identities.update({f"DST:{player['team']}": player["player_id"] for player in players if player["position"] == "DST"})
    return identities


def settled(games, now):
    """Every game final and past the grading grace window: the card no longer depends on the clock."""
    grace = timedelta(hours=GRACE_HOURS)
    return bool(games) and all(g["completed"] and g["kickoff"] is not None and g["kickoff"] + grace < now for g in games)


def implementation():
    return {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in IMPLEMENTATION_FILES}


def evidence(db, season, week, now, games, is_settled):
    """Name every stored row a card for this week is built from, without reading the rows.

    A week's card re-read ~65 MB of forecasts each run (656 MB a run over ten
    weeks, 2026-11-12) to rebuild a card that had not changed. This asks the
    database only which row versions exist: (id, xmin) names one, because xmin
    changes whenever a row is rewritten, so an edit in place is seen even on a
    table that keeps no content digest. What the build looks up in the live
    roster is recorded separately (`roster_lookups`).
    """
    row = db.execute(PRODUCTION_SQL + ", " + PICK_SQL + """
        SELECT (""" + REJECTED_SQL + """) rejected,
          (SELECT md5(coalesce(string_agg(concat_ws(':', p.id, p.xmin, r.run_id, r.xmin), ','
                    ORDER BY p.id::text COLLATE "C"), ''))
             FROM pick JOIN nfl_dfs_player_projections p ON p.id=pick.id
             JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id) production,
          (SELECT md5(coalesce(string_agg(concat_ws(':', s.id, s.xmin), ',' ORDER BY s.id), ''))
             FROM nfl_dfs_shadow_predictions s WHERE s.season=%(season)s AND s.week=%(week)s) shadows,
          (SELECT md5(coalesce(string_agg(concat_ws(':', e.run_digest, e.xmin, extract(epoch FROM e.as_of_at)), ','
                    ORDER BY e.run_digest), ''))
             FROM nfl_dfs_efficiency_runs e WHERE e.season=%(season)s AND e.week=%(week)s) efficiency,
          (SELECT md5(coalesce(string_agg(concat_ws(':', r.id, r.xmin, f.canonical_name), ',' ORDER BY r.id), ''))
             FROM nfl_dfs_player_week_results r LEFT JOIN ff_players f ON f.id=r.player_id
             WHERE r.season=%(season)s AND r.week=%(week)s) results""",
        {"season": season, "week": week, "now": now})[0]
    build = {p: hashlib.sha256(Path(p).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for p in BUILD_FILES}
    return artifact_digest({"version": EVIDENCE_VERSION, "report_version": VERSION, "season": season, "week": week,
                            "settled": is_settled, "implementation": implementation(), "build": build,
                            "games": sorted((dict(g) for g in games), key=lambda g: g["id"]), **dict(row)})


def roster_lookups(keys, identities, teams):
    """What the given roster answers for the lookups a build made."""
    return {"identities": {key: identities.get(key) for key in keys["identities"]},
            "team_fallback": {str(player): teams.get(int(player)) for player in keys["team_fallback"]}}


def inputs(db, season, week, now, games=None, players=None):
    """Everything `build_report` reads, and what the build asked the live roster.

    The second value holds the only two things a played week still takes from
    `ff_players`: which player an efficiency forecast's identity resolves to,
    and the current team of a player whose stored forecast carries no team
    (2026 week 1 only). Recording them lets a later run tell a roster change
    that matters to this week from the daily ones that do not.
    """
    games = games if games is not None else week_games(db, season, week)
    players = players if players is not None else roster(db, season)
    rejected = db.execute(PRODUCTION_SQL + " SELECT (" + REJECTED_SQL + ") n", {"season": season, "week": week, "now": now})
    production = db.execute(PRODUCTION_SQL + ", " + PICK_SQL + """
        SELECT p.*,r.model_version,r.model_config,r.seed,r.artifact_digest,
            GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
        FROM pick JOIN nfl_dfs_player_projections p ON p.id=pick.id
        JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id""", {"season": season, "week": week, "now": now})
    forecasts = [{"player_id": p["player_id"], "forecast_id": str(p["id"]), "variant": "production",
        "name": p["player_name"], "team": p["team"], "position": p["position"], "captured_at": p["captured_at"],
        "mean": p["model_proj_fpts"], "median": p["median_fpts"], "p10": p["floor_fpts"], "p90": p["ceiling_fpts"],
        "boom_probability": p["boom_rate"], "history_games": p["history_games"], "stat_means": p["stat_means"],
        "model_version": p["model_version"], "run_id": str(p["run_id"]), "input_digest": p["artifact_digest"],
        "source_evidence": p["source_evidence"], "config": p["model_config"], "seed": p["seed"],
        "feature_snapshot": p["feature_snapshot"]} for p in production if p["player_id"] is not None]
    # Ordered, so the same stored rows always build the same card: which
    # capture names a player in the card follows the order they are read in.
    shadows = db.execute("""SELECT p.*,f.team_abbrev current_team FROM nfl_dfs_shadow_predictions p
        JOIN ff_players f ON f.id=p.player_id WHERE p.season=%s AND p.week=%s ORDER BY p.id""", (season, week))
    fallback_teams = {}
    for s in shadows:
        p = s["payload"]
        if not p.get("team"):
            fallback_teams[s["player_id"]] = s["current_team"]
        # Legacy payloads lacked team; kickoff + current team is a qualified
        # fallback, never an arbitrary pairing after a roster move.
        matches = [g for g in games if g["kickoff"] == s["kickoff"] and s["current_team"] in (g["home_team"], g["away_team"])]
        team = p.get("team") or (s["current_team"] if len(matches) == 1 else None)
        base = {"player_id": s["player_id"], "forecast_id": str(s["id"]), "name": p["player_name"],
            "team": team, "position": p["position"], "captured_at": s["captured_at"],
            "history_games": p["history_games"], "run_id": s["study_run_id"], "input_digest": s["input_digest"],
            "source_evidence": {"history_digest": p["history_digest"], "study_digest": p["source_study_digest"],
                                "identity": "frozen_team" if p.get("team") else "legacy_current_team_plus_exact_kickoff"},
            "seed": p.get("seed"), "config": p.get("baseline_config"),
            "model_version": p.get("shadow_version", "shadow-v1")}
        forecasts.append({**base, "variant": "shadow_baseline", "mean": p["baseline"], "median": p.get("median"),
            "p10": p["p10"], "p90": p["p90"], "boom_probability": p["boom_probability"], "stat_means": p.get("stat_means", {})})
        forecasts.extend(context_forecasts(base, p))
        if p.get("candidate"):
            c = p["candidate"]
            forecasts.append({**base, "variant": "opportunity", "mean": c["prediction"], "median": c.get("median"),
                "p10": c["p10"], "p90": c["p90"], "boom_probability": c["boom_probability"],
                "stat_means": {}, "recipe_digest": c["recipe_digest"]})
    identities = identity_map(players)
    asked = set()
    efficiency_runs = db.execute("""SELECT run_digest,as_of_at,payload FROM nfl_dfs_efficiency_runs
        WHERE season=%s AND week=%s ORDER BY as_of_at,run_digest""", (season, week))
    for run in efficiency_runs:
        payload = run["payload"]
        for team_forecast in payload.get("forecasts", []):
            for projection in team_forecast.get("players", []):
                asked.add(str(projection.get("identity")))
                player_id = identities.get(str(projection.get("identity")))
                if player_id is None:
                    continue
                forecasts.append({
                    "player_id": player_id, "forecast_id": f"{run['run_digest']}:{projection.get('identity')}",
                    "variant": "efficiency_research", "name": projection["name"], "team": team_forecast["team"],
                    "position": projection["position"], "captured_at": run["as_of_at"],
                    "mean": projection["mean_fpts"], "median": projection["median_fpts"],
                    "p10": projection["p10_fpts"], "p90": projection["p90_fpts"],
                    "boom_probability": projection["boom_rate"], "history_games": projection["history_games"],
                    "stat_means": projection["stat_means"], "model_version": payload["version"],
                    "run_id": run["run_digest"], "input_digest": payload["dataset_digest"],
                    "source_evidence": {"workload_run_digest": payload["workload_run_digest"],
                        "coherence_scope": projection["coherence_scope"]}, "config": payload["config"],
                    "seed": projection["seed"],
                })
    # The player's name rides along so a result-only player (no forecast, off
    # the live roster since) can be listed under the game he actually played.
    results = db.execute("""SELECT r.*,f.canonical_name player_name FROM nfl_dfs_player_week_results r
        LEFT JOIN ff_players f ON f.id=r.player_id WHERE r.season=%s AND r.week=%s""", (season, week))
    lookups = roster_lookups({"identities": sorted(asked), "team_fallback": sorted(fallback_teams)}, identities, fallback_teams)
    return dict(games=games, players=players, forecasts=forecasts, results=results,
                prior_rejected=int(rejected[0]["n"])), lookups


def same_content(previous, report):
    """True when two reports differ only in when they were evaluated."""
    if previous is None:
        return False
    current = json.loads(json.dumps(report, default=str))
    return ({k: v for k, v in previous.items() if k != "evaluated_at"}
            == {k: v for k, v in current.items() if k != "evaluated_at"})


def content_digest(report):
    """Digest of a card without the time it was evaluated."""
    body = json.loads(json.dumps(report, default=str))
    return artifact_digest({k: v for k, v in body.items() if k != "evaluated_at"})


def persist(db, report):
    """Store the card unless the newest stored one says the same thing.

    The UI and review digest read one latest report per week. A report that
    differs from the newest stored one only in `evaluated_at` is not written:
    each is ~7 MB and the research job runs several times a day, so repeats
    cost 80-200 MB a day by 2026-09-30. The comparison uses the content digest
    kept in `nfl_dfs_report_card_builds`; reading the newest card back to
    compare it cost 18 MB a week on every run (2026-11-12). Returns the stored
    card's digest and this report's content digest.
    """
    digest, content = artifact_digest(report), content_digest(report)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT c.report_digest,
                    (SELECT b.content_digest FROM nfl_dfs_report_card_builds b
                      WHERE b.report_digest=c.report_digest LIMIT 1) content_digest
                FROM nfl_dfs_weekly_report_cards c WHERE c.season=%s AND c.week=%s
                ORDER BY c.created_at DESC, c.report_digest DESC LIMIT 1""", (report["season"], report["week"]))
            latest = cursor.fetchone()
            if latest:
                if latest["content_digest"] is not None:
                    same = latest["content_digest"] == content
                else:
                    # A card stored before content digests were kept: read it once.
                    cursor.execute("SELECT payload FROM nfl_dfs_weekly_report_cards WHERE report_digest=%s",
                                   (latest["report_digest"],))
                    same = same_content(cursor.fetchone()["payload"], report)
                if same:
                    return latest["report_digest"], content
            cursor.execute("""INSERT INTO nfl_dfs_weekly_report_cards(report_digest,season,week,payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (digest, report["season"], report["week"], Json(report, dumps=lambda x: json.dumps(x, default=str))))
    return digest, content


def remember(db, season, week, evidence_digest, lookups, content, report_digest):
    """Record which card these inputs produce. Returns the content they produced before, if any."""
    keys = {"identities": sorted(lookups["identities"]),
            "team_fallback": sorted(int(player) for player in lookups["team_fallback"])}
    roster_digest = artifact_digest(lookups)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT content_digest FROM nfl_dfs_report_card_builds
                WHERE season=%s AND week=%s AND evidence_digest=%s AND roster_digest=%s""",
                (season, week, evidence_digest, roster_digest))
            previous = cursor.fetchone()
            cursor.execute("""INSERT INTO nfl_dfs_report_card_builds
                    (season,week,evidence_digest,roster_digest,roster_keys,content_digest,report_digest)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (season,week,evidence_digest,roster_digest) DO UPDATE
                SET content_digest=EXCLUDED.content_digest, report_digest=EXCLUDED.report_digest, built_at=NOW()""",
                (season, week, evidence_digest, roster_digest, Json(keys), content, report_digest))
    return previous["content_digest"] if previous else None


def unchanged(db, season, week, evidence_digest, identities, now):
    """The newest card's digest when a rebuild would reproduce it, else None.

    True only when a recorded build read the same stored rows, the live roster
    still answers that build's lookups the same way, the card it produced is
    the newest one, and the build is younger than RECHECK_AFTER.
    """
    builds = db.execute("""SELECT roster_keys,roster_digest,report_digest,built_at FROM nfl_dfs_report_card_builds
        WHERE season=%s AND week=%s AND evidence_digest=%s""", (season, week, evidence_digest))
    if not builds:
        return None
    latest = db.execute("""SELECT report_digest FROM nfl_dfs_weekly_report_cards WHERE season=%s AND week=%s
        ORDER BY created_at DESC, report_digest DESC LIMIT 1""", (season, week))
    for build in builds:
        if not latest or build["report_digest"] != latest[0]["report_digest"] or build["built_at"] + RECHECK_AFTER <= now:
            continue
        keys = build["roster_keys"]
        players = "{" + ",".join(str(int(player)) for player in keys["team_fallback"]) + "}"  # one array literal
        teams = {row["id"]: row["team_abbrev"] for row in db.execute(
            "SELECT id,team_abbrev FROM ff_players WHERE id = ANY(%s::bigint[])", (players,))} if players != "{}" else {}
        if artifact_digest(roster_lookups(keys, identities, teams)) == build["roster_digest"]:
            return build["report_digest"]
    return None


def dated_after(data, now):
    """A row the build read but did not use yet: the same rows give a different card later."""
    return (any(r["computed_at"] > now for r in data["results"])
            or any(f["captured_at"] is not None and f["captured_at"] > now for f in data["forecasts"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--dry-run", action="store_true", help="read existing ledgers without schema changes or persistence")
    parser.add_argument("--output", type=Path, help="save the report locally (requires --week)")
    parser.add_argument("--rebuild", action="store_true", help="rebuild settled weeks even when their inputs are unchanged")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    if args.output and not args.week:
        parser.error("--output requires --week")
    db = PipelineDatabase(load_config().database_url, initialize_schema=not args.dry_run)
    weeks = [args.week] if args.week else [r["week"] for r in db.execute("""SELECT DISTINCT week FROM (
        SELECT week FROM nfl_dfs_projection_runs WHERE season=%s
        UNION SELECT week FROM nfl_dfs_shadow_predictions WHERE season=%s
        UNION SELECT week FROM nfl_season_games WHERE season=%s AND completed
        ) weeks WHERE week IS NOT NULL ORDER BY week""", (season, season, season))]
    players = roster(db, season)
    identities = identity_map(players)
    kept, contradicted = [], []
    for week in weeks:
        games = week_games(db, season, week)
        is_settled = settled(games, now)
        # A dry run neither reads nor writes the build record: it always builds.
        state = None if args.dry_run else evidence(db, season, week, now, games, is_settled)
        if state and is_settled and not args.rebuild and not args.output:
            digest = unchanged(db, season, week, state, identities, now)
            if digest:
                kept.append(week)
                print(json.dumps({"season": season, "week": week, "digest": digest, "unchanged": True}))
                continue
        data, lookups = inputs(db, season, week, now, games=games, players=players)
        report = build_report(season=season, week=week, now=now, **data)
        report["implementation"] = implementation()
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        if args.dry_run:
            digest = artifact_digest(report)
        else:
            digest, content = persist(db, report)
            # An unsettled week's record only saves `persist` re-reading the
            # card. A settled week's record licenses skipping the week, so it
            # is kept only when it is exact: nothing the build read is dated
            # after `now`, and the stored rows did not change while it ran.
            exact = not is_settled or (not dated_after(data, now) and
                                       evidence(db, season, week, now, week_games(db, season, week), is_settled) == state)
            if exact:
                before = remember(db, season, week, state, lookups, content, digest)
                if is_settled and before is not None and before != content:
                    contradicted.append(week)
        print(json.dumps({"season": season, "week": week, "digest": digest, "summary": report["summary"]}))
    if kept:
        print(f"::notice title=NFL report cards unchanged::weeks {kept} of {weeks}: stored inputs and roster lookups "
              "match the build behind the newest card, so they were not rebuilt")
    if not args.dry_run:
        # Trim right after writing rather than once a week from the post-week
        # job: every reader uses only the newest card per week, and between
        # weekly trims the table grew to ~430 MB (2026-11-11). A trimmed card
        # takes its build records with it.
        from ingest.nfl_dfs_storage import prune_report_cards
        print(json.dumps({"season": season, "report_card_retention": prune_report_cards(db, season, apply=True)}))
    if contradicted:
        raise SystemExit(f"NFL report card weeks {contradicted}: the same recorded inputs produced a different card. "
                         "The fresh cards are stored, but `evidence` is missing something the card depends on; "
                         "until that is found, run with --rebuild.")


if __name__ == "__main__":
    main()
