"""Capture/build paired pool-rule research arms; default is a database-read-only dry run.

python -m research.nfl_pool_rule_study --study classic --source-run UUID --main-slate --out PATH [--save]
python -m research.nfl_pool_rule_study --study showdown --source-run UUID --flagship-showdown --out PATH [--save]

Two pre-registered studies share this runner:
  classic   nfl-classic-te-wr-pool-v1   docs/nfl-classic-te-wr-pool-rule-study.md
  showdown  nfl-showdown-kicker-rule-v1 docs/nfl-showdown-kicker-rule-study.md

Explicit source selection and population attestation avoid guessing which build
was entered. Saves only new research_* optimizer runs, atomically, before lock,
and only from a clean tracked working tree. No defaults, slate rows, source
runs, or production optimizer code are modified.
"""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from config import load_config
from db.database import DatabaseManager
from research.nfl_classic_attribution_export import prior_opportunity, team

ROOT = Path(__file__).resolve().parents[1]

# One entry per pre-registered study. Changing an entry is a new study version.
STUDIES = {
    "classic": {"version": "nfl-classic-te-wr-pool-v1", "format": "classic", "games": "multi",
                "doc": "docs/nfl-classic-te-wr-pool-rule-study.md", "priorOpportunity": True,
                # One Classic main slate per week: enrollment is unique per week.
                "uniquePerGames": False},
    "showdown": {"version": "nfl-showdown-kicker-rule-v1", "format": "showdown", "games": "single",
                 "doc": "docs/nfl-showdown-kicker-rule-study.md", "priorOpportunity": False,
                 # Several Showdowns a week: enrollment is unique per game.
                 "uniquePerGames": True},
}
STUDY = STUDIES["classic"]["version"]  # backwards-compatible name


def study_by_version(version):
    for key, spec in STUDIES.items():
        if spec["version"] == version:
            return key, spec
    raise ValueError(f"Unknown study version {version}")


def encoded(value):
    return json.dumps(value, default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


class Reader:
    """Use one read-only repeatable-read snapshot for the complete capture."""
    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, params=()):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def enrollment_key(spec, season, week, game_ids):
    """Advisory-lock and uniqueness scope: per week (Classic), per game (Showdown)."""
    games = ",".join(str(g) for g in sorted(game_ids)) if spec["uniquePerGames"] else "*"
    return f"{spec['version']}:{season}:{week}:{games}"


def existing_pairs(reader, season, week, version=STUDY, game_ids=None):
    rows = reader.execute(
        """SELECT run_id, settings->'researchStudy'->>'arm' AS arm, status,
                  settings->'researchStudy'->'gameIds' AS game_ids
           FROM nfl_dfs_optimizer_runs
           WHERE settings->'researchStudy'->>'version' = %s
             AND settings->'researchStudy'->>'season' = %s
             AND settings->'researchStudy'->>'week' = %s""", (version, str(season), str(week)))
    if game_ids is None:
        return rows
    wanted = sorted(game_ids)
    # A row without recorded games cannot prove it is a different game: count it.
    return [r for r in rows if r.get("game_ids") is None or sorted(r["game_ids"]) == wanted]


def match_games(games, schedule, shape="multi"):
    matches = []
    if not isinstance(games, list) or len(set(games)) != len(games):
        raise ValueError("Salary slate games must be a list of unique games")
    if shape == "multi" and len(games) < 2:
        raise ValueError("A multi-game Classic slate with unique games is required")
    if shape == "single" and len(games) != 1:
        raise ValueError("A single-game Showdown slate is required")
    for game in games:
        parts = game.split("@")
        if len(parts) != 2:
            raise ValueError(f"Invalid salary game {game}")
        away, home = map(team, parts)
        rows = [r for r in schedule if team(r["away"]) == away and team(r["home"]) == home]
        if len(rows) != 1 or rows[0]["kickoff"] is None:
            raise ValueError(f"Missing or ambiguous canonical kickoff for {game}")
        matches.append(rows[0])
    return matches


def assert_forward(season, week, source_at, now, kickoff):
    if season != 2026 or not 6 <= week <= 18:
        raise ValueError("Registered population is 2026 weeks 6-18; no discovery/replay enrollment")
    if not source_at <= now < kickoff:
        raise ValueError("Source and both arms must be captured/saved before the first kickoff")


def implementation():
    files = sorted((ROOT / "web/src").rglob("*.ts")) + sorted((ROOT / "web/src").rglob("*.tsx"))
    files += [ROOT / "web/scripts/nfl-pool-rule-study-core.ts",
              ROOT / "web/scripts/research-nfl-pool-rule-study.ts", Path(__file__),
              ROOT / "research/nfl_classic_attribution_export.py",
              *(ROOT / spec["doc"] for spec in STUDIES.values()), ROOT / "web/package-lock.json"]
    hashes = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.exists()}

    def git(*args):
        return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()
    # Tracked changes only: the hashes above already pin every untracked web/src file.
    dirty = git("status", "--porcelain", "--untracked-files=no")
    return {"commit": git("rev-parse", "HEAD"), "dirty": bool(dirty), "dirtyFiles": dirty.splitlines(), "files": hashes,
            "digest": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def capture(db, source_id, study="classic"):
    UUID(source_id)
    spec = STUDIES[study]
    with db.connect() as conn:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        reader = Reader(conn)
        rows = reader.execute(
            """SELECT o.*, u.format AS slate_format, u.games, u.slate_signature, u.player_count,
                      p.season, p.week, p.as_of_at AS projection_as_of
               FROM nfl_dfs_optimizer_runs o JOIN nfl_dfs_slate_uploads u ON u.upload_id=o.upload_id
               JOIN nfl_dfs_projection_runs p ON p.run_id=o.projection_run_id
               WHERE o.run_id=%s""", (source_id,))
        if len(rows) != 1:
            raise ValueError("Source run missing or has no linked projection run")
        row = dict(rows[0])
        if (row["slate_format"] != spec["format"] or row["settings"].get("format") != spec["format"]
                or row["settings"].get("mode") != "gpp"):
            raise ValueError(f"Explicit saved {spec['format'].title()} GPP build required")
        if row["status"] not in ("complete", "partial") or row["generated_lineups"] < 1 or row["settings"].get("researchStudy"):
            raise ValueError("Source must be an ordinary saved build with lineups")
        now = reader.execute("SELECT clock_timestamp() AS now")[0]["now"]
        schedule = reader.execute(
            """SELECT g.id, g.kickoff, h.abbreviation AS home, a.abbreviation AS away
               FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
               JOIN nfl_teams a ON a.team_id=g.away_team_id WHERE g.season=%s AND g.week=%s""",
            (row["season"], row["week"]))
        matches = match_games(row["games"], schedule, spec["games"])
        kickoff = min(g["kickoff"] for g in matches)
        assert_forward(row["season"], row["week"], row["created_at"], now, kickoff)
        game_ids = [g["id"] for g in matches]
        if existing_pairs(reader, row["season"], row["week"], spec["version"], game_ids if spec["uniquePerGames"] else None):
            raise ValueError("This study slate already has saved arms; refusing a reroll")
        snapshot = row["input_snapshot"]
        if not isinstance(snapshot, list) or len(snapshot) != row["player_count"]:
            raise ValueError("Source snapshot does not contain the full salary pool")
        if {p.get("gameKey") for p in snapshot} != set(row["games"]):
            raise ValueError("Frozen player games do not match the salary slate")
        players = copy.deepcopy(snapshot)
        source = {k: row[k] for k in ("run_id", "upload_id", "projection_run_id", "created_at", "optimizer_version",
                                      "settings", "input_snapshot", "input_digest", "build_info")}
        common = {"version": spec["version"], "study": study, "pairId": str(uuid4()), "source": source,
                  "season": row["season"], "week": row["week"], "slateSignature": row["slate_signature"],
                  "gameIds": game_ids, "firstKickoff": kickoff, "capturedAt": now}
        if not spec["priorOpportunity"]:
            # The Showdown shape has no pool rules, so nothing reads prior opportunity.
            return json.loads(encoded({**common, "players": players, "priorOpportunityCapture": None,
                                       "implementation": implementation()}))
        # Use the same historical cutoff as the product's signal/target readers.
        # This is a NEW pregame capture, never a reconstruction claimed to have
        # been frozen with the source run. Participant rows lack version history.
        cutoff = row["projection_as_of"]
        if cutoff is None or cutoff > row["created_at"]:
            raise ValueError("Invalid projection evidence cutoff")
        prior, totals = prior_opportunity(reader, row["season"], row["week"], cutoff)
        identities = {r["id"]: r["gsis_id"] for r in reader.execute(
            "SELECT id, gsis_id FROM ff_players WHERE season=%s AND gsis_id IS NOT NULL", (row["season"],))}
        supplemented = []
        for p in players:
            if p["position"] not in ("RB", "WR", "TE") or "priorOpportunity" in p:
                continue
            rec = prior.get(identities.get(p.get("ffPlayerId")))
            total = totals.get(team(p["team"]))
            p["priorOpportunity"] = {**rec, "targetShare": rec["targets"] / total if total else None} if rec else None
            supplemented.append(p["dkPlayerId"])
        return json.loads(encoded({**common,
            "players": players, "priorOpportunityCapture": {"capturedAt": now, "labelCutoff": cutoff,
                "supplementedPlayerIds": supplemented, "source": "product prior-three-week participant queries",
                "limitation": "New pregame capture; participant rows have no historical versions. Missing records remain null.",
                "playerAggregates": prior, "teamTargets": totals}, "implementation": implementation()}))


def save_pair(db, captured, pair):
    """One transaction: enroll once, insert both arms, recheck lock, commit."""
    version = captured.get("version", STUDY)
    _, spec = study_by_version(version)
    if pair.get("version") != version or pair.get("pairId") != captured["pairId"] or [a["arm"] for a in pair["arms"]] != ["A", "B"]:
        raise ValueError("Invalid pair identity")
    impl = captured.get("implementation") or {}
    if impl.get("dirty"):
        raise ValueError("Working tree has uncommitted tracked changes; commit before saving study arms ("
                         + ", ".join(impl.get("dirtyFiles", [])[:5]) + ")")
    source = captured["source"]
    scoped = captured["gameIds"] if spec["uniquePerGames"] else None
    with db.connect() as conn:
        with conn.cursor() as cur:
            # Serializes repeated attempts, even from another process/source run.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (enrollment_key(spec, captured["season"], captured["week"], captured["gameIds"]),))
            reader = Reader(conn)
            supported = reader.execute(
                """SELECT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid='nfl_dfs_optimizer_runs'::regclass
                     AND conname='nfl_dfs_optimizer_runs_status_check'
                     AND pg_get_constraintdef(oid) LIKE '%%research_complete%%') AS ready""")[0]["ready"]
            if not supported:
                raise ValueError("Research run status migration is not installed; apply the repository schema migration first")
            if existing_pairs(reader, captured["season"], captured["week"], version, scoped):
                raise ValueError("This study slate already has saved arms; refusing a reroll")
            current = reader.execute("SELECT input_digest FROM nfl_dfs_optimizer_runs WHERE run_id=%s", (source["run_id"],))
            if len(current) != 1 or current[0]["input_digest"] != source["input_digest"]:
                raise ValueError("Source run changed during capture")

            def check_lock():
                games = reader.execute("SELECT id,kickoff FROM nfl_season_games WHERE id=ANY(%s) FOR SHARE", (captured["gameIds"],))
                if len(games) != len(captured["gameIds"]) or any(g["kickoff"] is None for g in games):
                    raise ValueError("Canonical schedule incomplete at persistence")
                now = reader.execute("SELECT clock_timestamp() AS now")[0]["now"]
                # Honor the earlier boundary if the schedule changed during work.
                kickoff = min(datetime.fromisoformat(captured["firstKickoff"]), *(g["kickoff"] for g in games))
                assert_forward(captured["season"], captured["week"], datetime.fromisoformat(source["created_at"]), now, kickoff)
                return now

            check_lock()
            ids = []
            for arm in pair["arms"]:
                run_id = str(uuid4())
                cur.execute(
                    """INSERT INTO nfl_dfs_optimizer_runs
                       (run_id,upload_id,projection_run_id,optimizer_version,mode,projection_source,settings,input_snapshot,
                        input_digest,build_info,requested_lineups,generated_lineups,status,failure_reason,created_at)
                       VALUES (%s,%s,%s,%s,'gpp',%s,%s::jsonb,%s::jsonb,%s,%s::jsonb,%s,%s,%s,%s,clock_timestamp())""",
                    (run_id, source["upload_id"], source["projection_run_id"], arm["optimizerVersion"],
                     arm["settings"]["projectionSource"], encoded(arm["settings"]).decode(), encoded(arm["inputSnapshot"]).decode(),
                     arm["inputDigest"], encoded(arm["buildInfo"]).decode(), arm["requestedLineups"], len(arm["lineups"]),
                     arm["status"], arm["failureReason"]))
                for l in arm["lineups"]:
                    cur.execute(
                        """INSERT INTO nfl_dfs_lineups (run_id,lineup_number,slots,player_ids,total_salary,projected_fpts,
                           floor_fpts,ceiling_fpts,projected_ownership,stack_summary)
                           VALUES (%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s::jsonb)""",
                        (run_id, l["lineupNumber"], encoded(l["slots"]).decode(), encoded(l["playerIds"]).decode(),
                         l["totalSalary"], l["projectedFpts"], l["floorFpts"], l["ceilingFpts"], l["projectedOwnership"],
                         encoded(l["stackSummary"]).decode()))
                ids.append({"arm": arm["arm"], "runId": run_id, "status": arm["status"], "built": len(arm["lineups"])})
            checked_at = check_lock()
        # DatabaseManager commits here; any error above rolls BOTH arms back.
    return {"pairId": captured["pairId"], "saved": ids, "lockCheckedAt": checked_at.isoformat()}


def archive(directory, name, value):
    data = encoded(value)
    with (directory / f"{name}.json.gz").open("xb") as f:
        f.write(gzip.compress(data, mtime=0))
    return {"file": f"{name}.json.gz", "sha256Uncompressed": hashlib.sha256(data).hexdigest(), "bytesUncompressed": len(data)}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--study", choices=sorted(STUDIES), default="classic")
    ap.add_argument("--source-run", required=True)
    ap.add_argument("--main-slate", action="store_true", help="Attest this is the weekly Classic main slate")
    ap.add_argument("--flagship-showdown", action="store_true",
                    help="Attest this is the game day's biggest DraftKings Showdown (the stat-api flagship)")
    ap.add_argument("--out", required=True, type=Path, help="New directory for immutable input/output artifacts")
    ap.add_argument("--save", action="store_true", help="Persist both research arms; omitted = database read-only")
    args = ap.parse_args(argv)
    if args.study == "classic" and not args.main_slate:
        ap.error("--main-slate is required for the Classic study; the script never guesses the study population")
    if args.study == "showdown" and not args.flagship_showdown:
        ap.error("--flagship-showdown is required for the Showdown study; the script never guesses the study population")
    if (args.study == "classic" and args.flagship_showdown) or (args.study == "showdown" and args.main_slate):
        ap.error("Attestation flag does not match --study")
    if args.out.exists():
        ap.error("--out must be a new directory; prior captures are never overwritten")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    captured = capture(db, args.source_run, args.study)
    if args.save and captured["implementation"]["dirty"]:
        ap.error("Tracked files have uncommitted changes; commit them (or run without --save): "
                 + ", ".join(captured["implementation"]["dirtyFiles"][:5]))
    args.out.mkdir(parents=True)
    input_path, output_path = args.out / "worker-input.json", args.out / "worker-output.json"
    input_path.write_bytes(encoded(captured))
    command = ["node", "-r", "./scripts/server-only-stub.cjs", "--import", "tsx",
               "scripts/research-nfl-pool-rule-study.ts", str(input_path.resolve()), str(output_path.resolve())]
    subprocess.run(command, cwd=ROOT / "web", check=True)
    pair = json.loads(output_path.read_text(encoding="utf-8"))
    manifest = {"study": captured["version"], "pairId": captured["pairId"], "databaseSaved": False,
                "files": [archive(args.out, "input", captured), archive(args.out, "pair", pair)]}
    manifest_path = args.out / "manifest.json"
    manifest_path.write_bytes(encoded(manifest))
    # Keep the uncompressed worker files too: failed workers remain inspectable.
    if args.save:
        receipt = save_pair(db, captured, pair)
        (args.out / "receipt.json").write_bytes(encoded(receipt))
        manifest["databaseSaved"] = True
        manifest_path.write_bytes(encoded(manifest))
        print(json.dumps(receipt))
    else:
        print(f"Dry run only; no optimizer runs saved. Artifacts: {args.out}")


if __name__ == "__main__":
    main()
