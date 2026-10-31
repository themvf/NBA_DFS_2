"""Timestamped NBA roster observations from ESPN (`nba-roster-capture-v1`).

The decision-time team source the role-reliability spec requires: which team
each player belongs to, as observed at a known time before a forecast freezes.
Roster membership is team identity only, never participation, a starting role
or minutes.

Each run reads ESPN's public team list and all 30 rosters and records one row in
`nba_roster_capture_runs`. `nba_roster_observations` is append-only and gains a
row only when an athlete's (team, status) changes, including a `team = NULL`
row when a previously rostered athlete is on no roster. A failed or partial run
is recorded as such and never treated as evidence.

    python -m ingest.nba_rosters            # capture now
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any

import requests

from model.nba_points_ladder import player_key

VERSION = "nba-roster-capture-v1"
TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
ROSTER_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{id}/roster"
EXPECTED_TEAMS = 30
MAX_RUN_AGE = timedelta(hours=24)

DDL = (
    """CREATE TABLE IF NOT EXISTS nba_roster_capture_runs (
        id BIGSERIAL PRIMARY KEY, version TEXT NOT NULL, run_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('complete','failed')), teams INTEGER NOT NULL,
        athletes INTEGER NOT NULL, changes INTEGER NOT NULL, error TEXT, payload_sha256 TEXT)""",
    """CREATE TABLE IF NOT EXISTS nba_roster_observations (
        id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL REFERENCES nba_roster_capture_runs(id),
        observed_at TIMESTAMPTZ NOT NULL, espn_athlete_id TEXT NOT NULL, player TEXT NOT NULL,
        player_key TEXT NOT NULL, espn_team_id TEXT, team TEXT, team_abbrev TEXT, status TEXT)""",
    "CREATE INDEX IF NOT EXISTS idx_nba_roster_obs_athlete ON nba_roster_observations(espn_athlete_id, observed_at DESC)",
    """CREATE OR REPLACE FUNCTION reject_nba_roster_mutation() RETURNS trigger AS $$
       BEGIN RAISE EXCEPTION 'NBA roster capture tables are append-only'; END $$ LANGUAGE plpgsql""",
    """DO $$ BEGIN
         IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nba_roster_obs_append_only') THEN
           CREATE TRIGGER nba_roster_obs_append_only BEFORE UPDATE OR DELETE ON nba_roster_observations
             FOR EACH ROW EXECUTE FUNCTION reject_nba_roster_mutation();
         END IF;
         IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nba_roster_runs_append_only') THEN
           CREATE TRIGGER nba_roster_runs_append_only BEFORE UPDATE OR DELETE ON nba_roster_capture_runs
             FOR EACH ROW EXECUTE FUNCTION reject_nba_roster_mutation();
         END IF; END $$""",
)


def parse_roster(team: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for a in payload.get("athletes") or []:
        if not a.get("id") or not (a.get("fullName") or a.get("displayName")):
            continue
        name = a.get("fullName") or a.get("displayName")
        out.append({"espn_athlete_id": str(a["id"]), "player": name, "player_key": player_key(name),
                    "espn_team_id": str(team["id"]), "team": team["displayName"], "team_abbrev": team.get("abbreviation"),
                    "status": ((a.get("status") or {}).get("type")) or None})
    return out


def fetch_all(session) -> tuple[list[dict[str, Any]], int, str]:
    r = session.get(TEAMS_URL, timeout=20)
    r.raise_for_status()
    teams = [t["team"] for t in r.json()["sports"][0]["leagues"][0]["teams"]]
    if len(teams) != EXPECTED_TEAMS:
        raise ValueError(f"ESPN listed {len(teams)} teams, expected {EXPECTED_TEAMS}")
    rows, digest = [], hashlib.sha256()
    for t in teams:
        resp = session.get(ROSTER_URL.format(id=t["id"]), timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        digest.update(json.dumps(payload, sort_keys=True).encode())
        team_rows = parse_roster(t, payload)
        if not team_rows:
            raise ValueError(f"empty roster for {t['displayName']}")
        rows.extend(team_rows)
    ids = [r["espn_athlete_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("an athlete appears on two rosters in one capture")
    return rows, len(teams), digest.hexdigest()


def changes(current: list[dict[str, Any]], latest: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows to append: new or changed (team, status), plus a team=NULL row for anyone who left every roster."""
    out, seen = [], set()
    for r in current:
        seen.add(r["espn_athlete_id"])
        prev = latest.get(r["espn_athlete_id"])
        if prev is None or (prev.get("espn_team_id"), prev.get("status")) != (r["espn_team_id"], r["status"]):
            out.append(r)
    for aid, prev in latest.items():
        if aid not in seen and prev.get("espn_team_id") is not None:
            out.append({"espn_athlete_id": aid, "player": prev["player"], "player_key": prev["player_key"],
                        "espn_team_id": None, "team": None, "team_abbrev": None, "status": "not_on_roster"})
    return out


def roster_at(db, cutoff: datetime) -> dict[str, dict[str, Any]]:
    """player_key -> {team, observed_at} from the newest complete run within 24 h before `cutoff`.

    Empty when no such run exists. A key shared by two rostered athletes is left out (ambiguous)."""
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('nba_roster_capture_runs') AS t")
        if fetch_rows(cur)[0]["t"] is None:
            return {}
        cur.execute("""SELECT id, run_at FROM nba_roster_capture_runs WHERE status = 'complete' AND run_at <= %s
                       AND run_at > %s ORDER BY run_at DESC LIMIT 1""", (cutoff, cutoff - MAX_RUN_AGE))
        runs = fetch_rows(cur)
        if not runs:
            return {}
        run_id, run_at = runs[0]["id"], runs[0]["run_at"]
        cur.execute("""SELECT DISTINCT ON (espn_athlete_id) espn_athlete_id, player_key, team, status
                       FROM nba_roster_observations WHERE run_id <= %s
                       ORDER BY espn_athlete_id, observed_at DESC, id DESC""", (run_id,))
        rows = [r for r in fetch_rows(cur) if r["team"]]
    by_key: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_key.setdefault(r["player_key"], []).append(r)
    return {k: {"team": v[0]["team"], "observed_at": run_at.isoformat(), "status": v[0]["status"]}
            for k, v in by_key.items() if len(v) == 1}


def main() -> int:
    from config import load_config
    from db.database import DatabaseManager, fetch_rows

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    run_at = datetime.now(timezone.utc)
    with db.connect() as conn:
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        conn.commit()
        try:
            with requests.Session() as s:
                rows, n_teams, digest = fetch_all(s)
        except (requests.RequestException, ValueError, KeyError) as e:
            cur.execute("""INSERT INTO nba_roster_capture_runs (version, run_at, status, teams, athletes, changes, error)
                           VALUES (%s, %s, 'failed', 0, 0, 0, %s)""", (VERSION, run_at, str(e)[:500]))
            conn.commit()
            print(f"roster capture FAILED: {e}")
            return 1
        cur.execute("""SELECT DISTINCT ON (espn_athlete_id) espn_athlete_id, player, player_key, espn_team_id, status
                       FROM nba_roster_observations ORDER BY espn_athlete_id, observed_at DESC, id DESC""")
        latest = {r["espn_athlete_id"]: r for r in fetch_rows(cur)}
        diff = changes(rows, latest)
        cur.execute("""INSERT INTO nba_roster_capture_runs (version, run_at, status, teams, athletes, changes, payload_sha256)
                       VALUES (%s, %s, 'complete', %s, %s, %s, %s) RETURNING id""",
                    (VERSION, run_at, n_teams, len(rows), len(diff), digest))
        run_id = cur.fetchone()["id"]
        for r in diff:
            cur.execute("""INSERT INTO nba_roster_observations (run_id, observed_at, espn_athlete_id, player, player_key,
                           espn_team_id, team, team_abbrev, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (run_id, run_at, r["espn_athlete_id"], r["player"], r["player_key"], r["espn_team_id"],
                         r["team"], r["team_abbrev"], r["status"]))
        conn.commit()
    print(f"{VERSION}: {n_teams} teams, {len(rows)} athletes, {len(diff)} changes recorded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
