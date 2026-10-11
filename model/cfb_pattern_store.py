"""Database storage for the CFB pattern watch.

The grader, the board and the page generator used to keep their state in
`artifacts/cfb_pattern_watch/*.jsonl` on one laptop. Since 2026-10-11 they run
as dispatched GitHub Actions jobs, whose runners start empty, so the state
lives in five append-only tables (db/schema.py):

    cfb_pattern_ledger        graded rows,           UNIQUE (trigger, matchup_id)
    cfb_pattern_selections    frozen selections,     UNIQUE (trigger, matchup_id)
    cfb_pattern_board_events  stale/watch/dropped,   UNIQUE (trigger, matchup_id, status)
    cfb_pattern_runs          one heartbeat per run of each job
    cfb_pattern_reports       rendered reader page,  UNIQUE content digest

Every write is INSERT ... ON CONFLICT DO NOTHING, and the first four tables
reject UPDATE and DELETE with a trigger: the store can add a record but never
change one. The record each module produces is kept whole in `payload`, so the
modules' own dict shapes are unchanged.

`import_files` copies the pre-migration jsonl files in once. Re-running it
inserts nothing new.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

from psycopg2.extras import Json

BASE = Path("artifacts") / "cfb_pattern_watch"


def _j(value) -> Json:
    return Json(value, dumps=lambda o: json.dumps(o, default=str, sort_keys=True))


def _payloads(db, sql: str, params=()) -> list[dict]:
    return [dict(r["payload"]) for r in db.execute(sql, params)]


# ── ledger ───────────────────────────────────────────────────────────────────
def read_ledger(db) -> list[dict]:
    return _payloads(db, "SELECT payload FROM cfb_pattern_ledger ORDER BY commence_time, id")


def append_ledger(db, rows: list[dict]) -> int:
    added = 0
    for r in rows:
        res = db.execute(
            """INSERT INTO cfb_pattern_ledger (study_version, trigger, matchup_id, game_date, commence_time, result, pnl_units, payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (trigger, matchup_id) DO NOTHING RETURNING id""",
            (r["study_version"], r["trigger"], int(r["matchup_id"]), r["game_date"], r["commence_time"],
             r["result"], r["pnl_units"], _j(r)))
        added += len(res)
    return added


# ── selections ───────────────────────────────────────────────────────────────
def read_selections(db) -> dict[tuple[str, int], dict]:
    return {(p["trigger"], int(p["matchup_id"])): p
            for p in _payloads(db, "SELECT payload FROM cfb_pattern_selections ORDER BY frozen_at, id")}


def append_selections(db, rows: list[dict]) -> int:
    added = 0
    for r in rows:
        res = db.execute(
            """INSERT INTO cfb_pattern_selections (trigger, matchup_id, frozen_at, commence_time, payload)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (trigger, matchup_id) DO NOTHING RETURNING id""",
            (r["trigger"], int(r["matchup_id"]), r["frozen_at"], r["commence_time"], _j(r)))
        added += len(res)
    return added


# ── board events ─────────────────────────────────────────────────────────────
def read_events(db) -> list[dict]:
    return _payloads(db, "SELECT payload FROM cfb_pattern_board_events ORDER BY recorded_at, id")


def read_event_keys(db) -> set[tuple[str, int, str]]:
    return {(r["trigger"], int(r["matchup_id"]), r["status"])
            for r in db.execute("SELECT trigger, matchup_id, status FROM cfb_pattern_board_events")}


def append_events(db, rows: list[dict]) -> int:
    added = 0
    for r in rows:
        res = db.execute(
            """INSERT INTO cfb_pattern_board_events (trigger, matchup_id, status, payload)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (trigger, matchup_id, status) DO NOTHING RETURNING id""",
            (r["trigger"], int(r["matchup_id"]), r["status"], _j(r)))
        added += len(res)
    return added


# ── run heartbeats ───────────────────────────────────────────────────────────
def read_runs(db, job: str) -> list[dict]:
    return _payloads(db, "SELECT payload FROM cfb_pattern_runs WHERE job = %s ORDER BY run_at, id", (job,))


def append_run(db, job: str, record: dict, run_at: str | datetime | None = None) -> None:
    if run_at is None:
        db.execute("INSERT INTO cfb_pattern_runs (job, payload) VALUES (%s, %s) RETURNING id", (job, _j(record)))
    else:
        db.execute("INSERT INTO cfb_pattern_runs (job, run_at, payload) VALUES (%s, %s, %s) RETURNING id",
                   (job, run_at, _j(record)))


# ── rendered page ────────────────────────────────────────────────────────────
_GENERATED = re.compile(r"generated \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC")
_RUN_COUNT = re.compile(r"The board has run \d+ times")


def content_digest(html: str) -> str:
    """Digest of the page with its clock and its board-run counter removed.

    A re-render that changes nothing but the time or the number of board runs
    is the same page and is not stored again (CLAUDE.md: never write a row
    whose content did not change). Every half-hourly board run would
    otherwise store a fresh copy.
    """
    normalized = _RUN_COUNT.sub("The board has run <n> times", _GENERATED.sub("generated <t>", html))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def save_report(db, html: str, summary: dict) -> bool:
    res = db.execute(
        """INSERT INTO cfb_pattern_reports (content_digest, html, summary) VALUES (%s, %s, %s)
           ON CONFLICT (content_digest) DO NOTHING RETURNING id""",
        (content_digest(html), html, _j(summary)))
    return bool(res)


def latest_report(db) -> dict | None:
    rows = db.execute("SELECT html, summary, generated_at FROM cfb_pattern_reports ORDER BY generated_at DESC, id DESC LIMIT 1")
    return dict(rows[0]) if rows else None


# ── one-time import of the pre-migration files ───────────────────────────────
def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.open(encoding="utf-8") if l.strip()]


def import_files(db, base: Path = BASE) -> dict:
    ledger = _jsonl(base / "ledger.jsonl")
    selections = _jsonl(base / "selections.jsonl")
    events = _jsonl(base / "events.jsonl")
    runs = _jsonl(base / "board_runs.jsonl")
    out = {"ledger": append_ledger(db, ledger), "selections": append_selections(db, selections),
           "events": append_events(db, events), "runs": 0}
    if not read_runs(db, "board"):
        for r in runs:
            ts = r.get("run_at") or ""
            # Reconstructed rows carry a local-time string; keep it in the payload, let the DB stamp now.
            append_run(db, "board", dict(r, imported_from="board_runs.jsonl"),
                       run_at=ts if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$", ts) else None)
            out["runs"] += 1
    return out
