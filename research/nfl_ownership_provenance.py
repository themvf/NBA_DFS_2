"""Classify and persist label provenance for every imported NFL DFS ownership row.

Task 2 of docs/nfl-ownership-handoff-2026-10-19.md; classes and rules in
model/nfl_ownership_provenance.py and docs/nfl-ownership-scorecard-v1.md
section 3. Reads `nfl_dfs_field_contests`, `nfl_dfs_field_ownership`, the linked
salary pool (`nfl_dfs_slate_players`) and, for stat-api contests, the fetched
users' stored lineups (`nfl_dfs_field_user_builds`) so a captain zero is called
synthesized only when no fetched user captained the player.

Writes ONLY to the append-only `nfl_ownership_label_provenance` (ON CONFLICT DO
NOTHING: an existing (contest, player, slot, version) row is never rewritten).

    python -m research.nfl_ownership_provenance [--mass-floor 0.995] [--dry-run] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from db.nfl_ownership_provenance_schema import STATEMENTS
from model import nfl_ownership_provenance as prov
from model.nfl_dfs_field_audit import normalize_name
from research.nfl_ownership_inventory import connect, rows


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.nfl_ownership_label_provenance') IS NOT NULL AS present")
        present = cur.fetchone()["present"]
        cur.execute("SELECT 1 FROM pg_trigger WHERE tgname = 'nfl_ownership_label_provenance_immutable'")
        if present and cur.fetchone():
            return
        for statement in STATEMENTS:
            cur.execute(statement)
    conn.commit()


def load_records(conn) -> tuple[list[prov.ContestRecord], dict[str, dict]]:
    contests = rows(conn, """
        SELECT contest_id, contest_name, format, season, week, slate_upload_id::text, entry_count, file_name,
               score_curve IS NOT NULL AS full_standings
        FROM nfl_dfs_field_contests ORDER BY season NULLS FIRST, week NULLS FIRST, contest_id""")
    records, meta = [], {}
    for c in contests:
        own = {r["normalized_name"]: {"drafted_pct": r["drafted_pct"], "drafted_by_slot": r["drafted_by_slot"] or {}}
               for r in rows(conn, """SELECT normalized_name, drafted_pct, drafted_by_slot
                                      FROM nfl_dfs_field_ownership WHERE contest_id = %s""", (c["contest_id"],))}
        pool = None
        if c["slate_upload_id"]:
            names = rows(conn, "SELECT name FROM nfl_dfs_slate_players WHERE upload_id = %s::uuid",
                         (c["slate_upload_id"],))
            pool = frozenset(normalize_name(r["name"]) for r in names) or None
        path = prov.import_path(c["contest_id"], c["file_name"])
        captained = flex_seen = None
        cpt_seat_pct: dict[str, set[float]] = {}
        users = 0
        if path == "stat_api":
            builds = rows(conn, "SELECT lineups FROM nfl_dfs_field_user_builds WHERE contest_id = %s",
                          (c["contest_id"],))
            users = len(builds)
            lineups = [l for b in builds for l in (b["lineups"] or [])]
            if lineups:
                cap, flex = prov.user_seats(lineups)
                captained, flex_seen = frozenset(cap), frozenset(flex)
                for l in lineups:
                    for seat in l.get("roster") or []:
                        if seat and str(seat[0]).upper() == "CPT" and len(seat) >= 7 and seat[6] is not None:
                            cpt_seat_pct.setdefault(normalize_name(seat[1]), set()).add(round(float(seat[6]), 4))
        records.append(prov.ContestRecord(
            contest_id=c["contest_id"], fmt=c["format"], path=path, full_standings=bool(c["full_standings"]),
            ownership=own, salary_pool=pool, captained=captained, flex_seen=flex_seen,
            slate_upload_id=c["slate_upload_id"]))
        meta[c["contest_id"]] = {**c, "users": users, "cpt_seat_pct": cpt_seat_pct}
    return records, meta


def verify(rec: prov.ContestRecord, m: dict) -> list[str]:
    """Checks of the stored record against the fetched lineups (stat-api) and the salary pool."""
    out = []
    if rec.salary_pool is not None:
        unmatched = sorted(set(rec.ownership) - rec.salary_pool)
        if unmatched:
            out.append(f"{len(unmatched)} ownership name(s) not in the linked salary pool: {', '.join(unmatched[:6])}")
    if rec.path == "stat_api" and rec.flex_seen is not None:
        seen = (rec.flex_seen | (rec.captained or frozenset()))
        unseen = sorted(set(rec.ownership) - seen)
        if unseen:
            out.append(f"{len(unseen)} ownership row(s) not in any stored lineup: {', '.join(unseen[:6])}")
        if rec.fmt == "showdown":
            bad = []
            for name, pcts in m["cpt_seat_pct"].items():
                stored = (rec.ownership.get(name) or {}).get("drafted_by_slot", {}).get("CPT")
                if stored is not None and all(abs(float(stored) - p) > 0.011 for p in pcts):
                    bad.append(f"{name} stored {stored} vs seat {sorted(pcts)}")
            if bad:
                out.append(f"{len(bad)} stored captain share(s) differ from the CPT seat: {'; '.join(bad[:4])}")
            missing = sorted(n for n in (rec.captained or ()) if n not in rec.ownership)
            if missing:
                out.append(f"{len(missing)} captained name(s) absent from the ownership table: {', '.join(missing)}")
            cap0 = sorted(n for n in (rec.captained or ()) if n in rec.ownership and
                          float((rec.ownership[n].get("drafted_by_slot") or {}).get("CPT") or 0) == 0.0)
            if cap0:
                out.append(f"{len(cap0)} captained player(s) stored with CPT 0 (rounded; kept observed): {', '.join(cap0)}")
    return out


def persist(conn, results: list[tuple[prov.ContestRecord, prov.ContestProvenance, dict]], params: dict) -> int:
    from psycopg2.extras import Json, execute_values

    written = 0
    with conn.cursor() as cur:
        for rec, p, m in results:
            values = [(l.contest_id, l.normalized_name, l.slot, prov.VERSION, l.label_class, l.label_pct, l.stored_pct,
                       l.basis, p.path, l.in_ownership_table, l.in_salary_pool, rec.slate_upload_id, p.mass,
                       p.complete, rec.full_standings, Json(params)) for l in p.labels]
            if not values:
                continue
            result = execute_values(cur, """
                INSERT INTO nfl_ownership_label_provenance
                  (contest_id, normalized_name, slot, version, label_class, label_pct, stored_pct, basis, import_path,
                   in_ownership_table, in_salary_pool, slate_upload_id, contest_mass, record_complete, full_standings,
                   params)
                VALUES %s
                ON CONFLICT (contest_id, normalized_name, slot, version) DO NOTHING
                RETURNING 1""", values, page_size=1000, fetch=True)
            written += len(result)
    conn.commit()
    return written


def render(results, params: dict, written: int | None, stored_total: int | None) -> str:
    lines = [f"# NFL DFS ownership — label provenance ({prov.VERSION})", "",
             f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `python -m research.nfl_ownership_provenance`. "
             f"Parameters: `{json.dumps(params)}` (mass floor PROVISIONAL until the scorecard is registered).", "",
             "Classes per docs/nfl-ownership-scorecard-v1.md section 3. One label per (contest, player, slot): "
             "`overall` everywhere, plus `CPT` and `FLEX` in Showdown. Salary-listed players with no ownership row are "
             "labelled too (reconciled_zero when the record is complete, else unresolved); contests with no linked "
             "salary pool cannot enumerate them. `complete` = DK export, or stat-api with mass >= floor AND full "
             "standings fetched.", ""]
    if written is not None:
        lines += [f"Persisted to `nfl_ownership_label_provenance`: {written} new rows this run; {stored_total} rows "
                  f"under `{prov.VERSION}` in total.", ""]
    else:
        lines += ["Dry run: nothing persisted.", ""]
    total = Counter()
    lines += ["## Counts by class per contest", "",
              "| contest | wk | format | import | entries | users fetched | mass | full standings | complete | slot | observed | reconciled_zero | synthesized_zero | unresolved |",
              "|---|---:|---|---|---:|---:|---:|---|---|---|---:|---:|---:|---:|"]
    for rec, p, m in results:
        for slot, counts in sorted(p.counts().items(), key=lambda kv: ("overall", "CPT", "FLEX").index(kv[0])):
            total.update({(slot, k): v for k, v in counts.items()})
            lines.append("| {} | {} | {} | {} | {:,} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                rec.contest_id, m["week"] if m["week"] is not None else "?", rec.fmt, p.path, m["entry_count"],
                m["users"] if p.path == "stat_api" else "-",
                f"{p.mass * 100 * prov.ROSTER_SLOTS[rec.fmt]:.2f}/{prov.ROSTER_SLOTS[rec.fmt] * 100}" if p.mass is not None else "-",
                "yes" if rec.full_standings else "no", "yes" if p.complete else "no", slot,
                counts["observed"], counts["reconciled_zero"], counts["synthesized_zero"], counts["unresolved"]))
    lines += ["", "Totals: " + "; ".join(
        f"{slot}: " + ", ".join(f"{c} {total[(slot, c)]}" for c in prov.CLASSES) for slot in ("overall", "CPT", "FLEX")), ""]
    lines += ["## Bases (why each class)", "", "| basis | class | rows |", "|---|---|---:|"]
    bases = Counter((l.basis, l.label_class) for _, p, _ in results for l in p.labels)
    for (basis, cls), n in sorted(bases.items()):
        lines.append(f"| {basis} | {cls} | {n} |")
    lines += ["", "## Notes and verification per contest", ""]
    for rec, p, m in results:
        notes = p.notes + verify(rec, m)
        if notes:
            lines.append(f"- **{rec.contest_id}**: " + " / ".join(notes))
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mass-floor", type=float, default=prov.DEFAULT_MASS_FLOOR)
    ap.add_argument("--dry-run", action="store_true", help="classify and report, write nothing")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    params = {"mass_floor": args.mass_floor, "mass_floor_status": "provisional", "version": prov.VERSION}
    conn = connect()
    try:
        records, meta = load_records(conn)
        results = [(r, prov.classify_contest(r, mass_floor=args.mass_floor), meta[r.contest_id]) for r in records]
        written = stored = None
        if not args.dry_run:
            ensure_table(conn)
            written = persist(conn, results, params)
            stored = rows(conn, "SELECT count(*) AS n FROM nfl_ownership_label_provenance WHERE version = %s",
                          (prov.VERSION,))[0]["n"]
        text = render(results, params, written, stored)
    finally:
        conn.close()
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}" + (f"; {written} rows persisted" if written is not None else ""))
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
