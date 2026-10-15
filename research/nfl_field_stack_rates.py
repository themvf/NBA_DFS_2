"""Field-wide stacking rates for the portfolio field model v2 (read, backfill, report).

    python -m research.nfl_field_stack_rates --season 2026            # report what is stored
    python -m research.nfl_field_stack_rates --season 2026 --backfill # fill missing rows first

`nfl-portfolio-objective-v1` failed its freeze gate because its sampled field
does not stack (docs/nfl-portfolio-objective-study.md). The registration asks
for a MEASURED field-wide stacking rate as the v2 input. The weekly flagship
import now stores one per contest in `nfl_dfs_field_structure` under
`nfl-dfs-field-stacks-v1` (model/nfl_dfs_field_stacks.py); this script fills
the rows for contests imported before that shipped and prints the pooled
rates a sampler would read.

Backfill, per stored stat-api contest without a stacks row:
  * Classic: re-fetch the FULL standings (every row carries its stack label;
    flagships answer in full with no key) with no user lineups, and store.
    One request per 1,000 entries, about 160 for a Millionaire.
  * Showdown: stat-api labels no showdown row, so the rate comes from the
    stored top finishers' lineups (`nfl_dfs_field_user_builds`), and the row
    says `population: top_users`. No requests.

Nothing here is tuned against realized thresholds; it reports what the field
did. Pooling is entry-weighted across contests of one format.
"""

from __future__ import annotations

import argparse
import sys
from types import SimpleNamespace

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_field_audit import persist_structure
from ingest.statapi_contest import Client, StatApiError, import_contest
from model import nfl_dfs_field_stacks as stacks


def stored_contests(db: DatabaseManager, season: int) -> list[dict]:
    return [dict(r) for r in db.execute(
        """SELECT c.contest_id, c.contest_name, c.format, c.week, c.entry_count, c.file_name,
                  s.summary AS stacks
             FROM nfl_dfs_field_contests c
             LEFT JOIN nfl_dfs_field_structure s ON s.contest_id = c.contest_id AND s.version = %s
            WHERE c.season = %s AND c.file_name LIKE 'stat-api:%%'
            ORDER BY c.week, c.format, c.entry_count DESC""", (stacks.VERSION, season))]


def build_lineups(db: DatabaseManager, contest_id: str) -> list[dict]:
    """The stored top users' lineups as `showdown_build_stacks` reads them."""
    out = []
    for r in db.execute("SELECT lineups FROM nfl_dfs_field_user_builds WHERE contest_id = %s", (contest_id,)):
        for l in r["lineups"] or []:
            out.append({"rank": l.get("rank"),
                        "players": [{"slot": seat[0], "team": seat[2], "position": seat[3]} for seat in l.get("roster") or []]})
    return out


def backfill(db: DatabaseManager, client: Client, rows: list[dict]) -> None:
    for c in rows:
        if c["stacks"] is not None:
            continue
        label = f"wk{c['week']} {c['format']} {c['contest_name'][:50]}"
        if c["format"] == "classic":
            statapi_id = int(str(c["file_name"]).split(":", 1)[1])
            args = SimpleNamespace(standings_rows=1000, all_standings=True, top_users=0, keep_top=100,
                                   ownership_min_coverage=0.95, upload=None)
            try:
                text = import_contest(args, client, db, statapi_id)
            except StatApiError as exc:
                print(f"{label}: import failed: {exc}")
                continue
            print(f"{label}: " + next((line.strip() for line in text.splitlines() if "stack shapes" in line), "no stack row"))
        else:
            lineups = build_lineups(db, c["contest_id"])
            if not lineups:
                print(f"{label}: no stored top-user lineups; nothing to measure")
                continue
            summary = stacks.showdown_build_stacks(lineups)
            persist_structure(db, c["contest_id"], summary)
            print(f"{label}: stack shapes from {summary['entries']:,} top-user lineups stored")


def pooled(rows: list[dict], fmt: str, bucket: str) -> dict | None:
    """Entry-weighted mean of every rate in one bucket across contests of one format."""
    picked = [r["stacks"]["by_rank"][bucket] for r in rows if r["format"] == fmt and r["stacks"]
              and r["stacks"]["by_rank"].get(bucket, {}).get("entries")]
    if not picked:
        return None
    total = sum(b["entries"] for b in picked)
    out: dict = {"entries": total, "contests": len(picked)}
    for key in picked[0]:
        if key == "entries":
            continue
        if isinstance(picked[0][key], dict):
            keys = sorted({k for b in picked for k in b.get(key, {})})
            out[key] = {k: round(sum(b.get(key, {}).get(k, 0.0) * b["entries"] for b in picked) / total, 4) for k in keys}
        else:
            out[key] = round(sum(b.get(key, 0.0) * b["entries"] for b in picked) / total, 4)
    return out


def report(rows: list[dict]) -> str:
    lines = []
    for c in rows:
        s = c["stacks"]
        if not s:
            lines.append(f"wk{c['week']} {c['format']:<8} {c['contest_name'][:48]:<48} no stacks row")
            continue
        whole = s["by_rank"]["all"]
        if c["format"] == "classic":
            lines.append(f"wk{c['week']} classic  {c['contest_name'][:48]:<48} n={whole['entries']:>7,} "
                         f"{'field' if s.get('complete') else 'PARTIAL'}  QB stack {whole['qb_stack']:.1%}  "
                         f"catchers {whole['qb_plus_catchers']}  TE {whole['qb_plus_te']:.1%}  bring-back {whole['bring_back_any']:.1%}")
        else:
            lines.append(f"wk{c['week']} showdown {c['contest_name'][:48]:<48} n={whole['entries']:>7,} top-users  "
                         f"split {whole['team_split']}  QB+mates {whole['qb_plus_teammates']}  K {whole['kicker']:.1%}")
    for fmt in ("classic", "showdown"):
        for bucket in stacks.RANK_BUCKETS:
            p = pooled(rows, fmt, bucket)
            if p:
                lines.append(f"\n{fmt} {bucket} pooled over {p['contests']} contests, {p['entries']:,} lineups"
                             + ("" if fmt == "classic" else " (top users only: selection-biased toward winners)"))
                for k, v in p.items():
                    if k not in ("entries", "contests"):
                        lines.append(f"  {k:<20} {v}")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--backfill", action="store_true", help="store stack rows for contests that lack one")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows = stored_contests(db, args.season)
    if args.backfill:
        backfill(db, Client(), rows)
        rows = stored_contests(db, args.season)
    print(report(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
