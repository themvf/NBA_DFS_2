"""Import Hello Rookie's published DraftKings Millionaire winning lineups.

    python -m ingest.hellorookie_winners                 # fetch the page once and store
    python -m ingest.hellorookie_winners --file page.html --dry-run
    python -m ingest.hellorookie_winners --report        # what is stored, by season

One page, one request, an identifying User-Agent, on request only (not
scheduled: the page is edited by hand roughly weekly in season and is the
publisher's work). robots.txt allows it (2026-10-15). Rows go to
`nfl_dfs_published_winners`, keyed by (source, season, week, contest, rank),
and are upserted so a corrected page replaces an earlier parse; the page's
sha256 is stored with each row. See model/hellorookie_winners.py for what the
data is and is not.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from model import hellorookie_winners as m

USER_AGENT = "Mozilla/5.0 NBADFS-v2 hellorookie-winners/1.0 (one page, on request)"


def fetch(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    return response.text


def persist(db: DatabaseManager, lineups: list[dict], url: str, digest: str) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """INSERT INTO nfl_dfs_published_winners
                     (source, season, week, contest, rank, version, players, total_salary, total_points, flags, source_url, page_digest)
                   VALUES %s
                   ON CONFLICT (source, season, week, contest, rank) DO UPDATE SET
                     version = EXCLUDED.version, players = EXCLUDED.players, total_salary = EXCLUDED.total_salary,
                     total_points = EXCLUDED.total_points, flags = EXCLUDED.flags, source_url = EXCLUDED.source_url,
                     page_digest = EXCLUDED.page_digest, captured_at = NOW()""",
                [(l["source"], l["season"], l["week"], l["contest"], 1, l["version"], Json(l["players"]), l["total_salary"],
                  l["total_points"], Json(l["flags"]), url, digest) for l in lineups])


def report(db: DatabaseManager) -> str:
    rows = db.execute("""SELECT season, week, total_points, total_salary, flags, players
                           FROM nfl_dfs_published_winners WHERE source = %s ORDER BY season DESC, week DESC""", (m.SOURCE,))
    if not rows:
        return "nothing stored"
    lines = [f"{len(rows)} published winners stored"]
    for r in rows:
        qb = next((p for p in r["players"] if p["slot"] == "QB"), {})
        own = [p["draft_pct"] for p in r["players"] if p["draft_pct"] is not None]
        lines.append(f"  {r['season']} w{r['week']:<2} {r['total_points'] or 0:7.2f} pts  ${r['total_salary'] or 0:>6,}  "
                     f"QB {qb.get('name', '?'):<16} own sum {sum(own):5.1f}%  min own {min(own) if own else 0:4.1f}%"
                     + (f"  [{', '.join(r['flags'])}]" if r["flags"] else ""))
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=m.SOURCE_URL)
    parser.add_argument("--file", type=Path, help="parse a saved copy of the page instead of fetching it")
    parser.add_argument("--dry-run", action="store_true", help="parse and print; write nothing")
    parser.add_argument("--report", action="store_true", help="print what is stored and exit")
    args = parser.parse_args(argv)
    db = None if args.dry_run else DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.report:
        print(report(db))
        return 0
    page = args.file.read_text(encoding="utf-8", errors="replace") if args.file else fetch(args.url)
    digest = hashlib.sha256(page.encode("utf-8")).hexdigest()
    lineups = m.parse_page(page)
    print(m.summary(lineups))
    if db is None:
        print("dry run: nothing written")
        return 0
    persist(db, lineups, args.url, digest)
    print(f"stored {len(lineups)} rows in nfl_dfs_published_winners (page sha256 {digest[:12]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
