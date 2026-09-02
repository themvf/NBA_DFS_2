"""Field pick percentage for survivor pools, from survivorgrid.com.

WHAT THIS IS AND IS NOT
-----------------------
`P%` here is one aggregator's estimate of NATIONAL pick share. It is not the
distribution inside any particular pool, and a small local pool may look
nothing like it. Every consumer of this table has to carry that caveat; the
field study that uses it states it as its most likely failure mode.

Verified 2026-08-31: robots.txt is `User-agent: * / Disallow:` (everything
permitted) and `/{season}/{week}` renders a server-side table of
EV / W% / P% / team / opponent+spread. There is no API, no versioning, and no
contract -- so this parser fails loudly on a shape change rather than writing
partial rows, which is the only safe behavior for a scrape.

Usage:
    python -m ingest.survivor_pick_popularity --season 2025 --weeks 1-18
    python -m ingest.survivor_pick_popularity --season 2026 --weeks 1
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_entities
import logging
import re
import time

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

BASE_URL = "https://www.survivorgrid.com"
SOURCE = "survivorgrid"

# survivorgrid's codes -> nfl_teams.abbreviation. Same class of mapping as the
# nflverse one; an unmapped code raises rather than dropping a team.
TEAM_CODE_OVERRIDES = {
    "LA": "LAR", "LAR": "LAR", "STL": "LAR",
    "WAS": "WSH", "WSH": "WSH",
    "JAC": "JAX", "JAX": "JAX",
    "AZ": "ARI", "ARI": "ARI",
    "OAK": "LV", "LV": "LV",
    "SD": "LAC", "LAC": "LAC",
    "TAM": "TB", "GNB": "GB", "KAN": "KC", "NWE": "NE", "NOR": "NO", "SFO": "SF",
}

_TAG = re.compile(r"<[^>]+>")
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_CELL = re.compile(r"<t[hd][^>]*>(.*?)</t[hd]>", re.S)


class ShapeChanged(RuntimeError):
    """The page no longer looks like the table this parser was written for."""


def _text(fragment: str) -> str:
    """Strip tags and decode HTML entities.

    The entity decode is load-bearing, not cosmetic. Team cells arrive as
    ``BUF &nbsp;(L)``, and stripping tags alone leaves the literal string
    ``&nbsp;`` glued to the abbreviation, which then fails the team-code
    match. The shape guard caught exactly that on the first real run -- which
    is the argument for having the guard refuse partial output rather than
    quietly store the four teams that happened to parse.
    """
    plain = html_entities.unescape(_TAG.sub(" ", fragment))
    return re.sub(r"\s+", " ", plain.replace("\xa0", " ")).strip()


def _percent(value: str) -> float | None:
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*%", value)
    return float(match.group(1)) / 100.0 if match else None


def _float(value: str) -> float | None:
    match = re.search(r"-?\d+(?:\.\d+)?", value)
    return float(match.group(0)) if match else None


def fetch_week(season: int, week: int, timeout: int = 30, attempts: int = 3) -> str:
    url = f"{BASE_URL}/{season}/{week}"
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0 (compatible; NBADFS-survivor/1.0)"},
            )
            if response.status_code == 404:
                raise FileNotFoundError(f"{url} not published yet")
            response.raise_for_status()
            return response.text
        except FileNotFoundError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then re-raised
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{url} fetch failed: {last}")


def parse_week(html: str) -> list[dict]:
    """Extract one row per team: abbreviation, pick %, win %, EV.

    Raises ShapeChanged rather than returning a short list, so a silent
    upstream redesign cannot look like "the field just did not pick anyone".
    """
    rows = _ROW.findall(html)
    if not rows:
        raise ShapeChanged("no table rows found")

    header = [_text(cell) for cell in _CELL.findall(rows[0])]
    if len(header) < 4 or header[:4] != ["EV ▼", "W%", "P%", "Team"]:
        # Tolerate the sort arrow moving between columns, but not the four
        # leading columns changing identity or order.
        stripped = [re.sub(r"[^A-Za-z%]", "", value).upper() for value in header[:4]]
        if stripped != ["EV", "W", "P", "TEAM"]:
            raise ShapeChanged(f"unexpected header: {header[:6]}")

    parsed: list[dict] = []
    for row in rows[1:]:
        cells = [_text(cell) for cell in _CELL.findall(row)]
        if len(cells) < 4:
            continue
        code = re.sub(r"\(.*?\)", "", cells[3]).strip().upper()
        if not code or not re.fullmatch(r"[A-Z]{2,3}", code):
            continue
        parsed.append(
            {
                "code": code,
                "ev": _float(cells[0]),
                "win_pct": _percent(cells[1]),
                "pick_pct": _percent(cells[2]),
            }
        )

    if len(parsed) < 24:
        raise ShapeChanged(f"only parsed {len(parsed)} teams; expected ~32")
    return parsed


def store_week(db: DatabaseManager, season: int, week: int, html: str, rows: list[dict]) -> int:
    by_abbrev = {
        row["abbreviation"]: row["team_id"]
        for row in db.execute("SELECT team_id, abbreviation FROM nfl_teams")
    }
    raw_hash = hashlib.sha256(html.encode("utf-8", "replace")).hexdigest()[:32]
    url = f"{BASE_URL}/{season}/{week}"
    written = 0

    for row in rows:
        mapped = TEAM_CODE_OVERRIDES.get(row["code"], row["code"])
        if mapped not in by_abbrev:
            raise ValueError(
                f"unmapped survivorgrid team code {row['code']!r} -- add it to "
                f"TEAM_CODE_OVERRIDES rather than dropping the row"
            )
        db.execute(
            """
            INSERT INTO survivor_pick_popularity
                (season, week, team_id, pick_pct, source_win_pct, source_ev,
                 source, source_url, raw_hash, captured_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (season, week, team_id, source, raw_hash) DO UPDATE SET
                pick_pct = EXCLUDED.pick_pct,
                source_win_pct = EXCLUDED.source_win_pct,
                source_ev = EXCLUDED.source_ev,
                captured_at = NOW()
            """,
            (season, week, by_abbrev[mapped], row["pick_pct"], row["win_pct"],
             row["ev"], SOURCE, url, raw_hash),
        )
        written += 1
    return written


def load(db: DatabaseManager, season: int, weeks: list[int]) -> dict[int, int | str]:
    results: dict[int, int | str] = {}
    for week in weeks:
        try:
            html = fetch_week(season, week)
        except FileNotFoundError:
            results[week] = "not published"
            continue
        try:
            results[week] = store_week(db, season, week, html, parse_week(html))
        except ShapeChanged as exc:
            results[week] = f"SHAPE CHANGED: {exc}"
        time.sleep(1.0)  # unmetered, unauthenticated, and someone else's server
    return results


def _weeks(spec: str) -> list[int]:
    if "-" in spec:
        low, high = spec.split("-", 1)
        return list(range(int(low), int(high) + 1))
    return [int(part) for part in spec.split(",") if part.strip()]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-18")
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    for week, outcome in load(db, args.season, _weeks(args.weeks)).items():
        print(f"  week {week:>2}: {outcome}")


if __name__ == "__main__":
    main()
