"""Hello Rookie's "Every DraftKings Millionaire Maker Winning Lineup" page, parsed.

https://hellorookie.com/draftkings-millionaire-maker-winners/ publishes the
rank-1 lineup of each week's DraftKings NFL Millionaire (Classic): position,
player, draft %, salary and points, one table per week, back to 2023. It is
the only free source of 2025 winners found on 2026-10-15 (stat-api needs Pro
for past seasons, dfsdb blocks us, DraftKings' export needs a login). What it
is NOT: standings, a score curve, ownership of anyone outside the nine
players, or a Showdown record. One outcome-selected lineup a week.

Season inference, because most headings carry no year: headings run from the
newest week down the page. Walking down, the week number falls within a
season and jumps UP where the previous season begins, so the season is the
page's publication year at the top and decrements at each jump. A heading
with an explicit year must agree with the inferred season or the parse fails
(it would mean the layout changed). Checked 2026-10-15 against DraftKings
Network's own breakdowns: the top run's week 1 (J. Fields, 193.92) and week
5 (246.82) are 2025.

Pure: no I/O. `VERSION` is stamped on every lineup.
"""

from __future__ import annotations

import html as html_lib
import re
from typing import Any

VERSION = "hellorookie-winners-v1"
SOURCE = "hellorookie"
SOURCE_URL = "https://hellorookie.com/draftkings-millionaire-maker-winners/"
CONTEST = "NFL Millionaire (Classic)"

CLASSIC_SLOTS = ("QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST")
SALARY_CAP = 50_000

_H2 = re.compile(r"(<h2[^>]*>.*?</h2>)", re.S)
_TABLE = re.compile(r"<table.*?</table>", re.S)
_TR = re.compile(r"<tr.*?</tr>", re.S)
_CELL = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S)
_TAG = re.compile(r"<[^>]+>")
_HEADING = re.compile(r"DraftKings\s+(?:(\d{4})\s+)?NFL\s+Week\s+(\d+)\s+(?:Millionaire\s+)?Winning Lineup", re.I)
_PUBLISHED = re.compile(r'"datePublished"\s*:\s*"(\d{4})-')


def _clean(fragment: str) -> str:
    return re.sub(r"\s+", " ", html_lib.unescape(_TAG.sub(" ", fragment))).strip()


def _num(text: str) -> float | None:
    t = text.replace("$", "").replace(",", "").replace("%", "").strip()
    if not t or t.upper() == "N/A":
        return None
    return float(t)


def parse_table(table_html: str) -> list[dict[str, Any]]:
    rows = [[_clean(c) for c in _CELL.findall(tr)] for tr in _TR.findall(table_html)]
    if not rows or [c.lower() for c in rows[0][:2]] != ["pos.", "player"]:
        raise ValueError(f"unexpected table header {rows[0] if rows else None}")
    out = []
    for r in rows[1:]:
        if len(r) < 5:
            raise ValueError(f"short row {r}")
        salary = _num(r[3])
        out.append({"slot": "DST" if r[0].upper().replace("/", "") == "DST" else r[0].upper(), "name": r[1],
                    "draft_pct": _num(r[2]), "salary": int(salary) if salary is not None else None, "points": _num(r[4])})
    return out


def parse_page(page_html: str) -> list[dict[str, Any]]:
    """Every winning lineup on the page, newest first, with its inferred season and validation flags."""
    published = _PUBLISHED.search(page_html)
    if not published:
        raise ValueError("page has no datePublished; the season of the unlabelled top run cannot be inferred")
    season = int(published.group(1))
    parts = _H2.split(page_html)
    lineups: list[dict[str, Any]] = []
    previous_week: int | None = None
    for i in range(1, len(parts), 2):
        match = _HEADING.search(_clean(parts[i]))
        if not match:
            continue
        explicit, week = (int(match.group(1)) if match.group(1) else None), int(match.group(2))
        if previous_week is not None and week > previous_week:
            season -= 1
        if explicit is not None and explicit != season:
            raise ValueError(f"heading says {explicit} but the inferred season is {season} (week {week}); page layout changed")
        previous_week = week
        tables = _TABLE.findall(parts[i + 1] if i + 1 < len(parts) else "")
        if len(tables) != 1:
            raise ValueError(f"{season} week {week}: expected one table, found {len(tables)}")
        players = parse_table(tables[0])
        flags = []
        if tuple(p["slot"] for p in players) != CLASSIC_SLOTS:
            flags.append("slots_not_classic")
        salaries = [p["salary"] for p in players]
        total_salary = sum(s for s in salaries if s is not None) if all(s is not None for s in salaries) else None
        if total_salary is None:
            flags.append("salary_missing")
        elif total_salary > SALARY_CAP:
            flags.append("salary_over_cap")  # the page's own arithmetic; recorded, not corrected
        points = [p["points"] for p in players]
        total_points = round(sum(points), 2) if all(p is not None for p in points) else None
        if total_points is None:
            flags.append("points_missing")
        if any(p["draft_pct"] is None for p in players):
            flags.append("draft_pct_missing")
        lineups.append({"source": SOURCE, "version": VERSION, "season": season, "week": week, "contest": CONTEST,
                        "season_explicit": explicit is not None, "players": players, "total_salary": total_salary,
                        "total_points": total_points, "flags": flags})
    if not lineups:
        raise ValueError("no winning-lineup headings found")
    # The page has repeated a table under two week headings (2024 weeks 7 and
    # 9, found 2026-10-15). Both rows are kept; the later week in page order
    # (the earlier week number) is flagged so a reader does not count one
    # lineup twice.
    seen: dict[tuple, tuple[int, int]] = {}
    for l in lineups:
        key = (l["season"], tuple((p["slot"], p["name"], p["points"]) for p in l["players"]))
        if key in seen:
            l["flags"].append(f"duplicate_of_week_{seen[key][1]}")
        else:
            seen[key] = (l["season"], l["week"])
    return lineups


def summary(lineups: list[dict[str, Any]]) -> str:
    by_season: dict[int, list[int]] = {}
    for l in lineups:
        by_season.setdefault(l["season"], []).append(l["week"])
    lines = [f"{len(lineups)} winning lineups ({VERSION})"]
    for season in sorted(by_season, reverse=True):
        weeks = sorted(by_season[season])
        missing = [w for w in range(1, 19) if w not in weeks]
        lines.append(f"  {season}: weeks {weeks[0]}-{weeks[-1]} ({len(weeks)}); missing {missing or 'none'}")
    flagged = [l for l in lineups if l["flags"]]
    if flagged:
        lines.append("  flagged: " + "; ".join(f"{l['season']} w{l['week']} {','.join(l['flags'])}" for l in flagged))
    return "\n".join(lines)
