"""Python port of the NBA points-line readers the Top Scorers page uses.

The page (``web/src/lib/nba-top-scorers.ts`` and ``nba-points-ladder.ts``)
turns each player's stored ``prop_odds_history`` books into a consensus
line, a de-vigged P(over) and, from the alternate ladder, an exceedance
curve S(x) = P(points > x) with floor (P10), ceiling (P90) and P(N+).

The nightly calibration grade (``model/nba_points_calibration.py``) has to
score exactly the numbers the page showed, so this module reproduces the
TypeScript arithmetic step for step. It is pinned to it by
``tests/fixtures/nba_points_ladder_fixture.json``, which
``web/scripts/write-nba-points-ladder-fixture.ts`` writes from the
TypeScript and both ``tests/test_nba_points_ladder.py`` and
``web/scripts/test-nba-points-ladder.ts`` assert against. Change the
TypeScript, regenerate the fixture, and this port must still match.

Ladder rules (see the TypeScript header for the reasoning):

- a rung quoted on both sides at one book is de-vigged as a pair;
- an over-only rung is divided by that book's main-line overround, which
  leaves some longshot margin in the top rungs (high-end chances read high);
- a rung from a book with no two-sided main line is skipped, never guessed;
- per point the median across books, forced non-increasing (pool adjacent
  violators weighted by book count); nothing is extrapolated past the ladder.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any
import unicodedata

LADDER_VERSION = "nba-points-ladder-v1"

THRESHOLDS: tuple[int, ...] = (20, 25, 30, 35, 40)
MIN_CURVE_POINTS = 3
_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})
# Our teams table and The Odds API spell one team differently.
TEAM_ALIASES = {"laclippers": "losangelesclippers"}


def name_key(name: str | None) -> str:
    """Accent-free, lowercase, suffix-free key ("Nikola Jokić" and "Nikola Jokic" match)."""
    text = unicodedata.normalize("NFKD", name or "")
    text = "".join(ch for ch in text if not (0x0300 <= ord(ch) <= 0x036F)).lower()
    text = "".join(ch if ("a" <= ch <= "z" or "0" <= ch <= "9" or ch == " ") else " " for ch in text)
    return "".join(word for word in text.split() if word and word not in _SUFFIXES)


def team_key(name: str | None) -> str:
    key = name_key(name)
    return TEAM_ALIASES.get(key, key)


def valid_american(price: Any) -> bool:
    return isinstance(price, (int, float)) and not isinstance(price, bool) and math.isfinite(price) and (price <= -100 or price >= 100)


def implied(american: float) -> float:
    return -american / (-american + 100) if american < 0 else 100 / (american + 100)


def median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def devig_over(over: Any, under: Any) -> float | None:
    if not valid_american(over) or not valid_american(under):
        return None
    o = implied(over)
    u = implied(under)
    return o / (o + u)


@dataclass(frozen=True)
class BookLine:
    book: str
    line: float
    over: float | None
    under: float | None
    p_over: float | None


def book_lines(books: dict[str, Any] | None) -> list[BookLine]:
    out: list[BookLine] = []
    for book, quote in (books or {}).items():
        if not isinstance(quote, dict) or quote.get("line") is None:
            continue
        try:
            line = float(quote["line"])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(line):
            continue
        over = quote.get("over")
        under = quote.get("under")
        out.append(BookLine(
            book=book,
            line=line,
            over=over if valid_american(over) else None,
            under=under if valid_american(under) else None,
            p_over=devig_over(over, under),
        ))
    return sorted(out, key=lambda b: b.book)


def consensus(lines: list[BookLine]) -> tuple[float, float | None] | None:
    """The line most books quote (median breaks a tie) and the median de-vigged P(over) at it."""
    if not lines:
        return None
    counts: dict[float, int] = {}
    for item in lines:
        counts[item.line] = counts.get(item.line, 0) + 1
    top = max(counts.values())
    modal = [value for value, n in counts.items() if n == top]
    mid = median([item.line for item in lines])
    line = modal[0]
    for value in modal:
        if abs(value - mid) < abs(line - mid):
            line = value
    probs = [item.p_over for item in lines if item.line == line and item.p_over is not None]
    return line, (median(probs) if probs else None)


@dataclass
class ExceedancePoint:
    point: float
    p: float
    books: int


@dataclass
class PointsDistribution:
    curve: list[ExceedancePoint]
    floor: float | None
    ceiling: float | None
    at_least: dict[int, float | None]
    ladder_books: int
    skipped_rungs: int
    version: str = LADDER_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "curve": [{"point": c.point, "p": c.p, "books": c.books} for c in self.curve],
            "floor": self.floor,
            "ceiling": self.ceiling,
            "atLeast": {str(k): v for k, v in self.at_least.items()},
            "ladderBooks": self.ladder_books,
            "skippedRungs": self.skipped_rungs,
        }


def _clamp(p: float) -> float:
    return min(0.999, max(0.001, p))


def non_increasing(points: list[ExceedancePoint]) -> list[ExceedancePoint]:
    """Pool adjacent violators so p never rises with x."""
    blocks: list[dict[str, Any]] = []
    for pt in points:
        blocks.append({"p": pt.p, "w": max(1, pt.books), "members": [pt]})
        while len(blocks) > 1 and blocks[-2]["p"] < blocks[-1]["p"]:
            b = blocks.pop()
            a = blocks.pop()
            w = a["w"] + b["w"]
            blocks.append({"p": (a["p"] * a["w"] + b["p"] * b["w"]) / w, "w": w, "members": a["members"] + b["members"]})
    out: list[ExceedancePoint] = []
    for block in blocks:
        for m in block["members"]:
            out.append(ExceedancePoint(point=m.point, p=block["p"], books=m.books))
    return out


def exceedance_at(curve: list[ExceedancePoint], x: float) -> float | None:
    """S(x) by linear interpolation; None outside the ladder."""
    if not curve or x < curve[0].point or x > curve[-1].point:
        return None
    for i, c in enumerate(curve):
        if c.point == x:
            return c.p
        if c.point > x:
            a = curve[i - 1]
            return a.p + ((x - a.point) / (c.point - a.point)) * (c.p - a.p)
    return None


def quantile_at(curve: list[ExceedancePoint], q: float) -> float | None:
    """The x where S(x) falls to q; None when the ladder does not reach q."""
    if not curve or curve[0].p < q or curve[-1].p > q:
        return None
    for i in range(1, len(curve)):
        a = curve[i - 1]
        b = curve[i]
        if a.p >= q and b.p <= q:
            if a.p == b.p:
                return a.point
            return a.point + ((a.p - q) / (a.p - b.p)) * (b.point - a.point)
    return None


def build_distribution(main: dict[str, dict[str, Any]], ladders: dict[str, dict[str, Any] | None]) -> PointsDistribution | None:
    by_point: dict[float, list[float]] = {}
    order: list[float] = []

    def add(point: float, p: float) -> None:
        if point not in by_point:
            by_point[point] = []
            order.append(point)
        by_point[point].append(_clamp(p))

    # Each book's main line is a rung too, and its overround de-vigs that book's ladder.
    overround: dict[str, float] = {}
    for book, q in main.items():
        line = q.get("line")
        if not isinstance(line, (int, float)) or isinstance(line, bool) or not math.isfinite(line):
            continue
        if not valid_american(q.get("over")) or not valid_american(q.get("under")):
            continue
        o = implied(q["over"])
        u = implied(q["under"])
        overround[book] = o + u
        add(float(line), o / (o + u))

    skipped = 0
    ladder_books = 0
    for book, entry in ladders.items():
        rungs = [
            r for r in ((entry or {}).get("ladder") or [])
            if isinstance(r, dict) and isinstance(r.get("point"), (int, float)) and not isinstance(r.get("point"), bool)
            and math.isfinite(r["point"]) and valid_american(r.get("price"))
        ]
        if not rungs:
            continue
        sides: dict[float, dict[str, float]] = {}
        for r in rungs:
            s = sides.setdefault(float(r["point"]), {})
            if r.get("side") == "over":
                s["over"] = r["price"]
            elif r.get("side") == "under":
                s["under"] = r["price"]
        m = overround.get(book)
        used = 0
        for point, s in sides.items():
            if "over" in s and "under" in s:
                o = implied(s["over"])
                add(point, o / (o + implied(s["under"])))
                used += 1
            elif m is not None and "over" in s:
                add(point, implied(s["over"]) / m)
                used += 1
            elif m is not None and "under" in s:
                add(point, 1 - implied(s["under"]) / m)
                used += 1
            else:
                skipped += 1
        if used > 0:
            ladder_books += 1

    if ladder_books == 0:
        return None
    raw = sorted(
        (ExceedancePoint(point=point, p=median(ps), books=len(ps)) for point, ps in by_point.items()),
        key=lambda c: c.point,
    )
    if len(raw) < MIN_CURVE_POINTS:
        return None
    curve = non_increasing(raw)
    return PointsDistribution(
        curve=curve,
        floor=quantile_at(curve, 0.9),
        ceiling=quantile_at(curve, 0.1),
        at_least={n: exceedance_at(curve, n - 0.5) for n in THRESHOLDS},
        ladder_books=ladder_books,
        skipped_rungs=skipped,
    )


def main_quotes(lines: list[BookLine]) -> dict[str, dict[str, Any]]:
    """The per-book main quotes the page hands to build_distribution."""
    return {b.book: {"line": b.line, "over": b.over, "under": b.under} for b in lines}


@dataclass
class PlayerReading:
    """Everything the page shows for one player, from one capture."""
    line: float
    p_over: float | None
    lines: list[BookLine] = field(default_factory=list)
    distribution: PointsDistribution | None = None


def read_player(books: dict[str, Any] | None, ladder_books: dict[str, Any] | None) -> PlayerReading | None:
    lines = book_lines(books)
    c = consensus(lines)
    if c is None:
        return None
    line, p_over = c
    return PlayerReading(line=line, p_over=p_over, lines=lines,
                         distribution=build_distribution(main_quotes(lines), ladder_books or {}))
