"""DraftKings payout tables, and the score-to-payout curve from complete standings.

stat-api stopped serving flagship standings in full on 2026-10-19 (top 100 rows
only), so it can no longer supply a score curve or a payout table. Two sources
still can, together:

- DraftKings' public contest endpoint (`/contests/v1/contests/{id}`, no auth)
  carries the contest's prize table by RANK (`payoutSummary`) and its entry fee.
- A DraftKings contest-standings export (only for contests you entered) carries
  every entry's points, so it maps rank to score.

Composing them gives the same score-to-payout curve `model/statapi_contest.py`
builds from complete stat-api standings, in the same shape, so the ghost grader
and anything else reading `payout_curve` need no second code path.

Ties: DraftKings splits a tied group's prizes evenly over the places the group
occupies. A tied entry is therefore paid the mean of those places' payouts.
"""

from __future__ import annotations

from model.statapi_contest import build_score_curve, payout_curve

VERSION = "dk-contest-payouts-v1"


def payout_table(payout_summary: list[dict] | None) -> list[list[float]]:
    """[[min rank, max rank, payout per place], ...] from DraftKings' payoutSummary.

    A place's payout is the sum of its payoutDescriptions' `value` (cash, plus a
    ticket's face value when DraftKings prices it). Places paying nothing are
    left out. Empty when the summary is missing.
    """
    out = []
    for tier in payout_summary or []:
        try:
            lo, hi = int(tier["minPosition"]), int(tier["maxPosition"])
        except (KeyError, TypeError, ValueError):
            continue
        value = sum(float(d.get("value") or 0) for d in tier.get("payoutDescriptions") or [])
        if value > 0 and 1 <= lo <= hi:
            out.append([lo, hi, round(value, 2)])
    return sorted(out)


def payout_at(table: list[list[float]], place: int) -> float:
    for lo, hi, value in table:
        if lo <= place <= hi:
            return value
    return 0.0


def paid_rows(points: list[float], table: list[list[float]]) -> list[dict]:
    """Every entry's points and the payout DraftKings gave it, ties split evenly.

    `points` is every entry's score (any order). Entries are placed best first;
    a group of equal scores occupying places p..q is each paid the mean payout
    of places p..q.
    """
    scores = sorted((p for p in points if p is not None), reverse=True)
    rows, i = [], 0
    while i < len(scores):
        j = i
        while j + 1 < len(scores) and scores[j + 1] == scores[i]:
            j += 1
        first, last = i + 1, j + 1
        share = sum(payout_at(table, place) for place in range(first, last + 1)) / (last - first + 1)
        rows.extend({"points": scores[i], "payout": round(share, 2)} for _ in range(first, last + 1))
        i = j + 1
    return rows


def curves(points: list[float], payout_summary: list[dict] | None, entry_fee: float | None) -> dict:
    """score_curve and payout_curve for a contest whose every entry's points are known.

    `payout_curve` is None (with the reason) when DraftKings returned no prize table.
    """
    table = payout_table(payout_summary)
    score_curve = build_score_curve(list(points)) or None
    if not table:
        return {"score_curve": score_curve, "payout_curve": None, "reason": "no prize table from DraftKings"}
    curve = payout_curve(paid_rows(points, table), float(entry_fee or 0))
    if curve is not None:
        curve["source"] = VERSION
        curve["paid_places"] = max(hi for _, hi, _ in table)
    return {"score_curve": score_curve, "payout_curve": curve,
            "reason": None if curve else "no entry reached a paid place"}
