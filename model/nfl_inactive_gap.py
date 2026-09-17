"""How often does a Questionable player never take the field?

Pure. The DB layer hands over statuses, kickoffs, projections and results;
this decides what they mean and sizes the prize.

The measurement is a proxy, and the proxy is the point of the anchors below.
We cannot see the inactive list — it is not in any feed we ingest — so
"recorded no stat line" stands in for "did not play". That proxy is imperfect:
a receiver who dresses and runs no route also has no row. Reporting OUT and
HEALTHY alongside QUESTIONABLE is what makes the middle number readable — OUT
should be near 100% and HEALTHY near zero, and if they are not, the proxy is
broken and the headline should not be believed.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

VERSION = "nfl-inactive-gap-v1"
ANCHOR_ORDER = ("OUT", "DOUBTFUL", "QUESTIONABLE", "HEALTHY")


def status_before_kickoff(captures: Iterable[Mapping[str, Any]] | None, commence) -> str | None:
    """Newest status captured before this player's own kickoff. Same rule the
    projection uses, so the measurement describes what the model actually saw."""
    if not captures or commence is None:
        return None
    eligible = [c for c in captures if c.get("captured_at") is not None and c["captured_at"] < commence]
    if not eligible:
        return None
    return max(eligible, key=lambda c: c["captured_at"]).get("status")


def summarize(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """rows: {status, played (bool), projected (float|None), week}."""
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        status = row.get("status")
        if not status:
            continue
        bucket = buckets.setdefault(status, {"status": status, "n": 0, "no_show": 0,
                                             "projected_on_no_shows": 0.0, "weeks": set()})
        bucket["n"] += 1
        bucket["weeks"].add(row.get("week"))
        if not row.get("played"):
            bucket["no_show"] += 1
            bucket["projected_on_no_shows"] += float(row.get("projected") or 0.0)
    for bucket in buckets.values():
        bucket["no_show_rate"] = bucket["no_show"] / bucket["n"] if bucket["n"] else None
        bucket["weeks"] = len(bucket["weeks"])
        bucket["points_per_week"] = (bucket["projected_on_no_shows"] / bucket["weeks"]
                                     if bucket["weeks"] else None)
    ordered = [buckets[s] for s in ANCHOR_ORDER if s in buckets]
    ordered += [b for s, b in sorted(buckets.items()) if s not in ANCHOR_ORDER]
    return {"version": VERSION, "buckets": ordered}


def proxy_is_credible(buckets: Iterable[Mapping[str, Any]]) -> tuple[bool, str]:
    """The anchors decide whether the headline can be believed at all.

    OUT players are definitionally absent, so if they are showing up with stat
    lines the join is wrong. HEALTHY players mostly play, so a high no-show
    rate there means the proxy is catching bench players rather than absences.
    """
    by_status = {b["status"]: b for b in buckets}
    out, healthy = by_status.get("OUT"), by_status.get("HEALTHY")
    if not out or out["n"] < 10:
        return False, "too few OUT players to anchor the proxy"
    if out["no_show_rate"] < 0.9:
        return False, (f"only {out['no_show_rate']:.0%} of OUT players recorded no stat line; "
                       "expected near 100% — the join or the status read is wrong")
    if healthy and healthy["n"] >= 25 and healthy["no_show_rate"] > 0.5:
        return False, (f"{healthy['no_show_rate']:.0%} of HEALTHY players recorded no stat line; "
                       "the proxy is measuring bench time, not absence")
    return True, "OUT and HEALTHY anchors behave as expected"


def render(summary: Mapping[str, Any], season: int, weeks: Iterable[int]) -> str:
    buckets = summary["buckets"]
    weeks = sorted(w for w in weeks if w is not None)
    span = f"week {weeks[0]}" if len(weeks) == 1 else f"weeks {weeks[0]}–{weeks[-1]}" if weeks else "no weeks"
    out = [f"## Inactive gap — {season}, {span}", "",
           "How often a pre-kickoff designation was followed by no stat line at all.", ""]
    if not buckets:
        return "\n".join(out + ["_No players carried a pre-kickoff status. Nothing to measure._", ""])

    credible, reason = proxy_is_credible(buckets)
    out += ["| Status | Players | No stat line | Rate | Projected points wasted | Per week |",
            "|---|--:|--:|--:|--:|--:|"]
    for b in buckets:
        out.append(f"| {b['status']} | {b['n']} | {b['no_show']} | {b['no_show_rate']:.0%} "
                   f"| {b['projected_on_no_shows']:.1f} | {b['points_per_week']:.1f} |")
    out.append("")

    if not credible:
        out += [f"> **Do not read the Questionable row yet — {reason}.**", ""]
        return "\n".join(out)

    q = next((b for b in buckets if b["status"] == "QUESTIONABLE"), None)
    out += [f"_Anchors check out: {reason}._", ""]
    if q and q["n"]:
        out += [f"**{q['no_show']} of {q['n']} Questionable players ({q['no_show_rate']:.0%}) never "
                f"recorded a stat line**, and we projected {q['projected_on_no_shows']:.1f} points "
                f"across them — about **{q['points_per_week']:.1f} points a week**.", "",
                "That is the ceiling on what an inactive feed could recover, before any of it is",
                "actually reachable: only the inactives published before a slate locks can change",
                "a lineup, so the realistic gain is a fraction of this.", ""]
    else:
        out += ["_No Questionable players in this sample._", ""]
    out += ["_Proxy: no stat line stands in for inactive, because no feed we ingest carries the "
            "inactive list. A player who dressed and never touched the ball counts as a no-show here._", ""]
    return "\n".join(out)
