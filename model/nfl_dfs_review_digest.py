"""Render a stored weekly report card as Markdown for a CI run summary.

Pure: takes a report payload, returns text. No database, no formatting of
anything it was not given. Reads the same frozen rows the Weekly Player Review
renders, so the two can never disagree about who moved.
"""
VERSION = "nfl-dfs-review-digest-v1"
VARIANT_LABELS = {"production": "Production model", "shadow_baseline": "Market-free baseline",
                  "opportunity": "Opportunity candidate", "efficiency_research": "Workload + efficiency research"}
FLEX_POSITIONS = ("RB", "WR", "TE")
POSITIONS = ("QB", "RB", "WR", "TE", "DST")


def _num(value):
    return value if isinstance(value, (int, float)) and value == value and abs(value) != float("inf") else None


def delta(row):
    """actual - projected, from the frozen `error`. None unless actually scored."""
    return None if row.get("actual") is None else _num(row.get("error"))


def _fmt(value, places=1):
    return "—" if value is None else f"{value:.{places}f}"


def _signed(value):
    if value is None:
        return "—"
    # Round first, then sign: a value that rounds to zero is neither positive
    # nor negative at this precision, and "-0.0" reads as a bias that isn't there.
    rounded = round(value, 1) + 0.0
    return "0.0" if rounded == 0 else f"{rounded:+.1f}"


def rows_for(report, variant):
    return [r for r in report.get("rows", []) if r.get("variant") == variant]


def accuracy(rows):
    """MAE/bias/coverage over scored rows only. None rather than 0 when empty."""
    scored = [r for r in rows if delta(r) is not None]
    intervals = [r for r in scored if r.get("interval_hit") is not None]
    n = len(scored)
    return {"n": n, "players": len(rows),
            "mae": sum(abs(delta(r)) for r in scored) / n if n else None,
            "bias": sum(delta(r) for r in scored) / n if n else None,
            "coverage": sum(bool(r["interval_hit"]) for r in intervals) / len(intervals) if intervals else None,
            "overdue": sum(bool(r.get("overdue")) for r in rows)}


def movers(rows, limit=10):
    """Split on the SIGN of the delta so no player can appear in both lists."""
    scored = [r for r in rows if delta(r) is not None]
    key = lambda r: (delta(r), r.get("name") or "")
    return ([r for r in sorted(scored, key=key, reverse=True) if delta(r) > 0][:limit],
            [r for r in sorted(scored, key=key) if delta(r) < 0][:limit])


def _mover_table(title, rows):
    if not rows:
        return [f"**{title}**", "", "_No scored player moved in this direction yet._", ""]
    lines = [f"**{title}**", "", "| # | Player | Proj | Final | Delta |", "|--:|---|--:|--:|--:|"]
    for i, r in enumerate(rows, 1):
        forecast = r.get("forecast") or {}
        lines.append(f"| {i} | {r.get('name','?')} ({r.get('position','?')} · {r.get('team','?')}) "
                     f"| {_fmt(_num(forecast.get('mean')))} | {_fmt(_num(r.get('actual')))} | {_signed(delta(r))} |")
    return lines + [""]


def _pct(value):
    return "—" if value is None else f"{value * 100:.0f}%"


def members_of(rows, position):
    """FLEX is an eligibility, not a position a player has."""
    wanted = FLEX_POSITIONS if position == "FLEX" else (position,)
    return [r for r in rows if r.get("position") in wanted]


def render(report, variant="production", limit=10):
    rows = rows_for(report, variant)
    head = accuracy(rows)
    out = [f"## NFL DFS — Week {report.get('week','?')}, {report.get('season','?')}", "",
           f"{VARIANT_LABELS.get(variant, variant)} · evaluated {report.get('evaluated_at','?')} · "
           f"{report.get('completed_games','?')}/{report.get('scheduled_games','?')} games complete", ""]
    if not rows:
        return "\n".join(out + [f"_No `{variant}` rows in this report card._", ""])
    out += ["| Scored | MAE | Actual − projected | P10–P90 coverage | Overdue |",
            "|--:|--:|--:|--:|--:|",
            f"| {head['n']} / {head['players']} | {_fmt(head['mae'], 2)} | {_signed(head['bias'])} "
            f"| {_pct(head['coverage'])} | {head['overdue']} |", ""]
    if head["n"] == 0:
        return "\n".join(out + ["_Nothing is scored yet — no games have produced usable results._", ""])
    exceeded, disappointed = movers(rows, limit)
    out += _mover_table(f"Top {limit} — exceeded projection", exceeded)
    out += _mover_table(f"Top {limit} — disappointed", disappointed)
    out += ["**By position**", "", "| Pos | Scored | MAE | Actual − projected |", "|---|--:|--:|--:|"]
    for position in POSITIONS + ("FLEX",):
        stats = accuracy(members_of(rows, position))
        if stats["n"]:
            out.append(f"| {position} | {stats['n']} | {_fmt(stats['mae'], 2)} | {_signed(stats['bias'])} |")
    return "\n".join(out + ["", f"_{report.get('missing_policy','')}_", ""])
