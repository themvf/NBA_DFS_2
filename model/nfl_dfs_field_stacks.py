"""How much of a DraftKings field stacks -- the input the portfolio field model lacks.

`nfl-portfolio-objective-v1` failed its freeze gate because its sampled field
draws opponents slot by slot from the ownership prior: nobody in that field
stacks, so its right tail sits 24-28 points under the realized top 1%
(docs/nfl-portfolio-objective-study.md). The registration names the missing
input as a MEASURED field-wide stacking rate. This module measures it; it
does not tune anything against realized thresholds.

Two populations, and they must never be read as the same thing:

  * Classic: stat-api labels EVERY standings row with its QB stack shape
    (`QB/2WR/TE/BB` = QB with two WRs and a TE from his team plus one
    bring-back from the opponent; `None` = no QB stack). With the full
    standings fetched this is the whole field, 160k entries.
  * Showdown: stat-api labels no showdown row at all (checked live
    2026-10-15 on three flagships, first and 50,000th rank). The only rosters
    we hold are the top finishers' every lineup (`nfl_dfs_field_user_builds`),
    so a showdown rate is measured from THAT population and stamped
    `population: "top_users"`. It over-represents winners; a sampler that
    reads it must say so.

Pure: no database access, no I/O. `VERSION` is stamped on every summary so a
change of definition never mixes with older rows in `nfl_dfs_field_structure`.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Iterable

VERSION = "nfl-dfs-field-stacks-v1"

#: Rank buckets reported beside the whole population. "top_1pct" is the top 1%
#: of the finishing order (at least one entry); "top_100" the first 100 ranks.
RANK_BUCKETS = ("all", "top_1pct", "top_100")

_PART = re.compile(r"^(\d*)(QB|WR|TE|RB|BB)$")


def parse_stack_label(label: str | None) -> dict[str, int] | None:
    """'QB/2WR/TE/BB' -> {'qb': 1, 'wr': 2, 'te': 1, 'rb': 0, 'bb': 1}; None -> None (no QB stack).

    Raises ValueError on a token outside stat-api's vocabulary, so a changed
    label format is a loud failure rather than a silently thinner field.
    """
    if label is None or str(label).strip() == "":
        return None
    out = {"qb": 0, "wr": 0, "te": 0, "rb": 0, "bb": 0}
    for token in str(label).split("/"):
        match = _PART.match(token.strip())
        if not match:
            raise ValueError(f"unknown stack token {token!r} in {label!r}")
        count = int(match.group(1) or 1)
        out[match.group(2).lower()] += count
    if out["qb"] != 1:
        raise ValueError(f"stack label without exactly one QB: {label!r}")
    return out


def _bucket_indices(ranks: list[int]) -> dict[str, list[int]]:
    order = sorted(range(len(ranks)), key=lambda i: ranks[i])
    n = len(order)
    return {"all": order, "top_1pct": order[: max(1, n // 100)], "top_100": [i for i in order if ranks[i] <= 100]}


def _shape_summary(shapes: list[dict[str, int] | None]) -> dict[str, Any]:
    """Rates over one bucket of parsed classic shapes (None = no QB stack)."""
    n = len(shapes)
    if n == 0:
        return {"entries": 0}
    stacked = [s for s in shapes if s is not None]
    catchers = Counter(min(s["wr"] + s["te"], 3) for s in stacked)  # 3 = "3+"
    bring = Counter(min(s["bb"], 2) for s in stacked)                # 2 = "2+"
    rate = lambda count: round(count / n, 4)
    return {
        "entries": n,
        "qb_stack": rate(len(stacked)),
        "no_qb_stack": rate(n - len(stacked)),
        # Pass catchers (WR+TE) stacked with the QB, as a share of ALL entries.
        "qb_plus_catchers": {str(k) if k < 3 else "3+": rate(v) for k, v in sorted(catchers.items())},
        "qb_plus_te": rate(sum(1 for s in stacked if s["te"] > 0)),
        "qb_plus_rb": rate(sum(1 for s in stacked if s["rb"] > 0)),
        "bring_back": {str(k) if k < 2 else "2+": rate(v) for k, v in sorted(bring.items())},
        "bring_back_any": rate(sum(1 for s in stacked if s["bb"] > 0)),
    }


def classic_field_stacks(standings: Iterable[dict], total_entries: int) -> dict[str, Any]:
    """Field-wide QB-stack rates from stat-api standings rows (`rank`, `stack`).

    `complete` is true only when every entry of the contest was fetched; a
    partial standings pull is reported as such and must not be read as the
    field (the top of the standings stacks more than the bottom).
    """
    ranks: list[int] = []
    shapes: list[dict[str, int] | None] = []
    labels: Counter[str] = Counter()
    for row in standings:
        if row.get("rank") is None:
            continue
        ranks.append(int(row["rank"]))
        shape = parse_stack_label(row.get("stack"))
        shapes.append(shape)
        labels[str(row.get("stack") or "none")] += 1
    n = len(ranks)
    if n == 0:
        raise ValueError("no standings rows")
    buckets = _bucket_indices(ranks)
    return {
        "version": VERSION,
        "format": "classic",
        "population": "field",
        "entries": n,
        "total_entries": int(total_entries or 0),
        "complete": bool(total_entries) and n >= int(total_entries),
        "labels": {label: count for label, count in labels.most_common(40)},
        "by_rank": {name: _shape_summary([shapes[i] for i in idx]) for name, idx in buckets.items()},
    }


def _showdown_shape(roster: list[dict]) -> dict[str, Any] | None:
    """One showdown roster (slot, team, position) -> team split, QB pairing, captain position."""
    teams = Counter(str(p.get("team") or "?") for p in roster)
    if len(roster) != 6 or len(teams) > 2:
        return None
    split = "-".join(str(c) for c in sorted(teams.values(), reverse=True))
    if len(teams) == 1:
        split = "6-0"
    captain = next((p for p in roster if str(p.get("slot") or "").upper() == "CPT"), None)
    qbs = [p for p in roster if str(p.get("position") or "").upper() == "QB"]
    pairing = None
    if qbs:
        qb_team = str(qbs[0].get("team") or "?")
        same = sum(1 for p in roster if str(p.get("team") or "?") == qb_team
                   and str(p.get("position") or "").upper() in ("WR", "TE", "RB"))
        pairing = min(same, 3)
    return {"split": split, "qb_count": len(qbs), "qb_plus_teammates": pairing,
            "captain_position": str((captain or {}).get("position") or "?").upper(),
            "kicker": any(str(p.get("position") or "").upper() == "K" for p in roster),
            "dst": any(str(p.get("position") or "").upper() == "DST" for p in roster)}


def showdown_build_stacks(lineups: Iterable[dict]) -> dict[str, Any]:
    """Showdown shape rates from stored lineups (`rank`, `players`: slot/team/position).

    The population is whoever's lineups we hold -- the top finishers -- never
    the field. The summary says so in `population`.
    """
    ranks: list[int] = []
    shapes: list[dict[str, Any]] = []
    unparsed = 0
    for lineup in lineups:
        shape = _showdown_shape(list(lineup.get("players") or []))
        if shape is None:
            unparsed += 1
            continue
        ranks.append(int(lineup.get("rank") or 10**9))
        shapes.append(shape)
    n = len(shapes)
    if n == 0:
        raise ValueError("no parseable showdown lineups")
    buckets = _bucket_indices(ranks)

    def summary(idx: list[int]) -> dict[str, Any]:
        rows = [shapes[i] for i in idx]
        m = len(rows)
        rate = lambda count: round(count / m, 4)
        with_qb = [s for s in rows if s["qb_count"] > 0]
        return {
            "entries": m,
            "team_split": {k: rate(v) for k, v in sorted(Counter(s["split"] for s in rows).items())},
            "any_qb": rate(len(with_qb)),
            "two_qbs": rate(sum(1 for s in rows if s["qb_count"] >= 2)),
            "qb_plus_teammates": {str(k) if k < 3 else "3+": rate(v)
                                  for k, v in sorted(Counter(s["qb_plus_teammates"] for s in with_qb).items())},
            "captain_position": {k: rate(v) for k, v in sorted(Counter(s["captain_position"] for s in rows).items())},
            "kicker": rate(sum(1 for s in rows if s["kicker"])),
            "dst": rate(sum(1 for s in rows if s["dst"])),
        }

    return {
        "version": VERSION,
        "format": "showdown",
        "population": "top_users",
        "entries": n,
        "unparsed": unparsed,
        "by_rank": {name: summary(idx) for name, idx in buckets.items()},
    }
