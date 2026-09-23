"""What did the field know that we did not?

## Why this exists

Two 2026 week-2 post-mortems found the same thing twice. Our own availability
evidence was UNKNOWN for all 670 players on the Sunday classic slate, with not
one row marked fresh, and we built 20 lineups anyway; 44% of the players we
projected as active recorded no stat line. On Monday the evidence WAS there --
Puka Nacua was freshly tagged QUESTIONABLE -- and he still took 14 of 40
captain slots and scored 0.0.

In both cases the rest of the field had already resolved it. Nacua was owned by
0.64% of entries. Zay Flowers, whom our own feed had ruled out before a DK
average restored him, was owned by 0.11%. Tua Tagovailoa, boosted to 24.8 by
the redistribution layer, was owned by 0.00% -- literally nobody.

Field ownership is the only read we ever get on what the market believed, and
DraftKings publishes it after the fact in the contest standings export. It
arrives far too late to pick a lineup with. It arrives exactly in time to tell
us where our information was behind, which is the thing that would actually
move results.

## What this is NOT for

Not for fading chalk. Measured on the same contest: the winning entry carried
106% cumulative field ownership, the top 1,000 carried 119%, the field as a
whole 110% -- and our 20 lineups carried 96%. We were already MORE contrarian
than the people who beat us. The winners were not clever about being different;
they were right about who would score. Nothing here should be wired into a
lineup objective as an ownership penalty.

## The signal

Three facts per player, and only one combination is interesting:

    what we thought   (our projection, ranked within his position)
    what the field thought   (%%drafted)
    what happened   (realized DK points)

A player we ranked highly, the field ignored, and who then scored nothing is
the market knowing something we did not. A player we ranked highly, the field
ignored, and who then scored well is a real edge. Counting both over a season
is the point; a single week cannot tell them apart from luck.

## Stated priors, not measurements

`IGNORED_BY_FIELD_PCT` and `POSITION_DEPTH` are judgement calls about what
"the field ignored him" and "we would plausibly have rostered him" mean. They
are deliberately loose and they are NOT tuned against outcomes -- tuning a
detector's thresholds on the results it is meant to detect is how a finding
gets manufactured. Change them for a stated reason, not to make a week look
better.

Pure: no database access, no I/O.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping, Sequence

VERSION = "nfl-dfs-field-audit-v1"

#: Below this share of entries, treat the field as having ignored the player.
#: Nacua landed at 0.64%, Flowers 0.11%, Tua 0.00%.
IGNORED_BY_FIELD_PCT = 1.0

#: How deep at each position we would plausibly have rostered someone across a
#: portfolio. A player outside this by our own projection is not evidence of
#: anything when the field ignores him -- the field ignored him and so did we.
POSITION_DEPTH = {"QB": 12, "RB": 24, "WR": 36, "TE": 12, "DST": 8, "K": 8}

#: ...and an absolute depth cut alone is meaningless on a small slate. A
#: showdown carries about 53 players, so "top 36 receivers" is every receiver,
#: and the first version of this flagged $200 bodies projected at 0.3 points --
#: players the field ignored and so did we, which is agreement, not a blind
#: spot. A player must also project at least this share of the best projection
#: at his position on that slate.
ROSTERABLE_PROJECTION_SHARE = 0.5

#: At or below this many realized points, the player did not produce. DK pays a
#: listed player 0 when he does not appear, so this catches "never played" and
#: "played and did nothing" together -- which is correct, because a lineup does
#: not care which.
NO_PRODUCTION_FPTS = 3.0

VERDICTS = ("MARKET_KNEW", "REAL_EDGE", "UNINFORMATIVE")


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", str(name or "").lower())


# --------------------------------------------------------------------------
# Parsing DraftKings' contest standings export
# --------------------------------------------------------------------------

_SLOT = re.compile(r"\b(CPT|FLEX|QB|RB|WR|TE|DST|K)\s+")


def parse_contest_export(rows: Iterable[Sequence[str]]) -> dict[str, Any]:
    """Split the export into its two side-by-side tables.

    DraftKings writes ONE csv holding two unrelated tables: entries on the left
    (Rank..Lineup) and a per-player ownership table on the right
    (Player, Roster Position, %Drafted, FPTS). Rows past the end of the
    ownership table are narrower, so width is what distinguishes them -- not
    emptiness, and not row order.

    ## The multiplier trap

    In a SHOWDOWN export the FPTS column is slot-specific: a player appears
    once as FLEX carrying his base score and once as CPT carrying 1.5x that.
    Reading whichever row happens to come last silently inflates every captain
    by 50%. Base score is therefore always taken from the FLEX row, and the CPT
    row is used only when a player was exclusively captained.

    In a CLASSIC export the slots do not multiply and every row for a player
    agrees, so the same rule is a no-op there.
    """
    entries: list[dict[str, Any]] = []
    fpts_by_slot: dict[str, dict[str, float]] = defaultdict(dict)
    drafted_by_slot: dict[str, dict[str, float]] = defaultdict(dict)
    display: dict[str, str] = {}

    for row in rows:
        if len(row) >= 6 and str(row[0]).strip():
            try:
                entries.append({
                    "rank": int(row[0]),
                    "entry_name": row[2],
                    "points": float(row[4] or 0),
                    "lineup": row[5],
                })
            except (TypeError, ValueError):
                pass
        if len(row) >= 11 and str(row[7]).strip():
            key = normalize_name(row[7])
            slot = str(row[8]).strip().upper()
            display.setdefault(key, str(row[7]).strip())
            try:
                fpts_by_slot[key][slot] = float(row[10] or 0)
                drafted_by_slot[key][slot] = float(str(row[9] or "0").rstrip("%") or 0)
            except (TypeError, ValueError):
                continue

    players: dict[str, dict[str, Any]] = {}
    for key, by_slot in fpts_by_slot.items():
        if "FLEX" in by_slot:
            base = by_slot["FLEX"]
        elif "CPT" in by_slot and len(by_slot) == 1:
            base = by_slot["CPT"] / 1.5      # only ever captained
        else:
            base = next(iter(by_slot.values()))
        players[key] = {
            "name": display.get(key, key),
            "normalized_name": key,
            # Total share of entries rostering him, across every slot he filled.
            "drafted_pct": round(sum(drafted_by_slot[key].values()), 4),
            "drafted_by_slot": dict(drafted_by_slot[key]),
            "fpts": round(base, 4),
        }

    scores = sorted((e["points"] for e in entries), reverse=True)
    return {
        "entries": entries,
        "players": players,
        "entry_count": len(entries),
        "winning_score": scores[0] if scores else None,
        "median_score": scores[len(scores) // 2] if scores else None,
        "min_score": scores[-1] if scores else None,
    }


def is_showdown(parsed: Mapping[str, Any]) -> bool:
    return any("CPT" in p["drafted_by_slot"] for p in parsed["players"].values())


# --------------------------------------------------------------------------
# The audit
# --------------------------------------------------------------------------

def _rank_within_position(players: Sequence[Mapping[str, Any]]) -> tuple[dict[int, int], dict[str, float]]:
    """1-based rank by our projection within position, plus each position's best.

    Ties broken by salary. The leader is returned so the caller can apply a
    slate-relative floor as well as an absolute depth cut.
    """
    ranks: dict[int, int] = {}
    leaders: dict[str, float] = {}
    by_pos: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for p in players:
        if p.get("our_proj") is not None:
            by_pos[p["position"]].append(p)
    for position, group in by_pos.items():
        ordered = sorted(group, key=lambda p: (-float(p["our_proj"]), -int(p["salary"])))
        leaders[position] = float(ordered[0]["our_proj"]) if ordered else 0.0
        for i, p in enumerate(ordered, 1):
            ranks[p["dk_player_id"]] = i
    return ranks, leaders


def audit_slate(
    slate_players: Sequence[Mapping[str, Any]],
    field: Mapping[str, Mapping[str, Any]],
    *,
    ignored_pct: float = IGNORED_BY_FIELD_PCT,
    no_production: float = NO_PRODUCTION_FPTS,
    depth: Mapping[str, int] = POSITION_DEPTH,
    projection_share: float = ROSTERABLE_PROJECTION_SHARE,
) -> dict[str, Any]:
    """Compare what we thought, what the field thought, and what happened.

    `slate_players` need `dk_player_id`, `name`, `position`, `salary`,
    `our_proj`, `is_out`. A player we ourselves ruled out is excluded: we did
    not disagree with the field about him, we agreed.
    """
    ranks, leaders = _rank_within_position(slate_players)
    rows: list[dict[str, Any]] = []
    unmatched: list[str] = []

    for p in slate_players:
        if p.get("is_out"):
            continue
        if p.get("our_proj") is None:
            continue
        observed = field.get(normalize_name(p["name"]))
        if observed is None:
            # Nobody in the contest rostered him AND DraftKings did not list
            # him. Absent from the table is 0% owned, not unknown.
            observed = {"drafted_pct": 0.0, "fpts": None}
            unmatched.append(p["name"])
        rank = ranks.get(p["dk_player_id"])
        cut = depth.get(p["position"], 0)
        floor = projection_share * leaders.get(p["position"], 0.0)
        rosterable = rank is not None and rank <= cut and float(p["our_proj"]) >= floor
        drafted = float(observed["drafted_pct"])
        fpts = observed.get("fpts")

        if not rosterable or drafted >= ignored_pct or fpts is None:
            verdict = "UNINFORMATIVE"
        elif fpts <= no_production:
            verdict = "MARKET_KNEW"
        else:
            verdict = "REAL_EDGE"

        rows.append({
            "name": p["name"], "position": p["position"], "salary": int(p["salary"]),
            "our_proj": round(float(p["our_proj"]), 2), "our_rank": rank,
            "field_pct": drafted, "actual": fpts, "verdict": verdict,
        })

    flagged = [r for r in rows if r["verdict"] != "UNINFORMATIVE"]
    market_knew = [r for r in flagged if r["verdict"] == "MARKET_KNEW"]
    real_edge = [r for r in flagged if r["verdict"] == "REAL_EDGE"]
    return {
        "version": VERSION,
        "thresholds": {"ignored_pct": ignored_pct, "no_production": no_production,
                       "position_depth": dict(depth), "projection_share": projection_share},
        "rows": rows,
        "flagged": sorted(flagged, key=lambda r: (-r["our_proj"])),
        "summary": {
            "considered": len(rows),
            "flagged": len(flagged),
            "market_knew": len(market_knew),
            "real_edge": len(real_edge),
            # Points we assigned to players the field had written off and who
            # then produced nothing. The cost of being behind, in our own units.
            "projected_points_on_market_knew": round(sum(r["our_proj"] for r in market_knew), 1),
            "unmatched_to_field_table": len(unmatched),
        },
    }


def pooled_summary(audits: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Across slates, because one week cannot separate this from luck.

    The ratio is what matters. Flagging players the field ignores is easy; the
    question is whether they turn out to be our blind spots or our edges, and
    only repetition answers it.
    """
    knew = sum(a["summary"]["market_knew"] for a in audits)
    edge = sum(a["summary"]["real_edge"] for a in audits)
    total = knew + edge
    return {
        "slates": len(audits),
        "market_knew": knew,
        "real_edge": edge,
        "flagged": total,
        "market_knew_share": round(knew / total, 3) if total else None,
        "projected_points_lost": round(
            sum(a["summary"]["projected_points_on_market_knew"] for a in audits), 1),
        # Below this many flagged players the split is not worth reading.
        "descriptive_only": total < 30,
    }
