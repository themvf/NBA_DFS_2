"""How the field built its lineups -- not just who it owned.

`nfl_dfs_field_ownership` keeps one number per player. A standings export also
lists every entry's whole lineup, and three things only exist at that level:

  * duplication -- how many entries share an identical lineup (the input the
    optimizer's field-duplication model has never had calibration data for);
  * pair co-ownership -- how much more often two players appear together than
    their separate ownership implies. A per-player model cannot represent a
    stack; this is the measurement of one;
  * who fills the field -- a few thousand heavy multi-entry users supply a
    large share of entries, so "ownership" is partly their decisions.

Everything here is a statement about ONE contest, computed from its file. The
raw lineups are not stored (300k rows a contest, and the file is the source);
this summary is the durable, comparable part. `VERSION` is stamped on every
row so a change of definition never silently mixes with older rows.

Not tuned against outcomes and not an ownership prediction: it describes what
the field did. It becomes a training target only after the 4-slate gate in
docs/nfl-ownership-model.md is met.

Pure: no database access, no I/O.
"""

from __future__ import annotations

import re
from collections import Counter
from itertools import combinations
from typing import Any, Iterable, Sequence

VERSION = "nfl-dfs-field-structure-v1"

#: Roster size by format. A parsed lineup of any other size is a parse failure.
SLOTS_BY_FORMAT = {"classic": 9, "showdown": 6}

#: Players considered for pair co-ownership (top N by ownership). A pair table
#: over every player is quadratic and almost all of it is noise below 1%.
PAIR_PLAYER_LIMIT = 40
#: Pairs rarer than this share of entries are not reported.
PAIR_MIN_JOINT = 0.01
PAIR_LIMIT = 300

#: Parse failures above this share mean the export format changed; refuse.
MAX_PARSE_FAILURE_SHARE = 0.001

SLOT_TOKEN = re.compile(r"(?:^|\s)(CPT|FLEX|QB|RB|WR|TE|DST)\s")
ENTRY_NAME = re.compile(r"^(.*?)\s*\((\d+)/(\d+)\)\s*$")

DUP_BUCKETS = ((1, 1, "1"), (2, 2, "2"), (3, 5, "3-5"), (6, 10, "6-10"), (11, 50, "11-50"), (51, 10**9, "51+"))


def parse_lineup(text: str) -> list[tuple[str, str]]:
    """'QB Dak Prescott RB ...' -> [(slot, name), ...] in file order."""
    parts = SLOT_TOKEN.split(str(text or "").strip())
    # split() with one group yields [prefix, slot, rest, slot, rest, ...]
    return [(parts[i], parts[i + 1].strip()) for i in range(1, len(parts) - 1, 2)]


def parse_entry_name(name: str) -> tuple[str, int | None]:
    """'user (13/20)' -> ('user', 20). A name without the suffix is its own user."""
    match = ENTRY_NAME.match(str(name or "").strip())
    if not match:
        return str(name or "").strip(), None
    return match.group(1), int(match.group(3))


def _bucket(count: int) -> str:
    for low, high, label in DUP_BUCKETS:
        if low <= count <= high:
            return label
    return DUP_BUCKETS[-1][2]


def analyze(entries: Iterable[Sequence[Any]], fmt: str) -> dict[str, Any]:
    """Summarize a contest. `entries` yields (rank, entry_name, lineup_text).

    Raises ValueError if the lineup format does not parse -- a silent partial
    summary would be mistaken for a measurement of the whole field.
    """
    if fmt not in SLOTS_BY_FORMAT:
        raise ValueError(f"unknown format {fmt!r}")
    size = SLOTS_BY_FORMAT[fmt]

    lineups: list[tuple[int, str, tuple[str, ...], tuple[str, ...]]] = []
    failures = 0
    for rank, entry_name, text in entries:
        parsed = parse_lineup(text)
        if len(parsed) != size:
            failures += 1
            continue
        names = tuple(name for _, name in parsed)
        # Order-free identity for duplication. Slots are kept in the key for
        # showdown, where the captain is part of what makes a lineup distinct.
        key = tuple(sorted(f"{slot}:{name}" if fmt == "showdown" else name for slot, name in parsed))
        lineups.append((int(rank), entry_name, key, names))

    total = len(lineups) + failures
    if total == 0:
        raise ValueError("no entries")
    if failures / total > MAX_PARSE_FAILURE_SHARE:
        raise ValueError(f"{failures} of {total} lineups did not parse as {fmt}; the export format may have changed")
    n = len(lineups)

    # -- who fills the field ------------------------------------------------
    per_user: Counter[str] = Counter()
    max_entries: Counter[int] = Counter()
    for _, entry_name, _, _ in lineups:
        user, cap = parse_entry_name(entry_name)
        per_user[user] += 1
        if cap is not None:
            max_entries[cap] += 1
    heavy = {threshold: sum(c for c in per_user.values() if c >= threshold) / n for threshold in (5, 20)}
    users = {
        "distinct": len(per_user),
        "entries_per_user_median": float(sorted(per_user.values())[len(per_user) // 2]),
        "entries_per_user_max": max(per_user.values()),
        "users_ge_20": sum(1 for c in per_user.values() if c >= 20),
        "entry_share_from_users_ge_5": round(heavy[5], 4),
        "entry_share_from_users_ge_20": round(heavy[20], 4),
        "entry_share_by_max_entries": {str(k): round(v / n, 4) for k, v in max_entries.most_common(8)},
    }

    # -- duplication --------------------------------------------------------
    counts = Counter(key for _, _, key, _ in lineups)
    dup_of = [counts[key] for _, _, key, _ in lineups]
    entries_hist: Counter[str] = Counter(_bucket(c) for c in dup_of)
    lineups_hist: Counter[str] = Counter(_bucket(c) for c in counts.values())
    by_rank = sorted(range(n), key=lambda i: lineups[i][0])
    top = by_rank[: max(1, n // 100)]           # top 1% of the finishing order
    duplication = {
        "unique_lineups": len(counts),
        "entry_share_in_duplicated_lineup": round(sum(1 for c in dup_of if c > 1) / n, 4),
        "max_duplicates": max(counts.values()),
        "entries_by_duplicate_count": {label: entries_hist.get(label, 0) for _, _, label in DUP_BUCKETS},
        "lineups_by_duplicate_count": {label: lineups_hist.get(label, 0) for _, _, label in DUP_BUCKETS},
        "top_1pct_entry_share_duplicated": round(sum(1 for i in top if dup_of[i] > 1) / len(top), 4),
        "top_1pct_entries": len(top),
    }

    # -- pair co-ownership --------------------------------------------------
    own: Counter[str] = Counter()
    for _, _, _, names in lineups:
        own.update(set(names))
    leaders = [name for name, _ in own.most_common(PAIR_PLAYER_LIMIT)]
    leader_set = set(leaders)
    joint: Counter[tuple[str, str]] = Counter()
    for _, _, _, names in lineups:
        present = sorted(leader_set.intersection(names))
        if len(present) > 1:
            joint.update(combinations(present, 2))
    pairs = []
    for (a, b), count in joint.items():
        share = count / n
        if share < PAIR_MIN_JOINT:
            continue
        expected = (own[a] / n) * (own[b] / n)
        pairs.append({"a": a, "b": b, "joint": round(share, 4),
                      "own_a": round(own[a] / n, 4), "own_b": round(own[b] / n, 4),
                      "lift": round(share / expected, 3) if expected else None})
    pairs.sort(key=lambda p: -(p["lift"] or 0) * p["joint"])

    return {
        "version": VERSION,
        "format": fmt,
        "entries": n,
        "parse_failures": failures,
        "users": users,
        "duplication": duplication,
        "pairs": {"player_limit": PAIR_PLAYER_LIMIT, "min_joint": PAIR_MIN_JOINT, "rows": pairs[:PAIR_LIMIT]},
    }
