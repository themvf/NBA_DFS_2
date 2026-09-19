"""NFL slate specials: frozen contract constants.

This module is the single source of truth for which DK specials families the
programme covers, what kind of thing each family's selections are, and how a
slate scope is defined. The simulator itself (``build_slate``/``simulate``/
``readout``) lands in P0.5 -- see ``docs/nfl-slate-specials-handoff.md`` §3.1.

It exists this early, holding only constants, because P0's market-capture tool
(``ingest/nfl_specials_market.py``) has to validate a ``--family`` argument
against the same tuple the simulator will read out. Declaring FAMILIES in two
places would be the ``DETECTOR_REGISTRY`` problem from CLAUDE.md -- two copies
kept in sync by hand -- and inside one language there is no excuse for it.
"""

from __future__ import annotations

from datetime import datetime

MODEL_VERSION = "nfl-specials-v1"
N_DRAWS = 50_000

FAMILIES: tuple[str, ...] = (
    "highest_scoring_game", "lowest_scoring_game",
    "highest_scoring_team", "lowest_scoring_team",
    "most_passing_yards", "most_receiving_yards",
    "first_td_scorer", "first_qb_td_pass", "first_qb_int",
)
MAGNITUDE_FAMILIES = FAMILIES[:6]   # Layers A+B only
TIMING_FAMILIES = FAMILIES[6:]      # need Layer C

# Families whose DK overround, measured in P0, was wide enough that our number
# cannot be compared to a fair price: they are read for calibration only and
# never rated above 1 star. EMPTY UNTIL MEASURED -- P0's gate (§7) fills it
# from two Sundays of real captures. Do not populate it from a guess.
FAMILIES_CALIBRATION_ONLY: frozenset[str] = frozenset()

# What a selection in each family names. The capture tool resolves a pasted
# label to a selection key according to this, and the readout keys its
# probabilities the same way, so ledger rows join to captures without fuzzy
# matching at grade time (§3.4).
SELECTION_KIND: dict[str, str] = {
    "highest_scoring_game": "game",
    "lowest_scoring_game": "game",
    "highest_scoring_team": "team",
    "lowest_scoring_team": "team",
    "most_passing_yards": "player",
    "most_receiving_yards": "player",
    "first_td_scorer": "player",
    "first_qb_td_pass": "player",
    "first_qb_int": "player",
}

# Positions a family's selections can legally have. Used to narrow name
# matching at capture time: "most passing yards" is a quarterback market, so a
# running back who happens to share a surname is not a candidate. A family with
# no entry accepts any offensive position.
FAMILY_POSITIONS: dict[str, frozenset[str]] = {
    "most_passing_yards": frozenset({"QB"}),
    "first_qb_td_pass": frozenset({"QB"}),
    "first_qb_int": frozenset({"QB"}),
    "most_receiving_yards": frozenset({"WR", "TE", "RB"}),
    "first_td_scorer": frozenset({"QB", "RB", "WR", "TE"}),
}

# Exclusion is by scope, never by down-weighting: a 4:25 kickoff is not in the
# 1pm market at all.
SLATE_SCOPES: dict[str, object] = {
    "sunday_all": lambda kickoff_et: kickoff_et.weekday() == 6,
    "sunday_1pm": lambda kickoff_et: kickoff_et.weekday() == 6 and kickoff_et.hour == 13,
}


def in_scope(scope: str, kickoff_et: datetime) -> bool:
    """True when a kickoff (in US/Eastern) belongs to ``scope``."""
    if scope not in SLATE_SCOPES:
        raise ValueError(f"unknown slate scope {scope!r}; expected one of {sorted(SLATE_SCOPES)}")
    return bool(SLATE_SCOPES[scope](kickoff_et))


def selection_kind(family: str) -> str:
    if family not in SELECTION_KIND:
        raise ValueError(f"unknown specials family {family!r}; expected one of {list(FAMILIES)}")
    return SELECTION_KIND[family]
