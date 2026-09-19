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

RANKED_FAMILIES: tuple[str, ...] = (
    "highest_scoring_game", "lowest_scoring_game",
    "highest_scoring_team", "lowest_scoring_team",
    "most_passing_yards", "most_receiving_yards", "most_rushing_yards",
    "first_td_scorer", "first_qb_td_pass", "first_qb_int",
)

# DK's "All Teams to Score ..." markets. A different KIND of question: there is
# nothing to rank, so the only honest answer is a probability, computed by
# multiplying a fitted per-team rate across the teams in the window rather than
# derived from any projected mean. See model/nfl_team_event_fit.py.
PROPOSITION_FAMILIES: tuple[str, ...] = (
    "all_teams_td", "all_teams_two_td", "all_teams_fg", "all_teams_td_and_fg",
    "all_teams_passing_td", "all_teams_rushing_td", "all_teams_score",
)

FAMILIES: tuple[str, ...] = RANKED_FAMILIES + PROPOSITION_FAMILIES
MAGNITUDE_FAMILIES = RANKED_FAMILIES[:7]   # Layers A+B only
TIMING_FAMILIES = RANKED_FAMILIES[7:]      # need Layer C

# family -> the event key in artifacts/nfl_team_event_rates.json
PROPOSITION_EVENT: dict[str, str] = {
    "all_teams_td": "td",
    "all_teams_two_td": "two_td",
    "all_teams_fg": "fg",
    "all_teams_td_and_fg": "td_and_fg",
    "all_teams_passing_td": "passing_td",
    "all_teams_rushing_td": "rushing_td",
    "all_teams_score": "any_points",
}

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
    "most_rushing_yards": "player",
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
    "most_rushing_yards": frozenset({"RB", "QB", "WR"}),
    "first_td_scorer": frozenset({"QB", "RB", "WR", "TE"}),
}

# Exclusion is by scope, never by down-weighting: a 4:25 kickoff is not in the
# 1pm market at all.
# DK slices Sunday four ways, and they are NOT interchangeable. In particular
# its "1pm, 4.05pm & 4.25pm" market is `sunday_main`, which EXCLUDES Sunday
# Night Football -- comparing our `sunday_all` number to that market would be
# comparing a 15-game slate to a 14-game one.
SLATE_SCOPES: dict[str, object] = {
    "sunday_all": lambda kickoff_et: kickoff_et.weekday() == 6,
    "sunday_1pm": lambda kickoff_et: kickoff_et.weekday() == 6 and kickoff_et.hour == 13,
    "sunday_late": lambda kickoff_et: kickoff_et.weekday() == 6 and kickoff_et.hour == 16,
    "sunday_main": lambda kickoff_et: kickoff_et.weekday() == 6 and kickoff_et.hour in (13, 16),
}

SCOPE_LABELS: dict[str, str] = {
    "sunday_all": "All Sunday games",
    "sunday_1pm": "1pm ET only",
    "sunday_late": "4.05 & 4.25pm ET",
    "sunday_main": "1pm, 4.05 & 4.25pm ET",
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

# What each family ranks by, and where that number comes from. `proxy` means the
# ranking stat is a stand-in for the question rather than an answer to it.
#
# Measured over 2023-2025 regular seasons (walk-forward, means from prior weeks
# only), the highest projected player led the week:
#
#     most_receiving_yards   11.1%   actual leader's median rank 15   top-20: 60%
#     most_passing_yards     15.6%   actual leader's median rank 11   top-20: 78%
#     any touchdown (proxy)   4.4%   actual leader's median rank 25   top-20: 47%
#
# That is ~90x better than chance among ~840 candidates, so the ordering carries
# real signal -- and the leader is still usually not our number one. Four
# ranking keys were screened (mean, prior max, prior p90, P(>= a slate-winning
# threshold)); none beat the mean, so the mean stays. Present these as deep
# ranked lists, never as a pick.
RANKING_STAT: dict[str, tuple[str, bool]] = {
    "highest_scoring_game": ("expected_total_points", False),
    "lowest_scoring_game": ("expected_total_points", False),
    "highest_scoring_team": ("implied_team_points", False),
    "lowest_scoring_team": ("implied_team_points", False),
    "most_passing_yards": ("passing_yards", False),
    "most_receiving_yards": ("receiving_yards", False),
    "most_rushing_yards": ("rushing_yards", False),
    # Expected touchdowns is not P(scores first): that needs drive order and
    # clock, which is Layer C. Ranked and labelled as a proxy until then.
    "first_td_scorer": ("expected_touchdowns", True),
    "first_qb_td_pass": ("passing_tds", True),
    "first_qb_int": ("passing_interceptions", True),
}

# Families ranked ascending -- the question asks for the lowest, not the highest.
ASCENDING_FAMILIES: frozenset[str] = frozenset({"lowest_scoring_game", "lowest_scoring_team"})

# How deep to publish. Chosen from the coverage measured above: a shallow list
# would hide the actual leader most weeks.
BOARD_DEPTH: dict[str, int] = {
    "highest_scoring_game": 16, "lowest_scoring_game": 16,
    "highest_scoring_team": 32, "lowest_scoring_team": 32,
    "most_passing_yards": 32, "most_receiving_yards": 50, "most_rushing_yards": 40,
    "first_td_scorer": 60, "first_qb_td_pass": 32, "first_qb_int": 32,
}


for _family, _event in PROPOSITION_EVENT.items():
    SELECTION_KIND[_family] = "proposition"
    RANKING_STAT[_family] = (f"p_{_event}", False)
    BOARD_DEPTH[_family] = 1


def ranking_stat(family: str) -> tuple[str, bool]:
    """Return ``(stat_key, is_proxy)`` for a family."""
    if family not in RANKING_STAT:
        raise ValueError(f"unknown specials family {family!r}; expected one of {list(FAMILIES)}")
    return RANKING_STAT[family]
