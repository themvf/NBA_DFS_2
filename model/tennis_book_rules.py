"""Versioned Tennis settlement rules for frozen execution books.

Only rules verified from an official book source are automated.  An incomplete
match at any unverified book becomes ``manual_review`` instead of being forced
through a generic rule that can manufacture ROI.
"""

from __future__ import annotations

from typing import Any

RULE_VERSION = "tennis-book-rules-2026-08-29"
DK_RULES_URL = "https://sportsbook.draftkings.com/help/sport-rules/tennis"

_EXECUTION_BOOKS = {
    "draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers",
}


def tennis_rule_snapshot(book: str | None, market: str) -> dict[str, Any]:
    """Metadata frozen with a quote so later rule changes cannot rewrite history."""
    if book == "draftkings":
        mode = ("official_winner_wins_other_void_after_point"
                if market == "moneyline" else "void_unless_unconditionally_determined")
        source = DK_RULES_URL
        verified = True
    else:
        mode = "manual_review_on_incomplete"
        source = None
        verified = False
    return {
        "tennis_rule_version": RULE_VERSION,
        "tennis_rule_book": book,
        "tennis_rule_market": market,
        "tennis_rule_mode": mode,
        "tennis_rule_verified": verified,
        "tennis_rule_source": source,
    }


def settle_tennis_selection(
    *, book: str | None, market: str, selection_side: str,
    winner_side: str | None, completion_status: str | None,
    home_games: int | None = None, away_games: int | None = None,
    line: float | None = None, total_bet: str | None = None,
) -> str | None:
    """Return won/lost/void/manual_review under the frozen book rule."""
    status = completion_status or "unknown"
    if status in {"scheduled", "unknown"}:
        return None
    if status == "completed":
        if market == "moneyline" and winner_side in {"home", "away"}:
            return "won" if selection_side == winner_side else "lost"
        if market == "total" and None not in (home_games, away_games, line) and total_bet:
            total = int(home_games) + int(away_games)
            if total == float(line):
                return "void"
            return "won" if (total > float(line)) == (total_bet == "Over") else "lost"
        return None
    if status in {"walkover", "cancelled"}:
        return "void"
    if status == "awarded":
        return "manual_review"
    if status != "retired":
        return "manual_review"
    if book not in _EXECUTION_BOOKS or book != "draftkings":
        return "manual_review"
    if market == "moneyline":
        if winner_side not in {"home", "away"}:
            return "manual_review"
        # Official DK rule: after at least one completed point, the governing-
        # body winner wins and every other match-moneyline selection is void.
        return "won" if selection_side == winner_side else "void"
    if market == "total":
        if None in (home_games, away_games, line) or not total_bet:
            return "void"
        played = int(home_games) + int(away_games)
        # A crossed line is unconditionally determined without needing the
        # unavailable point-by-point score state. Other retired totals void.
        if played > float(line):
            return "won" if total_bet == "Over" else "lost"
        return "void"
    return "manual_review"
