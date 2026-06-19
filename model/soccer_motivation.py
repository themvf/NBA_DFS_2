"""Matchday-3 motivation / dead-rubber signal for the group stage.

The bivariate-Poisson match model is blind to *game state*: on the final group
matchday it projects an already-qualified team at full strength when they will
rest starters and play for a draw, and treats a must-win side the same as a dead
one. Markets price this; our model doesn't — so we systematically misprice MD3
games (false "over"/favorite edges on dead rubbers).

This module computes each MD3 team's qualification state from the standings we
already have (``soccer_groups`` + completed scores) via exact points-based
clinch enumeration over the 2 remaining group games, and returns a small,
pre-anchor adjustment to the match prediction:

  * **total_factor** (<1) — dampen expected goals when sides have eased off.
  * **sup_shift** (goals, toward home) — tilt supremacy to the more motivated side.

State per team (points-based, top-2 within the group):
  * ``secure``  — clinched a top-2 spot in EVERY remaining-game scenario.
  * ``alive``   — can still reach top-2 and not yet clinched (result matters).
  * ``fringe``  — cannot reach top-2 (only the best-third-placed path remains).

Best-third-placed teams (32 of 48 advance) keep *some* motivation, so ``fringe``
is treated as reduced — not zero. The adjustment is deliberately modest: the
market already prices most of this, so the value is (a) removing our false edges
on dead rubbers and (b) catching games where the market under-adjusts.
"""

from __future__ import annotations

import itertools
import logging
from collections import defaultdict

from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# Motivation scores used for the supremacy tilt (higher = tries harder).
_MOTIV = {"alive": 2, "fringe": 1, "secure": 0}
_SUP_SCALE = 0.12          # goals of supremacy per unit motivation gap (max ~0.24)

# Expected-goals dampening by the pair of states (lower = quieter game).
_TOTAL_FACTOR = {
    frozenset(["secure", "secure"]): 0.90,   # textbook dead rubber
    frozenset(["secure", "alive"]): 0.95,
    frozenset(["secure", "fringe"]): 0.93,
    frozenset(["fringe", "fringe"]): 0.96,
    frozenset(["alive", "fringe"]): 1.00,
    frozenset(["alive", "alive"]): 1.00,     # full intensity
}


def _standings(db: DatabaseManager) -> dict[int, dict]:
    """Points per team from completed *intra-group* games."""
    grp = {r["team_id"]: r["group_label"] for r in db.execute(
        "SELECT team_id, group_label FROM soccer_groups")}
    pts: dict[int, int] = defaultdict(int)
    played: dict[int, int] = defaultdict(int)
    for c in db.execute(
        "SELECT home_team_id h, away_team_id a, home_score hs, away_score s "
        "FROM soccer_matchups WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    ):
        if grp.get(c["h"]) and grp.get(c["h"]) == grp.get(c["a"]):
            played[c["h"]] += 1
            played[c["a"]] += 1
            if c["hs"] > c["s"]:
                pts[c["h"]] += 3
            elif c["hs"] < c["s"]:
                pts[c["a"]] += 3
            else:
                pts[c["h"]] += 1
                pts[c["a"]] += 1
    return {t: {"group": g, "pts": pts[t], "played": played[t]} for t, g in grp.items()}


def _clinch_states(team_pts: dict[int, int], remaining: list[tuple[int, int]]) -> dict[int, str]:
    """Classify each of a group's 4 teams as secure / alive / fringe.

    Enumerates all 3^len(remaining) outcomes of the remaining group games on
    points only, with pessimistic vs optimistic tie handling:
      * secure  — guaranteed top-2 in every scenario (loses all tiebreaks).
      * fringe  — cannot reach top-2 in any scenario (wins all tiebreaks).
      * alive   — otherwise.
    """
    teams = list(team_pts)
    guaranteed_top2 = {t: True for t in teams}   # AND across scenarios
    possible_top2 = {t: False for t in teams}    # OR across scenarios

    for combo in itertools.product((0, 1, 2), repeat=len(remaining)):
        final = dict(team_pts)
        for (h, a), outcome in zip(remaining, combo):
            if outcome == 0:
                final[h] += 3
            elif outcome == 1:
                final[h] += 1
                final[a] += 1
            else:
                final[a] += 3
        for t in teams:
            above = sum(1 for o in teams if o != t and final[o] > final[t])
            tied = sum(1 for o in teams if o != t and final[o] == final[t])
            # pessimistic (worst) rank = above + tied + 1; optimistic = above + 1
            if above + tied >= 2:
                guaranteed_top2[t] = False
            if above < 2:
                possible_top2[t] = True

    states = {}
    for t in teams:
        if guaranteed_top2[t]:
            states[t] = "secure"
        elif not possible_top2[t]:
            states[t] = "fringe"
        else:
            states[t] = "alive"
    return states


def compute_motivation(db: DatabaseManager, game_date: str | None = None) -> dict[int, dict]:
    """Return {matchup_id: {state, label, total_factor, sup_shift, home_state, away_state}}
    for upcoming MD3 group fixtures (both teams have played exactly 2 group games)."""
    standings = _standings(db)
    where = "sm.game_date = %s" if game_date else "sm.game_date >= CURRENT_DATE"
    params: tuple = (game_date,) if game_date else ()
    upcoming = db.execute(
        f"""
        SELECT sm.id, sm.home_team_id h, sm.away_team_id a,
               hn.name hname, an.name aname
        FROM soccer_matchups sm
        JOIN soccer_teams hn ON hn.team_id = sm.home_team_id
        JOIN soccer_teams an ON an.team_id = sm.away_team_id
        WHERE {where} AND sm.home_score IS NULL
        """,
        params,
    )

    # Group the upcoming intra-group fixtures by group label.
    by_group: dict[str, list[dict]] = defaultdict(list)
    for u in upcoming:
        gh = standings.get(u["h"], {}).get("group")
        ga = standings.get(u["a"], {}).get("group")
        if gh and gh == ga:
            by_group[gh].append(u)

    out: dict[int, dict] = {}
    for label, fixtures in by_group.items():
        # MD3 = the group's final round: exactly 2 remaining games, both teams on 2 played.
        if len(fixtures) != 2:
            continue
        members = [t for t, s in standings.items() if s["group"] == label]
        if len(members) != 4 or any(standings[t]["played"] != 2 for t in members):
            continue
        team_pts = {t: standings[t]["pts"] for t in members}
        remaining = [(f["h"], f["a"]) for f in fixtures]
        states = _clinch_states(team_pts, remaining)

        for f in fixtures:
            hs, as_ = states[f["h"]], states[f["a"]]
            total_factor = _TOTAL_FACTOR.get(frozenset([hs, as_]), 1.0)
            sup_shift = _SUP_SCALE * (_MOTIV[hs] - _MOTIV[as_])
            if hs == as_ == "secure":
                lbl = "Dead rubber — both through"
            elif hs == "secure" or as_ == "secure":
                secure_team = f["hname"] if hs == "secure" else f["aname"]
                lbl = f"{secure_team} through — may rest"
            elif hs == as_ == "alive":
                lbl = "Win-and-in — both motivated"
            else:
                lbl = f"{hs}/{as_}"
            out[f["id"]] = {
                "state": "dead_rubber" if total_factor < 0.94 else "live",
                "label": lbl,
                "total_factor": round(total_factor, 3),
                "sup_shift": round(sup_shift, 3),
                "home_state": hs,
                "away_state": as_,
            }
    return out
