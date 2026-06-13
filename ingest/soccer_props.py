"""Fetch World Cup player markets (first scorer + anytime scorer) per event.

The Odds API encodes these as: outcome name="Yes", description=<player>, price=
american.  Only the "Yes" side is posted.  Both markets list players from BOTH
teams mixed together with no team tag — so the first-scorer model uses each
player's SHARE of total expected goals (vig approximately cancels in the share)
rather than per-team splits, which avoids needing rosters.

Returns parsed consensus structures; the modeling/rating lives in
model/soccer_first_scorer.py.  Player props are only fetched for near-term
fixtures (books don't post them far out, and it keeps Odds API credit use low).
"""

from __future__ import annotations

import logging
import unicodedata

import requests

from model.soccer_bet_rating import american_to_decimal, american_to_prob

logger = logging.getLogger(__name__)

SPORT_KEY = "soccer_fifa_world_cup"
REGIONS = "us,uk,eu"
ODDS_BASE = "https://api.the-odds-api.com/v4"

_ANYTIME = "player_goal_scorer_anytime"
_FIRST = "player_first_goal_scorer"
# Description text marking the non-player "no goalscorer" outcome.
_NO_SCORER_HINTS = ("no goalscorer", "no goal scorer", "no goal")


def norm_player(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return " ".join(text.lower().split())


def fetch_player_markets(api_key: str, event_id: str, regions: str = REGIONS) -> dict | None:
    """Fetch + parse anytime and first-scorer markets for one event.

    Returns:
        {
          "anytime": {norm: {"name": str, "prob_raw": float, "book_count": int}},
          "first":   {norm: {"name": str, "prob_vigfree": float,
                              "best_odds": int, "best_book": str, "book_count": int}},
          "no_scorer_prob": float,   # vig-free P(no goalscorer), if posted
        }
    or None if the request failed.  Markets absent from the feed yield empty dicts.
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{event_id}/odds",
            params={
                "apiKey": api_key,
                "regions": regions,
                "markets": f"{_ANYTIME},{_FIRST}",
                "oddsFormat": "american",
            },
            timeout=25,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.debug("Player markets fetch failed for %s: %s", event_id, e)
        return None

    # Accumulate implied probs across all books.
    anytime_acc: dict[str, dict] = {}        # norm → {name, prob_sum, n}
    first_acc: dict[str, dict] = {}          # norm → {name, prob_sum, n, best_decimal, best_odds, best_book}
    no_scorer_sum, no_scorer_n = 0.0, 0

    for bm in data.get("bookmakers") or []:
        book = bm.get("key")
        for mk in bm.get("markets", []):
            key = mk.get("key")
            if key not in (_ANYTIME, _FIRST):
                continue
            for o in mk.get("outcomes", []):
                desc = (o.get("description") or "").strip()
                price = o.get("price")
                if price is None or not desc:
                    continue
                low = desc.lower()
                prob = american_to_prob(price)
                if any(h in low for h in _NO_SCORER_HINTS):
                    # "No goalscorer" leg — counts toward the first-scorer overround only.
                    if key == _FIRST:
                        no_scorer_sum += prob
                        no_scorer_n += 1
                    continue
                npl = norm_player(desc)
                if key == _ANYTIME:
                    e = anytime_acc.setdefault(npl, {"name": desc, "prob_sum": 0.0, "n": 0})
                    e["prob_sum"] += prob
                    e["n"] += 1
                else:  # first scorer
                    dec = american_to_decimal(price)
                    e = first_acc.setdefault(
                        npl,
                        {"name": desc, "prob_sum": 0.0, "n": 0,
                         "best_decimal": 0.0, "best_odds": None, "best_book": None},
                    )
                    e["prob_sum"] += prob
                    e["n"] += 1
                    if dec > e["best_decimal"]:
                        e["best_decimal"] = dec
                        e["best_odds"] = price
                        e["best_book"] = book

    anytime = {
        npl: {"name": e["name"], "prob_raw": e["prob_sum"] / e["n"], "book_count": e["n"]}
        for npl, e in anytime_acc.items() if e["n"] > 0
    }

    # Vig-free first-scorer probs: normalize average implied by the overround
    # (sum of all player Yes legs + the no-goalscorer leg).
    first_avg = {npl: e["prob_sum"] / e["n"] for npl, e in first_acc.items() if e["n"] > 0}
    no_scorer_avg = (no_scorer_sum / no_scorer_n) if no_scorer_n else 0.0
    overround = sum(first_avg.values()) + no_scorer_avg
    first = {}
    for npl, e in first_acc.items():
        if e["n"] == 0:
            continue
        first[npl] = {
            "name": e["name"],
            "prob_vigfree": (first_avg[npl] / overround) if overround > 0 else first_avg[npl],
            "best_odds": e["best_odds"],
            "best_book": e["best_book"],
            "book_count": e["n"],
        }

    return {
        "anytime": anytime,
        "first": first,
        "no_scorer_prob": (no_scorer_avg / overround) if overround > 0 else 0.0,
    }
