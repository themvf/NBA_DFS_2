"""Fetch the World Cup schedule + Vegas odds into soccer_matchups.

Single data source: **The Odds API** (already integrated for NBA/MLB).  Soccer
has no free first-party schedule API, but the Odds API event-odds feed already
carries fixtures (commence time, home/away nation) alongside the markets, so one
call seeds both the schedule and the lines.

Key differences from NBA/MLB:
  * 3-way moneyline — home / Draw / away.  Implied probabilities are vig-removed
    across all three outcomes.
  * ``vegas_total`` is GOALS (e.g. 2.75), not points/runs.
  * ``home_implied`` / ``away_implied`` are expected goals per side, split from
    the total by the supremacy implied by win probability.
  * Nations are auto-created from the feed name when not pre-seeded, with accent
    normalization so "Côte d'Ivoire"/"Ivory Coast" style variants still match.

Usage:
    python -m ingest.soccer_schedule                    # all upcoming WC fixtures
    python -m ingest.soccer_schedule --date 2026-06-13  # one matchday
"""

from __future__ import annotations

import argparse
import logging
import unicodedata
from datetime import date, datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_soccer_team_name_cache,
    upsert_soccer_matchup,
    upsert_soccer_team,
)
from model.soccer_bet_rating import american_to_prob, prob_to_american

logger = logging.getLogger(__name__)

SPORT_KEY = "soccer_fifa_world_cup"
# US books (FanDuel/DraftKings) DO post World Cup markets; uk,eu add depth and
# consensus stability.  Player props (DFS phase) use the same region set.
REGIONS = "us,uk,eu"
ODDS_BASE = "https://api.the-odds-api.com/v4"

# Pinnacle public guest API — no auth, lowest vig (~2%), sharpest book.
_PINNACLE_BASE = "https://guest.api.arcadia.pinnacle.com/0.1"
_PINNACLE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "x-api-key": "CmX2KcMrXuFmNg6YFbmTxE0y9CblMOzm",
}
_PINNACLE_WC_LEAGUE = 2686

# Expected goal difference at a 100%/0% win-prob gap, before clamping.  A 60/20
# home/away split (~0.40 prob gap) → ~0.9 goal supremacy, which matches typical
# World Cup -0.75/-1.0 Asian-handicap favorites.
_SUPREMACY_SCALE = 2.2
_MAX_SUPREMACY = 2.5


def _normalize_name(name: str) -> str:
    """Casefold + strip accents/punctuation for robust nation matching."""
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _resolve_team_id(
    db: DatabaseManager,
    name: str,
    norm_cache: dict[str, int],
) -> int | None:
    """Look up a nation by normalized name; auto-create from the feed if new."""
    if not name:
        return None
    key = _normalize_name(name)
    if key in norm_cache:
        return norm_cache[key]
    team_id = upsert_soccer_team(db, name=name)
    if team_id:
        norm_cache[key] = team_id
    return team_id


def _ml_to_raw(ml: int) -> float:
    """American moneyline → implied probability (with vig)."""
    if ml > 0:
        return 100 / (ml + 100)
    return abs(ml) / (abs(ml) + 100)


def _three_way_probs(
    home_ml: int | None,
    draw_ml: int | None,
    away_ml: int | None,
) -> tuple[float | None, float | None, float | None]:
    """Vig-removed home/draw/away probabilities from a 3-way moneyline."""
    if home_ml is None or draw_ml is None or away_ml is None:
        return None, None, None
    raw_h, raw_d, raw_a = _ml_to_raw(home_ml), _ml_to_raw(draw_ml), _ml_to_raw(away_ml)
    overround = raw_h + raw_d + raw_a
    if overround <= 0:
        return None, None, None
    return (
        round(raw_h / overround, 4),
        round(raw_d / overround, 4),
        round(raw_a / overround, 4),
    )


def _consensus_american(prices: list[int]) -> int | None:
    """Consensus American odds by averaging in IMPLIED-PROBABILITY space.

    Averaging American odds arithmetically is invalid (you cannot mean +100 and
    −120); average the implied probabilities and convert back.
    """
    if not prices:
        return None
    avg_prob = sum(american_to_prob(p) for p in prices) / len(prices)
    return prob_to_american(avg_prob)


def _consensus_total_prices(
    total_books: list[tuple[float, int | None, int | None]],
    consensus_line: float | None,
) -> tuple[int | None, int | None]:
    """Consensus Over/Under prices from books posting the consensus line.

    Only books within 0.13 of the consensus line are used (a 2.5 over price ≠ a
    3.0 over price), and prices are averaged in probability space.
    """
    if not total_books or consensus_line is None:
        return None, None
    over_prices = [op for (ln, op, _up) in total_books
                   if op is not None and abs(ln - consensus_line) < 0.13]
    under_prices = [up for (ln, _op, up) in total_books
                    if up is not None and abs(ln - consensus_line) < 0.13]
    return _consensus_american(over_prices), _consensus_american(under_prices)


def _compute_implied_goals(
    vegas_total: float | None,
    prob_home: float | None,
    prob_away: float | None,
) -> tuple[float | None, float | None]:
    """Split the goal total into per-side expected goals using win supremacy.

    Approximate (V1): the favorite's share scales with (p_home − p_away).  Real
    Poisson/bivariate goal models belong in the projection phase; this is enough
    to drive game-environment scaling for the Vegas model.
    """
    if vegas_total is None:
        return None, None
    if prob_home is None or prob_away is None:
        half = round(vegas_total / 2, 2)
        return half, half
    supremacy = max(-_MAX_SUPREMACY, min(_MAX_SUPREMACY, (prob_home - prob_away) * _SUPREMACY_SCALE))
    # Clamp the favorite's share so neither side's implied goals go negative on a
    # low-total game with extreme supremacy (e.g. total 1.75, supremacy 2.5).
    floor = 0.05
    home_implied = vegas_total / 2 + supremacy / 2
    home_implied = max(floor, min(vegas_total - floor, home_implied))
    away_implied = vegas_total - home_implied
    return round(home_implied, 2), round(away_implied, 2)


def _fetch_pinnacle_h2h(norm_cache: dict[str, int]) -> dict[tuple[int, int], tuple[float, float, float]]:
    """Fetch Pinnacle WC 2026 h2h odds.

    Returns {(home_team_id, away_team_id): (prob_home, prob_draw, prob_away)},
    vig-removed multiplicatively.  Pinnacle's WC vig is ~2%, so these fair
    probabilities are the sharpest comparison point available.
    """
    try:
        r = requests.get(
            f"{_PINNACLE_BASE}/leagues/{_PINNACLE_WC_LEAGUE}/matchups",
            headers=_PINNACLE_HEADERS, timeout=15,
        )
        r.raise_for_status()
        matchups = r.json()
    except requests.RequestException as e:
        logger.warning("Pinnacle h2h fetch failed: %s", e)
        return {}

    # Only regular (non-special) full-match h2h — filter out prop markets
    # (corners, cards) whose team names contain parenthetical qualifiers.
    regular = [
        m for m in matchups
        if not m.get("special")
        and len(m.get("participants", [])) == 2
        and "(" not in m["participants"][0].get("name", "")
        and "(" not in m["participants"][1].get("name", "")
    ]

    result: dict[tuple[int, int], tuple[float, float, float]] = {}
    for m in regular:
        parts = m.get("participants", [])
        home_part = next((p for p in parts if p.get("alignment") == "home"), parts[0])
        away_part = next((p for p in parts if p.get("alignment") == "away"), parts[1])

        home_id = norm_cache.get(_normalize_name(home_part.get("name", "")))
        away_id = norm_cache.get(_normalize_name(away_part.get("name", "")))
        if not home_id or not away_id:
            continue

        try:
            mr = requests.get(
                f"{_PINNACLE_BASE}/matchups/{m['id']}/markets/straight",
                headers=_PINNACLE_HEADERS, timeout=10,
            )
            mr.raise_for_status()
            markets = mr.json()
        except requests.RequestException:
            continue

        for mkt in markets:
            if mkt.get("type") != "moneyline" or mkt.get("period") != 0:
                continue
            # Pinnacle uses designation: "home" / "draw" / "away" (not participantId).
            by_desig = {p.get("designation"): p["price"] for p in mkt.get("prices", [])}
            h_price = by_desig.get("home")
            a_price = by_desig.get("away")
            d_price = by_desig.get("draw")

            if h_price is None or a_price is None:
                continue

            raw_h = _ml_to_raw(h_price)
            raw_a = _ml_to_raw(a_price)
            raw_d = _ml_to_raw(d_price) if d_price is not None else 0.0
            total = raw_h + raw_d + raw_a
            if total <= 0:
                continue

            result[(home_id, away_id)] = (
                round(raw_h / total, 4),
                round(raw_d / total, 4) if d_price is not None else None,
                round(raw_a / total, 4),
            )
            break

    logger.info("Pinnacle h2h: %d matches mapped", len(result))
    return result


def fetch_schedule_and_odds(
    db: DatabaseManager,
    api_key: str,
    game_date: str | None = None,
) -> int:
    """Fetch World Cup fixtures + 3-way odds, upsert into soccer_matchups.

    When game_date is given (YYYY-MM-DD), only fixtures kicking off on that UTC
    date are written; otherwise all upcoming fixtures returned by the feed.
    Returns the number of matchups upserted.
    """
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch soccer schedule")
        return 0

    try:
        resp = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/odds/",
            params={
                "apiKey": api_key,
                "regions": REGIONS,
                "markets": "h2h,totals",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=20,
        )
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API soccer request failed: %s", e)
        return 0

    if not events:
        print("No upcoming World Cup fixtures returned by the odds feed")
        return 0

    # {normalized_name: team_id} for fast matching + in-loop auto-create reuse.
    norm_cache = {
        _normalize_name(name): tid
        for name, tid in build_soccer_team_name_cache(db).items()
    }

    # Fetch Pinnacle h2h once for the whole batch — degrades gracefully if unavailable.
    pinnacle_h2h = _fetch_pinnacle_h2h(norm_cache)

    upserted = 0
    skipped_date = 0
    for ev in events:
        commence_iso = ev.get("commence_time")
        if not commence_iso:
            continue
        try:
            commence_dt = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        ev_date = commence_dt.astimezone(timezone.utc).date().isoformat()
        if game_date and ev_date != game_date:
            skipped_date += 1
            continue

        home_name = ev.get("home_team", "")
        away_name = ev.get("away_team", "")
        if not home_name or not away_name:
            continue

        home_team_id = _resolve_team_id(db, home_name, norm_cache)
        away_team_id = _resolve_team_id(db, away_name, norm_cache)
        if not home_team_id or not away_team_id:
            logger.warning("Could not resolve teams: %s vs %s", home_name, away_name)
            continue

        # Consensus across ALL bookmakers: 3-way h2h + goal totals.
        home_prices: list[int] = []
        draw_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        # Per-book (line, over_price, under_price) so we can average O/U prices at
        # the consensus line for the totals bet model.
        total_books: list[tuple[float, int | None, int | None]] = []
        for bm in ev.get("bookmakers") or []:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        nm = o.get("name", "")
                        if nm == home_name:
                            home_prices.append(o["price"])
                        elif nm == away_name:
                            away_prices.append(o["price"])
                        elif nm.lower() == "draw":
                            draw_prices.append(o["price"])
                elif market["key"] == "totals":
                    over = next((o for o in market.get("outcomes", []) if o.get("name") == "Over"), None)
                    under = next((o for o in market.get("outcomes", []) if o.get("name") == "Under"), None)
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                        total_books.append((
                            float(over["point"]),
                            over.get("price"),
                            under.get("price") if under else None,
                        ))

        home_ml = _consensus_american(home_prices)
        draw_ml = _consensus_american(draw_prices)
        away_ml = _consensus_american(away_prices)
        # Soccer goal totals move in 0.25 steps (2.5, 2.75) — round to nearest 0.25.
        vegas_total = round(sum(total_points) / len(total_points) * 4) / 4 if total_points else None
        over_odds, under_odds = _consensus_total_prices(total_books, vegas_total)

        p_home, p_draw, p_away = _three_way_probs(home_ml, draw_ml, away_ml)
        home_implied, away_implied = _compute_implied_goals(vegas_total, p_home, p_away)

        pin = pinnacle_h2h.get((home_team_id, away_team_id))
        pin_home = pin[0] if pin else None
        pin_draw = pin[1] if pin else None
        pin_away = pin[2] if pin else None

        mid = upsert_soccer_matchup(
            db,
            game_date=ev_date,
            game_id=ev.get("id"),
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            commence_time=commence_dt,
            vegas_total=vegas_total,
            home_ml=home_ml,
            draw_ml=draw_ml,
            away_ml=away_ml,
            vegas_prob_home=p_home,
            vegas_prob_draw=p_draw,
            vegas_prob_away=p_away,
            home_implied=home_implied,
            away_implied=away_implied,
            over_odds=over_odds,
            under_odds=under_odds,
            pinnacle_prob_home=pin_home,
            pinnacle_prob_draw=pin_draw,
            pinnacle_prob_away=pin_away,
        )
        if mid:
            upserted += 1

    msg = f"Soccer: {upserted} fixtures upserted with Vegas lines"
    if game_date:
        msg += f" for {game_date}"
        if skipped_date:
            msg += f" ({skipped_date} other-date fixtures skipped)"
    print(msg)
    return upserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch World Cup schedule + odds")
    parser.add_argument("--date", help="Kickoff date YYYY-MM-DD (UTC). Default: all upcoming")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_schedule_and_odds(db, config.odds_api.api_key, args.date)

    # Write our model predictions (our_* columns) for the same fixtures.
    try:
        from model.soccer_predictions import predict_and_write
        predict_and_write(db, game_date=args.date)
    except Exception as exc:
        logger.warning("Soccer predictions skipped: %s", exc)
