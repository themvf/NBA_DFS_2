"""Fetch tennis match schedules + Vegas odds into tennis_matches (both tours).

Odds only, from The Odds API. The Odds API scopes tennis by TOURNAMENT
(tennis_atp_wimbledon, tennis_atp_us_open, ...), so active tournament keys are
**auto-discovered** each run from the free ``/v4/sports`` endpoint — hardcoding
Wimbledon meant the whole tennis pipeline went dark the day the final was
played, with the US Open series starting two weeks later.

The feed carries fixtures (commence time, both player names) alongside three
markets, so one bulk call per tournament seeds both the schedule and the lines:

  * ``h2h``     — 2-way moneyline (no draw).  Vig removed across the two sides.
  * ``totals``  — total games O/U (e.g. 22.5).
  * ``spreads`` — game/set handicap for the favorite (e.g. -4.5 / -1.5).

Exact decision policy must use the per-book quote trail: Pinnacle is the
fair-price reference and DraftKings is the preferred execution price when both
are present. Legacy match fields remain consensus compatibility fields only.

Consensus is computed by averaging in IMPLIED-PROBABILITY space across all books
(averaging American odds arithmetically is invalid).  Player names are stored
inline from the feed — no separate players table for the odds-only MVP.

Per-book prices are appended to game_odds_history (books JSONB) for the
sharp-movement/CLV instrumentation, and matches that have already started are
skipped so closing lines stay frozen (the feed serves live prices in-play —
the 6h cadence vs ~2.5h matches made this a routine overwrite before).

Usage:
    python -m ingest.tennis_schedule                    # all upcoming, both tours
    python -m ingest.tennis_schedule --tour atp         # one tour
    python -m ingest.tennis_schedule --date 2026-06-29  # one match-day (UTC)
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import insert_game_odds_history_rows
from ingest.tennis_foundation import ingest_live_event_quotes
from model.soccer_bet_rating import american_to_prob, prob_to_american

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
# us + uk + eu = DraftKings/FanDuel, UK books, Pinnacle (sharp reference).
#
# us_ex REMOVED 2026-08-24. It is billed like any other region (cost =
# markets x regions, so it was 25% of every tennis odds call: 12 credits
# instead of 9) and it returned Polymarket on 0 of 2,838 paid tennis
# captures in the preceding 30 days -- measured, not assumed. Every one of
# the 577 Polymarket tennis rows in game_odds_history came from
# ingest/polymarket_tennis.py, which reads Polymarket's own Gamma API for
# FREE. We were paying for a feed we already get for nothing, and not even
# receiving it. This is also why detector health reports
# tennis/pinnacle_polymarket_delta as DEAD (0 alerts ever): the data never
# arrived on this path. Re-adding us_ex would need evidence the provider
# has started serving tennis exchange prices -- check the books JSONB, not
# the docs.
REGIONS = "us,uk,eu"


class TennisOddsDiscoveryError(RuntimeError):
    """The Odds API could not be queried to establish Tennis coverage."""


def discover_tournaments(api_key: str) -> list[tuple[str, str, str]]:
    """Active tennis tournaments from the free /v4/sports endpoint.

    Returns [(tour 'ATP'|'WTA', sport_key, tournament title), ...]. An empty
    result means no provider-covered active tournament was captured; it does not
    establish that the real ATP/WTA calendar has no events. Raises
    TennisOddsDiscoveryError on an API failure so callers do not mislabel an
    unhealthy pipeline as missing provider coverage.
    """
    try:
        r = requests.get(f"{ODDS_BASE}/sports", params={"apiKey": api_key}, timeout=20)
        r.raise_for_status()
        sports = r.json()
    except requests.RequestException as e:
        logger.warning("Odds API /sports discovery failed: %s", e)
        raise TennisOddsDiscoveryError("Odds API Tennis tournament discovery failed") from e
    out = []
    for s in sports:
        key = s.get("key", "")
        if not s.get("active"):
            continue
        if key.startswith("tennis_atp_"):
            out.append(("ATP", key, s.get("title") or key))
        elif key.startswith("tennis_wta_"):
            out.append(("WTA", key, s.get("title") or key))
    return out


def _debug_dump_discovery(api_key: str) -> None:
    """One-off diagnostic: print EVERY /v4/sports entry whose key or title
    mentions tennis, regardless of the tennis_atp_*/tennis_wta_* prefix
    filter or active flag discover_tournaments() applies. No DB writes.

    Exists to answer a real question raised live 2026-08-24: is a tennis
    tournament genuinely missing from The Odds API's feed, or is our own
    prefix/active filter silently dropping something the provider actually
    offers? discover_tournaments() already queries ALL of /v4/sports (no
    server-side sport filter) and filters client-side -- this dump shows
    that same raw response unfiltered, so the two are directly comparable."""
    r = requests.get(f"{ODDS_BASE}/sports", params={"apiKey": api_key}, timeout=20)
    r.raise_for_status()
    sports = r.json()
    print(f"/v4/sports: {len(sports)} total entries (all sports, not just tennis)")
    tennis_like = [
        s for s in sports
        if "tennis" in str(s.get("key", "")).lower() or "tennis" in str(s.get("title", "")).lower()
    ]
    print(f"{len(tennis_like)} entries mentioning 'tennis' (key or title), any active state:")
    for s in tennis_like:
        matches_filter = str(s.get("key", "")).startswith(("tennis_atp_", "tennis_wta_"))
        print(
            f"  key={s.get('key')!r} title={s.get('title')!r} "
            f"active={s.get('active')} group={s.get('group')!r} "
            f"matches_our_prefix_filter={matches_filter}"
        )


def _consensus_american(prices: list[int]) -> int | None:
    """Consensus American odds by averaging in implied-probability space."""
    if not prices:
        return None
    avg_prob = sum(american_to_prob(p) for p in prices) / len(prices)
    return prob_to_american(avg_prob)


def _two_way_probs(home_ml: int | None, away_ml: int | None) -> tuple[float | None, float | None]:
    """Vig-removed home/away probabilities from a 2-way moneyline."""
    if home_ml is None or away_ml is None:
        return None, None
    rh, ra = american_to_prob(home_ml), american_to_prob(away_ml)
    total = rh + ra
    if total <= 0:
        return None, None
    return round(rh / total, 4), round(ra / total, 4)


def _consensus_handicap_line(points: list[float]) -> float | None:
    """Most common handicap line across books (tennis spreads cluster tightly)."""
    if not points:
        return None
    # Round to nearest 0.5 and take the mode; ties → median-ish first.
    from collections import Counter
    rounded = [round(p * 2) / 2 for p in points]
    return Counter(rounded).most_common(1)[0][0]


def fetch_tournament(
    db: DatabaseManager,
    api_key: str,
    tour_label: str,
    sport_key: str,
    tournament: str,
    game_date: str | None,
    *,
    event_ids: list[str] | None = None,
    bookmakers: str | None = None,
    markets: str = "h2h,totals,spreads",
    request_audit: dict | None = None,
) -> int:
    """Fetch one tournament's fixtures + odds, upsert into tennis_matches. Returns count."""
    try:
        params = {
            "apiKey": api_key,
            "markets": markets,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        else:
            params["regions"] = REGIONS
        if event_ids:
            params["eventIds"] = ",".join(sorted(set(event_ids)))
        resp = requests.get(
            f"{ODDS_BASE}/sports/{sport_key}/odds/",
            params=params,
            timeout=20,
        )
        if request_audit is not None:
            request_audit.update({
                "endpoint": str(resp.url).split("?", 1)[0],
                "status": resp.status_code,
                "requests_last": resp.headers.get("x-requests-last"),
                "requests_used": resp.headers.get("x-requests-used"),
                "requests_remaining": resp.headers.get("x-requests-remaining"),
            })
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API %s request failed: %s", sport_key, e)
        return 0

    upserted = 0
    skipped_live = 0
    now = datetime.now(timezone.utc)
    captured_at = now.replace(microsecond=0)
    capture_key = captured_at.isoformat()
    history_rows: list[dict] = []
    for ev in events:
        commence_iso = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not commence_iso or not home or not away:
            continue
        try:
            commence_dt = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        # In-play guard: after first serve the feed serves LIVE prices, and with
        # a 6h cadence vs ~2.5h matches an unguarded run routinely replaced the
        # closing line. Started matches keep their last pre-match odds.
        if commence_dt <= now:
            skipped_live += 1
            continue
        ev_date = commence_dt.astimezone(timezone.utc).date().isoformat()
        if game_date and ev_date != game_date:
            continue

        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        over_prices: list[int] = []
        under_prices: list[int] = []
        hcap_points: list[float] = []
        hcap_home_prices: list[int] = []
        hcap_away_prices: list[int] = []
        books_detail: dict[str, dict] = {}
        books = ev.get("bookmakers") or []

        for bm in books:
            book = books_detail.setdefault(bm.get("key", "?"), {"last_update": bm.get("last_update")})
            for market in bm.get("markets", []):
                key = market.get("key")
                outs = market.get("outcomes", [])
                if key == "h2h":
                    for o in outs:
                        if o.get("name") == home:
                            home_prices.append(o["price"])
                            book["ml_home"] = o["price"]
                        elif o.get("name") == away:
                            away_prices.append(o["price"])
                            book["ml_away"] = o["price"]
                elif key == "totals":
                    over = next((o for o in outs if o.get("name") == "Over"), None)
                    under = next((o for o in outs if o.get("name") == "Under"), None)
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                        over_prices.append(over["price"])
                        book["total_line"] = float(over["point"])
                        book["over"] = over.get("price")
                        if under:
                            under_prices.append(under["price"])
                            book["under"] = under.get("price")
                elif key == "spreads":
                    h = next((o for o in outs if o.get("name") == home), None)
                    a = next((o for o in outs if o.get("name") == away), None)
                    if h and h.get("point") is not None:
                        hcap_points.append(float(h["point"]))
                        hcap_home_prices.append(h["price"])
                        book["spread_home"] = float(h["point"])
                        book["spread_price"] = h.get("price")
                        book["spread_home_price"] = h.get("price")
                        if a:
                            hcap_away_prices.append(a["price"])
                            book["spread_away"] = float(a["point"]) if a.get("point") is not None else None
                            book["spread_away_price"] = a.get("price")

        home_ml = _consensus_american(home_prices)
        away_ml = _consensus_american(away_prices)
        p_home, p_away = _two_way_probs(home_ml, away_ml)
        total_raw = sum(total_points) / len(total_points) if total_points else None
        total_line = round(total_raw * 2) / 2 if total_raw is not None else None
        over_odds = _consensus_american(over_prices)
        under_odds = _consensus_american(under_prices)
        set_handicap = _consensus_handicap_line(hcap_points)
        hcap_home = _consensus_american(hcap_home_prices)
        hcap_away = _consensus_american(hcap_away_prices)

        row = db.execute_one(
            """
            INSERT INTO tennis_matches (
                game_id, tour, tournament, match_date, commence_time,
                home_player, away_player, home_ml, away_ml,
                home_win_prob, away_win_prob, total_games_line, over_odds, under_odds,
                set_handicap, handicap_home_odds, handicap_away_odds, n_books, fetched_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (game_id) DO UPDATE SET
                commence_time = EXCLUDED.commence_time,
                home_ml = EXCLUDED.home_ml,
                away_ml = EXCLUDED.away_ml,
                home_win_prob = EXCLUDED.home_win_prob,
                away_win_prob = EXCLUDED.away_win_prob,
                total_games_line = EXCLUDED.total_games_line,
                over_odds = EXCLUDED.over_odds,
                under_odds = EXCLUDED.under_odds,
                set_handicap = EXCLUDED.set_handicap,
                handicap_home_odds = EXCLUDED.handicap_home_odds,
                handicap_away_odds = EXCLUDED.handicap_away_odds,
                n_books = EXCLUDED.n_books,
                fetched_at = NOW()
            RETURNING id
            """,
            (
                ev.get("id"), tour_label, tournament, ev_date, commence_dt, home, away,
                home_ml, away_ml, p_home, p_away, total_line, over_odds, under_odds,
                set_handicap, hcap_home, hcap_away, len(books),
            ),
        )
        upserted += 1
        if row:
            try:
                canonical = ingest_live_event_quotes(
                    db,
                    tour=tour_label,
                    tournament=tournament,
                    raw_event=ev,
                    captured_at=captured_at,
                )
                db.execute(
                    """
                    UPDATE tennis_matches SET
                        canonical_event_id=%s, event_revision_id=%s,
                        home_player_id=%s, away_player_id=%s
                    WHERE id=%s
                    """,
                    (canonical["event_id"], canonical["event_revision_id"],
                     canonical["home_player_id"], canonical["away_player_id"], row["id"]),
                )
            except Exception as exc:  # noqa: BLE001 -- legacy schedule remains available
                logger.exception(
                    "Canonical Tennis event/quote ingestion failed for %s (%s): %s",
                    ev.get("id"), tournament, exc,
                )
            history_rows.append(
                {
                    "sport": "tennis",
                    "matchup_id": row["id"],
                    "event_id": ev.get("id"),
                    "game_date": ev_date,
                    "home_team_name": home,
                    "away_team_name": away,
                    "bookmaker_count": len(books),
                    "home_ml": home_ml,
                    "away_ml": away_ml,
                    "vegas_total": total_line,
                    "vegas_total_raw": total_raw,
                    "vegas_prob_home": p_home,
                    "capture_key": capture_key,
                    "captured_at": captured_at,
                    "books": books_detail or None,
                }
            )

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)
    msg = f"Tennis {tour_label} [{tournament}]: {upserted} matches upserted with Vegas lines"
    if game_date:
        msg += f" for {game_date}"
    if skipped_live:
        msg += f" ({skipped_live} in-play matches skipped — closing lines frozen)"
    print(msg)
    return upserted


def fetch_schedule_and_odds(db: DatabaseManager, api_key: str,
                            tour: str | None = None, game_date: str | None = None) -> int:
    if not api_key:
        raise TennisOddsDiscoveryError("ODDS_API_KEY not set; cannot fetch Tennis schedule")
    tournaments = discover_tournaments(api_key)
    if not tournaments:
        print("Tennis: provider_not_covered — no active tournaments in the Odds API feed")
        return 0
    want = {"atp": "ATP", "wta": "WTA"}.get(tour or "", None)
    total = 0
    for tour_label, sport_key, title in tournaments:
        if want and tour_label != want:
            continue
        total += fetch_tournament(db, api_key, tour_label, sport_key, title, game_date)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch tennis schedules + odds (auto-discovered tournaments)")
    parser.add_argument("--tour", choices=["atp", "wta"], help="One tour only (default: both)")
    parser.add_argument("--date", help="Kickoff date YYYY-MM-DD (UTC). Default: all upcoming")
    parser.add_argument(
        "--debug-discovery", action="store_true",
        help="Dump every /v4/sports entry mentioning tennis, unfiltered (diagnostic, no DB writes)",
    )
    args = parser.parse_args()

    config = load_config()

    if args.debug_discovery:
        _debug_dump_discovery(config.odds_api.api_key)
    else:
        db = DatabaseManager(config.database_url)
        fetch_schedule_and_odds(db, config.odds_api.api_key, args.tour, args.date)
