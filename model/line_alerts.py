"""Sharp line-movement alerts — detect, notify, and AUDIT (Edge-Finding P2).

Runs after every odds capture. Three game-line detectors over
game_odds_history (plus prop detectors below):

  * **pinnacle_divergence** — Pinnacle's vig-free probability sits >=
    _PIN_GAP_MIN_PP off the retail-consensus probability on some side of an
    upcoming game. Pinnacle is the sharp reference; the side it prices HIGHER
    than retail is the sharp side, and retail is offering a stale price on it.
    (The "Sharp side" chip in the Line Movement panel.)
  * **pinnacle_polymarket_delta** — Pinnacle's vig-free probability differs
    from Polymarket by >= 2pp; the higher Pinnacle side is frozen for audit.
  * **steam** — between the last two captures, >= _STEAM_MIN_BOOKS books moved
    the same side by >= _STEAM_MIN_MOVE_PP. Synchronized moves are informed
    money; solo moves are book position management. (Confirmed "Jump".)
  * **walking** — the retail consensus has drifted >= _WALK_MIN_PP toward a
    side since OPEN (the first capture of the fixture), a slow walk rather than
    a single-interval jump. (The "Walking" chip in the Line Movement panel.)
    First-breach: recorded once, at the first capture where the drift clears
    the threshold, so its outcome/CLV audit is honest and pre-registered.

Every alert is an IMMUTABLE ledger row frozen at trigger time (first breach
only — re-scans never rewrite it), then graded by ``settle``:

    clv_pp  — close_prob − alert_prob: did the market close toward the flagged
              side? The primary audit metric; converges in days.
    outcome — did the flagged side win the game (soccer graded on the
              90-minute regulation score, matching betting convention).

``report`` prints the backtest by sport × alert type. If an alert type shows
no positive CLV, its thresholds are noise — retire or retune them; that is the
whole point of making alerts a ledger instead of a toast.

Notifications: if TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID are set, new alerts
are pushed via the Telegram bot API. Absent secrets = silent skip; the ledger
row is written regardless, so the audit never depends on delivery.

Usage:
    python -m model.line_alerts --sport mlb          # scan + settle + notify
    python -m model.line_alerts --sport soccer
    python -m model.line_alerts --report             # backtest, all sports
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from model.line_movement import _MATCHUP_TBL, _book_fair_home
from model.soccer_bet_rating import american_to_decimal
from model.tennis_book_rules import settle_tennis_selection, tennis_rule_snapshot

logger = logging.getLogger(__name__)

_PIN_GAP_MIN_PP = 2.0     # Pinnacle vs retail consensus, probability points
_PIN_POLY_GAP_MIN_PP = 2.0  # Pinnacle vs Polymarket, probability points
_STEAM_MIN_BOOKS = 3      # books moving together between consecutive captures
_STEAM_MIN_MOVE_PP = 1.5  # per-book move threshold, probability points
_WALK_MIN_PP = 2.0        # consensus drift toward a side since open (slow walk)
_TENNIS_PIN_FORWARD_TYPE = "pinnacle_favorite_forward"
_TENNIS_PIN_FORWARD_VERSION = "tennis-pin-favorite-v1"
_TENNIS_PIN_FORWARD_TARGET = 100
_TENNIS_PIN_FORWARD_MIN_RETAIL_BOOKS = 3
_MODEL_NEUTRAL_GAP_PP = 0.5
_MAX_MODEL_GAP_PP = 15.0
# dk_value: EV of DraftKings' OFFERED price judged by Pinnacle's vig-free fair
# number — EV = pin_fair × dk_decimal − 1. Positive means DK is paying more
# than sharp fair value: directly exploitable at the book the user bets at,
# no model or prediction required. DK's two-way vig is ~4.5%, so clearing +2%
# EV means DK lags Pinnacle by ~4pp of juice-adjusted probability — a real
# stale line, not feed noise.
# Longshot guard: proportional de-vigging OVERSTATES longshot fair probs
# (favorite-longshot bias), so a +1800 price can show fake +5% EV from pure
# de-vig skew — the same tail failure mode that sank the gameline models.
# No dk_value alert above decimal 11 (~+1000); the --dk-board still displays
# everything so nothing is hidden.
_DK_VALUE_MIN_EV = 0.02
_DK_VALUE_MAX_DECIMAL = 11.0
_DK_BOOK = "draftkings"
_NFL_STEAM_LINE_MOVE = 0.5
_NFL_WALK_LINE_MOVE = 1.0

# Sports wired into the alert pipeline. NBA joins when the season resumes —
# nba_matchups has no commence_time column yet, which scan()/settle()/dk_board
# all join on.
_ALERT_SPORTS = ("mlb", "nfl", "soccer", "tennis")

# ── Detector health ──────────────────────────────────────────────────────
# Found via manual DB audit (2026-08-17): scan_tennis_totals had run every
# cycle since it shipped on 2026-07-02 and produced ZERO alerts, ever — 0 of
# 2,891 DraftKings captures for tennis have ever carried a total_line, so
# `if dk.get("total_line") is None: continue` fired silently, forever. A
# "ran fine, found nothing" cycle and a "ran fine, is structurally incapable
# of ever finding anything" cycle look identical in logs — same exit code,
# same "0 new alerts" line. This registry makes that distinction explicit
# instead of relying on someone stumbling onto it by hand again.
#
# `deployed_at` is the date the CODE shipped (from git history), not the date
# of the first alert. Using first-alert-date as a proxy would systematically
# make every detector look newer than it is and under-flag real bugs — a
# detector that took 12 days to find its first legitimate signal is healthy;
# one that never has isn't, and only a code-ship date tells them apart.
_HEALTH_MIN_DAYS = 14          # don't judge a detector before it's had this long
_HEALTH_OPPORTUNITY_DAYS = 14  # "has this sport had eligible games recently"

DETECTOR_REGISTRY: list[dict] = [
    # Generic scan() detectors (moneyline-based, reused across every sport).
    # pinnacle_divergence/dk_value/steam shipped 2026-07-02 (263ec4d/6d6fe75);
    # walking followed 2026-07-07 (c889b1d); NFL joined _ALERT_SPORTS and
    # pinnacle_polymarket_delta shipped for ALL sports together on 2026-08-01
    # (c65fdcc) — so mlb/soccer/tennis's Pin/Poly delta is dated 08-01 too,
    # not 07-02, even though those sports' other detectors are older.
    {"sport": "mlb", "alert_type": "pinnacle_divergence", "deployed_at": date(2026, 7, 2)},
    {"sport": "mlb", "alert_type": "dk_value", "deployed_at": date(2026, 7, 2)},
    {"sport": "mlb", "alert_type": "steam", "deployed_at": date(2026, 7, 2)},
    {"sport": "mlb", "alert_type": "walking", "deployed_at": date(2026, 7, 7)},
    {"sport": "mlb", "alert_type": "pinnacle_polymarket_delta", "deployed_at": date(2026, 8, 1)},
    {"sport": "soccer", "alert_type": "pinnacle_divergence", "deployed_at": date(2026, 7, 2)},
    {"sport": "soccer", "alert_type": "dk_value", "deployed_at": date(2026, 7, 2)},
    {"sport": "soccer", "alert_type": "steam", "deployed_at": date(2026, 7, 2)},
    {"sport": "soccer", "alert_type": "walking", "deployed_at": date(2026, 7, 7)},
    {"sport": "soccer", "alert_type": "pinnacle_polymarket_delta", "deployed_at": date(2026, 8, 1)},
    {"sport": "tennis", "alert_type": "pinnacle_divergence", "deployed_at": date(2026, 7, 2)},
    {"sport": "tennis", "alert_type": _TENNIS_PIN_FORWARD_TYPE, "deployed_at": date(2026, 8, 29)},
    {"sport": "tennis", "alert_type": "dk_value", "deployed_at": date(2026, 7, 2)},
    {"sport": "tennis", "alert_type": "steam", "deployed_at": date(2026, 7, 2)},
    {"sport": "tennis", "alert_type": "walking", "deployed_at": date(2026, 7, 7)},
    {"sport": "tennis", "alert_type": "pinnacle_polymarket_delta", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "pinnacle_divergence", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "dk_value", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "steam", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "walking", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "pinnacle_polymarket_delta", "deployed_at": date(2026, 8, 1)},
    # Prop/derivative detectors (own scan functions, own alert_type strings).
    {"sport": "mlb", "alert_type": "dk_prop_value", "deployed_at": date(2026, 7, 2)},
    {"sport": "mlb", "alert_type": "prop_line_gap", "deployed_at": date(2026, 7, 2)},
    {"sport": "tennis", "alert_type": "dk_prop_value", "deployed_at": date(2026, 7, 2)},
    {"sport": "tennis", "alert_type": "prop_line_gap", "deployed_at": date(2026, 7, 2)},
    {"sport": "nfl", "alert_type": "total_steam", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "spread_steam", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "total_walking", "deployed_at": date(2026, 8, 1)},
    {"sport": "nfl", "alert_type": "spread_walking", "deployed_at": date(2026, 8, 1)},
    # soccer's prop_outlier (ATGS) is intentionally excluded: RETIRED
    # 2026-08-13 as a confirmed loser, not dead — it's supposed to be silent.
]


def check_detector_health(db: DatabaseManager) -> list[dict]:
    """Classify every registered (sport, alert_type) as too_new / no_opportunity
    / dead / active.

    "dead" requires ALL of: deployed >= _HEALTH_MIN_DAYS ago (enough time to
    judge), the sport has had eligible game data in the last
    _HEALTH_OPPORTUNITY_DAYS days (so a dormant sport, e.g. soccer between
    World Cups, isn't mistaken for a broken detector), and zero alerts have
    EVER fired. That's precisely the shape of the scan_tennis_totals bug.
    """
    today = date.today()
    results: list[dict] = []
    for entry in DETECTOR_REGISTRY:
        sport, alert_type, deployed_at = entry["sport"], entry["alert_type"], entry["deployed_at"]
        days_deployed = (today - deployed_at).days
        counted = db.execute_one(
            "SELECT COUNT(*)::int AS n, MAX(created_at) AS last_at "
            "FROM line_alerts WHERE sport=%s AND alert_type=%s",
            (sport, alert_type),
        )
        alerts_ever = int(counted["n"]) if counted else 0
        last_alert_at = counted["last_at"] if counted else None
        opp = db.execute_one(
            "SELECT COUNT(DISTINCT date_trunc('day', captured_at))::int AS days "
            "FROM game_odds_history WHERE sport=%s AND captured_at >= NOW() - (%s || ' days')::interval",
            (sport, _HEALTH_OPPORTUNITY_DAYS),
        )
        opportunity_days = int(opp["days"]) if opp else 0
        if days_deployed < _HEALTH_MIN_DAYS:
            status = "too_new"
        elif opportunity_days == 0:
            status = "no_opportunity"
        elif alerts_ever == 0:
            status = "dead"
        else:
            status = "active"
        results.append({
            "sport": sport,
            "alert_type": alert_type,
            "deployed_at": deployed_at.isoformat(),
            "days_deployed": days_deployed,
            "alerts_ever": alerts_ever,
            "last_alert_at": last_alert_at.isoformat() if last_alert_at else None,
            "opportunity_days": opportunity_days,
            "status": status,
        })
    return results

# Grading sources per sport: (home score col, away score col). Soccer uses the
# 90-minute regulation score — a knockout tie decided in extra time is a DRAW
# for market purposes (the Belgium 3-2 aet lesson). Tennis uses the winner col.
_SCORE_COLS = {
    "mlb": ("home_score", "away_score"),
    "nba": ("home_score", "away_score"),
    "nfl": ("home_score", "away_score"),
    "soccer": ("COALESCE(reg_home_score, home_score)", "COALESCE(reg_away_score, away_score)"),
}


def _game_side_outcome(sport: str, home_score: int, away_score: int, side: str) -> str:
    """Grade a frozen home/away game-line selection from a final score."""
    winner = "home" if home_score > away_score else "away" if away_score > home_score else "draw"
    if sport == "nfl" and winner == "draw":
        return "void"
    return "won" if winner == side else "lost"


def _nfl_line_outcome(
    market: str, side: str, trigger_line: float, home_score: int, away_score: int,
) -> str:
    if market == "spread":
        margin = home_score + trigger_line - away_score
        selection_margin = margin if side == "home" else -margin
    elif market == "total":
        margin = home_score + away_score - trigger_line
        selection_margin = margin if side == "over" else -margin
    else:
        raise ValueError(f"unsupported NFL line market: {market}")
    if abs(selection_margin) < 1e-9:
        return "void"
    return "won" if selection_margin > 0 else "lost"


def _nfl_line_clv(market: str, side: str, trigger_line: float, close_line: float) -> float:
    if market == "spread":
        return trigger_line - close_line if side == "home" else close_line - trigger_line
    if market == "total":
        return close_line - trigger_line if side == "over" else trigger_line - close_line
    raise ValueError(f"unsupported NFL line market: {market}")


def _book_fair_side(book: dict, side: str) -> float | None:
    """Vig-free P(side) from one book's (2- or 3-way) moneyline."""
    try:
        from model.soccer_bet_rating import american_to_prob
        legs = {}
        for leg, key in (("home", "ml_home"), ("away", "ml_away"), ("draw", "ml_draw")):
            v = book.get(key)
            if v is not None:
                legs[leg] = american_to_prob(int(v))
        if side not in legs:
            return None
        total = sum(legs.values())
        return legs[side] / total if total > 0 else None
    except (TypeError, ValueError):
        return None


def _retail_fair_side(books: dict, side: str) -> float | None:
    """Mean vig-free P(side) across retail books, excluding sharp exchanges."""
    probs, _ = _retail_probabilities(books, side)
    return sum(probs) / len(probs) if probs else None


def _retail_probabilities(books: dict, side: str,
                          keys: set[str] | None = None) -> tuple[list[float], set[str]]:
    """Return usable retail probabilities and the books that supplied them."""
    allowed = set(books) if keys is None else keys
    used: set[str] = set()
    probs: list[float] = []
    for key in allowed:
        if key in {"pinnacle", "polymarket"} or key not in books:
            continue
        probability = _book_fair_side(books[key], side)
        if probability is not None:
            used.add(key)
            probs.append(probability)
    return probs, used


def _comparable_retail_probabilities(opening: dict, current: dict, side: str
                                     ) -> tuple[float | None, float | None, int]:
    """Consensus at two times over the exact same retail-book intersection."""
    keys = (set(opening) & set(current)) - {"pinnacle", "polymarket"}
    open_probs, open_used = _retail_probabilities(opening, side, keys)
    current_probs, current_used = _retail_probabilities(current, side, keys)
    used = open_used & current_used
    if not used:
        return None, None, 0
    # Recompute after intersecting successfully parsed quotes; a malformed leg
    # in either snapshot must not alter the composition of just one endpoint.
    open_probs, _ = _retail_probabilities(opening, side, used)
    current_probs, _ = _retail_probabilities(current, side, used)
    return sum(open_probs) / len(open_probs), sum(current_probs) / len(current_probs), len(used)


def _pinnacle_polymarket_signals(books: dict) -> list[dict]:
    """Return sides where Pinnacle prices meaningfully above Polymarket."""
    pin = books.get("pinnacle")
    poly = books.get("polymarket")
    if not pin or not poly:
        return []
    signals = []
    for side in _sides({"pinnacle": pin, "polymarket": poly}):
        pin_prob = _book_fair_side(pin, side)
        poly_prob = _book_fair_side(poly, side)
        if pin_prob is None or poly_prob is None:
            continue
        gap_pp = (pin_prob - poly_prob) * 100
        if gap_pp >= _PIN_POLY_GAP_MIN_PP:
            signals.append({
                "side": side,
                "alert_prob": poly_prob,
                "sharp_prob": pin_prob,
                "details": {
                    "gap_pp": round(gap_pp, 2),
                    "pinnacle_prob": round(pin_prob, 6),
                    "polymarket_prob": round(poly_prob, 6),
                },
            })
    return signals


_SIDE_KEY = {"home": "ml_home", "away": "ml_away", "draw": "ml_draw"}


def _dk_side_odds(book: dict, side: str) -> int | None:
    v = book.get(_SIDE_KEY[side])
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _book_price_prob(book: dict, side: str) -> float | None:
    """The book's OFFERED implied probability (vig included) — what you pay."""
    from model.soccer_bet_rating import american_to_prob
    odds = _dk_side_odds(book, side)
    return american_to_prob(odds) if odds is not None else None


def _dk_value_ev(pin: dict, dk: dict, side: str) -> float | None:
    """EV of a 1-unit bet at DK's price, judged by Pinnacle's vig-free fair prob."""
    fair = _book_fair_side(pin, side)
    odds = _dk_side_odds(dk, side)
    if fair is None or odds is None:
        return None
    return fair * american_to_decimal(odds) - 1


def _sides(books: dict) -> list[str]:
    has_draw = any("ml_draw" in b for b in books.values())
    return ["home", "away", "draw"] if has_draw else ["home", "away"]


def freeze_execution_price(books: dict, *, market: str, side: str) -> dict:
    """Freeze the price this alert could actually have been taken at.

    Without this an alert is ungradable economically -- you can count wins but
    never compute ROI, and a win rate against an ASSUMED -110 is not a result.
    77 of 80 settled NFL alerts had no frozen price before this existed.

    SAME-PROPOSITION IS ENFORCED BY CONSTRUCTION. `_consensus_book_line` is a
    MEAN across books, so it routinely lands on a number (2.83) that no book
    offers -- pricing "at the consensus" would invent a bet. Instead the books
    are grouped by the line they actually post, the MODAL line is taken (the
    most widely available, therefore most genuinely bettable proposition), and
    the best price is chosen among the books at that line only. A spread of
    -2.5 and one of -3.0 are different bets and are never compared.

    Returns {} when nothing is priceable, so the caller records the absence
    rather than fabricating a number.
    """
    if market == "moneyline":
        price_key, line_key = _SIDE_KEY.get(side, ""), None
    elif market == "spread":
        price_key = "spread_home_price" if side == "home" else "spread_away_price"
        line_key = "spread_home" if side == "home" else "spread_away"
    elif market == "total":
        price_key, line_key = ("over" if side == "over" else "under"), "total_line"
    else:
        return {}

    by_line: dict[float | None, list[tuple[str, int, float]]] = {}
    for book_key in _EXECUTION_BOOKS:
        quote = books.get(book_key)
        if not isinstance(quote, dict):
            continue
        line = None
        if line_key is not None:
            raw = quote.get(line_key)
            if raw is None:
                continue
            try:
                line = float(raw)
            except (TypeError, ValueError):
                continue
        price = quote.get(price_key)
        if price is None:
            continue
        try:
            dec = american_to_decimal(int(price))
        except (TypeError, ValueError):
            continue
        by_line.setdefault(line, []).append((book_key, int(price), dec))
    if not by_line:
        return {"exec_price_available": False}

    # Modal line, ties broken toward the better price for us.
    line = max(by_line, key=lambda k: (len(by_line[k]), max(d for _, _, d in by_line[k])))
    book_key, price, dec = max(by_line[line], key=lambda t: t[2])
    out = {
        "exec_book": book_key,
        "exec_odds": price,
        "exec_decimal": round(dec, 4),
        "exec_books_at_line": len(by_line[line]),
        "exec_price_available": True,
        # Legacy key names: the ROI/backtest queries and _selection_prices read
        # these. They hold the EXECUTION book's price, which is why the UI
        # reports the distinct-book count instead of claiming "@ DK".
        "dk_odds": price,
        "dk_decimal": round(dec, 4),
        "clv_book": book_key,
    }
    if line is not None:
        out["exec_line"] = line
    return out


def _freeze_game_price(sport: str, books: dict, *, market: str, side: str) -> dict:
    """Freeze an executable quote and, for Tennis, its settlement contract."""
    priced = freeze_execution_price(books, market=market, side=side)
    if sport == "tennis" and priced.get("exec_book"):
        priced.update(tennis_rule_snapshot(priced["exec_book"], market))
    return priced


def _tennis_pin_favorite_forward_details(books: dict, side: str,
                                         retail: float, gap_pp: float) -> dict | None:
    """Freeze a candidate for the prospective favorite-only Tennis cohort."""
    _, retail_books = _retail_probabilities(books, side)
    if retail <= 0.5 or len(retail_books) < _TENNIS_PIN_FORWARD_MIN_RETAIL_BOOKS:
        return None
    priced = _freeze_game_price(
        "tennis", books, market="moneyline", side=side)
    if not priced.get("exec_price_available"):
        return None
    return {
        "program_version": _TENNIS_PIN_FORWARD_VERSION,
        "forward_test_target": _TENNIS_PIN_FORWARD_TARGET,
        "gap_pp": round(gap_pp, 2),
        "retail_books": len(retail_books),
        "market": "moneyline",
        **priced,
    }


def _consensus_book_line(books: dict, key: str) -> float | None:
    values = []
    for book in books.values():
        try:
            if book.get(key) is not None:
                values.append(float(book[key]))
        except (TypeError, ValueError):
            continue
    return sum(values) / len(values) if values else None


def _nfl_market_signals(current: dict, previous: dict | None, opening: dict | None) -> list[dict]:
    """Return NFL spread/total first-breach candidates from exact book lines."""
    signals: list[dict] = []
    for market, key, down_side, up_side in (
        ("spread", "spread_home", "home", "away"),
        ("total", "total_line", "under", "over"),
    ):
        current_line = _consensus_book_line(current, key)
        if current_line is None:
            continue
        if previous:
            moves = []
            for book_key in set(previous) & set(current):
                try:
                    before = float(previous[book_key][key])
                    after = float(current[book_key][key])
                except (KeyError, TypeError, ValueError):
                    continue
                moves.append(after - before)
            down = [move for move in moves if move <= -_NFL_STEAM_LINE_MOVE]
            up = [move for move in moves if move >= _NFL_STEAM_LINE_MOVE]
            movers, side = (down, down_side) if len(down) >= len(up) else (up, up_side)
            if len(movers) >= _STEAM_MIN_BOOKS:
                signals.append({
                    "alert_type": f"{market}_steam",
                    "side": side,
                    "details": {
                        "market": market,
                        "selection": side,
                        "trigger_line": round(current_line, 3),
                        "current_line": round(current_line, 3),
                        "interval_delta": round(sum(movers) / len(movers), 3),
                        "books_moved": len(movers),
                        "grading_version": "nfl-lines-v1",
                    },
                })
        if opening:
            open_line = _consensus_book_line(opening, key)
            if open_line is not None:
                drift = current_line - open_line
                if abs(drift) >= _NFL_WALK_LINE_MOVE:
                    side = up_side if drift > 0 else down_side
                    signals.append({
                        "alert_type": f"{market}_walking",
                        "side": side,
                        "details": {
                            "market": market,
                            "selection": side,
                            "trigger_line": round(current_line, 3),
                            "open_line": round(open_line, 3),
                            "current_line": round(current_line, 3),
                            "delta": round(drift, 3),
                            "grading_version": "nfl-lines-v1",
                        },
                    })
    return signals


def _notify(alerts: list[dict]) -> None:
    """Push new alerts to any configured channel; silent no-op otherwise.

    Channels (set the secret, get the pushes — the ledger row is written
    regardless, so the audit never depends on delivery):
      * Telegram — TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
      * Discord  — DISCORD_WEBHOOK_URL (a channel webhook URL; no bot needed)
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not alerts or (not discord_url and not (token and chat_id)):
        return
    for a in alerts:
        d = a.get("details") or {}
        if a["alert_type"] in ("dk_prop_value", "prop_line_gap"):
            label = {"pitcher_strikeouts": "Strikeouts",
                     "batter_total_bases": "Total Bases",
                     "pitcher_hits_allowed": "Hits Allowed",
                     "pitcher_earned_runs": "Earned Runs",
                     "pitcher_outs": "Outs Recorded",
                     "total_games": "Total Games"}.get(d.get("market", ""), d.get("market", ""))
            text = (f"💰 {a['sport'].upper()} PROP: {a['matchup']}\n"
                    f"{d.get('player')} {label} {d.get('bet')} {d.get('line')} "
                    f"@ DK {d.get('dk_odds', '?'):+}"
                    + (f"  EV +{d.get('ev_pct')}%" if d.get("ev_pct") else
                       f"  (Pinnacle line: {d.get('pin_line')})"))
        elif a["alert_type"] == "prop_outlier":
            text = (f"💰 {a['sport'].upper()} ATGS: {a['matchup']}\n"
                    f"{d.get('player')} to score @ DK {d.get('dk_odds', '?'):+}  "
                    f"+{d.get('edge_vs_median_pct')}% vs market median")
        elif a["alert_type"] == "dk_value":
            text = (f"💰 {a['sport'].upper()} DK VALUE: {a['matchup']}\n"
                    f"Bet {a['side']} @ DK {d.get('dk_odds', '?'):+}  "
                    f"EV {d.get('ev_pct', '?')}% vs Pinnacle fair "
                    f"{a['sharp_prob']*100:.1f}%")
        elif a.get("sharp_prob") is not None:
            text = (f"🚨 {a['sport'].upper()} {a['alert_type']}: {a['matchup']}\n"
                    f"side={a['side']}  retail={a['alert_prob']*100:.1f}%  "
                    f"sharp={a['sharp_prob']*100:.1f}%")
        else:
            text = f"🚨 {a['sport'].upper()} {a['alert_type']}: {a['matchup']} side={a['side']}"
        if token and chat_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=10,
                )
            except requests.RequestException as e:
                logger.warning("Telegram notify failed: %s", e)
        if discord_url:
            try:
                requests.post(discord_url, json={"content": text}, timeout=10)
            except requests.RequestException as e:
                logger.warning("Discord notify failed: %s", e)


def scan(db: DatabaseManager, sport: str) -> int:
    """Detect breaches on UPCOMING games' latest captures. Returns new alerts."""
    matchup_tbl = _MATCHUP_TBL[sport]
    rows = db.execute(
        f"""
        SELECT DISTINCT ON (h.matchup_id)
               h.matchup_id, h.game_date, h.home_team_name, h.away_team_name,
               h.captured_at, h.capture_key, h.books, m.commence_time
        FROM game_odds_history h
        JOIN {matchup_tbl} m ON m.id = h.matchup_id
        WHERE h.sport = %s AND h.books IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM jsonb_object_keys(h.books) AS source(book_key)
            WHERE source.book_key <> 'polymarket'
          )
          AND m.commence_time > NOW()
        ORDER BY h.matchup_id, h.captured_at DESC
        """,
        (sport,),
    )
    new_alerts: list[dict] = []
    for r in rows:
        books = r["books"] or {}
        label = f"{r['away_team_name']} @ {r['home_team_name']}"
        # ── Pinnacle divergence ──
        pin = books.get("pinnacle")
        if pin:
            for side in _sides(books):
                sharp = _book_fair_side(pin, side)
                retail = _retail_fair_side(books, side)
                if sharp is None or retail is None:
                    continue
                gap_pp = (sharp - retail) * 100
                if gap_pp >= _PIN_GAP_MIN_PP:
                    forward_details = (_tennis_pin_favorite_forward_details(
                        books, side, retail, gap_pp) if sport == "tennis" else None)
                    alert_type = (_TENNIS_PIN_FORWARD_TYPE
                                  if forward_details is not None
                                  else "pinnacle_divergence")
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type=alert_type, side=side,
                        alert_prob=retail, sharp_prob=sharp,
                        details=(forward_details or {
                            "gap_pp": round(gap_pp, 2),
                            "n_books": len(books),
                            **_freeze_game_price(
                                sport, books, market="moneyline", side=side),
                        }),
                    ))
        # ── Pinnacle vs Polymarket disagreement ──
        for signal in _pinnacle_polymarket_signals(books):
            new_alerts.extend(_insert(
                db, sport=sport, r=r, label=label,
                alert_type="pinnacle_polymarket_delta", side=signal["side"],
                alert_prob=signal["alert_prob"], sharp_prob=signal["sharp_prob"],
                details=signal["details"],
            ))
        # ── DraftKings value (Pinnacle fair vs DK's offered price) ──
        dk = books.get(_DK_BOOK)
        if pin and dk:
            for side in _sides(books):
                ev = _dk_value_ev(pin, dk, side)
                if ev is None:
                    continue
                dk_odds = _dk_side_odds(dk, side)
                if (dk_odds is not None
                        and american_to_decimal(dk_odds) >= _DK_VALUE_MAX_DECIMAL):
                    continue  # longshot — de-vig skew manufactures fake EV
                if ev >= _DK_VALUE_MIN_EV:
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="dk_value", side=side,
                        alert_prob=_book_price_prob(dk, side),  # DK implied, vig incl.
                        sharp_prob=_book_fair_side(pin, side),
                        details={
                            "ev_pct": round(ev * 100, 2),
                            "dk_odds": dk_odds,
                            "dk_decimal": round(american_to_decimal(dk_odds), 4)
                            if dk_odds is not None else None,
                            "exec_book": "draftkings",
                            "exec_odds": dk_odds,
                            "exec_decimal": round(american_to_decimal(dk_odds), 4)
                            if dk_odds is not None else None,
                            "exec_price_available": dk_odds is not None,
                            **(tennis_rule_snapshot("draftkings", "moneyline")
                               if sport == "tennis" else {}),
                        },
                    ))
        # ── Steam (needs the previous capture) ──
        prev = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND captured_at < %s
              AND books IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM jsonb_object_keys(books) AS source(book_key)
                WHERE source.book_key <> 'polymarket'
              )
            ORDER BY captured_at DESC LIMIT 1
            """,
            (sport, r["matchup_id"], r["captured_at"]),
        )
        if prev and prev["books"]:
            pb = prev["books"]
            for side in _sides(books):
                moves = []
                for key in set(pb) & set(books):
                    p0 = _book_fair_side(pb[key], side)
                    p1 = _book_fair_side(books[key], side)
                    if p0 is not None and p1 is not None:
                        moves.append((p1 - p0) * 100)
                movers = [m for m in moves if m >= _STEAM_MIN_MOVE_PP]
                if len(movers) >= _STEAM_MIN_BOOKS:
                    retail = _retail_fair_side(books, side)
                    pin_p = _book_fair_side(books["pinnacle"], side) if "pinnacle" in books else None
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="steam", side=side,
                        alert_prob=retail, sharp_prob=pin_p,
                        details={"books_moved": len(movers),
                                 "avg_move_pp": round(sum(movers) / len(movers), 2),
                                 "market": "moneyline",
                                 **_freeze_game_price(sport, books, market="moneyline",
                                                      side=side)},
                    ))
        # ── Walking (slow consensus drift >= _WALK_MIN_PP toward a side since
        #    OPEN — the first capture of this fixture, not the previous one). ──
        first = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND books IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM jsonb_object_keys(books) AS source(book_key)
                WHERE source.book_key <> 'polymarket'
              )
            ORDER BY captured_at ASC LIMIT 1
            """,
            (sport, r["matchup_id"]),
        )
        if first and first["books"]:
            fb = first["books"]
            for side in _sides(books):
                p_open, p_now, overlap_books = _comparable_retail_probabilities(
                    fb, books, side)
                if p_open is None or p_now is None:
                    continue
                drift_pp = (p_now - p_open) * 100
                if drift_pp >= _WALK_MIN_PP:
                    pin_p = _book_fair_side(books["pinnacle"], side) if "pinnacle" in books else None
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="walking", side=side,
                        alert_prob=p_now, sharp_prob=pin_p,
                        details={"open_pp": round(p_open * 100, 2),
                                 "now_pp": round(p_now * 100, 2),
                                 "drift_pp": round(drift_pp, 2),
                                 "overlap_books": overlap_books,
                                 "market": "moneyline",
                                 **_freeze_game_price(sport, books, market="moneyline",
                                                      side=side)},
                    ))
        if sport == "nfl":
            previous_books = prev["books"] if prev and prev.get("books") else None
            opening_books = first["books"] if first and first.get("books") else None
            for signal in _nfl_market_signals(books, previous_books, opening_books):
                # Freeze the price at trigger. Without it these alerts can be
                # counted but never graded economically -- the state 77 of 80
                # settled NFL alerts were in before 2026-08-15.
                priced = freeze_execution_price(
                    books, market=signal["details"]["market"], side=signal["side"])
                new_alerts.extend(_insert(
                    db,
                    sport=sport,
                    r=r,
                    label=label,
                    alert_type=signal["alert_type"],
                    side=signal["side"],
                    alert_prob=(1 / priced["exec_decimal"]
                                if priced.get("exec_decimal") else None),
                    sharp_prob=None,
                    details={**signal["details"], **priced},
                ))
    if new_alerts:
        print(f"Line alerts ({sport}): {len(new_alerts)} new — "
              + ", ".join(f"{a['alert_type']}:{a['matchup']}/{a['side']}" for a in new_alerts))
        _notify(new_alerts)
    return len(new_alerts)


def _mlb_model_signal_context(db, r, side: str, alert_prob: float | None) -> dict:
    """Freeze the latest pre-trigger MLB model comparison with a line alert.

    This makes the combined movement/model signal auditable later without
    changing the generic alert columns or rewriting old first-breach rows.
    """
    if alert_prob is None or side not in ("home", "away"):
        return {}
    snapshot = db.execute_one(
        """
        SELECT s.raw_prediction, s.created_at, pr.model_version
        FROM mlb_game_prediction_snapshots s
        JOIN mlb_prediction_runs pr ON pr.id = s.run_id
        WHERE s.matchup_id = %s AND s.market = 'moneyline'
          AND pr.origin = 'prospective'
          AND s.created_at <= %s
        ORDER BY s.created_at DESC, s.id DESC
        LIMIT 1
        """,
        (r["matchup_id"], r["captured_at"]),
    )
    if not snapshot or snapshot["raw_prediction"] is None:
        return {}
    home_probability = float(snapshot["raw_prediction"])
    model_probability = home_probability if side == "home" else 1 - home_probability
    if model_probability <= 0.02 or model_probability >= 0.98:
        return {
            "model_probability": round(model_probability, 6),
            "model_agreement": "unavailable_extreme",
            "model_version": snapshot["model_version"],
            "model_kind": "raw_market_anchored",
        }
    model_gap_pp = (model_probability - float(alert_prob)) * 100
    if abs(model_gap_pp) > _MAX_MODEL_GAP_PP:
        return {
            "model_probability": round(model_probability, 6),
            "model_agreement": "unavailable_extreme_gap",
            "model_version": snapshot["model_version"],
            "model_kind": "raw_market_anchored",
        }
    agreement = (
        "agree" if model_gap_pp > _MODEL_NEUTRAL_GAP_PP
        else "disagree" if model_gap_pp < -_MODEL_NEUTRAL_GAP_PP
        else "neutral"
    )
    snapshot_at = snapshot["created_at"]
    return {
        "model_probability": round(model_probability, 6),
        "model_gap_pp": round(model_gap_pp, 2),
        "model_agreement": agreement,
        "model_version": snapshot["model_version"],
        "model_snapshot_at": snapshot_at.isoformat() if hasattr(snapshot_at, "isoformat") else str(snapshot_at),
        "model_kind": "raw_market_anchored",
    }


def _insert(db, *, sport, r, label, alert_type, side, alert_prob, sharp_prob, details) -> list[dict]:
    """First-breach insert; returns [alert] only when a NEW row was created."""
    if sport == "mlb":
        details = {**details, **_mlb_model_signal_context(db, r, side, alert_prob)}
    # ── Polymarket-confirmed flag ──────────────────────────────────────────
    # When Polymarket is present in the same capture AND its price for the
    # alert's side agrees with the sharp reference (i.e., Poly already moved
    # in the direction the alert says DK is stale), tag poly_confirmed=True.
    # This is a confidence-grading attribute, not a standalone signal — it
    # answers "does a different participant pool independently confirm this?"
    books = r.get("books") or {}
    poly = books.get("polymarket")
    if poly and alert_type in ("dk_value", "dk_prop_value", "prop_line_gap",
                               "pinnacle_divergence", _TENNIS_PIN_FORWARD_TYPE,
                               "steam"):
        poly_prob = _book_fair_side(poly, side)
        if poly_prob is not None and sharp_prob is not None and alert_prob is not None:
            # "Confirmed" = Poly's price for this side is ABOVE the retail
            # consensus (same direction as the sharp book), suggesting the
            # stale-line thesis has independent market support.
            poly_agrees = poly_prob > alert_prob
            details = {**details,
                       "poly_confirmed": poly_agrees,
                       "poly_prob": round(poly_prob, 4)}
        elif poly_prob is not None:
            details = {**details, "poly_confirmed": None, "poly_prob": round(poly_prob, 4)}
    rows = db.execute(
        """
        INSERT INTO line_alerts (sport, matchup_id, game_date, matchup, commence_time,
                                 alert_type, side, capture_key, alert_prob, sharp_prob, details_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sport, matchup_id, alert_type, side) DO NOTHING
        RETURNING id
        """,
        (sport, r["matchup_id"], r["game_date"], label, r["commence_time"],
         alert_type, side, r["capture_key"], alert_prob, sharp_prob, json.dumps(details)),
    )
    if not rows:
        return []
    return [{"sport": sport, "matchup": label, "alert_type": alert_type,
             "side": side, "alert_prob": alert_prob, "sharp_prob": sharp_prob,
             "details": details}]


# ── MLB player props (dk_prop_value / prop_line_gap) ─────────────────────────
# The props analog of dk_value, where the soft-book thesis is strongest: DK's
# prop lines are algorithmic and slow. Two detectors:
#   dk_prop_value — SAME line at DK and Pinnacle; de-vig Pinnacle's two-way to
#     fair and price DK's side: EV = pin_fair × dk_decimal − 1. Threshold is
#     higher than game lines (props carry more vig + feed staleness).
#   prop_line_gap — DK's line sits ≥ 1.0 off Pinnacle's (K's 8.5 vs 9.5): the
#     stale-line signature; the value side at DK is mechanical (Over at the
#     lower line / Under at the higher), no distribution assumption needed.
# prop_line_gap is DEMOTED to a measurement control (2026-08-15), not retired.
# Same-book same-line CLV over n=439 settled: -0.13%, 95% CI [-0.25%, -0.04%] --
# entirely below zero, i.e. DK's price moves AGAINST the flagged side by close.
# Root cause is structural, not a threshold: a |line gap| says the two books
# disagree but NOT which one is stale, so the direction is a coin flip.
# It keeps scanning because detectors run on already-captured data and cost no
# credits, and a known-negative signal is useful for validating the measurement
# itself. It must never be surfaced as a play -- the UI renders it as CONTROL.
# Reviving it as actionable requires a new, separately pre-registered study.
_PROP_LINE_GAP_IS_CONTROL_ONLY = True

# Books this user can actually place a bet at. Used by EVERY market
# (game lines and props) -- it is a jurisdiction fact, not a prop concern. A price at a book they cannot
# reach is a reference, never a recommendation -- so ONLY these are eligible to
# execute an alert. Pinnacle is deliberately absent: it is the fair-value
# anchor and is not bettable in this jurisdiction. ESPN BET / Hard Rock / Fliff
# post far more prop markets and are captured, but stay OUT until jurisdiction
# is confirmed; adding one is a one-line change that bumps the detector version.
_EXECUTION_BOOKS = (
    "draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers",
)

# v1 = DraftKings-only trigger (the 73 settled observations that measured
# CLV +1.29%, 95% CI [+0.43%, +2.32%]; robust -- drop the top 10 and it is
# still +0.27%, CI [+0.08%, +0.54%], so real but SMALLER than the headline).
# v2 = best price across every executable book. Superseded after 10 rows, see
# below; those rows stay as audit history and are excluded from every cohort.
# v3 = v2 plus the model-disagreement gate.
#
# WHY v2 WAS WRONG: taking the max EV across N books while holding the
# threshold fixed is a BIASED ESTIMATOR. It selects the book whose quote is
# most erroneous in our favour, and quote error lives in the same tail as
# genuine value -- so the observed 5x volume rise was partly a looser bar, not
# more signal. Measured: every executable book's median EV is ~-6.5% (i.e.
# exactly the two-way hold, 6.1-7.2%), so a +3% alert is a ~9.5pp tail outlier.
# Raising the threshold does NOT fix it -- at 6.0% the best-of-6 rate is still
# 15x the DraftKings-only rate at 3.0%, because the selection is over books,
# not over the threshold.
#
# A REJECTED FIX, recorded so it is not retried: requiring the executing book's
# OWN de-vigged fair to disagree with Pinnacle's. It rejected NOTHING on live
# data, because it is arithmetically circular. Proportional de-vig forces a
# book's two sides to sum to 1, so "posted price is generous" and "own fair
# differs from Pinnacle" are the SAME statement. At 6-8% hold, EV >= 3% already
# implies 1.7-6.9pp of apparent "disagreement" by construction. Separating
# margin placement from genuine model disagreement needs an ASYMMETRIC de-vig
# (Shin or power), which is a real change and is not attempted here.
#
# THE FIX ACTUALLY SHIPPED (v3): separate SELECTION from EXECUTION.
#   selection  - trigger on DraftKings ALONE, exactly as v1 did. One book, no
#                max-of-N, no selection bias, and it is the trigger whose CLV
#                is validated (n=73, +1.29%, robust to dropping the top 10).
#   execution  - having selected, take the best same-line price across the
#                executable books. The choice was already made on unbiased
#                evidence, so shopping the price afterwards is pure D1-style
#                line-shopping gain (+1.2-1.9%/bet measured) and cannot bias
#                which propositions get flagged.
# CLV continuity is preserved: `dk_decimal` still holds DraftKings' price, so
# entry-vs-close stays measured DK-against-DK and remains poolable with v1's 73
# observations. The best-price execution is carried alongside as an overlay.
_PROP_DETECTOR_VERSION = "prop-value-v3-dk-trigger-best-exec"

_PROP_VALUE_MIN_EV = 0.03
_PROP_LINE_GAP_MIN = 1.0
_PROP_MARKET_LABEL = {
    "pitcher_strikeouts": "K", "batter_total_bases": "TB",
    "pitcher_hits_allowed": "H-allowed", "pitcher_earned_runs": "ER", "pitcher_outs": "Outs",
}


def _prop_pair(book: dict) -> tuple[float, float, float] | None:
    """(line, fair_over, fair_under) from one book's two-way, vig removed."""
    from model.soccer_bet_rating import american_to_prob
    if book.get("line") is None or book.get("over") is None or book.get("under") is None:
        return None
    try:
        po, pu = american_to_prob(int(book["over"])), american_to_prob(int(book["under"]))
    except (TypeError, ValueError):
        return None
    total = po + pu
    if total <= 0:
        return None
    return float(book["line"]), po / total, pu / total


def scan_props(db: DatabaseManager) -> int:
    """Detect DK-vs-Pinnacle prop value on upcoming MLB games. Returns new alerts."""
    rows = db.execute(
        """
        SELECT DISTINCT ON (event_id, market, player)
               event_id, matchup_id, game_date, commence_time,
               home_team_name, away_team_name, market, player, books, capture_key, captured_at
        FROM prop_odds_history
        WHERE sport = 'mlb' AND commence_time > NOW() AND matchup_id IS NOT NULL
        ORDER BY event_id, market, player, captured_at DESC
        """
    )
    new_alerts: list[dict] = []
    for r in rows:
        books = r["books"] or {}
        dk, pin = books.get(_DK_BOOK), books.get("pinnacle")
        if not dk or not pin:
            continue
        pin_p = _prop_pair(pin)
        if pin_p is None or dk.get("line") is None:
            continue
        pin_line, fair_over, fair_under = pin_p
        dk_line = float(dk["line"])
        label = f"{r['away_team_name']} @ {r['home_team_name']}"
        mk = _PROP_MARKET_LABEL.get(r["market"], r["market"])
        shim = {"matchup_id": r["matchup_id"], "game_date": r["game_date"],
                "commence_time": r["commence_time"], "capture_key": r["capture_key"]}

        # ── Same-line price value, BEST EXECUTABLE BOOK (prop-value-v2) ──────
        # v1 checked DraftKings only. v2 checks every executable book against
        # the same Pinnacle fair value and fires ONE alert at the best price.
        #
        # One alert per proposition, not one per book, for two reasons:
        #   economic  — you would only ever place the bet once, at the best price
        #   statistical — six books quoting one player are ~one observation, and
        #                 counting them as six would inflate n and shrink every
        #                 confidence interval (the clustering trap).
        # Dedup on (sport, matchup_id, alert_type, side) already collapses them,
        # but relying on insert order would pick an arbitrary book, not the best.
        for side, fair, price_key in (("Over", fair_over, "over"),
                                      ("Under", fair_under, "under")):
            # ── SELECTION: DraftKings alone. One book, no max-of-N. ──────────
            if dk_line != pin_line:
                continue              # different line = different proposition
            dk_price = dk.get(price_key)
            if dk_price is None:
                continue
            try:
                dk_dec = american_to_decimal(int(dk_price))
            except (TypeError, ValueError):
                continue
            if dk_dec >= _DK_VALUE_MAX_DECIMAL:
                continue              # longshot: de-vig skew fakes EV in the tail
            ev = fair * dk_dec - 1
            if ev < _PROP_VALUE_MIN_EV:
                continue

            # ── EXECUTION: now shop the SAME proposition for the best price.
            # Runs only after selection, so it cannot influence what is flagged.
            book_key, price, dec = _DK_BOOK, int(dk_price), dk_dec
            n_qualifying = 0
            for cand in _EXECUTION_BOOKS:
                bq = books.get(cand)
                if not bq or bq.get("line") is None:
                    continue
                if float(bq["line"]) != pin_line:
                    continue
                cp = bq.get(price_key)
                if cp is None:
                    continue
                try:
                    cd = american_to_decimal(int(cp))
                except (TypeError, ValueError):
                    continue
                n_qualifying += int(fair * cd - 1 >= _PROP_VALUE_MIN_EV)
                if cd > dec:
                    book_key, price, dec = cand, int(cp), cd
            new_alerts.extend(_insert(
                db, sport="mlb", r=shim, label=label,
                alert_type="dk_prop_value",
                side=f"{r['player']} {mk} {side[0]}{pin_line}",
                alert_prob=1 / dec, sharp_prob=fair,
                details={"market": r["market"], "player": r["player"],
                         "line": pin_line, "bet": side,
                         # canonical execution keys
                         "exec_book": book_key,
                         "exec_odds": price,
                         "exec_decimal": round(dec, 4),
                         "books_qualifying": n_qualifying,
                         "detector_version": _PROP_DETECTOR_VERSION,
                         # The book whose entry/close pair defines CLV. It is
                         # the SELECTION book (DraftKings), not the execution
                         # book -- `dk_decimal` below is DK's price, so grading
                         # must read DK's close or it compares two books.
                         "clv_book": _DK_BOOK,
                         # Execution overlay: best same-line price found AFTER
                         # selection. Reported separately so best-price ROI can
                         # be measured without contaminating the trigger.
                         "exec_gain_pct": round((dec / dk_dec - 1) * 100, 2),
                         # dk_* remain DRAFTKINGS' price -- the selection book.
                         # CLV is entry-vs-close at DK on both sides of the
                         # comparison, so v3 stays poolable with v1's n=73.
                         "dk_odds": int(dk_price),
                         "dk_decimal": round(dk_dec, 4),
                         "ev_pct": round(ev * 100, 2)},
            ))
        # ── Stale-line gap — CONTROL ONLY, DK-vs-Pinnacle, DELIBERATELY v1 ──
        # Left exactly as it was (DraftKings only, no best-price selection) so
        # it stays comparable with the 439 settled observations that measured
        # its CLV at -0.13%, 95% CI [-0.25%, -0.04%]. Changing a control's
        # trigger would destroy the baseline it exists to provide.
        if dk_line != pin_line and abs(dk_line - pin_line) >= _PROP_LINE_GAP_MIN:
            bet = "Over" if dk_line < pin_line else "Under"
            price = dk.get("over" if bet == "Over" else "under")
            dec = american_to_decimal(int(price)) if price is not None else None
            new_alerts.extend(_insert(
                db, sport="mlb", r=shim, label=label,
                alert_type="prop_line_gap",
                side=f"{r['player']} {mk} {bet[0]}{dk_line}",
                alert_prob=(1 / dec) if dec else None,
                sharp_prob=fair_over if bet == "Over" else fair_under,
                details={"market": r["market"], "player": r["player"],
                         "line": dk_line, "pin_line": pin_line, "bet": bet,
                         "gap": round(abs(dk_line - pin_line), 1),
                         "dk_odds": int(price) if price is not None else None,
                         "dk_decimal": round(dec, 4) if dec else None},
            ))
    if new_alerts:
        print(f"Prop alerts (mlb): {len(new_alerts)} new — "
              + ", ".join(f"{a['alert_type']}:{a['side']}" for a in new_alerts[:6]))
        _notify(new_alerts)
    return len(new_alerts)


# ── Tennis total-games (the tennis "prop-equivalent": the Odds API carries no
# tennis player props at all — player_aces etc. are invalid market keys — but
# match total games is a two-sided DK+Pinnacle market already in the per-book
# game_odds_history captures, and it settles mechanically from the final score).
_TENNIS_TOTALS_MIN_EV = 0.03
_TENNIS_TOTALS_LINE_GAP = 2.0   # games; tennis totals cluster tightly (~21.5-23.5)


def scan_tennis_totals(db: DatabaseManager) -> int:
    """DK-vs-Pinnacle value on tennis match total games (same-line EV + line gap)."""
    rows = db.execute(
        """
        SELECT DISTINCT ON (h.matchup_id)
               h.matchup_id, h.game_date, h.home_team_name, h.away_team_name,
               h.capture_key, h.books, m.commence_time
        FROM game_odds_history h
        JOIN tennis_matches m ON m.id = h.matchup_id
        WHERE h.sport = 'tennis' AND h.books IS NOT NULL AND m.commence_time > NOW()
        ORDER BY h.matchup_id, h.captured_at DESC
        """
    )
    new_alerts: list[dict] = []
    for r in rows:
        books = r["books"] or {}
        dk, pin = books.get(_DK_BOOK), books.get("pinnacle")
        if not dk or not pin:
            continue
        pin_p = _prop_pair(pin)
        if pin_p is None or dk.get("total_line") is None:
            continue
        pin_line, fair_over, fair_under = pin_p
        dk_line = float(dk["total_line"])
        label = f"{r['away_team_name']} @ {r['home_team_name']}"
        shim = {"matchup_id": r["matchup_id"], "game_date": r["game_date"],
                "commence_time": r["commence_time"], "capture_key": r["capture_key"]}
        if dk_line == pin_line:
            for side, fair, price_key in (("Over", fair_over, "over"), ("Under", fair_under, "under")):
                price = dk.get(price_key)
                if price is None:
                    continue
                dec = american_to_decimal(int(price))
                if dec >= _DK_VALUE_MAX_DECIMAL:
                    continue
                ev = fair * dec - 1
                if ev >= _TENNIS_TOTALS_MIN_EV:
                    new_alerts.extend(_insert(
                        db, sport="tennis", r=shim, label=label,
                        alert_type="dk_prop_value",
                        side=f"Games {side[0]}{dk_line}",
                        alert_prob=1 / dec, sharp_prob=fair,
                        details={"market": "total_games", "bet": side,
                                 "line": dk_line, "player": label,
                                 "dk_odds": int(price),
                                 "dk_decimal": round(dec, 4),
                                 "exec_book": "draftkings",
                                 "exec_odds": int(price),
                                 "exec_decimal": round(dec, 4),
                                 "exec_price_available": True,
                                 "ev_pct": round(ev * 100, 2),
                                 **tennis_rule_snapshot("draftkings", "total")},
                    ))
        elif abs(dk_line - pin_line) >= _TENNIS_TOTALS_LINE_GAP:
            bet = "Over" if dk_line < pin_line else "Under"
            price = dk.get("over" if bet == "Over" else "under")
            dec = american_to_decimal(int(price)) if price is not None else None
            new_alerts.extend(_insert(
                db, sport="tennis", r=shim, label=label,
                alert_type="prop_line_gap",
                side=f"Games {bet[0]}{dk_line}",
                alert_prob=(1 / dec) if dec else None,
                sharp_prob=fair_over if bet == "Over" else fair_under,
                details={"market": "total_games", "bet": bet, "player": label,
                         "line": dk_line, "pin_line": pin_line,
                         "gap": round(abs(dk_line - pin_line), 1),
                         "dk_odds": int(price) if price is not None else None,
                         "dk_decimal": round(dec, 4) if dec else None,
                         "exec_book": "draftkings",
                         "exec_odds": int(price) if price is not None else None,
                         "exec_decimal": round(dec, 4) if dec else None,
                         "exec_price_available": price is not None,
                         **tennis_rule_snapshot("draftkings", "total")},
            ))
    if new_alerts:
        print(f"Tennis totals alerts: {len(new_alerts)} new — "
              + ", ".join(f"{a['matchup']} {a['side']}" for a in new_alerts[:5]))
        _notify(new_alerts)
    return len(new_alerts)


def settle_tennis_totals(db: DatabaseManager) -> int:
    """Grade Tennis totals under the rule frozen with the execution quote."""
    open_alerts = db.execute(
        """
        SELECT a.*, m.home_games, m.away_games, m.winner, m.completion_status
        FROM line_alerts a JOIN tennis_matches m ON m.id = a.matchup_id
        WHERE a.sport = 'tennis' AND a.alert_type IN ('dk_prop_value', 'prop_line_gap')
          AND a.settled_at IS NULL AND m.winner IS NOT NULL
        """
    )
    graded = 0
    for a in open_alerts:
        d = a["details_json"] or {}
        book = d.get("exec_book") or d.get("clv_book")
        if book is None and d.get("dk_odds") is not None:
            book = "draftkings"
        outcome = settle_tennis_selection(
            book=book, market="total", selection_side=a["side"],
            winner_side=a["winner"], completion_status=a["completion_status"],
            home_games=a["home_games"], away_games=a["away_games"],
            line=float(d["line"]), total_bet=d["bet"],
        )
        if outcome is None:
            continue
        g = _grade_alert_prices(db, a)
        g["grading_json"] = {
            **(g["grading_json"] or {}),
            **tennis_rule_snapshot(book, "total"),
            "completion_status": a["completion_status"],
        }
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE line_alerts SET outcome = %s, settled_at = NOW(), "
                "dk_close_decimal = %s, dk_clv_pct = %s, pin_close_prob = %s, "
                "convergence = %s, dk_survival_min = %s, grading_json = %s, comparison_status = %s, grading_version = %s, "
                "details_json = details_json || jsonb_build_object('actual', %s) "
                "WHERE id = %s AND settled_at IS NULL",
                (outcome, g["dk_close_decimal"], g["dk_clv_pct"], g["pin_close_prob"],
                 g["convergence"], g["dk_survival_min"], json.dumps(g["grading_json"]),
                 g["comparison_status"], g["grading_version"],
                 (int(a["home_games"]) + int(a["away_games"]))
                 if a["home_games"] is not None and a["away_games"] is not None else None,
                 a["id"]),
            )
            if cur.rowcount:
                _append_grade_history_cur(cur, a["id"], g, outcome=outcome)
                graded += 1
    if graded:
        print(f"Tennis totals alerts: {graded} graded")
    return graded


# ── World Cup anytime-goalscorer outlier (Pinnacle posts no WC player props,
# so the anchor is the market MEDIAN across ~8 books; DK paying ≥10% over the
# median decimal is the stale-price flag). Model-free, longshot-guarded, and
# graded like everything else — plus true ROI at DK's frozen price.
_SOCCER_OUTLIER_MIN_RATIO = 0.10   # DK decimal ≥ 10% above the median decimal
_SOCCER_OUTLIER_MIN_BOOKS = 5      # need a real median, not a 2-book coin flip


# Retired 2026-08-13 — see scan_props_soccer.
_SOCCER_PROP_OUTLIER_RETIRED = True


def scan_props_soccer(db: DatabaseManager) -> int:
    """RETIRED 2026-08-13 — confirmed loser, not merely unproven.

    Flagged WC anytime-scorer prices where DK looked like a fat outlier against
    the overround-normalized median book.  Settled record at frozen DK prices:

        n=101, 6 won (5.9%), median DK decimal 8.00 (12.5% implied),
        -65.0u, ROI -64.4%, date-clustered 95% CI [-85.6%, -44.7%]

    The CI lies entirely below zero, so this is a CONFIRMED negative — the
    standing rule ("an alert type with no positive CLV is noise") mandates
    retirement rather than a threshold tweak.  The signal was backwards: when
    DK priced a player longer than the normalized median, DK was right.

    The documented DNP-as-loss conservative bias does NOT rescue it.  Books
    void ATGS when the player never takes the field, and we have no lineup
    feed; but breakeven would require 65 of the 101 flagged players (64%) to
    have been no-shows, which is not credible for players priced at a median
    12.5% to score.  At a generous 20-25% DNP rate the detector still runs
    -52% to -56%.

    Deliberately NOT deleted: the ledger is append-only and the 101 rows stay
    as audit history.  ``settle_props_soccer`` still runs so anything already
    open finishes grading.  Reviving this requires a new, separately
    pre-registered study — not a parameter change to this function.
    """
    logger.info("scan_props_soccer is retired (confirmed -64.4%% ROI over n=101) — no scan")
    return 0


def _scan_props_soccer_retired_impl(db: DatabaseManager) -> int:
    """Preserved body of the retired detector. Not called. See scan_props_soccer."""
    from collections import defaultdict
    from model.soccer_bet_rating import american_to_prob

    rows = db.execute(
        """
        SELECT DISTINCT ON (event_id, player)
               event_id, matchup_id, game_date, commence_time,
               home_team_name, away_team_name, player, books, capture_key
        FROM prop_odds_history
        WHERE sport = 'soccer' AND market = 'player_goal_scorer_anytime'
          AND commence_time > NOW() AND matchup_id IS NOT NULL
        ORDER BY event_id, player, captured_at DESC
        """
    )
    by_event: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_event[r["event_id"]].append(r)

    new_alerts: list[dict] = []
    for players in by_event.values():
        # Per-book overround across every player this book prices in the game.
        book_total: dict[str, float] = defaultdict(float)
        book_count: dict[str, int] = defaultdict(int)
        for r in players:
            for key, b in (r["books"] or {}).items():
                if b.get("yes") is not None:
                    try:
                        book_total[key] += american_to_prob(int(b["yes"]))
                        book_count[key] += 1
                    except (TypeError, ValueError):
                        pass
        # A book must price most of the slate for its normalizer to mean anything.
        min_players = max(5, int(0.5 * max(book_count.values(), default=0)))
        usable = {k for k, n in book_count.items() if n >= min_players and book_total[k] > 0}

        for r in players:
            books = r["books"] or {}
            dk = books.get(_DK_BOOK)
            if not dk or dk.get("yes") is None or _DK_BOOK not in usable:
                continue
            try:
                dk_dec = american_to_decimal(int(dk["yes"]))
                dk_norm = american_to_prob(int(dk["yes"])) / book_total[_DK_BOOK]
                other_norms = sorted(
                    american_to_prob(int(b["yes"])) / book_total[k]
                    for k, b in books.items()
                    if k != _DK_BOOK and k in usable and b.get("yes") is not None)
            except (TypeError, ValueError):
                continue
            if len(other_norms) < _SOCCER_OUTLIER_MIN_BOOKS - 1:
                continue
            if dk_dec >= _DK_VALUE_MAX_DECIMAL:
                continue  # longshot — tail prices are where outlier noise lives
            median_norm = other_norms[len(other_norms) // 2]
            if dk_norm <= 0:
                continue
            # DK prices the player relatively LONGER than the median book.
            ratio = median_norm / dk_norm - 1
            if ratio < _SOCCER_OUTLIER_MIN_RATIO:
                continue
            shim = {"matchup_id": r["matchup_id"], "game_date": r["game_date"],
                    "commence_time": r["commence_time"], "capture_key": r["capture_key"]}
            new_alerts.extend(_insert(
                db, sport="soccer", r=shim,
                label=f"{r['away_team_name']} @ {r['home_team_name']}",
                alert_type="prop_outlier",
                side=f"{r['player']} ATGS",
                alert_prob=1 / dk_dec,  # DK implied (vig incl.)
                # Median share re-inflated to DK's own margin scale so the two
                # columns are the same kind of number ("what DK *should* imply").
                sharp_prob=median_norm * book_total[_DK_BOOK],
                details={"market": "player_goal_scorer_anytime",
                         "player": r["player"], "bet": "to score",
                         "dk_odds": int(dk["yes"]),
                         "dk_decimal": round(dk_dec, 4),
                         "edge_vs_median_pct": round(ratio * 100, 1),
                         "n_books": len(books)},
            ))
    if new_alerts:
        print(f"WC prop alerts: {len(new_alerts)} new — "
              + ", ".join(a["side"] for a in new_alerts[:6]))
        _notify(new_alerts)
    return len(new_alerts)


def settle_props_soccer(db: DatabaseManager) -> int:
    """Grade WC anytime-scorer alerts from the goal timeline (90 minutes only).

    DK settles soccer player props on the 90-minute match, so a goal with
    minute > 90 (extra time) does NOT count — soccer_match_goals minutes are
    period-capped by TheSportsDB (90+X stoppage stores as 90), which matches
    the convention exactly. Grading waits for reg scores (the timeline-
    completeness gate). CONSERVATIVE BIAS, documented: a player who never
    entered the match grades 'lost' here where a book would void — we have no
    lineup feed, so measured ROI understates rather than inflates.
    """
    open_alerts = db.execute(
        """
        SELECT a.*, m.game_id
        FROM line_alerts a JOIN soccer_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'soccer' AND a.alert_type = 'prop_outlier'
          AND a.settled_at IS NULL
          AND m.reg_home_score IS NOT NULL AND m.reg_away_score IS NOT NULL
        """
    )
    import unicodedata

    def norm(s: str) -> str:
        return " ".join(unicodedata.normalize("NFKD", s or "")
                        .encode("ascii", "ignore").decode("ascii").lower().split())

    graded = 0
    for a in open_alerts:
        d = a["details_json"] or {}
        target = norm(d.get("player", ""))
        scorers = db.execute(
            "SELECT player_name FROM soccer_match_goals WHERE game_id = %s AND goal_minute <= 90",
            (a["game_id"],),
        )
        tt = set(target.split())
        scored = any(
            (lambda nl: tt <= set(nl.split()) or set(nl.split()) <= tt)(norm(s["player_name"]))
            for s in scorers
        )
        g = _grade_alert_prices(db, a)
        db.execute(
            "UPDATE line_alerts SET outcome = %s, dk_close_decimal = %s, dk_clv_pct = %s, "
            "pin_close_prob = %s, convergence = %s, dk_survival_min = %s, grading_json = %s, comparison_status = %s, grading_version = %s, "
            "settled_at = NOW() WHERE id = %s",
            ("won" if scored else "lost", g["dk_close_decimal"], g["dk_clv_pct"],
             g["pin_close_prob"], g["convergence"], g["dk_survival_min"], json.dumps(g["grading_json"]), g["comparison_status"], g["grading_version"], a["id"]),
        )
        _append_grade_history(db, a["id"], g, outcome=("won" if scored else "lost"))
        graded += 1
    if graded:
        print(f"WC prop alerts: {graded} graded from the goal timeline")
    return graded


# Pitching-stat markets read directly off the boxscore's per-player pitching
# dict -- verified against a real completed game's boxscore (2026-07-08):
# 'outs' is a direct field (7.0 IP -> outs=21, 5.0 IP -> outs=15), no need to
# parse the 'inningsPitched' string.
_PITCHING_STAT_FIELD = {
    "pitcher_strikeouts": "strikeOuts",
    "pitcher_hits_allowed": "hits",
    "pitcher_earned_runs": "earnedRuns",
    "pitcher_outs": "outs",
}


def _mlb_boxscore_stat(game_pk: str, player: str, market: str) -> float | None:
    """Actual stat value (pitching or batter total bases) from the free MLB boxscore."""
    import unicodedata

    def norm(s: str) -> str:
        return " ".join(unicodedata.normalize("NFKD", s or "")
                        .encode("ascii", "ignore").decode("ascii").lower().split())
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore", timeout=20)
        r.raise_for_status()
        box = r.json()
    except requests.RequestException as e:
        logger.warning("MLB boxscore fetch failed for %s: %s", game_pk, e)
        return None
    target = norm(player)
    for team in box.get("teams", {}).values():
        for p in (team.get("players") or {}).values():
            if norm(p.get("person", {}).get("fullName", "")) != target:
                continue
            stats = p.get("stats", {})
            pitch_field = _PITCHING_STAT_FIELD.get(market)
            if pitch_field is not None:
                v = (stats.get("pitching") or {}).get(pitch_field)
                return float(v) if v is not None else None
            bat = stats.get("batting") or {}
            if not bat:
                return None
            tb = bat.get("totalBases")
            if tb is not None:
                return float(tb)
            h = bat.get("hits", 0); d = bat.get("doubles", 0)
            t = bat.get("triples", 0); hr = bat.get("homeRuns", 0)
            return float((h - d - t - hr) + 2 * d + 3 * t + 4 * hr)
    return None


def settle_props(db: DatabaseManager) -> int:
    """Grade prop alerts from the MLB boxscore: Over/Under vs the frozen line."""
    open_alerts = db.execute(
        """
        SELECT a.*, m.game_id AS game_pk, m.home_score, m.away_score
        FROM line_alerts a JOIN mlb_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'mlb' AND a.alert_type IN ('dk_prop_value', 'prop_line_gap')
          AND a.settled_at IS NULL
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        """
    )
    graded = 0
    for a in open_alerts:
        d = a["details_json"] or {}
        actual = _mlb_boxscore_stat(str(a["game_pk"]), d.get("player", ""), d.get("market", ""))
        if actual is None:
            continue  # DNP or name mismatch — retried next pass; stays pending
        line, bet = float(d["line"]), d["bet"]
        if actual == line:
            outcome = "void"
        elif (actual > line) == (bet == "Over"):
            outcome = "won"
        else:
            outcome = "lost"
        g = _grade_alert_prices(db, a)
        db.execute(
            "UPDATE line_alerts SET outcome = %s, settled_at = NOW(), "
            "dk_close_decimal = %s, dk_clv_pct = %s, pin_close_prob = %s, "
            "convergence = %s, dk_survival_min = %s, grading_json = %s, comparison_status = %s, grading_version = %s, "
            "details_json = details_json || jsonb_build_object('actual', %s) WHERE id = %s",
            (outcome, g["dk_close_decimal"], g["dk_clv_pct"], g["pin_close_prob"],
             g["convergence"], g["dk_survival_min"], json.dumps(g["grading_json"]), g["comparison_status"], g["grading_version"], actual, a["id"]),
        )
        _append_grade_history(db, a["id"], g, outcome=outcome)
        graded += 1
    if graded:
        print(f"Prop alerts (mlb): {graded} graded from boxscores")
    return graded


def _selection_prices(a, books: dict) -> tuple[float | None, float | None]:
    """(execution_decimal, pinnacle_fair_prob) for THIS alert's exact selection
    from one capture snapshot. Same-line only for props — a moved line is a
    different proposition and grades as price-gone, not price-moved.

    Which book's close to read, across three detector generations:
      v3  `clv_book` = the SELECTION book (DraftKings). `dk_decimal` holds DK's
          price, so CLV is DK-entry vs DK-close and stays poolable with v1.
      v2  `exec_book` only -- that generation stored the execution book's price
          in `dk_decimal`, so grading correctly follows `exec_book`.
      v1  neither key -- DraftKings.
    Reading the wrong one compares an entry at one book to a close at another.
    """
    d = a["details_json"] or {}
    dk = books.get(d.get("clv_book") or d.get("exec_book") or _DK_BOOK)
    pin = books.get("pinnacle")
    dk_dec = None
    pin_fair = None
    market = d.get("market")
    if market == "player_goal_scorer_anytime":
        price = dk.get("yes") if dk else None
        if price is not None:
            try:
                dk_dec = american_to_decimal(int(price))
            except (TypeError, ValueError):
                pass
        # no Pinnacle for WC props — pin_fair stays None
    elif market:
        # ANY over/under player-prop market (was a hardcoded 6-market list, which
        # would silently fail to grade every market added after it was written).
        line_key = "total_line" if market == "total_games" else "line"
        bet = d.get("bet")
        if dk and dk.get(line_key) == d.get("line"):
            price = dk.get("over" if bet == "Over" else "under")
            if price is not None:
                try:
                    dk_dec = american_to_decimal(int(price))
                except (TypeError, ValueError):
                    pass
        if pin and pin.get(line_key) == d.get("line"):
            pp = _prop_pair({"line": pin.get(line_key),
                             "over": pin.get("over"), "under": pin.get("under")})
            if pp is not None:
                pin_fair = pp[1] if bet == "Over" else pp[2]
    else:  # game-side moneyline selection
        if dk:
            price = dk.get(_SIDE_KEY.get(a["side"], ""))
            if price is not None:
                try:
                    dk_dec = american_to_decimal(int(price))
                except (TypeError, ValueError):
                    pass
        if pin:
            pin_fair = _book_fair_side(pin, a["side"])
    return dk_dec, pin_fair


# Two DISTINCT thresholds (deliberately separate, not one reused value):
_MOVEMENT_EPSILON = 0.005   # 0.5pp — categorical "who moved" noise floor
_MIN_GAP_FOR_RATIO = 0.01   # 1.0pp — below this initial gap the closure RATIO
                            # is unstable (0.10→0.02pp reads as 80% closure on
                            # economically trivial movement), so it's nulled;
                            # absolute pp movement is still recorded and always
                            # outranks the ratio in any summary.
_CONV_EPS = _MOVEMENT_EPSILON  # back-compat alias
_GRADING_VERSION = "convergence_v2"
# Settled-count floor below which the report shows raw W-L-P + a
# "descriptive-only" tag instead of a rate — never implying statistical
# conclusions from single digits. A floor for INDEPENDENT bets; correlated
# same-slate props carry less information than the count suggests.
_MIN_SETTLED_FOR_CI = 30


def _grade_alert_prices(db, a) -> dict:
    """Full price-context grading over the alert→close capture series.

    Stores enough RAW price context to classify movement later rather than
    collapsing it into one CLV scalar:
      * dk_clv_pct     — execution CLV (positive = the recommended selection
                         got more expensive at DK).
      * pin_close_prob — the reference close as its own quantity.
      * convergence    — a PATH classification (who moved), NOT a quality
                         verdict. REFERENCE_CONVERGED_TO_EXECUTION means the
                         sharp move was more consistent with DK's original
                         price than the reference's — evidence DK may have led
                         price discovery, not proof. DIVERGENCE_PERSISTED is
                         explicitly neutral (a no-CLV bet can still win/lose on
                         noise); its value is that it isolates a testable
                         population, not that any single row was "good".
      * grading_json   — movement MAGNITUDE alongside the label (gap initial/
                         final/max-closure, gap_closure_ratio, per-book moves,
                         epsilon, n_captures) + interval-censored survival
                         bounds. The survival metric is OBSERVED QUOTE
                         PERSISTENCE, not verified execution availability.
      * dk_survival_min — the survival UPPER bound (back-compat scalar).
    """
    out = {"dk_close_decimal": None, "dk_clv_pct": None, "pin_close_prob": None,
           "convergence": None, "dk_survival_min": None, "grading_json": None,
           "comparison_status": "NO_REFERENCE", "grading_version": _GRADING_VERSION}
    d = a["details_json"] or {}
    entry_dec = d.get("dk_decimal")

    # Proposition comparability — EXPLICIT, not inferred downstream from
    # alert_type. Determines convergence eligibility; a new alert type must be
    # classified here to enter (or be kept out of) the convergence path.
    at = a["alert_type"]
    if at in ("dk_value", "dk_prop_value"):
        comp, elig, reason = "SAME_PROPOSITION", True, None
    elif at == "prop_line_gap":
        comp, elig, reason = "DIFFERENT_LINE", False, "dk_and_pinnacle_on_different_lines"
    elif at == "prop_outlier":
        comp, elig, reason = "NO_REFERENCE", False, "no_pinnacle_wc_props_median_anchor"
    else:  # pinnacle_divergence, steam — not DK-vs-sharp same-line propositions
        comp, elig, reason = "NO_REFERENCE", False, "not_a_dk_reference_alert"
    out["comparison_status"] = comp

    if entry_dec is None:
        out["grading_json"] = {"comparison_status": comp, "convergence_eligible": elig,
                               "convergence_exclusion_reason": reason,
                               "grading_version": _GRADING_VERSION}
        return out
    entry_dec = float(entry_dec)

    is_prop_src = (a["alert_type"] in ("dk_prop_value", "prop_line_gap", "prop_outlier")
                   and d.get("market") != "total_games")
    if is_prop_src:
        caps = db.execute(
            """SELECT captured_at, books FROM prop_odds_history
               WHERE sport = %s AND matchup_id = %s AND market = %s AND player = %s
                 AND captured_at <= %s
               ORDER BY captured_at ASC""",
            (a["sport"], a["matchup_id"], d.get("market"), d.get("player"),
             a["commence_time"]),
        )
    else:
        caps = db.execute(
            """SELECT captured_at, books FROM game_odds_history
               WHERE sport = %s AND matchup_id = %s AND books IS NOT NULL
                 AND captured_at <= %s
               ORDER BY captured_at ASC""",
            (a["sport"], a["matchup_id"], a["commence_time"]),
        )

    series = [(c["captured_at"], *_selection_prices(a, c["books"] or {})) for c in caps]
    # Slice from the TRIGGERING capture: the alert's created_at lands moments
    # AFTER the capture that fired it, so a naive >= created_at filter drops
    # the entry snapshot (and scans can also fire off captures much older than
    # created_at). Entry = last capture at/before created_at, else the first.
    entry_idx = 0
    for idx, (ts, _dk, _pp) in enumerate(series):
        if ts <= a["created_at"]:
            entry_idx = idx
    series = series[entry_idx:]
    # A single usable capture can't measure movement — downgrade eligibility.
    if elig and len(series) < 2:
        comp, elig, reason = "INSUFFICIENT_CAPTURE", False, "fewer_than_2_captures"
        out["comparison_status"] = comp
    grading = {"n_captures": len(series),
               "movement_epsilon_pp": _MOVEMENT_EPSILON * 100,
               "min_gap_for_ratio_pp": _MIN_GAP_FOR_RATIO * 100,
               "comparison_status": comp, "convergence_eligible": elig,
               "convergence_exclusion_reason": reason,
               "grading_version": _GRADING_VERSION}

    # ── Observed quote persistence (interval-censored by capture cadence) ──
    # The true change time lies in (last_same_at, first_changed_at]; we can only
    # bound it. This is NOT verified execution availability — it only says the
    # exact alerted quote was still observed at the last matching capture.
    last_same_ts = series[0][0] if series else a["created_at"]
    for ts, dk_dec, _pin in (series[1:] if series else []):
        if dk_dec is None or abs(dk_dec - entry_dec) > 1e-9:
            lower = round((last_same_ts - a["created_at"]).total_seconds() / 60, 1)
            upper = round((ts - a["created_at"]).total_seconds() / 60, 1)
            out["dk_survival_min"] = upper  # back-compat: the UPPER bound
            grading.update(survival_lower_min=max(lower, 0.0), survival_upper_min=upper,
                           last_same_at=last_same_ts.isoformat(),
                           first_changed_at=ts.isoformat())
            break
        last_same_ts = ts
    else:
        # Never observed to change — right-censored at the last capture.
        grading.update(survival_lower_min=round(
            (last_same_ts - a["created_at"]).total_seconds() / 60, 1),
            survival_upper_min=None, last_same_at=last_same_ts.isoformat(),
            first_changed_at=None)

    # Closes: last capture with a usable value for each side.
    dk_close = next((dk for _, dk, _p in reversed(series) if dk is not None), None)
    pin_close = next((pp for _, _dk, pp in reversed(series) if pp is not None), None)
    if dk_close:
        out["dk_close_decimal"] = dk_close
        out["dk_clv_pct"] = round((entry_dec / dk_close - 1) * 100, 2)
    if pin_close is not None:
        out["pin_close_prob"] = round(pin_close, 4)

    # close_status makes the persisted-to-close DENOMINATOR explicit — an alert
    # without an observable, comparable close is NOT a failure to persist and
    # must be reported separately, never flattered into the numerator:
    #   OBSERVED         — a same-proposition DK quote existed at close.
    #   MARKET_CHANGED   — DK still quoted the market at close but at a DIFFERENT
    #                      line (a different proposition; no comparable close).
    #   CLOSE_UNAVAILABLE— DK no longer quoted the selection at close.
    if dk_close is not None:
        grading["close_status"] = "OBSERVED"
    else:
        last_dk = (caps[-1]["books"] or {}).get(_DK_BOOK) if caps else None
        grading["close_status"] = "MARKET_CHANGED" if last_dk else "CLOSE_UNAVAILABLE"

    # ── Movement magnitude, stored ALONGSIDE the categorical label so
    # near-boundary cases (a row that misses ε by 0.01pp) aren't treated as
    # fundamentally different, and epsilon sensitivity is testable later. ──
    # Gap is signed from the recommended side: gap = P_pinnacle_fair − P_dk_implied.
    # For a value alert the entry gap is POSITIVE (Pinnacle rates the side higher
    # than DK's price implies). Convergence = the gap shrinking toward zero.
    # Convergence/gap math runs ONLY on SAME_PROPOSITION alerts (comparability
    # decided explicitly above). DIFFERENT_LINE / NO_REFERENCE rows still get
    # execution CLV + survival, but convergence and gap magnitudes stay NULL —
    # not-applicable-by-design, never a grading failure.
    pin_alert = a["sharp_prob"]
    if elig and dk_close and pin_close is not None and pin_alert is not None:
        pin_alert = float(pin_alert)
        dk_impl_entry, dk_impl_close = 1 / entry_dec, 1 / dk_close
        gap_initial = pin_alert - dk_impl_entry
        gap_final = pin_close - dk_impl_close
        d_dk = dk_impl_close - dk_impl_entry     # + = DK made the side more expensive
        d_pin = pin_close - pin_alert            # + = sharp fair moved toward the side
        # Max closure across the whole path (best convergence point, not just close).
        min_abs_gap = abs(gap_initial)
        for _ts, dk_dec_t, pin_t in series:
            if dk_dec_t and pin_t is not None:
                min_abs_gap = min(min_abs_gap, abs(pin_t - 1 / dk_dec_t))
        # Ratio is NULLED (not zero) when the initial gap is below the
        # min-meaningful threshold — otherwise trivial pp moves read as large
        # percentages. Absolute closure (pp) is always recorded and outranks it.
        ratio_ok = abs(gap_initial) >= _MIN_GAP_FOR_RATIO
        gcr = ((abs(gap_initial) - abs(gap_final)) / abs(gap_initial)) if ratio_ok else None
        grading.update(
            gap_initial_pp=round(gap_initial * 100, 3),
            gap_final_pp=round(gap_final * 100, 3),
            gap_abs_closure_pp=round((abs(gap_initial) - abs(gap_final)) * 100, 3),
            gap_max_closure_pp=round((abs(gap_initial) - min_abs_gap) * 100, 3),
            gap_closure_ratio=round(gcr, 4) if gcr is not None else None,
            gap_closure_ratio_suppressed=(not ratio_ok),
            d_dk_pp=round(d_dk * 100, 3),      # execution movement toward the bet
            d_pin_pp=round(d_pin * 100, 3),    # reference movement toward the bet
        )
        # Convergence is a PATH classification (who moved), NOT a quality verdict.
        # REFERENCE_CONVERGED_TO_EXECUTION = the sharp move was more consistent
        # with DK's original price than the reference's — evidence DK may have
        # led price discovery, not proof.
        dk_up, dk_down = d_dk > _CONV_EPS, d_dk < -_CONV_EPS
        pin_up, pin_down = d_pin > _CONV_EPS, d_pin < -_CONV_EPS
        if dk_up and pin_up:
            out["convergence"] = "BOTH_MOVED_TOWARD_BET"
        elif dk_down and pin_down:
            out["convergence"] = "BOTH_MOVED_AGAINST_BET"
        elif dk_up:
            out["convergence"] = "EXECUTION_CONVERGED_TO_REFERENCE"
        elif pin_down:
            out["convergence"] = "REFERENCE_CONVERGED_TO_EXECUTION"
        else:
            out["convergence"] = "DIVERGENCE_PERSISTED"

    out["grading_json"] = grading
    return out


def _append_grade_history_cur(cur, alert_id: int, g: dict, outcome=None) -> None:
    """Append one idempotent grade using the caller's transaction."""
    cur.execute(
        """SELECT 1 FROM alert_grades WHERE alert_id = %s AND is_current
             AND grading_version = %s AND convergence IS NOT DISTINCT FROM %s
             AND comparison_status IS NOT DISTINCT FROM %s
             AND outcome IS NOT DISTINCT FROM %s""",
        (alert_id, g["grading_version"], g["convergence"], g["comparison_status"], outcome),
    )
    if cur.fetchone():
        return
    cur.execute("UPDATE alert_grades SET is_current = FALSE WHERE alert_id = %s AND is_current",
                (alert_id,))
    cur.execute(
        """INSERT INTO alert_grades
             (alert_id, grading_version, comparison_status, convergence, outcome,
              dk_clv_pct, grading_json, is_current)
           VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)""",
        (alert_id, g["grading_version"], g["comparison_status"], g["convergence"],
         outcome, g["dk_clv_pct"], json.dumps(g["grading_json"])),
    )


def _append_grade_history(db, alert_id: int, g: dict, outcome=None) -> None:
    """Backward-compatible transactional grade append."""
    with db.connect() as conn:
        _append_grade_history_cur(conn.cursor(), alert_id, g, outcome)


def _dk_execution_clv(db, a) -> tuple[float | None, float | None]:
    """Back-compat shim over _grade_alert_prices (call sites updated in place)."""
    g = _grade_alert_prices(db, a)
    return g["dk_close_decimal"], g["dk_clv_pct"]


def settle(db: DatabaseManager, sport: str) -> int:
    """Grade alerts whose games have started: CLV always, outcome when scored."""
    matchup_tbl = _MATCHUP_TBL[sport]
    # Game-side alert types ONLY: prop alerts (dk_prop_value / prop_line_gap /
    # prop_outlier) carry player-market side strings that can never equal
    # 'home'/'away'/'draw', so grading them here silently marked every one
    # 'lost' regardless of the actual stat. They have their own settlers
    # (settle_props / settle_props_soccer / settle_tennis_totals).
    open_alerts = db.execute(
        "SELECT * FROM line_alerts WHERE sport = %s AND settled_at IS NULL "
        "AND alert_type IN ('pinnacle_divergence', 'pinnacle_favorite_forward', 'pinnacle_polymarket_delta', 'steam', 'dk_value', 'walking') "
        "AND commence_time IS NOT NULL AND commence_time <= NOW()",
        (sport,),
    )
    graded = 0
    for a in open_alerts:
        # CLV: vig-free P(side) at the last pre-commence per-book capture.
        close_source = ("AND books ? 'polymarket'"
                        if a["alert_type"] == "pinnacle_polymarket_delta"
                        else """AND EXISTS (
                          SELECT 1 FROM jsonb_object_keys(books) AS source(book_key)
                          WHERE source.book_key <> 'polymarket'
                        )""")
        close = db.execute_one(
            f"""
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND books IS NOT NULL
              AND captured_at <= %s
              {close_source}
            ORDER BY captured_at DESC LIMIT 1
            """,
            (sport, a["matchup_id"], a["commence_time"]),
        )
        close_prob = None
        if close and close["books"]:
            if a["alert_type"] == "pinnacle_polymarket_delta":
                poly = close["books"].get("polymarket")
                close_prob = _book_fair_side(poly, a["side"]) if poly else None
            else:
                close_prob = _retail_fair_side(close["books"], a["side"])
        clv_pp = ((close_prob - float(a["alert_prob"])) * 100
                  if close_prob is not None and a["alert_prob"] is not None else None)

        # Outcome from the final score / winner (soccer: 90' regulation score).
        outcome = None
        if sport == "tennis":
            m = db.execute_one(
                f"SELECT winner, completion_status FROM {matchup_tbl} WHERE id = %s",
                (a["matchup_id"],),
            )
            if m:
                details = a["details_json"] or {}
                book = details.get("exec_book") or details.get("clv_book")
                if book is None and details.get("dk_odds") is not None:
                    book = "draftkings"
                outcome = settle_tennis_selection(
                    book=book, market="moneyline", selection_side=a["side"],
                    winner_side=m["winner"],
                    completion_status=m["completion_status"],
                )
        else:
            hs_col, as_col = _SCORE_COLS[sport]
            m = db.execute_one(
                f"SELECT {hs_col} AS hs, {as_col} AS as_ FROM {matchup_tbl} WHERE id = %s",
                (a["matchup_id"],),
            )
            if m and m["hs"] is not None and m["as_"] is not None:
                hs, as_ = int(m["hs"]), int(m["as_"])
                outcome = _game_side_outcome(sport, hs, as_, a["side"])

        # Settle once we have at least the CLV grade; outcome may lag scores
        # and is filled in the same pass on a later run if still NULL then.
        if clv_pp is None and outcome is None:
            continue
        g = _grade_alert_prices(db, a)
        if sport == "tennis" and m:
            g["grading_json"] = {
                **(g["grading_json"] or {}),
                **tennis_rule_snapshot(book, "moneyline"),
                "completion_status": m["completion_status"],
            }
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE line_alerts SET close_prob = %s, clv_pp = %s, outcome = %s, "
                "dk_close_decimal = %s, dk_clv_pct = %s, pin_close_prob = %s, "
                "convergence = %s, dk_survival_min = %s, grading_json = %s, comparison_status = %s, grading_version = %s, "
                "settled_at = CASE WHEN %s::text IS NOT NULL THEN NOW() ELSE settled_at END "
                "WHERE id = %s",
                (close_prob, clv_pp, outcome, g["dk_close_decimal"], g["dk_clv_pct"],
                 g["pin_close_prob"], g["convergence"], g["dk_survival_min"],
                 json.dumps(g["grading_json"]), g["comparison_status"],
                 g["grading_version"], outcome, a["id"]),
            )
            _append_grade_history_cur(cur, a["id"], g, outcome=outcome)
        graded += 1
    if sport == "nfl":
        graded += _settle_nfl_line_alerts(db)
    if graded:
        print(f"Line alerts ({sport}): {graded} graded")
    return graded


def _settle_nfl_line_alerts(db: DatabaseManager) -> int:
    alerts = db.execute(
        """
        SELECT a.*, m.home_score, m.away_score
        FROM line_alerts a
        JOIN nfl_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'nfl' AND a.settled_at IS NULL
          AND a.alert_type IN ('spread_steam', 'spread_walking', 'total_steam', 'total_walking')
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        """
    )
    graded = 0
    for alert in alerts:
        details = alert["details_json"] or {}
        market = details.get("market")
        side = alert["side"]
        try:
            trigger_line = float(details["trigger_line"])
        except (KeyError, TypeError, ValueError):
            logger.error("NFL line alert %s is missing trigger_line", alert["id"])
            continue
        close = db.execute_one(
            """
            SELECT home_spread, COALESCE(vegas_total_raw, vegas_total) AS total
            FROM game_odds_history
            WHERE sport = 'nfl' AND matchup_id = %s AND captured_at <= %s
            ORDER BY captured_at DESC LIMIT 1
            """,
            (alert["matchup_id"], alert["commence_time"]),
        )
        close_value = close.get("home_spread" if market == "spread" else "total") if close else None
        if close_value is None:
            continue
        close_line = float(close_value)
        outcome = _nfl_line_outcome(
            market,
            side,
            trigger_line,
            int(alert["home_score"]),
            int(alert["away_score"]),
        )
        line_clv = _nfl_line_clv(market, side, trigger_line, close_line)
        grading_json = {
            "market": market,
            "selection": side,
            "trigger_line": trigger_line,
            "close_line": close_line,
            "line_clv": round(line_clv, 3),
            "home_score": int(alert["home_score"]),
            "away_score": int(alert["away_score"]),
        }
        grade = {
            "grading_version": "nfl-lines-v1",
            "comparison_status": "SAME_PROPOSITION",
            "convergence": None,
            "dk_clv_pct": None,
            "grading_json": grading_json,
        }
        db.execute(
            """
            UPDATE line_alerts SET outcome = %s, settled_at = NOW(),
                grading_json = %s, comparison_status = %s, grading_version = %s
            WHERE id = %s
            """,
            (outcome, json.dumps(grading_json), grade["comparison_status"],
             grade["grading_version"], alert["id"]),
        )
        _append_grade_history(db, alert["id"], grade, outcome=outcome)
        graded += 1
    return graded


def report(db: DatabaseManager) -> None:
    """The audit: does each alert type beat the close, and win at the flagged rate?

    dk_value additionally gets true ROI: 1 unit staked at DK's frozen price per
    settled alert — the direct answer to "is betting these lines profitable".
    """
    health = check_detector_health(db)
    dead = [h for h in health if h["status"] == "dead"]
    print("=== Detector health — has each detector EVER fired since it shipped? ===")
    if dead:
        for h in dead:
            print(f"  DEAD  {h['sport']:<8}{h['alert_type']:<22} deployed={h['deployed_at']} "
                  f"({h['days_deployed']}d ago) — 0 alerts, {h['opportunity_days']}d of eligible "
                  f"games in the last {_HEALTH_OPPORTUNITY_DAYS}d. Check the field/market this detector "
                  f"reads actually exists in the captured books.")
    else:
        print("  none dead — every detector past its judging window has fired at least once")
    quiet = [h for h in health if h["status"] in ("too_new", "no_opportunity")]
    if quiet:
        print("  (" + "; ".join(
            f"{h['sport']}/{h['alert_type']}: {h['status']}" for h in quiet) + ")")
    print()

    rows = db.execute(
        """
        SELECT sport, alert_type, COUNT(*) n,
               COUNT(*) FILTER (WHERE clv_pp IS NOT NULL) n_clv,
               ROUND(AVG(clv_pp)::numeric, 2) avg_clv_pp,
               ROUND(AVG((clv_pp > 0)::int)::numeric, 2) beat_close,
               COUNT(*) FILTER (WHERE outcome IN ('won','lost')) n_out,
               ROUND(AVG((outcome = 'won')::int)
                     FILTER (WHERE outcome IN ('won','lost'))::numeric, 3) win_rate,
               ROUND(AVG(alert_prob)
                     FILTER (WHERE outcome IN ('won','lost'))::numeric, 3) implied_rate,
               ROUND(SUM(CASE WHEN outcome = 'won'
                              THEN (details_json->>'dk_decimal')::numeric - 1
                              WHEN outcome = 'lost' THEN -1 END)
                     FILTER (WHERE details_json ? 'dk_decimal')::numeric, 2) dk_units,
               COUNT(*) FILTER (WHERE dk_clv_pct IS NOT NULL) n_dkclv,
               ROUND(AVG(dk_clv_pct)::numeric, 2) avg_dk_clv
        FROM line_alerts
        GROUP BY sport, alert_type ORDER BY sport, alert_type
        """
    )
    print("\n=== Line-alert backtest — CLV (beat the close?) + outcomes (win at the flagged rate?) ===")
    if not rows:
        print("  (no alerts recorded yet)")
    for r in rows:
        line = (f"  {r['sport']:<8}{r['alert_type']:<22} n={r['n']:>4}  "
                f"CLV: n={r['n_clv']} avg={r['avg_clv_pp'] or 0:+}pp beat-close={r['beat_close'] or 0}  "
                f"outcomes: n={r['n_out']} win={r['win_rate']} implied={r['implied_rate']}")
        if (r["alert_type"] in ("dk_value", "dk_prop_value", "prop_line_gap")
                and r["dk_units"] is not None and r["n_out"]):
            line += f"  ROI@DK: {r['dk_units']:+}u/{r['n_out']} ({float(r['dk_units'])/r['n_out']*100:+.1f}%)"
        if r["n_dkclv"]:
            line += f"  execCLV: {r['avg_dk_clv']:+}% (n={r['n_dkclv']})"
        print(line)

    # ── EV-tier calibration (monotonicity): a real signal should RANK ──
    # opportunities — higher claimed EV should win/return more. If 6% edges
    # perform no better than 1% edges, the numeric precision is decorative.
    tiers = db.execute(
        """
        SELECT CASE WHEN ev >= 8 THEN '8%%+'
                    WHEN ev >= 5 THEN '5-8%%'
                    WHEN ev >= 3 THEN '3-5%%'
                    ELSE '<3%%' END AS tier,
               MIN(ev) AS tier_min,
               COUNT(*) n,
               COUNT(*) FILTER (WHERE outcome IN ('won','lost')) n_out,
               COUNT(*) FILTER (WHERE outcome = 'won') wins,
               COUNT(*) FILTER (WHERE outcome = 'lost') losses,
               COUNT(*) FILTER (WHERE outcome = 'void') pushes,
               ROUND(AVG((outcome = 'won')::int)
                     FILTER (WHERE outcome IN ('won','lost'))::numeric, 3) win_rate,
               ROUND(SUM(CASE WHEN outcome = 'won'
                              THEN (details_json->>'dk_decimal')::numeric - 1
                              WHEN outcome = 'lost' THEN -1 END)::numeric, 2) units,
               ROUND(AVG(dk_clv_pct)::numeric, 2) avg_dk_clv,
               ROUND(AVG((pin_close_prob - sharp_prob) * 100)
                     FILTER (WHERE pin_close_prob IS NOT NULL)::numeric, 2) ref_clv_pp,
               -- persisted-to-close as an EXPLICIT numerator/denominator:
               -- denominator = alerts with an OBSERVABLE comparable close only.
               COUNT(*) FILTER (WHERE dk_close_decimal IS NOT NULL) n_obsclose,
               COUNT(*) FILTER (WHERE dk_survival_min IS NULL AND dk_close_decimal IS NOT NULL) n_persist,
               COUNT(*) FILTER (WHERE grading_json->>'close_status' = 'MARKET_CHANGED') n_mktchg,
               COUNT(*) FILTER (WHERE grading_json->>'close_status' = 'CLOSE_UNAVAILABLE') n_noclose
        FROM (
            SELECT *, COALESCE((details_json->>'ev_pct')::numeric,
                               (details_json->>'edge_vs_median_pct')::numeric) AS ev
            FROM line_alerts
            WHERE alert_type IN ('dk_value','dk_prop_value','prop_outlier')
        ) t
        WHERE ev IS NOT NULL
        GROUP BY 1 ORDER BY tier_min
        """
    )
    if tiers:
        print("\n  Claimed-EV tier calibration — three separate monotonicity verdicts:")
        print("    signal (refCLV rises w/ tier) | tradability (ROI rises among survivors) | decay (survival falls w/ tier)")
        for t in tiers:
            roi = (f"{float(t['units'])/t['n_out']*100:+.1f}%" if t["units"] is not None and t["n_out"] else "—")
            # Persisted-to-close as numerator/denominator (denominator = alerts
            # with an OBSERVABLE comparable close). Alerts without one are shown
            # separately, never flattered into the numerator or counted as
            # failures to persist. Cadence-robust (no mixed-cadence median);
            # per-row interval bounds live in grading_json for stratified
            # interval-censored survival analysis once the sample supports it.
            persist = (f"{t['n_persist']}/{t['n_obsclose']}" if t["n_obsclose"] else "—")
            excl = []
            if t["n_mktchg"]:  excl.append(f"{t['n_mktchg']} line-moved")
            if t["n_noclose"]: excl.append(f"{t['n_noclose']} close-unavail")
            excl_s = f" (+{', '.join(excl)})" if excl else ""
            # Below the threshold, show raw counts + a DESCRIPTIVE-ONLY tag —
            # never a rate that implies statistical conclusions from ~single
            # digits. (30 is a floor for INDEPENDENT bets; correlated same-slate
            # props carry less information, so treat even 30 cautiously.)
            status = "descriptive-only" if t["n_out"] < _MIN_SETTLED_FOR_CI else "n≥floor"
            print(f"    {t['tier']:<6} n={t['n']:>4} settled={t['n_out']:>3} "
                  f"(W{t['wins']}-L{t['losses']}-P{t['pushes']})  "
                  f"win={t['win_rate'] if t['win_rate'] is not None else '—'}  "
                  f"ROI@DK={roi}  refCLV={t['ref_clv_pp'] if t['ref_clv_pp'] is not None else '—'}pp  "
                  f"execCLV={t['avg_dk_clv'] if t['avg_dk_clv'] is not None else '—'}%  "
                  f"persisted={persist}{excl_s}  [{status}]")

    conv = db.execute(
        """SELECT convergence, COUNT(*) n,
              COUNT(*) FILTER (WHERE outcome = 'won') wins,
              COUNT(*) FILTER (WHERE outcome = 'lost') losses,
              ROUND(AVG((grading_json->>'gap_closure_ratio')::numeric)::numeric, 2) avg_gcr
           FROM line_alerts WHERE convergence IS NOT NULL
           GROUP BY 1 ORDER BY n DESC"""
    )
    if conv:
        print("\n  Gap-convergence PATH classification (who moved — not a quality verdict; each row is a")
        print("  testable population, e.g. DIVERGENCE_PERSISTED is neutral until its ROI/calibration is measured):")
        for c in conv:
            print(f"    {c['convergence']:<34} n={c['n']:>4}  W{c['wins']}-L{c['losses']}  "
                  f"avgGapClosure={c['avg_gcr'] if c['avg_gcr'] is not None else '—'}  [descriptive-only]")

    comp = db.execute(
        """SELECT comparison_status, COUNT(*) n,
              COUNT(*) FILTER (WHERE convergence IS NOT NULL) has_conv
           FROM line_alerts WHERE comparison_status IS NOT NULL
           GROUP BY 1 ORDER BY n DESC"""
    )
    if comp:
        print("\n  Proposition comparability (NULL convergence on non-SAME_PROPOSITION = not-applicable-BY-DESIGN):")
        for c in comp:
            print(f"    {c['comparison_status']:<22} n={c['n']:>4}  with-convergence={c['has_conv']}")
    print()


def dk_board(db: DatabaseManager) -> None:
    """Live board: every upcoming game's DK price vs Pinnacle fair, sorted by EV.

    Shows ALL sides (including negative EV) so the vig is visible — the point
    is to see which DK lines are stale, not to pretend everything is a bet.
    """
    print("\n=== DraftKings vs Pinnacle — live board (EV of 1u at DK judged by Pinnacle fair) ===")
    for sport in _ALERT_SPORTS:
        matchup_tbl = _MATCHUP_TBL[sport]
        rows = db.execute(
            f"""
            SELECT DISTINCT ON (h.matchup_id)
                   h.home_team_name, h.away_team_name, h.books, m.commence_time
            FROM game_odds_history h
            JOIN {matchup_tbl} m ON m.id = h.matchup_id
            WHERE h.sport = %s AND h.books IS NOT NULL AND m.commence_time > NOW()
            ORDER BY h.matchup_id, h.captured_at DESC
            """,
            (sport,),
        )
        board = []
        for r in rows:
            books = r["books"] or {}
            pin, dk = books.get("pinnacle"), books.get(_DK_BOOK)
            if not pin or not dk:
                continue
            names = {"home": r["home_team_name"], "away": r["away_team_name"], "draw": "Draw"}
            for side in _sides(books):
                ev = _dk_value_ev(pin, dk, side)
                if ev is None:
                    continue
                odds = _dk_side_odds(dk, side)
                fair = _book_fair_side(pin, side)
                board.append((ev, f"{r['away_team_name']} @ {r['home_team_name']}",
                              names[side], odds, fair))
        if not board:
            continue
        board.sort(reverse=True)
        print(f"\n  {sport.upper()}")
        for ev, game, pick, odds, fair in board[:12]:
            flag = " <-- BET-GRADE VALUE" if ev >= _DK_VALUE_MIN_EV else ""
            print(f"    {game:<40} {pick:<22} DK {odds:>+5}  pin-fair {fair*100:5.1f}%  EV {ev*100:+5.1f}%{flag}")
    print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp1252 console
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Sharp line-movement alerts + audit")
    parser.add_argument("--sport", choices=list(_ALERT_SPORTS), help="Scan + settle one sport")
    parser.add_argument("--report", action="store_true", help="Print the backtest")
    parser.add_argument("--dk-board", action="store_true",
                        help="Live DraftKings-vs-Pinnacle EV board, all sports")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    if args.sport:
        scan(db, args.sport)
        settle(db, args.sport)
        if args.sport == "mlb":
            scan_props(db)
            settle_props(db)
        if args.sport == "soccer":
            # scan_props_soccer is RETIRED (2026-08-13) — see its docstring.
            # Settlement still runs so any alert already in the ledger finishes
            # grading; the ledger is append-only and keeps the full history.
            settle_props_soccer(db)
        if args.sport == "tennis":
            scan_tennis_totals(db)
            settle_tennis_totals(db)
    if args.dk_board:
        dk_board(db)
    if args.report or (not args.sport and not args.dk_board):
        report(db)
