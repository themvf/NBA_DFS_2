"""Sharp line-movement alerts — detect, notify, and AUDIT (Edge-Finding P2).

Runs after every odds capture. Two detectors over game_odds_history:

  * **pinnacle_divergence** — Pinnacle's vig-free probability sits >=
    _PIN_GAP_MIN_PP off the retail-consensus probability on some side of an
    upcoming game. Pinnacle is the sharp reference; the side it prices HIGHER
    than retail is the sharp side, and retail is offering a stale price on it.
  * **steam** — between the last two captures, >= _STEAM_MIN_BOOKS books moved
    the same side by >= _STEAM_MIN_MOVE_PP. Synchronized moves are informed
    money; solo moves are book position management.

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
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from model.line_movement import _MATCHUP_TBL, _book_fair_home
from model.soccer_bet_rating import american_to_decimal

logger = logging.getLogger(__name__)

_PIN_GAP_MIN_PP = 2.0     # Pinnacle vs retail consensus, probability points
_STEAM_MIN_BOOKS = 3      # books moving together between consecutive captures
_STEAM_MIN_MOVE_PP = 1.5  # per-book move threshold, probability points
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

# Sports wired into the alert pipeline. NBA joins when the season resumes —
# nba_matchups has no commence_time column yet, which scan()/settle()/dk_board
# all join on.
_ALERT_SPORTS = ("mlb", "soccer", "tennis")

# Grading sources per sport: (home score col, away score col). Soccer uses the
# 90-minute regulation score — a knockout tie decided in extra time is a DRAW
# for market purposes (the Belgium 3-2 aet lesson). Tennis uses the winner col.
_SCORE_COLS = {
    "mlb": ("home_score", "away_score"),
    "nba": ("home_score", "away_score"),
    "soccer": ("COALESCE(reg_home_score, home_score)", "COALESCE(reg_away_score, away_score)"),
}


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
    """Mean vig-free P(side) across all non-Pinnacle books."""
    probs = [p for key, b in books.items() if key != "pinnacle"
             for p in [_book_fair_side(b, side)] if p is not None]
    return sum(probs) / len(probs) if probs else None


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
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="pinnacle_divergence", side=side,
                        alert_prob=retail, sharp_prob=sharp,
                        details={"gap_pp": round(gap_pp, 2),
                                 "n_books": len(books)},
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
                        details={"ev_pct": round(ev * 100, 2),
                                 "dk_odds": dk_odds,
                                 "dk_decimal": round(american_to_decimal(dk_odds), 4)
                                 if dk_odds is not None else None},
                    ))
        # ── Steam (needs the previous capture) ──
        prev = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND captured_at < %s
              AND books IS NOT NULL
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
                                 "avg_move_pp": round(sum(movers) / len(movers), 2)},
                    ))
    if new_alerts:
        print(f"Line alerts ({sport}): {len(new_alerts)} new — "
              + ", ".join(f"{a['alert_type']}:{a['matchup']}/{a['side']}" for a in new_alerts))
        _notify(new_alerts)
    return len(new_alerts)


def _insert(db, *, sport, r, label, alert_type, side, alert_prob, sharp_prob, details) -> list[dict]:
    """First-breach insert; returns [alert] only when a NEW row was created."""
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
_PROP_VALUE_MIN_EV = 0.03
_PROP_LINE_GAP_MIN = 1.0
_PROP_MARKET_LABEL = {"pitcher_strikeouts": "K", "batter_total_bases": "TB"}


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

        # Same-line price value at DK.
        if dk_line == pin_line:
            for side, fair, price_key in (("Over", fair_over, "over"), ("Under", fair_under, "under")):
                price = dk.get(price_key)
                if price is None:
                    continue
                dec = american_to_decimal(int(price))
                if dec >= _DK_VALUE_MAX_DECIMAL:
                    continue
                ev = fair * dec - 1
                if ev >= _PROP_VALUE_MIN_EV:
                    new_alerts.extend(_insert(
                        db, sport="mlb", r=shim, label=label,
                        alert_type="dk_prop_value",
                        side=f"{r['player']} {mk} {side[0]}{dk_line}",
                        alert_prob=1 / dec, sharp_prob=fair,
                        details={"market": r["market"], "player": r["player"],
                                 "line": dk_line, "bet": side,
                                 "dk_odds": int(price),
                                 "dk_decimal": round(dec, 4),
                                 "ev_pct": round(ev * 100, 2)},
                    ))
        # Stale-line gap (regardless of prices).
        elif abs(dk_line - pin_line) >= _PROP_LINE_GAP_MIN:
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
                                 "ev_pct": round(ev * 100, 2)},
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
                         "dk_decimal": round(dec, 4) if dec else None},
            ))
    if new_alerts:
        print(f"Tennis totals alerts: {len(new_alerts)} new — "
              + ", ".join(f"{a['matchup']} {a['side']}" for a in new_alerts[:5]))
        _notify(new_alerts)
    return len(new_alerts)


def settle_tennis_totals(db: DatabaseManager) -> int:
    """Grade tennis totals alerts from final games; retirements void (book rule)."""
    open_alerts = db.execute(
        """
        SELECT a.*, m.home_games, m.away_games, m.winner
        FROM line_alerts a JOIN tennis_matches m ON m.id = a.matchup_id
        WHERE a.sport = 'tennis' AND a.alert_type IN ('dk_prop_value', 'prop_line_gap')
          AND a.settled_at IS NULL AND m.winner IS NOT NULL
        """
    )
    graded = 0
    for a in open_alerts:
        d = a["details_json"] or {}
        if a["winner"] == "retired":
            outcome = "void"
        elif a["home_games"] is None or a["away_games"] is None:
            continue  # winner known but games not filled yet — next pass
        else:
            total = int(a["home_games"]) + int(a["away_games"])
            line, bet = float(d["line"]), d["bet"]
            if total == line:
                outcome = "void"
            elif (total > line) == (bet == "Over"):
                outcome = "won"
            else:
                outcome = "lost"
        db.execute(
            "UPDATE line_alerts SET outcome = %s, settled_at = NOW(), "
            "details_json = details_json || jsonb_build_object('actual', %s) WHERE id = %s",
            (outcome,
             (int(a["home_games"]) + int(a["away_games"]))
             if a["home_games"] is not None and a["away_games"] is not None else None,
             a["id"]),
        )
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


def scan_props_soccer(db: DatabaseManager) -> int:
    """Flag WC anytime-scorer prices where DK is a fat outlier vs the median.

    A raw price median is the WRONG anchor here: uk/eu books carry far heavier
    ATGS margin than DK, so every DK price looks "long" against them and the
    first raw scan flagged 24% of the board — book-style artifact, not edge.
    Instead each book's implied probabilities are normalized by that book's
    own total over all players in the game (its overround), making shares
    comparable across margin structures; the flag fires only when DK prices a
    player RELATIVELY longer than the median book does.
    """
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
        db.execute(
            "UPDATE line_alerts SET outcome = %s, settled_at = NOW() WHERE id = %s",
            ("won" if scored else "lost", a["id"]),
        )
        graded += 1
    if graded:
        print(f"WC prop alerts: {graded} graded from the goal timeline")
    return graded


def _mlb_boxscore_stat(game_pk: str, player: str, market: str) -> float | None:
    """Actual K's (pitcher) or total bases (batter) from the free MLB boxscore."""
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
            if market == "pitcher_strikeouts":
                v = (stats.get("pitching") or {}).get("strikeOuts")
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
        db.execute(
            "UPDATE line_alerts SET outcome = %s, settled_at = NOW(), "
            "details_json = details_json || jsonb_build_object('actual', %s) WHERE id = %s",
            (outcome, actual, a["id"]),
        )
        graded += 1
    if graded:
        print(f"Prop alerts (mlb): {graded} graded from boxscores")
    return graded


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
        "AND alert_type IN ('pinnacle_divergence', 'steam', 'dk_value') "
        "AND commence_time IS NOT NULL AND commence_time <= NOW()",
        (sport,),
    )
    graded = 0
    for a in open_alerts:
        # CLV: vig-free P(side) at the last pre-commence per-book capture.
        close = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND books IS NOT NULL
              AND captured_at <= %s
            ORDER BY captured_at DESC LIMIT 1
            """,
            (sport, a["matchup_id"], a["commence_time"]),
        )
        close_prob = None
        if close and close["books"]:
            close_prob = _retail_fair_side(close["books"], a["side"])
        clv_pp = ((close_prob - float(a["alert_prob"])) * 100
                  if close_prob is not None and a["alert_prob"] is not None else None)

        # Outcome from the final score / winner (soccer: 90' regulation score).
        outcome = None
        if sport == "tennis":
            m = db.execute_one(f"SELECT winner FROM {matchup_tbl} WHERE id = %s", (a["matchup_id"],))
            if m and m["winner"] in ("home", "away"):
                outcome = "won" if m["winner"] == a["side"] else "lost"
            elif m and m["winner"] == "retired":
                outcome = "void"
        else:
            hs_col, as_col = _SCORE_COLS[sport]
            m = db.execute_one(
                f"SELECT {hs_col} AS hs, {as_col} AS as_ FROM {matchup_tbl} WHERE id = %s",
                (a["matchup_id"],),
            )
            if m and m["hs"] is not None and m["as_"] is not None:
                hs, as_ = int(m["hs"]), int(m["as_"])
                winner = "home" if hs > as_ else "away" if as_ > hs else "draw"
                outcome = "won" if winner == a["side"] else "lost"

        # Settle once we have at least the CLV grade; outcome may lag scores
        # and is filled in the same pass on a later run if still NULL then.
        if clv_pp is None and outcome is None:
            continue
        db.execute(
            "UPDATE line_alerts SET close_prob = %s, clv_pp = %s, outcome = %s, "
            "settled_at = CASE WHEN %s::text IS NOT NULL THEN NOW() ELSE settled_at END "
            "WHERE id = %s",
            (close_prob, clv_pp, outcome, outcome, a["id"]),
        )
        graded += 1
    if graded:
        print(f"Line alerts ({sport}): {graded} graded")
    return graded


def report(db: DatabaseManager) -> None:
    """The audit: does each alert type beat the close, and win at the flagged rate?

    dk_value additionally gets true ROI: 1 unit staked at DK's frozen price per
    settled alert — the direct answer to "is betting these lines profitable".
    """
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
                     FILTER (WHERE details_json ? 'dk_decimal')::numeric, 2) dk_units
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
        print(line)
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
            scan_props_soccer(db)
            settle_props_soccer(db)
        if args.sport == "tennis":
            scan_tennis_totals(db)
            settle_tennis_totals(db)
    if args.dk_board:
        dk_board(db)
    if args.report or (not args.sport and not args.dk_board):
        report(db)
