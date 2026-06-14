"""First-goal-scorer model (firstscorer-v3) — stat-driven + market blend.

v1 used raw anytime market shares → deflated favorites due to longshot vig.
v2 (power de-vig) fixed calibration by solving k so Σ -ln(1-p^k) = Λ but
relied entirely on market data for the "our model" component.

v3 replaces the market-derived component with a **genuine statistical signal**:
historical xG/90 rates from StatsBomb World Cup + continental tournament data
(ingested by ingest.soccer_player_history).  The stat model is independent of
the first-scorer market, so disagreements between our rates and market pricing
are real edge signals.

Pipeline:
  1. Load combined player stats from soccer_player_stats (xg_per_90 per player).
  2. For each fixture, distribute the team's match xG (our_home_xg / our_away_xg)
     across players proportional to their historical xg_per_90, with position-
     based fallbacks for players not in our DB.
  3. Convert per-player λ to first-scorer probability via Poisson superposition:
       P(player p scores first) = (λ_p / Λ) × (1 − e^(−Λ))
  4. Power-de-vig the first-scorer market to get market_fair.
  5. Blend: our_prob = _W_STAT × stat_prob + (1 − _W_STAT) × market_fair.
     (market retains 60% weight to capture current form, injuries, lineup news)

Reference for edge = market_fair; EV uses best offered odds.

Usage:
    python -m model.soccer_first_scorer
    python -m model.soccer_first_scorer --hours 96
"""

from __future__ import annotations

import argparse
import logging
import math
import unicodedata

from config import load_config
from db.database import DatabaseManager
from db.queries import get_all_soccer_player_stats
from ingest.soccer_props import fetch_player_markets
from model.soccer_bet_rating import new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "firstscorer-v3"
DEFAULT_WINDOW_HOURS = 72
_CLAMP_HI = 0.999
_MIN_ANYTIME_PROB = 0.01
# How much weight to give our stat-based estimate vs the de-vigged market.
_W_STAT = 0.40

# xG/90 population baselines by position for players not in the stats DB.
# Calibrated from typical WC player populations (StatsBomb WC 2018+2022 averages).
_POS_XG_DEFAULT = {
    "FW": 0.220,
    "MF": 0.065,
    "DF": 0.022,
    "GK": 0.003,
}
_UNKNOWN_XG_DEFAULT = 0.065  # fallback when position unknown


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum() or ch == " ").strip()


def _build_fuzzy_lookup(stat_lookup: dict[str, dict]) -> dict[str, dict]:
    """Build a secondary lookup that also indexes by subsets of name tokens.

    StatsBomb uses full legal names ("Lionel Andrés Messi Cuccittini") while
    the Odds API uses display names ("Lionel Messi").  For each player, index
    by every subsequence of adjacent tokens so "lionel messi" maps to the row
    for "lionel andres messi cuccittini".

    Ambiguous keys (same token-subset matches multiple players) are not indexed
    so we fall back to the position default rather than guess.
    """
    # Primary: exact key (already done via stat_lookup)
    # Secondary: for each player name, all sub-token spans of length 2+
    token_map: dict[str, list[dict]] = {}  # candidate_key → [rows]
    for norm_name, row in stat_lookup.items():
        tokens = norm_name.split()
        n = len(tokens)
        # All contiguous spans of 2+ tokens up to the full name.
        for start in range(n):
            for end in range(start + 2, n + 1):
                key = " ".join(tokens[start:end])
                if key not in token_map:
                    token_map[key] = []
                token_map[key].append(row)

    # Only keep unambiguous entries (single match per key).
    return {k: rows[0] for k, rows in token_map.items() if len(rows) == 1}


def _lookup_player(
    norm_name: str,
    stat_lookup: dict[str, dict],
    fuzzy_lookup: dict[str, dict],
) -> dict | None:
    """Look up a player by normalized display name with fuzzy fallback."""
    if norm_name in stat_lookup:
        return stat_lookup[norm_name]
    # Try token-subset: "lionel messi" should find "lionel andres messi cuccittini"
    return fuzzy_lookup.get(norm_name)


def _clamp(p: float, lo: float = 1e-6, hi: float = _CLAMP_HI) -> float:
    return min(max(p, lo), hi)


# ── Market de-vig helpers (unchanged from v2) ─────────────────────────────────

def power_devig_exclusive(raw_probs: list[float]) -> list[float]:
    """De-vig a mutually-exclusive market: find k so Σ pᵏ = 1, return [pᵏ]."""
    probs = [_clamp(p) for p in raw_probs if p > 0]
    if not probs:
        return []
    if sum(probs) <= 1.0:
        return probs
    lo, hi = 1.0, 12.0
    for _ in range(60):
        k = (lo + hi) / 2
        s = sum(p ** k for p in probs)
        if s > 1.0:
            lo = k
        else:
            hi = k
    k = (lo + hi) / 2
    return [p ** k for p in probs]


# ── Stat-based first-scorer model ─────────────────────────────────────────────

def _compute_stat_first_probs(
    player_norm_names: list[str],
    team_match_xg: float,
    total_match_xg: float,
    stat_lookup: dict[str, dict],
    fuzzy_lookup: dict[str, dict],
) -> dict[str, float]:
    """Return {norm_name: first_scorer_prob} using historical xG/90 rates.

    Steps:
    1. Look up each player's xg_per_90 (exact then fuzzy); fall back to position default.
    2. Scale to this match: λ_p = (xg_per_90 / Σ xg_per_90) × team_match_xg.
    3. P(scores first) = (λ_p / Λ_total) × (1 - e^(-Λ_total)).
    """
    if not player_norm_names or team_match_xg <= 0 or total_match_xg <= 0:
        return {}

    # Look up historical xG/90 per player; fall back to position defaults.
    xg90: dict[str, float] = {}
    for nname in player_norm_names:
        row = _lookup_player(nname, stat_lookup, fuzzy_lookup)
        if row and row.get("xg_per_90") and row["xg_per_90"] > 0:
            xg90[nname] = float(row["xg_per_90"])
        elif row and row.get("position"):
            pos = (row["position"] or "MF").upper()[:2]
            xg90[nname] = _POS_XG_DEFAULT.get(pos, _UNKNOWN_XG_DEFAULT)
        else:
            xg90[nname] = _UNKNOWN_XG_DEFAULT

    total_xg90 = sum(xg90.values()) or 1.0

    # Each player's expected goals in this specific match.
    lambda_p = {
        nname: (rate / total_xg90) * team_match_xg
        for nname, rate in xg90.items()
    }

    # Poisson first-scorer formula: P(p scores first) = (λ_p / Λ) × (1 - e^(-Λ))
    p_at_least_one = 1.0 - math.exp(-total_match_xg)
    result: dict[str, float] = {}
    for nname, lam in lambda_p.items():
        result[nname] = (lam / total_match_xg) * p_at_least_one

    return result


def predict_and_record(
    db: DatabaseManager,
    api_key: str,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> int:
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch first-scorer markets")
        return 0

    # Load player stats once (the full combined dataset) + build fuzzy name index.
    stat_lookup = get_all_soccer_player_stats(db, season="combined")
    fuzzy_lookup = _build_fuzzy_lookup(stat_lookup)
    logger.info("Loaded %d players from soccer_player_stats (%d fuzzy keys)",
                len(stat_lookup), len(fuzzy_lookup))

    # Clear UNLOCKED pending first-scorer rows before re-rating (no orphans;
    # locked closing lines and settled rows are preserved for the backtest).
    db.execute(
        "DELETE FROM soccer_bets WHERE bet_type = 'first_scorer' "
        "AND status = 'pending' AND locked = FALSE",
    )

    fixtures = db.execute(
        """
        SELECT sm.id, sm.game_id, sm.commence_time, sm.our_total_pred,
               sm.our_home_xg, sm.our_away_xg,
               h.name AS home, a.name AS away
        FROM soccer_matchups sm
        JOIN soccer_teams h ON h.team_id = sm.home_team_id
        JOIN soccer_teams a ON a.team_id = sm.away_team_id
        WHERE sm.game_id IS NOT NULL
          AND sm.commence_time IS NOT NULL
          AND sm.commence_time >= NOW()
          AND sm.commence_time <= NOW() + (%s || ' hours')::interval
          AND sm.our_total_pred IS NOT NULL
        ORDER BY sm.commence_time ASC
        """,
        (str(window_hours),),
    )
    if not fixtures:
        print("First scorer: no near-term fixtures with predictions to process")
        return 0

    capture_key = new_capture_key()
    written = 0
    stat_hits = 0
    stat_misses = 0

    for fx in fixtures:
        markets = fetch_player_markets(api_key, fx["game_id"])
        if not markets or not markets["first"]:
            continue

        # Match-level totals from our prediction model.
        total_xg = float(fx["our_total_pred"])
        home_xg = float(fx["our_home_xg"] or total_xg / 2)
        away_xg = float(fx["our_away_xg"] or total_xg / 2)
        p_at_least_one = 1.0 - math.exp(-total_xg)

        fs_items = [(npl, fs) for npl, fs in markets["first"].items() if fs.get("prob_raw")]
        if not fs_items:
            continue

        # ── Market de-vig (same as v2) ──
        raw_list = [fs["prob_raw"] for _, fs in fs_items]
        raw_list.append(markets.get("no_scorer_raw", 0.0) or 0.0)
        devigged = power_devig_exclusive(raw_list)
        market_fair: dict[str, float] = {
            fs_items[i][0]: devigged[i]
            for i in range(len(fs_items))
        } if devigged else {}

        # ── Stat-based model ──
        # Identify which team each player belongs to by name matching (rough:
        # home/away are team-level markets and the Odds API lists both teams
        # together, so we allocate by checking the anytime market's team context
        # or fall back to assigning all players to use the total match xG).
        # Since we can't reliably split by team from the market alone, we use
        # total_match_xg in the denominator and each player's share proportionally.
        # This is equivalent to assuming the stat model knows the individual
        # rates correctly relative to each other (which it does) — the absolute
        # magnitude is set by total_xg anyway.
        home_norm = _norm(fx["home"])
        away_norm = _norm(fx["away"])

        # Try to assign players to home/away via team membership in stat_lookup.
        def team_xg_for(player_nname: str) -> float:
            row = stat_lookup.get(player_nname)
            if row:
                tnorm = _norm(row.get("team_name") or "")
                if home_norm and tnorm and (home_norm in tnorm or tnorm in home_norm):
                    return home_xg
                if away_norm and tnorm and (away_norm in tnorm or tnorm in away_norm):
                    return away_xg
            return total_xg / 2  # unknown: assume half the match total

        all_nnames = [npl for npl, _ in fs_items]
        # Use total_xg context: compute per-team groups where possible.
        home_players = [n for n in all_nnames if abs(team_xg_for(n) - home_xg) < 0.01]
        away_players = [n for n in all_nnames if abs(team_xg_for(n) - away_xg) < 0.01]
        other_players = [n for n in all_nnames if n not in home_players and n not in away_players]

        stat_probs: dict[str, float] = {}
        if home_players:
            stat_probs.update(_compute_stat_first_probs(
                home_players, home_xg, total_xg, stat_lookup, fuzzy_lookup))
        if away_players:
            stat_probs.update(_compute_stat_first_probs(
                away_players, away_xg, total_xg, stat_lookup, fuzzy_lookup))
        if other_players:
            stat_probs.update(_compute_stat_first_probs(
                other_players, total_xg / 2, total_xg, stat_lookup, fuzzy_lookup))

        # Track stat coverage for reporting.
        for npl in all_nnames:
            if _lookup_player(npl, stat_lookup, fuzzy_lookup):
                stat_hits += 1
            else:
                stat_misses += 1

        with db.connect() as conn:
            for npl, fs in fs_items:
                if fs["best_odds"] is None:
                    continue
                m_stat = stat_probs.get(npl)
                m_market = market_fair.get(npl)
                if m_stat is None and m_market is None:
                    continue

                # Blend: stat model gets _W_STAT weight, de-vigged market gets rest.
                if m_stat is not None and m_market is not None:
                    our_prob = _W_STAT * m_stat + (1 - _W_STAT) * m_market
                elif m_stat is not None:
                    our_prob = m_stat
                else:
                    our_prob = m_market

                ref = m_market if m_market is not None else our_prob
                if our_prob <= 0:
                    continue

                record_bet(
                    db,
                    model_version=MODEL_VERSION,
                    bet_type="first_scorer",
                    scope=str(fx["game_id"]),
                    selection_label=fs["name"],
                    our_prob=our_prob,
                    capture_key=capture_key,
                    market_odds=fs["best_odds"],
                    market_prob=ref,
                    book=fs["best_book"],
                    matchup_id=fx["id"],
                    event_commence=fx["commence_time"],
                    longshot_odds_cap=True,
                    conn=conn,
                    inputs={
                        "stat_prob": round(m_stat, 4) if m_stat is not None else None,
                        "market_fair": round(m_market, 4) if m_market is not None else None,
                        "blended": round(our_prob, 4),
                        "match_total_xg": round(total_xg, 4),
                        "stat_hit": _lookup_player(npl, stat_lookup, fuzzy_lookup) is not None,
                        "book_count": fs["book_count"],
                        "fixture": f"{fx['home']} v {fx['away']}",
                    },
                )
                written += 1

    stat_total = stat_hits + stat_misses
    coverage_pct = (stat_hits / stat_total * 100) if stat_total > 0 else 0
    print(
        f"First scorer (v3): {written} bets rated across {len(fixtures)} fixtures "
        f"(stat coverage {coverage_pct:.0f}% — {stat_hits}/{stat_total} players)"
    )
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="First-scorer bet model (v3, stat-driven)")
    parser.add_argument("--hours", type=int, default=DEFAULT_WINDOW_HOURS, help="Look-ahead window")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_record(db, config.odds_api.api_key, args.hours)
