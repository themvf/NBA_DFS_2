"""First-goal-scorer model (firstscorer-v5) — stat-driven + market blend.

v5 (2026-07-07) fixes three correctness bugs found while reviewing an
Argentina–Egypt slate; outputs change materially, hence the version bump
(keeps buggy-v4 rows out of the fixed model's calibration backtest):
  1. NAME MATCHING — the fuzzy lookup only indexed contiguous token spans,
     so display names that drop middle names ("Lionel Messi") never matched
     legal names ("Lionel Andres Messi Cuccittini"), silently dropping stars
     to the position default. Now also index first+last and first+second-to-
     last (Latin double surnames put the common paternal name second-to-last).
     Coverage on that slate went 32% -> 62%.
  2. NO SCORER — the "no goalscorer" (0-0) outcome was being fed into the xG
     share allocation as if it were a player (bogus ~2% instead of the correct
     P(no goal)=e^(-Λ)≈9%, and it diluted every real player's share). It is now
     separated out with stat prob = e^(-Λ).
  3. NORMALIZATION — blended probabilities summed to ~1.15, inflating every
     selection. The full mutually-exclusive set (players + no-scorer) is now
     renormalized to sum to 1.
  4. NO-DATA PLAYERS DEFER TO MARKET — a player absent from soccer_player_stats
     (common for weaker sides, e.g. Egypt) used to get the flat position-
     default xG blended 40/60 with the market, manufacturing a fake edge from
     noise. Now such players defer entirely to the de-vigged market
     (our_prob = market_fair, stat_prob = None), so our independent value/edge
     is honestly N/A for them and only real where we have data.


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

2026-07-01 honesty fix: first-scorer stars are hard-capped at 2★
(_FS_MAX_STARS). The settled ledger shows the 1★ tier is perfectly calibrated
(expected 2.1% vs realized 2.1%, n=1,163) — the de-vig works — but every
selection rated ≥2★ went 1-for-47: the "value" flags are model noise against a
300-500% overround market, same verdict as totals and moneyline. The model's
worth is calibration and record-keeping, not finding plays.

Usage:
    python -m model.soccer_first_scorer
    python -m model.soccer_first_scorer --hours 96
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import unicodedata

from config import load_config
from db.database import DatabaseManager
from db.queries import get_all_soccer_player_stats
from ingest.soccer_props import fetch_player_markets
from model.soccer_bet_rating import new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "firstscorer-v5"
DEFAULT_WINDOW_HOURS = 72
_CLAMP_HI = 0.999
_MIN_ANYTIME_PROB = 0.01
# Hard star cap (2026-07-01): ≥2★ selections went 1/47 settled — no edge over
# the de-vigged market. 2★ = "neutral"; never surface a first-scorer play.
_FS_MAX_STARS = 2
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

# Early-goal rate adjustment: fraction of goals scored in first half (≤45 min).
# League baseline ≈ 45% of goals are first-half (WC data skews slightly under 50%).
# Players above baseline get a boost; below get penalized.
# Adjustment is blended 50/50 with raw xg_per_90 to avoid over-fitting small samples.
_EARLY_GOAL_BASELINE = 0.45
_MIN_GOALS_FOR_EARLY_RATE = 3   # need at least 3 historical goals to apply adjustment

# The "no goalscorer" (0-0) outcome in the first-scorer market. It is NOT a
# player and must never enter the xG share allocation -- its probability is
# P(no goal) = e^(-Λ), the Poisson zero. Matches "No Scorer"/"No Goalscorer".
_NO_SCORER_RE = re.compile(r"\bno\b.*\b(?:goal)?scorer\b|\bno\s+goal\b", re.IGNORECASE)


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
                token_map.setdefault(key, []).append(row)
        # First + a surname token -- catches display names that DROP middle
        # names, which contiguous spans miss: "Lionel Messi" must map to
        # "Lionel Andres Messi Cuccittini" ("lionel messi" is not a contiguous
        # span). Index BOTH first+last and first+second-to-last, because Latin
        # double surnames put the common paternal name second-to-last
        # ("...Messi Cuccittini" -> display uses "Messi", the -2 token). This
        # was silently dropping stars (Messi, Lautaro, etc.) to the default.
        if n >= 3:
            token_map.setdefault(f"{tokens[0]} {tokens[-1]}", []).append(row)
            token_map.setdefault(f"{tokens[0]} {tokens[-2]}", []).append(row)

    # Keep only keys that resolve to a single DISTINCT player. Dedup by row
    # identity, not list length: for 3-token names the first+second-to-last
    # key equals the contiguous first-two-token span, so the same row is added
    # twice -- counting length would wrongly flag it "ambiguous" and drop a
    # valid match (e.g. "Nahuel Molina Lucero").
    return {
        k: rows[0]
        for k, rows in token_map.items()
        if len({id(r) for r in rows}) == 1
    }


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

def _early_goal_multiplier(row: dict | None) -> float:
    """Compute an early-goal timing multiplier for a player's xG/90 rate.

    Players who score more first-half goals are more likely to be the match's
    first scorer.  We multiply their effective xg_per_90 by this factor so the
    Poisson share allocation rewards early-goal tendency.

    Multiplier = 0.5 + 0.5 × (early_goal_rate / baseline).
    This blends 50% raw rate with 50% timing-adjusted rate, keeping extreme values
    within [0.3×, 1.6×] of the raw xG/90.  Applied only when the player has ≥3
    historical goals (small samples revert to multiplier=1.0).
    """
    if row is None:
        return 1.0
    goals = row.get("goals") or 0
    if goals < _MIN_GOALS_FOR_EARLY_RATE:
        return 1.0
    early_rate = row.get("early_goal_rate")
    if early_rate is None:
        return 1.0
    raw_mult = float(early_rate) / _EARLY_GOAL_BASELINE
    # Blend: 50% raw + 50% timing-adjusted → multiplier range stays reasonable
    mult = 0.5 + 0.5 * raw_mult
    return max(0.3, min(mult, 1.6))


def _compute_stat_first_probs(
    player_norm_names: list[str],
    team_match_xg: float,
    total_match_xg: float,
    stat_lookup: dict[str, dict],
    fuzzy_lookup: dict[str, dict],
) -> dict[str, float]:
    """Return {norm_name: first_scorer_prob} using historical xG/90 × early-goal rate.

    Steps:
    1. Look up each player's xg_per_90 (exact then fuzzy); fall back to position default.
    2. Apply early_goal_rate timing multiplier (v4 addition).
    3. Scale to this match: λ_p = (effective_xg90 / Σ effective_xg90) × team_match_xg.
    4. P(scores first) = (λ_p / Λ_total) × (1 - e^(-Λ_total)).
    """
    if not player_norm_names or team_match_xg <= 0 or total_match_xg <= 0:
        return {}

    # Look up historical xG/90 per player; fall back to position defaults.
    # Apply early-goal timing multiplier to produce an effective rate.
    effective_xg90: dict[str, float] = {}
    for nname in player_norm_names:
        row = _lookup_player(nname, stat_lookup, fuzzy_lookup)
        if row and row.get("xg_per_90") and row["xg_per_90"] > 0:
            base_rate = float(row["xg_per_90"])
        elif row and row.get("position"):
            pos = (row["position"] or "MF").upper()[:2]
            base_rate = _POS_XG_DEFAULT.get(pos, _UNKNOWN_XG_DEFAULT)
        else:
            base_rate = _UNKNOWN_XG_DEFAULT
        effective_xg90[nname] = base_rate * _early_goal_multiplier(row)

    total_eff_xg90 = sum(effective_xg90.values()) or 1.0

    # Each player's expected goals in this specific match.
    lambda_p = {
        nname: (rate / total_eff_xg90) * team_match_xg
        for nname, rate in effective_xg90.items()
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

        all_fs = [(npl, fs) for npl, fs in markets["first"].items() if fs.get("prob_raw")]
        if not all_fs:
            continue

        # Separate the "no goalscorer" (0-0) outcome from real players -- it
        # must NOT be treated as a player in the xG share allocation (that gave
        # it a nonsense ~2% instead of the correct e^(-Λ) ≈ 9%, and diluted
        # every real player's share).
        player_items = [(n, fs) for n, fs in all_fs if not _NO_SCORER_RE.search(fs["name"])]
        no_scorer_item = next((x for x in all_fs if _NO_SCORER_RE.search(x[1]["name"])), None)

        # ── Market de-vig over the full mutually-exclusive set (players + no-scorer) ──
        devig_names = [n for n, _ in player_items]
        raw_list = [fs["prob_raw"] for _, fs in player_items]
        ns_raw = (no_scorer_item[1]["prob_raw"] if no_scorer_item
                  else (markets.get("no_scorer_raw") or 0.0))
        raw_list.append(ns_raw)
        devigged = power_devig_exclusive(raw_list)
        market_fair = {devig_names[i]: devigged[i] for i in range(len(devig_names))} if devigged else {}
        market_fair_ns = devigged[-1] if devigged else None

        # ── Stat-based model (players only) ──
        home_norm = _norm(fx["home"])
        away_norm = _norm(fx["away"])

        def team_xg_for(player_nname: str) -> float:
            row = stat_lookup.get(player_nname)
            if row:
                tnorm = _norm(row.get("team_name") or "")
                if home_norm and tnorm and (home_norm in tnorm or tnorm in home_norm):
                    return home_xg
                if away_norm and tnorm and (away_norm in tnorm or tnorm in away_norm):
                    return away_xg
            return total_xg / 2  # unknown: assume half the match total

        all_nnames = [npl for npl, _ in player_items]
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
        stat_prob_ns = math.exp(-total_xg)  # P(no goal) = Poisson zero

        for npl in all_nnames:
            if _lookup_player(npl, stat_lookup, fuzzy_lookup):
                stat_hits += 1
            else:
                stat_misses += 1

        # ── Blend, then RENORMALIZE to a proper mutually-exclusive distribution
        #    (players + no-scorer sum to 1) before recording. Blending two
        #    distributions over slightly different support left the raw sum at
        #    ~1.15, inflating every our_prob. ──
        def _blend(m_stat, m_market):
            if m_stat is not None and m_market is not None:
                return _W_STAT * m_stat + (1 - _W_STAT) * m_market
            return m_stat if m_stat is not None else m_market

        candidates = []  # (fs, npl, m_stat, m_market, blended)
        for npl, fs in player_items:
            if fs["best_odds"] is None:
                continue
            m_market = market_fair.get(npl)
            # Only claim an independent stat view for players we actually have
            # data on. For an unknown player (e.g. an Egypt squad member absent
            # from soccer_player_stats) DEFER to the market -- do NOT blend in
            # the flat position default, which would manufacture a fake edge
            # from noise. Our stat_prob/edge is then N/A for that player.
            has_data = _lookup_player(npl, stat_lookup, fuzzy_lookup) is not None
            m_stat = stat_probs.get(npl) if has_data else None
            if m_stat is None and m_market is None:
                continue
            b = _blend(m_stat, m_market)
            if b and b > 0:
                candidates.append((fs, npl, m_stat, m_market, b))
        if no_scorer_item and no_scorer_item[1]["best_odds"] is not None:
            b = _blend(stat_prob_ns, market_fair_ns)
            if b and b > 0:
                candidates.append((no_scorer_item[1], None, stat_prob_ns, market_fair_ns, b))

        total_blended = sum(c[4] for c in candidates) or 1.0

        with db.connect() as conn:
            for fs, npl, m_stat, m_market, b in candidates:
                our_prob = b / total_blended  # normalized -> the set sums to 1
                ref = m_market if m_market is not None else our_prob
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
                    max_stars=_FS_MAX_STARS,
                    conn=conn,
                    inputs={
                        "stat_prob": round(m_stat, 4) if m_stat is not None else None,
                        "market_fair": round(m_market, 4) if m_market is not None else None,
                        "blended_raw": round(b, 4),
                        "our_prob": round(our_prob, 4),
                        "match_total_xg": round(total_xg, 4),
                        "stat_hit": npl is not None and _lookup_player(npl, stat_lookup, fuzzy_lookup) is not None,
                        "is_no_scorer": npl is None,
                        "book_count": fs["book_count"],
                        "fixture": f"{fx['home']} v {fx['away']}",
                    },
                )
                written += 1

    stat_total = stat_hits + stat_misses
    coverage_pct = (stat_hits / stat_total * 100) if stat_total > 0 else 0
    print(
        f"First scorer ({MODEL_VERSION}): {written} bets rated across {len(fixtures)} fixtures "
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
