"""DFS projection model for MLB batters and pitchers.

Two independent models — batters and pitchers share no logic.

Batter model:
  1. Base: per-game rates from mlb_batter_stats (EWMA-smoothed via Phase 3)
  2. Game environment: team implied total / LEAGUE_AVG_TEAM_TOTAL
  3. Park: runs_factor (all contact), hr_factor (HR-specific)
  4. Pitcher quality: xFIP / LEAGUE_AVG_XFIP scales hit/walk production
  5. L/R split: wrc_plus_vs_X / wrc_plus overall (ratio vs baseline)
  6. Batting order PA weight: lineup spots 1-2 get ~8% more PAs than average

Pitcher model:
  1. Base: ip_pg, k_per_9, bb_per_9, ERA/xFIP, WHIP → per-start rates
  2. Opposing lineup: team_wrc_plus scales ER/H; team_k_pct scales Ks
  3. Park: runs_factor inversely scales ER/H
  4. Win probability: blended historical win_pct + today's team moneyline

DK MLB scoring:
  Batters:  1B×3 | 2B×5 | 3B×8 | HR×10 | RBI×2 | R×2 | BB×2 | HBP×2 | SB×5
  Pitchers: IP×2.25 | K×2 | W+4 | ER-2 | H-0.6 | BB-0.6

League average constants calibrated to 2024-25 MLB:
  LEAGUE_AVG_TEAM_TOTAL = 4.5  (runs/game/team)
  LEAGUE_AVG_XFIP       = 4.20 (starter xFIP)
  LEAGUE_AVG_K_PCT      = 0.225 (22.5% team K rate)
"""

from __future__ import annotations

# ── League average constants ──────────────────────────────────────────────────

MLB_LEAGUE_AVG_TEAM_TOTAL = 4.5    # runs per team per game
MLB_LEAGUE_AVG_XFIP       = 4.20   # starter xFIP league average
MLB_LEAGUE_AVG_K_PCT      = 0.225  # team K rate (22.5% of PA)

# PA weight by batting order slot.
# Slots 1-2 accumulate ~8% more plate appearances than the 5-6 average.
# Slots 7-9 see ~7% fewer PA.  Based on empirical MLB PA-per-slot distributions.
_ORDER_PA_FACTOR: dict[int, float] = {
    1: 1.08, 2: 1.08,
    3: 1.05, 4: 1.05,
    5: 1.00, 6: 1.00,
    7: 0.93, 8: 0.93, 9: 0.93,
}


# ── Public DK scoring formulas ────────────────────────────────────────────────
# Single source of truth — imported by mlb_stats.py for avg_fpts_pg computation.

def dk_batter_fpts(
    singles: float, doubles: float, triples: float, hr: float,
    rbi: float, runs: float, bb: float, hbp: float, sb: float,
) -> float:
    """DraftKings MLB batter FPTS from per-game rates."""
    return (
        singles * 3.0
        + doubles * 5.0
        + triples * 8.0
        + hr      * 10.0
        + rbi     * 2.0
        + runs    * 2.0
        + bb      * 2.0
        + hbp     * 2.0
        + sb      * 5.0
    )


def dk_pitcher_fpts(
    ip: float, k: float, er: float, h: float, bb: float, win_prob: float,
) -> float:
    """DraftKings MLB pitcher FPTS per start/appearance.

    win_prob: expected probability of earning the +4 win bonus.
    """
    return (
        ip * 2.25
        + k  * 2.0
        - er * 2.0
        - h  * 0.6
        - bb * 0.6
        + win_prob * 4.0
    )


# ── Batter projection ─────────────────────────────────────────────────────────

def compute_batter_projection(
    batter: dict,
    matchup: dict,
    opp_sp: dict | None,
    park: dict | None,
    is_home: bool = False,
) -> float | None:
    """Compute DK MLB batter projection for a single game.

    Args:
        batter:  mlb_batter_stats row.
        matchup: mlb_matchups row.
        opp_sp:  mlb_pitcher_stats row for the opposing starting pitcher
                 (None if unknown or RP — no pitcher-quality adjustment applied).
        park:    mlb_park_factors row for the home ballpark (None = neutral).
        is_home: True if batter's team is the home team.

    Returns:
        Projected DK FPTS, or None if data is insufficient.
    """
    if (batter.get("games") or 0) < 3:
        return None

    # Base per-game rates (stored from Phase 3 EWMA-smoothed ingestion)
    singles_pg = float(batter.get("singles_pg") or 0.0)
    doubles_pg = float(batter.get("doubles_pg") or 0.0)
    triples_pg = float(batter.get("triples_pg") or 0.0)
    hr_pg      = float(batter.get("hr_pg")      or 0.0)
    rbi_pg     = float(batter.get("rbi_pg")     or 0.0)
    runs_pg    = float(batter.get("runs_pg")    or 0.0)
    hbp_pg     = float(batter.get("hbp_pg")    or 0.0)
    sb_pg      = float(batter.get("sb_pg")      or 0.0)

    # BB/game: derived from bb_pct (rate) × pa_pg (stored separately)
    # FanGraphs BB% is a decimal: 0.085 = 8.5%
    bb_pct = float(batter.get("bb_pct") or 0.085)
    pa_pg  = float(batter.get("pa_pg")  or 4.0)
    bb_pg  = bb_pct * pa_pg

    # Sanity check — any offensive production at all?
    if (singles_pg + doubles_pg + hr_pg + rbi_pg + runs_pg + bb_pg) < 0.05:
        return None

    # ── Game environment: team-specific implied run total ─────────────────────
    # home_implied / away_implied already use the moneyline split formula
    # (same compute_team_implied_total logic applied at schedule ingest time).
    if is_home:
        team_implied = float(matchup.get("home_implied") or
                             (matchup.get("vegas_total") or 9.0) / 2.0)
    else:
        team_implied = float(matchup.get("away_implied") or
                             (matchup.get("vegas_total") or 9.0) / 2.0)

    env_factor = _cap(team_implied / MLB_LEAGUE_AVG_TEAM_TOTAL, 0.50, 2.00)

    # ── Park factor ────────────────────────────────────────────────────────────
    runs_pf = _cap(float(park.get("runs_factor") or 1.0), 0.70, 1.30) if park else 1.0
    hr_pf   = _cap(float(park.get("hr_factor")   or 1.0), 0.70, 1.50) if park else 1.0

    # ── Batting order PA weight ────────────────────────────────────────────────
    order = batter.get("batting_order")
    order_factor = _ORDER_PA_FACTOR.get(int(order), 1.0) if order else 1.0

    # ── Pitcher quality: xFIP scales contact production ───────────────────────
    # xFIP > LEAGUE_AVG_XFIP → weaker pitcher → more offense (factor > 1.0)
    if opp_sp:
        sp_qual = float(opp_sp.get("xfip") or opp_sp.get("era") or MLB_LEAGUE_AVG_XFIP)
        xfip_factor = _cap(sp_qual / MLB_LEAGUE_AVG_XFIP, 0.60, 1.80)
    else:
        xfip_factor = 1.0   # no pitcher data — neutral assumption

    # ── L/R split: ratio of split wRC+ to overall wRC+ ────────────────────────
    # Isolates the hand-specific advantage vs the batter's own baseline.
    # wrc_plus_vs_l / wrc_plus_vs_r are NULL until Phase 6 split ingestion —
    # handle gracefully with matchup_factor = 1.0.
    matchup_factor = 1.0
    if (opp_sp and opp_sp.get("hand")
            and batter.get("wrc_plus") and batter["wrc_plus"] > 0):
        hand = str(opp_sp["hand"]).upper()
        wrc_vs = (batter.get("wrc_plus_vs_l") if hand == "L"
                  else batter.get("wrc_plus_vs_r"))
        if wrc_vs:
            raw = float(wrc_vs) / float(batter["wrc_plus"])
            matchup_factor = _cap(raw, 0.50, 1.75)

    # ── Composite adjustment factors ─────────────────────────────────────────
    # hit_factor: affects all contact-dependent outcomes (singles/doubles/triples/RBI/R)
    hit_factor  = _cap(env_factor * runs_pf * xfip_factor * order_factor * matchup_factor,
                       0.30, 3.00)
    # hr_factor_adj: HR gets its own park multiplier (hr_pf instead of runs_pf)
    hr_factor_adj = _cap(env_factor * hr_pf * xfip_factor * order_factor * matchup_factor,
                         0.30, 3.00)
    # bb/hbp: pitcher-controlled; no park effect on walks or HBP
    walk_factor = _cap(env_factor * xfip_factor * order_factor, 0.30, 3.00)
    # sb: baserunning skill; env + order only (not park or pitcher quality)
    sb_factor   = _cap(env_factor * order_factor, 0.30, 3.00)

    # ── Projected per-game rates ───────────────────────────────────────────────
    fpts = dk_batter_fpts(
        singles = singles_pg * hit_factor,
        doubles = doubles_pg * hit_factor,
        triples = triples_pg * hit_factor,
        hr      = hr_pg      * hr_factor_adj,
        rbi     = rbi_pg     * hit_factor,
        runs    = runs_pg    * hit_factor,
        bb      = bb_pg      * walk_factor,
        hbp     = hbp_pg     * walk_factor,
        sb      = sb_pg      * sb_factor,
    )
    return round(fpts, 2) if fpts > 0 else None


# ── Pitcher projection ────────────────────────────────────────────────────────

def compute_pitcher_projection(
    pitcher: dict,
    matchup: dict,
    opp_team: dict | None,
    park: dict | None,
    is_home: bool = False,
) -> float | None:
    """Compute DK MLB pitcher projection for a single start or appearance.

    Args:
        pitcher:  mlb_pitcher_stats row.
        matchup:  mlb_matchups row.
        opp_team: mlb_team_stats row for the opposing team — provides
                  team_wrc_plus (scales ER/H) and team_k_pct (scales Ks).
        park:     mlb_park_factors row for the home ballpark.
        is_home:  True if pitcher's team is the home team.

    Returns:
        Projected DK FPTS, or None if data is insufficient.
    """
    if (pitcher.get("games") or 0) < 2:
        return None

    ip_pg = float(pitcher.get("ip_pg") or 0.0)
    if ip_pg < 0.5:
        return None   # pitcher who never records an out

    k_per_9  = float(pitcher.get("k_per_9")  or 0.0)
    bb_per_9 = float(pitcher.get("bb_per_9") or 0.0)
    era      = float(pitcher.get("era")      or 4.50)
    whip     = float(pitcher.get("whip")     or 1.30)
    # xFIP is a better ERA estimator than ERA itself (stabilizes faster, removes
    # HR variance).  Fall back to ERA if xFIP is unavailable.
    xfip     = float(pitcher.get("xfip") or era)

    # ── Per-start base stats ──────────────────────────────────────────────────
    ip = ip_pg
    k  = k_per_9  / 9.0 * ip
    bb = bb_per_9 / 9.0 * ip
    # Expected ER: use xFIP/9 × IP for a cleaner signal than raw ERA
    er = xfip     / 9.0 * ip
    # Expected H: derive from WHIP (H + BB per inning)
    h  = max(0.0, whip * ip - bb)

    # ── Opposing lineup quality ───────────────────────────────────────────────
    opp_wrc   = float((opp_team.get("team_wrc_plus") or 100.0)) if opp_team else 100.0
    opp_k_pct = float((opp_team.get("team_k_pct") or MLB_LEAGUE_AVG_K_PCT)) if opp_team else MLB_LEAGUE_AVG_K_PCT

    opp_wrc_factor = _cap(opp_wrc / 100.0, 0.60, 1.60)
    # A high-K team strikes out more → pitcher benefits
    opp_k_factor   = _cap(opp_k_pct / MLB_LEAGUE_AVG_K_PCT, 0.60, 1.60)

    # ── Park factor ────────────────────────────────────────────────────────────
    runs_pf = _cap(float(park.get("runs_factor") or 1.0), 0.70, 1.30) if park else 1.0

    # ── Adjusted per-start stats ──────────────────────────────────────────────
    # More Ks vs team that strikes out a lot:
    adj_k  = k  * opp_k_factor
    # More ER/H vs strong lineup at a run-friendly park:
    adj_er = er * opp_wrc_factor * runs_pf
    adj_h  = h  * opp_wrc_factor * runs_pf
    # BB: pitcher-controlled (team walk rate is a secondary signal — skip for simplicity)
    adj_bb = bb

    # ── Win probability ───────────────────────────────────────────────────────
    # Blend historical win rate + today's team moneyline.
    # For RPs (win_pct ≈ 0), this naturally collapses to near-zero.
    historical_win_rate = float(pitcher.get("win_pct") or 0.0)
    team_win_prob       = _win_prob(matchup, is_home)
    effective_win_prob  = (historical_win_rate + team_win_prob) / 2.0 if historical_win_rate > 0 else 0.0

    fpts = dk_pitcher_fpts(
        ip=ip, k=adj_k, er=adj_er, h=adj_h, bb=adj_bb,
        win_prob=effective_win_prob,
    )
    return round(fpts, 2) if fpts > 0 else None


# ── Internal helpers ──────────────────────────────────────────────────────────

def _win_prob(matchup: dict, is_home: bool) -> float:
    """Vig-free win probability from moneylines, or vegas_prob_home fallback."""
    home_ml = matchup.get("home_ml")
    away_ml = matchup.get("away_ml")
    if home_ml and away_ml:
        raw_home = _ml_to_raw(int(home_ml))
        raw_away = _ml_to_raw(int(away_ml))
        total = raw_home + raw_away
        if total > 0:
            home_prob = raw_home / total
            return home_prob if is_home else (1.0 - home_prob)
    vph = matchup.get("vegas_prob_home")
    if vph is not None:
        return float(vph) if is_home else (1.0 - float(vph))
    return 0.50


def _ml_to_raw(ml: int) -> float:
    """Convert American moneyline to raw (vig-inclusive) implied probability."""
    if ml >= 0:
        return 100.0 / (ml + 100.0)
    return abs(ml) / (abs(ml) + 100.0)


def _cap(value: float, low: float, high: float) -> float:
    """Clamp value to [low, high]."""
    return max(low, min(high, value))
