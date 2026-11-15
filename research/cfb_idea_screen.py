"""CFB idea screen: many candidate angles, one test, one correction.

A screening tool, not a study. Every idea is turned into a pregame signal and
tested the same way against the CLOSING spread or total:

  spread ideas   y = +1 / 0 / -1 (home covered / push / failed) regressed on
                 the signal (home minus away, so a positive slope means "the
                 side the signal points to covers more")
  total ideas    y = +1 / 0 / -1 (over / push / under) regressed on the signal

Standard errors are clustered by season-week. Discovery is every season through
2024; 2025 is the confirmation season and 2026 to date a second look (line-only
ideas, closes from our own captures). Discovery p-values are corrected across
the whole family with Benjamini-Hochberg; an idea "survives" only if it clears
q <= 0.10 in discovery AND has the same sign with one-sided p < 0.05 in 2025.
Placebo signals (name length, alphabet, moon phase, ...) run through the same
pipe: the share of them that pass is the screen's own false-positive rate.

A survivor is a candidate for a forward, pre-registered trigger. Nothing here
is evidence of an edge: 2022-25 closes come from CFBD's historical feed, prices
are not used (a cover rate must clear 52.4% at -110), and the family is large.

    python -m research.cfb_idea_screen [--out artifacts/cfb_idea_screen]
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

VERSION = "cfb-idea-screen-v1"
DISCOVERY_THROUGH = 2024
CONFIRM = 2025
SECOND_LOOK = 2026
BH_Q = 0.10
CONFIRM_P = 0.05
MIN_FLAGGED = 20
ET = ZoneInfo("America/New_York")

HIGH_ALTITUDE_HOME = {"Wyoming", "Air Force", "Colorado", "Colorado State", "Utah", "BYU",
                      "Utah State", "New Mexico", "Nevada"}
ACADEMIES = {"Army", "Navy", "Air Force"}
POWER = {"SEC", "Big Ten", "Big 12", "ACC", "Pac-12"}
SCRIMMAGE = ("Rush", "Pass Reception", "Pass Incompletion", "Sack", "Rushing Touchdown",
             "Passing Touchdown", "Pass Interception Return", "Interception",
             "Interception Return Touchdown", "Fumble Recovery (Own)",
             "Fumble Recovery (Opponent)", "Fumble Return Touchdown", "Pass Completion")
GIVEAWAY = ("INT", "FUMBLE", "INT TD", "FUMBLE RETURN TD")


# ── loading ─────────────────────────────────────────────────────────────────
def _q(db, sql: str, params=None) -> pd.DataFrame:
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return pd.DataFrame(fetch_rows(cur))


def load(db) -> dict[str, pd.DataFrame]:
    games = _q(db, """
        SELECT m.id AS game_id, m.cfbd_game_id, m.season, m.season_type, m.week, m.commence_time,
               m.home_team_id, m.away_team_id, m.neutral_site, m.conference_game,
               m.home_score, m.away_score, m.home_line_scores, m.away_line_scores,
               m.went_to_overtime, v.latitude, v.longitude
        FROM cfb_matchups m LEFT JOIN cfb_venues v ON v.venue_id = m.venue_id
        WHERE m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND m.commence_time IS NOT NULL AND m.season >= 2016""")
    teams = _q(db, "SELECT team_id, name FROM cfb_teams")
    lines = _q(db, """
        SELECT game_id, market_type, line_designation, home_value, home_conference, away_conference
        FROM cfb_historical_game_lines
        WHERE is_canonical_reference AND market_type IN ('spread', 'total')
          AND line_designation IN ('historical_reference', 'source_reported_open')
        ORDER BY id""")
    live = _q(db, """
        WITH caps AS (
          SELECT h.matchup_id, h.captured_at, h.home_spread, h.vegas_total, m.commence_time
          FROM game_odds_history h JOIN cfb_matchups m ON m.id = h.matchup_id
          WHERE h.sport = 'cfb' AND m.season = %s AND h.captured_at < m.commence_time)
        SELECT matchup_id AS game_id,
               (array_agg(home_spread ORDER BY captured_at DESC) FILTER (WHERE home_spread IS NOT NULL))[1] AS close_spread,
               (array_agg(vegas_total ORDER BY captured_at DESC) FILTER (WHERE vegas_total IS NOT NULL))[1] AS close_total,
               (array_agg(home_spread ORDER BY captured_at) FILTER (WHERE home_spread IS NOT NULL))[1] AS open_spread,
               (array_agg(vegas_total ORDER BY captured_at) FILTER (WHERE vegas_total IS NOT NULL))[1] AS open_total
        FROM caps GROUP BY 1""", (SECOND_LOOK,))
    drives = _q(db, """
        SELECT game_id, offense_team_id AS team_id, count(*) AS drives, sum(yards) AS yards,
               sum((drive_result = ANY(%s))::int) AS giveaways
        FROM cfb_drives WHERE offense_team_id IS NOT NULL GROUP BY 1, 2""", (list(GIVEAWAY),))
    plays = _q(db, """
        WITH d AS (
          SELECT game_id, offense_team_id AS team_id, cfbd_drive_id,
                 bool_or(yards_to_goal <= 20 AND play_type = ANY(%s)) AS red_zone,
                 sum((play_type = ANY(%s))::int) AS snaps,
                 sum((play_type IN ('Rushing Touchdown', 'Passing Touchdown') AND yards_gained >= 40)::int) AS long_td,
                 max(CASE WHEN play_type IN ('Rushing Touchdown', 'Passing Touchdown') THEN 7
                          WHEN play_type = 'Field Goal Good' THEN 3 ELSE 0 END) AS points
          FROM cfb_plays WHERE offense_team_id IS NOT NULL GROUP BY 1, 2, 3)
        SELECT game_id, team_id, sum(snaps) AS snaps, sum(long_td) AS long_td,
               sum(red_zone::int) AS rz_trips, sum(CASE WHEN red_zone THEN points ELSE 0 END) AS rz_points
        FROM d GROUP BY 1, 2""", (list(SCRIMMAGE), list(SCRIMMAGE)))
    passers = _q(db, """
        SELECT DISTINCT ON (cfbd_game_id, team) cfbd_game_id, team, cfbd_player_id
        FROM cfb_player_game_stats WHERE pass_att >= 5
        ORDER BY cfbd_game_id, team, pass_att DESC, cfbd_player_id""")
    return {"games": games, "teams": teams, "lines": lines, "live": live,
            "drives": drives, "plays": plays, "passers": passers}


# ── frame ───────────────────────────────────────────────────────────────────
def _tz_behind(lon: float | None) -> float:
    """Hours behind Eastern, from longitude (venue time zones are not stored)."""
    if lon is None or (isinstance(lon, float) and math.isnan(lon)):
        return float("nan")
    return 0 if lon > -85.5 else 1 if lon > -101 else 2 if lon > -114.5 else 3 if lon > -140 else 5


def _moon(ts: pd.Timestamp) -> float:
    """Moon illumination 0..1 (synodic approximation; a placebo, precision is irrelevant)."""
    days = (ts - pd.Timestamp("2000-01-06 18:14", tz="UTC")).total_seconds() / 86400
    return (1 - math.cos(2 * math.pi * (days % 29.530588) / 29.530588)) / 2


def build(data: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(game frame with lines and outcomes, team-game frame with prior-game features)."""
    g = data["games"].copy()
    names = dict(zip(data["teams"]["team_id"], data["teams"]["name"]))
    g["home"] = g["home_team_id"].map(names)
    g["away"] = g["away_team_id"].map(names)
    g["commence_time"] = pd.to_datetime(g["commence_time"], utc=True)

    ln = data["lines"]
    if not ln.empty:
        for mk, des, col in (("spread", "historical_reference", "close_spread"),
                             ("total", "historical_reference", "close_total"),
                             ("spread", "source_reported_open", "open_spread"),
                             ("total", "source_reported_open", "open_total")):
            sub = ln[(ln.market_type == mk) & (ln.line_designation == des)].drop_duplicates("game_id")
            g[col] = g["game_id"].map(dict(zip(sub.game_id, sub.home_value)))
        conf = ln.drop_duplicates("game_id")
        g["home_conf"] = g["game_id"].map(dict(zip(conf.game_id, conf.home_conference)))
        g["away_conf"] = g["game_id"].map(dict(zip(conf.game_id, conf.away_conference)))
    live = data["live"]
    if not live.empty:
        live = live.set_index("game_id")
        for col in ("close_spread", "close_total", "open_spread", "open_total"):
            fill = g["game_id"].map(live[col]).astype(float)
            g[col] = g[col].astype(float).where(g["season"] != SECOND_LOOK, fill)
    for col in ("close_spread", "close_total", "open_spread", "open_total"):
        g[col] = pd.to_numeric(g[col], errors="coerce")

    g["margin"] = g["home_score"] - g["away_score"]
    g["points"] = g["home_score"] + g["away_score"]
    g["ats_home"] = g["margin"] + g["close_spread"]          # > 0: home covered
    g["tot_res"] = g["points"] - g["close_total"]            # > 0: over
    g["y_ats"] = np.sign(g["ats_home"])
    g["y_tot"] = np.sign(g["tot_res"])
    g["cluster"] = g["season"] * 100 + g["week"] + np.where(g["season_type"] == "postseason", 50, 0)
    g["et_hour"] = g["commence_time"].dt.tz_convert(ET).dt.hour + g["commence_time"].dt.tz_convert(ET).dt.minute / 60
    g["month"] = g["commence_time"].dt.tz_convert(ET).dt.month
    g["moon"] = g["commence_time"].map(_moon)
    g = g.sort_values(["commence_time", "game_id"]).reset_index(drop=True)

    # A team's home time zone: the median longitude of its own non-neutral home games.
    home_lon = g[~g["neutral_site"].astype(bool)].groupby("home_team_id")["longitude"].median()
    g["home_tz"] = g["home_team_id"].map(home_lon).map(_tz_behind)
    g["away_tz"] = g["away_team_id"].map(home_lon).map(_tz_behind)

    # Team-game frame, one row per team per game, in time order.
    box = data["drives"].merge(data["plays"], on=["game_id", "team_id"], how="outer") \
        if not data["drives"].empty else pd.DataFrame(columns=["game_id", "team_id"])
    sides = []
    for side, opp in (("home", "away"), ("away", "home")):
        t = g[["game_id", "cfbd_game_id", "season", "commence_time", "went_to_overtime", "ats_home",
               "close_spread", "margin", "neutral_site"]].copy()
        t["team_id"] = g[f"{side}_team_id"]
        t["opp_id"] = g[f"{opp}_team_id"]
        t["team"] = g[side]
        t["is_home"] = side == "home"
        sign = 1 if side == "home" else -1
        t["team_margin"] = sign * g["margin"]
        t["team_spread"] = sign * g["close_spread"]
        t["team_ats"] = np.sign(sign * g["ats_home"])
        t["at_hawaii"] = (side == "away") & (g["home"] == "Hawai'i") & ~g["neutral_site"].astype(bool)
        sides.append(t)
    tg = pd.concat(sides, ignore_index=True)
    if not box.empty:
        for col in ("drives", "yards", "giveaways", "snaps", "long_td", "rz_trips", "rz_points"):
            box[col] = pd.to_numeric(box[col], errors="coerce")
        tg = tg.merge(box, on=["game_id", "team_id"], how="left")
        opp = box.rename(columns={"team_id": "opp_id", "yards": "opp_yards", "giveaways": "opp_giveaways",
                                  "snaps": "opp_snaps"})[["game_id", "opp_id", "opp_yards", "opp_giveaways", "opp_snaps"]]
        tg = tg.merge(opp, on=["game_id", "opp_id"], how="left")
    else:
        for col in ("yards", "giveaways", "snaps", "long_td", "rz_trips", "rz_points",
                    "opp_yards", "opp_giveaways", "opp_snaps"):
            tg[col] = np.nan
    ps = data["passers"]
    if not ps.empty:
        tg = tg.merge(ps.rename(columns={"team": "team", "cfbd_player_id": "passer"}),
                      on=["cfbd_game_id", "team"], how="left")
    else:
        tg["passer"] = None

    tg["to_margin"] = tg["opp_giveaways"] - tg["giveaways"]
    tg["game_snaps"] = tg["snaps"] + tg["opp_snaps"]
    disc = tg["season"] <= DISCOVERY_THROUGH
    trips = tg.loc[disc, "rz_trips"].sum()
    per_trip = tg.loc[disc, "rz_points"].sum() / trips if trips else float("nan")
    tg["rz_luck"] = tg["rz_points"] - per_trip * tg["rz_trips"]
    yd = (tg["yards"] - tg["opp_yards"]).astype(float)
    fit = disc & yd.notna() & tg["team_margin"].notna()
    slope = float(np.polyfit(yd[fit], tg.loc[fit, "team_margin"].astype(float), 1)[0]) if fit.sum() > 50 else float("nan")
    tg["margin_luck"] = tg["team_margin"] - slope * yd

    tg = tg.sort_values(["team_id", "commence_time", "game_id"]).reset_index(drop=True)
    grp = tg.groupby(["team_id", "season"], sort=False)
    prev = lambda col: grp[col].shift(1)                                         # noqa: E731
    tg["rest_days"] = (tg["commence_time"] - grp["commence_time"].shift(1)).dt.total_seconds() / 86400
    tg["p_ot"] = prev("went_to_overtime").astype(float)
    tg["p_margin"] = prev("team_margin")
    tg["p_spread"] = prev("team_spread")
    tg["p_to"] = prev("to_margin")
    tg["p_rz_luck"] = prev("rz_luck")
    tg["p_long_td"] = prev("long_td")
    tg["p_margin_luck"] = prev("margin_luck")
    tg["p_at_hawaii"] = prev("at_hawaii").astype(float)
    tg["p_passer"] = prev("passer")
    tg["qb_change"] = np.where(tg["passer"].notna() & tg["p_passer"].notna(),
                               (tg["passer"] != tg["p_passer"]).astype(float), np.nan)
    # Season-to-date means over PRIOR games only (shift before the expanding mean).
    for col, out in (("to_margin", "std_to"), ("game_snaps", "std_pace")):
        shifted = grp[col].shift(1)
        tg[out] = shifted.groupby([tg["team_id"], tg["season"]]).transform(
            lambda s: s.expanding(min_periods=3).mean())
    a1, a2, a3 = grp["team_ats"].shift(1), grp["team_ats"].shift(2), grp["team_ats"].shift(3)
    tg["ats_streak"] = np.where((a1 == 1) & (a2 == 1) & (a3 == 1), 1.0,
                                np.where((a1 == -1) & (a2 == -1) & (a3 == -1), -1.0,
                                         np.where(a3.notna(), 0.0, np.nan)))
    tg["p_upset_win"] = np.where(tg["p_spread"].notna() & tg["p_margin"].notna(),
                                 ((tg["p_spread"] >= 7) & (tg["p_margin"] > 0)).astype(float), np.nan)
    tg["p_blowout_loss"] = np.where(tg["p_margin"].notna(), (tg["p_margin"] <= -28).astype(float), np.nan)
    tg["p_blowout_win"] = np.where(tg["p_margin"].notna(), (tg["p_margin"] >= 28).astype(float), np.nan)
    tg["bye"] = np.where(tg["rest_days"].notna(), (tg["rest_days"] >= 13).astype(float), np.nan)
    tg["short"] = np.where(tg["rest_days"].notna(), (tg["rest_days"] <= 5.5).astype(float), np.nan)
    return g, tg


# ── signals ─────────────────────────────────────────────────────────────────
def _diff(g: pd.DataFrame, tg: pd.DataFrame, col: str) -> pd.Series:
    """home value minus away value of a team-game feature (NaN if either is missing)."""
    h = tg[tg["is_home"]].set_index("game_id")[col]
    a = tg[~tg["is_home"]].set_index("game_id")[col]
    return (g["game_id"].map(h).astype(float) - g["game_id"].map(a).astype(float))


def _sum(g: pd.DataFrame, tg: pd.DataFrame, col: str) -> pd.Series:
    h = tg[tg["is_home"]].set_index("game_id")[col]
    a = tg[~tg["is_home"]].set_index("game_id")[col]
    return (g["game_id"].map(h).astype(float) + g["game_id"].map(a).astype(float))


def signals(g: pd.DataFrame, tg: pd.DataFrame) -> list[dict]:
    """Every idea as {id, group, label, market, signal, plays} (plays = needs play data)."""
    out: list[dict] = []

    def add(idea, group, label, market, sig, plays=False):
        out.append({"id": idea, "group": group, "label": label, "market": market,
                    "signal": pd.Series(sig, index=g.index, dtype=float), "plays": plays})

    spread = g["close_spread"]
    fav_side = np.where(spread < 0, 1.0, np.where(spread > 0, -1.0, np.nan))   # +1: home is the favorite
    absp = spread.abs()
    neutral = g["neutral_site"].astype(bool)

    # Luck and regression (drives and plays)
    add("L1a", "luck", "Turnover margin, last game", "spread", _diff(g, tg, "p_to"), True)
    add("L1b", "luck", "Turnover margin per game, season to date", "spread", _diff(g, tg, "std_to"), True)
    add("L2", "luck", "Red-zone points above average, last game", "spread", _diff(g, tg, "p_rz_luck"), True)
    add("L3", "luck", "Touchdowns of 40+ yards, last game", "spread", _diff(g, tg, "p_long_td"), True)
    add("L3t", "luck", "Touchdowns of 40+ yards, last game (both teams)", "total", _sum(g, tg, "p_long_td"), True)
    add("L4", "luck", "Margin beyond what yardage explains, last game", "spread", _diff(g, tg, "p_margin_luck"), True)
    add("L5a", "luck", "Underdog of 17+ points", "spread", np.where(absp >= 17, -fav_side, 0.0))
    add("L5b", "luck", "Game with a 17+ point favorite", "total", np.where(absp >= 17, 1.0, 0.0))
    pace = _sum(g, tg, "std_pace")
    add("L6", "luck", "Combined plays per game, season to date", "total", pace - pace.mean(), True)

    # Schedule, situation and market shape
    add("S7a", "market", "Spread moved toward the team, open to close", "spread", g["open_spread"] - spread)
    add("S7b", "market", "Spread moved 2+ points toward the team", "spread",
        np.where((g["open_spread"] - spread).abs() >= 2, np.sign(g["open_spread"] - spread), 0.0)
        + (g["open_spread"] - spread) * 0)
    add("S7c", "market", "Total moved up, open to close", "total", g["close_total"] - g["open_total"])
    add("S8", "market", "Covered three straight (minus: failed three straight)", "spread", _diff(g, tg, "ats_streak"))
    add("S9a", "situation", "Played overtime last game", "spread", _diff(g, tg, "p_ot"))
    add("S9b", "situation", "Won last game as a 7+ point underdog", "spread", _diff(g, tg, "p_upset_win"))
    add("S9c", "situation", "Lost last game by 28+", "spread", _diff(g, tg, "p_blowout_loss"))
    add("S9d", "situation", "Won last game by 28+", "spread", _diff(g, tg, "p_blowout_win"))
    add("S10a", "situation", "Extra rest days (capped at 10)", "spread", _diff(g, tg, "rest_days").clip(-10, 10))
    add("S10b", "situation", "Coming off a bye (13+ days)", "spread", _diff(g, tg, "bye"))
    add("S10c", "situation", "Short week (5 days or fewer)", "spread", _diff(g, tg, "short"))
    cold = (g["month"] >= 11) & (g["latitude"] >= 40) & ~neutral
    add("S11", "situation", "November or later at a northern venue (40N+)", "total",
        np.where(g["latitude"].notna(), cold.astype(float), np.nan))
    frac = absp % 1
    on_key = absp.isin([3, 7, 10, 14])
    above = absp.isin([3.5, 7.5, 10.5, 14.5])
    below = absp.isin([2.5, 6.5, 9.5, 13.5])
    add("S12a", "market", "Favorite on a key number (3, 7, 10, 14)", "spread", np.where(on_key, fav_side, 0.0) + frac * 0)
    add("S12b", "market", "Underdog getting the hook (3.5, 7.5, 10.5, 14.5)", "spread", np.where(above, -fav_side, 0.0) + frac * 0)
    add("S12c", "market", "Favorite laying just under a key number (2.5, 6.5, ...)", "spread", np.where(below, fav_side, 0.0) + frac * 0)
    nonconf = ~g["conference_game"].astype(bool) & g["home_conf"].notna() & g["away_conf"].notna()
    for idea, label, members in (("S13a", "Power-conference team vs. the rest (non-conference)", POWER),
                                 ("S13b", "SEC team in a non-conference game", {"SEC"}),
                                 ("S13c", "Big Ten team in a non-conference game", {"Big Ten"})):
        d = g["home_conf"].isin(members).astype(float) - g["away_conf"].isin(members).astype(float)
        add(idea, "situation", label, "spread", np.where(nonconf, d, np.nan))
    acad = g["home"].isin(ACADEMIES).astype(float) + g["away"].isin(ACADEMIES).astype(float)
    add("S14", "situation", "Service academy in the game", "total", acad)
    add("S16", "situation", "Different quarterback than last game", "spread", _diff(g, tg, "qb_change"))

    # Crazy on purpose
    home_body = g["et_hour"] - g["home_tz"]
    away_body = g["et_hour"] - g["away_tz"]
    add("C17a", "crazy", "Body clock: kickoff before 10 a.m. home time", "spread",
        (home_body < 10).astype(float) - (away_body < 10).astype(float) + (home_body + away_body) * 0)
    late_h = (g["home_tz"] == 0) & (g["et_hour"] >= 22)
    late_a = (g["away_tz"] == 0) & (g["et_hour"] >= 22)
    add("C17b", "crazy", "Eastern team in a 10 p.m. ET or later kickoff", "spread",
        late_h.astype(float) - late_a.astype(float) + (g["home_tz"] + g["away_tz"]) * 0)
    alt = g["home"].isin(HIGH_ALTITUDE_HOME) & ~g["away"].isin(HIGH_ALTITUDE_HOME) & ~neutral
    add("C18a", "crazy", "High-altitude home team hosting a lower-altitude visitor", "spread", alt.astype(float))
    add("C18b", "crazy", "Home team is Hawai'i", "spread", ((g["home"] == "Hawai'i") & ~neutral).astype(float))
    add("C18c", "crazy", "Week after a trip to Hawai'i", "spread", _diff(g, tg, "p_at_hawaii"))
    for idea, label, lo, hi in (("C19a", "noon ET window (before 2 p.m.)", 0, 14),
                                ("C19b", "afternoon window (2 to 6 p.m. ET)", 14, 18),
                                ("C19c", "prime time (6 to 9:30 p.m. ET)", 18, 21.5),
                                ("C19d", "late night (9:30 p.m. ET or later)", 21.5, 30)):
        slot = (g["et_hour"] >= lo) & (g["et_hour"] < hi)
        add(idea, "crazy", f"Favorite in the {label}", "spread", np.where(slot, fav_side, 0.0) + absp * 0)
        add(idea + "t", "crazy", f"Game in the {label}", "total", slot.astype(float))

    # Placebos: should show nothing.
    hl, al = g["home"].fillna("").str.len(), g["away"].fillna("").str.len()
    vowels = lambda s: s.fillna("").str.lower().str.count("[aeiou]")             # noqa: E731
    add("P1", "placebo", "Longer team name", "spread", (hl - al).astype(float))
    add("P2", "placebo", "Team first in the alphabet", "spread", np.where(g["home"] < g["away"], 1.0, -1.0))
    add("P3", "placebo", "More vowels in the team name", "spread", (vowels(g["home"]) - vowels(g["away"])).astype(float))
    add("P4", "placebo", "Even team id", "spread", (g["home_team_id"] % 2 == 0).astype(float) - (g["away_team_id"] % 2 == 0).astype(float))
    add("P5", "placebo", "Home team under a bright moon", "spread", g["moon"] - 0.5)
    add("P6", "placebo", "Moon brightness", "total", g["moon"] - 0.5)
    add("P7", "placebo", "Even day of the month", "total", (g["commence_time"].dt.tz_convert(ET).dt.day % 2 == 0).astype(float))
    add("P8", "placebo", "Even game id", "total", (g["cfbd_game_id"] % 2 == 0).astype(float))
    return out


# ── statistics ──────────────────────────────────────────────────────────────
def _phi(z: float) -> float:
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def clustered(x: np.ndarray, y: np.ndarray, clusters: np.ndarray) -> dict | None:
    """Slope of y on standardized x with season-week clustered standard error."""
    if len(x) < 30 or np.nanstd(x) == 0:
        return None
    xs = (x - x.mean()) / x.std()
    yc = y - y.mean()
    sxx = float((xs * xs).sum())
    beta = float((xs * yc).sum() / sxx)
    resid = yc - beta * xs
    score = pd.Series(xs * resid).groupby(clusters).sum().to_numpy()
    k = len(score)
    if k < 5:
        return None
    se = math.sqrt(float((score ** 2).sum()) * k / (k - 1)) / sxx
    if se == 0:
        return None
    z = beta / se
    return {"beta": beta, "se": se, "z": z, "p": 2 * (1 - _phi(abs(z))), "clusters": k}


def record(sig: np.ndarray, y: np.ndarray) -> dict:
    """Record of the side the signal points to (signal != 0)."""
    on = sig != 0
    res = np.sign(sig[on]) * y[on]
    w, l, p = int((res > 0).sum()), int((res < 0).sum()), int((res == 0).sum())
    return {"n": int(on.sum()), "w": w, "l": l, "p": p, "rate": w / (w + l) if w + l else None}


def test(idea: dict, g: pd.DataFrame, mask: pd.Series) -> dict | None:
    y = g["y_ats"] if idea["market"] == "spread" else g["y_tot"]
    ok = mask & idea["signal"].notna() & y.notna()
    if ok.sum() < 30:
        return None
    x, yy = idea["signal"][ok].to_numpy(float), y[ok].to_numpy(float)
    if (x != 0).sum() < MIN_FLAGGED:      # a handful of flagged games gives a meaningless z
        return None
    fit = clustered(x, yy, g.loc[ok, "cluster"].to_numpy())
    if fit is None:
        return None
    return {**fit, "games": int(ok.sum()), **{f"side_{k}": v for k, v in record(x, yy).items()}}


def benjamini_hochberg(pvals: list[float]) -> list[float]:
    m = len(pvals)
    order = sorted(range(m), key=lambda i: pvals[i])
    q = [0.0] * m
    running = 1.0
    for rank, i in reversed(list(enumerate(order, 1))):
        running = min(running, pvals[i] * m / rank)
        q[i] = running
    return q


def halftime_table(g: pd.DataFrame) -> list[dict]:
    """Descriptive only: a favorite's final cover rate by halftime state (no live price exists)."""
    rows = []
    d = g[g["close_spread"].notna() & (g["close_spread"] != 0) & g["home_line_scores"].notna()].copy()
    half = lambda ls: sum(ls[:2]) if isinstance(ls, list) and len(ls) >= 2 else np.nan  # noqa: E731
    fav = np.where(d["close_spread"] < 0, 1, -1)
    d["fav_half"] = fav * (d["home_line_scores"].map(half) - d["away_line_scores"].map(half))
    d["fav_cover"] = fav * d["y_ats"]
    for label, cond in (("Favorite trailing at the half", d["fav_half"] < 0),
                        ("Favorite tied at the half", d["fav_half"] == 0),
                        ("Favorite ahead, by less than the spread", (d["fav_half"] > 0) & (d["fav_half"] < d["close_spread"].abs())),
                        ("Favorite ahead by the spread or more", d["fav_half"] >= d["close_spread"].abs())):
        sub = d[cond & d["fav_cover"].notna()]
        w, l = int((sub["fav_cover"] > 0).sum()), int((sub["fav_cover"] < 0).sum())
        rows.append({"state": label, "games": len(sub), "w": w, "l": l, "rate": w / (w + l) if w + l else None})
    return rows


def run(data: dict[str, pd.DataFrame]) -> dict:
    g, tg = build(data)
    lined = g["close_spread"].notna() | g["close_total"].notna()
    masks = {"discovery": lined & (g["season"] <= DISCOVERY_THROUGH),
             "confirm": lined & (g["season"] == CONFIRM),
             "second_look": lined & (g["season"] == SECOND_LOOK)}
    results = []
    for idea in signals(g, tg):
        row = {k: idea[k] for k in ("id", "group", "label", "market", "plays")}
        for name, mask in masks.items():
            row[name] = test(idea, g, mask)
        results.append(row)
    tested = [r for r in results if r["discovery"]]
    for r, q in zip(tested, benjamini_hochberg([r["discovery"]["p"] for r in tested])):
        r["q"] = q
        c = r["confirm"]
        same = bool(c and c["z"] * r["discovery"]["z"] > 0)
        r["confirm_one_sided_p"] = (1 - _phi(abs(c["z"]))) if same else (None if not c else 1 - (1 - _phi(abs(c["z"]))))
        r["passes_discovery"] = q <= BH_Q
        r["survives"] = bool(r["passes_discovery"] and same and r["confirm_one_sided_p"] < CONFIRM_P)
    seasons = g.loc[lined].groupby("season").size().to_dict()
    return {"version": VERSION, "games_by_season": {int(k): int(v) for k, v in seasons.items()},
            "play_seasons": sorted(int(s) for s in tg.loc[tg["snaps"].notna(), "season"].unique()),
            "results": results, "halftime": halftime_table(g[g["season"] <= CONFIRM])}


# ── report ──────────────────────────────────────────────────────────────────
def _side(t: dict | None) -> str:
    if not t or not t.get("side_n"):
        return "n/a"
    rate = f"{t['side_rate']:.1%}" if t["side_rate"] is not None else "n/a"
    return f"{t['side_w']}-{t['side_l']}-{t['side_p']} ({rate})"


def markdown(res: dict) -> str:
    rows = res["results"]
    tested = [r for r in rows if r.get("discovery")]
    real = [r for r in tested if r["group"] != "placebo"]
    placebo = [r for r in tested if r["group"] == "placebo"]
    lines = [f"# CFB idea screen ({res['version']})", "",
             f"Games with a closing line by season: {res['games_by_season']}. Play data: {res['play_seasons']}.",
             f"Discovery = seasons through {DISCOVERY_THROUGH}; confirmation = {CONFIRM}; second look = {SECOND_LOOK} to date.",
             f"Tested: {len(real)} ideas and {len(placebo)} placebos. Passed discovery (q <= {BH_Q}): "
             f"{sum(r['passes_discovery'] for r in real)} ideas, {sum(r['passes_discovery'] for r in placebo)} placebos. "
             f"Raw p < 0.05 in discovery: {sum(r['discovery']['p'] < 0.05 for r in real)} of {len(real)} ideas, "
             f"{sum(r['discovery']['p'] < 0.05 for r in placebo)} of {len(placebo)} placebos. "
             f"Survived confirmation: {sum(r['survives'] for r in real)} ideas, {sum(r['survives'] for r in placebo)} placebos.", "",
             "Record = the side the signal points to (W-L-P). z > 0 means that side won more.", "",
             "| id | idea | market | discovery record | z | q | 2025 record | z | 2026 record | z | verdict |",
             "|---|---|---|---|---:|---:|---|---:|---|---:|---|"]
    for r in sorted(tested, key=lambda r: r["discovery"]["p"]):
        d, c, s = r["discovery"], r["confirm"], r["second_look"]
        verdict = "SURVIVES" if r["survives"] else "passed discovery, failed 2025" if r["passes_discovery"] else "dead"
        lines.append(f"| {r['id']} | {r['label']} | {r['market']} | {_side(d)} | {d['z']:+.2f} | {r['q']:.2f} | "
                     f"{_side(c)} | {c['z']:+.2f} |" if c else
                     f"| {r['id']} | {r['label']} | {r['market']} | {_side(d)} | {d['z']:+.2f} | {r['q']:.2f} | n/a | n/a |")
        lines[-1] += (f" {_side(s)} | {s['z']:+.2f} |" if s else " n/a | n/a |") + f" {verdict} |"
    skipped = [r for r in rows if not r.get("discovery")]
    if skipped:
        lines += ["", "Not testable (too few games with the signal): " + ", ".join(f"{r['id']} {r['label']}" for r in skipped)]
    lines += ["", "## Halftime state and the favorite's final cover (descriptive; no live price is stored)", "",
              "| state | games | record | rate |", "|---|---:|---|---:|"]
    for h in res["halftime"]:
        lines.append(f"| {h['state']} | {h['games']} | {h['w']}-{h['l']} | {h['rate']:.1%} |" if h["rate"] is not None
                     else f"| {h['state']} | {h['games']} | n/a | n/a |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="artifacts/cfb_idea_screen")
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    res = run(load(db))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(res, indent=1, default=float), encoding="utf-8")
    report = markdown(res)
    (out / "report.md").write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
