"""NFL team-game score projection by situational comparables.

WHAT THIS IS
------------
A descriptive, back-tested projection tool. It characterises a team-game by HOW the
offence played (a situational fingerprint), finds the most similar historical
team-games, and reports the distribution of points those comparable offences
ACTUALLY scored. The output is a range, not a point guess.

WHY IT LOOKS LIKE THIS (the honest history behind the design)
-------------------------------------------------------------
This design is what survived a sequence of back-tests, each of which killed a more
complicated idea:
  - Reconstructing points from drive archetypes leaked the target into the features
    (a `td_drive_rate` feature is nearly the score itself). FIXED: the target is the
    REAL final score from nflverse (home_score/away_score), independent of our labels,
    and outcome-flavoured features (td rate, epa) are excluded. Features are style /
    process only.
  - Conditioning on the opposing defence was tested THREE times (one season blurred,
    a decade blurred, a decade with leak-free situational interaction terms) and made
    the error WORSE every time. Defence identity is noisier than offence identity, and
    an offence's own splits already reflect the defences it faced. So this model does
    NOT condition on the opponent. That is a measured finding, not an omission.
  - Situational splits (third down, early down, trailing, red zone) DID help: MAE
    5.35 -> 5.04 over the blurred model. They are included.

MEASURED PERFORMANCE (leave-one-out, 2016-2025, real-score target)
------------------------------------------------------------------
  MAE ~5.0 points; 10-90 coverage ~83%; baseline (guess league mean) ~8.0;
  within-team-season game-to-game noise floor ~9.1. The model captures most of the
  REDUCIBLE signal; the rest is irreducible game variance at this resolution.

Descriptive only. No predictive edge or betting claim; not tested against a market.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# Situational offensive fingerprint. Style / process only — never the score or a
# near-copy of it (no td_drive_rate, no epa). Adding a feature here that co-varies
# with the target reintroduces the leak the design exists to avoid.
FEATURES = [
    "success",        # overall play success rate
    "explosive",      # explosive-play rate (gain >= 20)
    "third",          # success rate on 3rd/4th down
    "early",          # success rate on 1st/2nd down
    "trail",          # success rate when trailing
    "redzone",        # success rate inside the 20
    "pressure_faced", # pressure rate on dropbacks
    "sack",           # sack rate on dropbacks
    "giveaway",       # giveaways per drive
    "first_down",     # first-down rate per play
]

DEFAULT_K = 40
LO_PCTL, HI_PCTL = 10, 90


@dataclass
class Projection:
    median: float
    lo: float          # 10th percentile of comps
    hi: float          # 90th percentile of comps
    q25: float
    q75: float
    n: int
    comp_points: np.ndarray


def offensive_fingerprint(plays: pd.DataFrame) -> dict:
    """Situational fingerprint for one team's scrimmage plays in one game.

    Expects the columns emitted by model.nfl_play_archetypes.label_plays / the
    nfl_pbp_archetypes table: down, score_differential, yardline_100, success,
    explosive, qb_dropback, pressure, had_sack, turnover_type, first_down, drive.
    """
    g = plays[plays["down"].notna()].copy()
    if g.empty:
        return {f: np.nan for f in FEATURES}
    g["success"] = _as_float(g["success"])
    g["explosive"] = _as_float(g["explosive"])
    score_diff = pd.to_numeric(g["score_differential"], errors="coerce")
    yl = pd.to_numeric(g["yardline_100"], errors="coerce")
    late = g["down"].isin([3, 4])
    dbk = g[g["qb_dropback"] == True]
    drives = g.drop_duplicates(subset=["drive"])
    n_dr = max(1, len(drives))
    return {
        "success": g["success"].mean(),
        "explosive": g["explosive"].mean(),
        "third": g.loc[late, "success"].mean() if late.any() else np.nan,
        "early": g.loc[~late, "success"].mean() if (~late).any() else np.nan,
        "trail": g.loc[score_diff < 0, "success"].mean() if (score_diff < 0).any() else np.nan,
        "redzone": g.loc[yl <= 20, "success"].mean() if (yl <= 20).any() else np.nan,
        "pressure_faced": pd.to_numeric(dbk["pressure"], errors="coerce").mean() if len(dbk) else np.nan,
        "sack": dbk["had_sack"].astype(float).mean() if len(dbk) else 0.0,
        "giveaway": g["turnover_type"].notna().sum() / n_dr,
        "first_down": _as_float(g["first_down"]).mean(),
    }


def _as_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype("boolean").astype("float"), errors="coerce") \
        if s.dtype == object or str(s.dtype) == "boolean" else pd.to_numeric(s, errors="coerce")


class ScoreComps:
    """Fitted comparables engine over a reference set of team-games.

    reference: DataFrame with the FEATURES columns plus a `points` column (the REAL
    final points the team scored — from nflverse home/away score, NOT reconstructed)
    and an `id` column (e.g. game_id + team) so leave-one-out can exclude self.
    """

    def __init__(self, reference: pd.DataFrame):
        missing = [c for c in FEATURES + ["points"] if c not in reference.columns]
        if missing:
            raise ValueError(f"reference is missing columns: {missing}")
        self.ref = reference.dropna(subset=FEATURES + ["points"]).reset_index(drop=True)
        self.mu = self.ref[FEATURES].mean()
        self.sd = self.ref[FEATURES].std().replace(0, 1)
        self._Z = ((self.ref[FEATURES] - self.mu) / self.sd).values
        self._pts = self.ref["points"].to_numpy(dtype=float)

    def _standardise(self, fp: dict) -> np.ndarray:
        row = np.array([fp[f] for f in FEATURES], dtype=float)
        return (row - self.mu.values) / self.sd.values

    def project(self, fingerprint: dict, k: int = DEFAULT_K,
                exclude_index: int | None = None) -> Projection:
        z = self._standardise(fingerprint)
        d = np.sqrt(((self._Z - z) ** 2).sum(axis=1))
        if exclude_index is not None:
            d[exclude_index] = np.inf
        idx = np.argsort(d)[:k]
        comp = self._pts[idx]
        lo, hi = np.percentile(comp, [LO_PCTL, HI_PCTL])
        q25, q75 = np.percentile(comp, [25, 75])
        return Projection(float(np.median(comp)), float(lo), float(hi),
                          float(q25), float(q75), len(comp), comp)

    def backtest(self, k: int = DEFAULT_K) -> dict:
        """Leave-one-out calibration + error over the reference set."""
        covered, abs_err = 0, []
        for i in range(len(self.ref)):
            d = np.sqrt(((self._Z - self._Z[i]) ** 2).sum(axis=1))
            d[i] = np.inf
            idx = np.argsort(d)[:k]
            comp = self._pts[idx]
            lo, hi = np.percentile(comp, [LO_PCTL, HI_PCTL])
            covered += int(lo <= self._pts[i] <= hi)
            abs_err.append(abs(np.median(comp) - self._pts[i]))
        pts = self._pts
        return {
            "n": len(self.ref),
            "coverage_pct": covered / len(self.ref) * 100,
            "mae": float(np.mean(abs_err)),
            "baseline_mae": float(np.mean(np.abs(pts - pts.mean()))),
        }


# --------------------------------------------------------------------------- #
# Reference-set construction from the live data sources.
# --------------------------------------------------------------------------- #
def build_reference(seasons: range | list[int], database_url: str | None = None) -> pd.DataFrame:
    """Assemble the reference team-game table: situational fingerprint (from the
    labelled nfl_pbp_archetypes table) + REAL final points (from nflverse)."""
    from config import load_config
    from db.database import DatabaseManager
    from model.nfl_drive_archetypes import load_pbp

    url = database_url or load_config().database_url
    db = DatabaseManager(url)
    lo, hi = min(seasons), max(seasons)
    rows = db.execute(
        """SELECT game_id, season, posteam, drive, down, score_differential,
                  yardline_100, success, explosive, qb_dropback, pressure,
                  had_sack, turnover_type, first_down
           FROM nfl_pbp_archetypes
           WHERE season BETWEEN %s AND %s AND posteam IS NOT NULL AND down IS NOT NULL""",
        (lo, hi),
    )
    plays = pd.DataFrame([dict(r) for r in rows])

    fps = []
    for (gid, team), g in plays.groupby(["game_id", "posteam"]):
        fp = offensive_fingerprint(g)
        fp.update(id=f"{gid}:{team}", game_id=gid, season=int(g["season"].iloc[0]), team=team)
        fps.append(fp)
    feat = pd.DataFrame(fps)

    # REAL final score per (game, team), independent of our labels.
    truth = []
    for s in seasons:
        pbp = load_pbp(s, None)
        fin = pbp.dropna(subset=["home_score", "away_score"]).groupby("game_id").agg(
            home_team=("home_team", "first"), away_team=("away_team", "first"),
            home_score=("home_score", "max"), away_score=("away_score", "max"),
        ).reset_index()
        for _, r in fin.iterrows():
            truth.append((r["game_id"], r["home_team"], int(r["home_score"])))
            truth.append((r["game_id"], r["away_team"], int(r["away_score"])))
    truth = pd.DataFrame(truth, columns=["game_id", "team", "points"])

    return feat.merge(truth, on=["game_id", "team"], how="inner")


def team_fingerprint(reference: pd.DataFrame, team: str, season: int | None = None) -> dict:
    """Average situational fingerprint for a team (optionally one season) from the
    reference set — the identity to project future scoring from."""
    sub = reference[reference["team"] == team]
    if season is not None:
        sub = sub[sub["season"] == season]
    if sub.empty:
        raise ValueError(f"no reference rows for team={team} season={season}")
    return {f: sub[f].mean() for f in FEATURES}
