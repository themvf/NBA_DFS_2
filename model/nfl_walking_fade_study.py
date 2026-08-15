"""NFL total_walking fade — pre-registered study (registered 2026-08-15).

Sealed BEFORE Week 1 (2026-09-09) so the regular season is a clean test.

HYPOTHESIS
    `total_walking` flags OVERSHOOT. Taking the side OPPOSITE the flagged one,
    at trigger, obtains a better number than the closing line.

DISCOVERY SAMPLE (frozen, cannot confirm) — 14 preseason alerts, 12 games:
    mean -0.85 points, game-clustered 95% CI [-1.114, -0.531], 1 of 14 moved
    toward the flagged side (one-sided p=0.0009).

Three reasons that is a hunch and not a finding: preseason is a different
regime (starters rest), n=14, and it is 1 of 8 detectors examined. There is
also a mechanical alternative n=14 cannot exclude — the detector is DEFINED as
"the line has wandered furthest from open", and selecting the extreme of a
noisy series then observing regression toward the mean is arithmetic, not
inefficiency.

KILL CRITERION
    CI includes zero at n>=100 => dead. No re-slicing to over-only/under-only,
    no threshold re-tuning, no migration to spread_walking. A variant is a new
    study with its own registration.

Usage:
    python -m model.nfl_walking_fade_study
"""

from __future__ import annotations

import collections
import json
import logging
import random
import statistics

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# ── Frozen registration ──────────────────────────────────────────────────────
STUDY_VERSION = "nfl-walking-fade-v1"
REGISTERED_AT = "2026-08-15"
SEASON_START = "2026-09-09"          # Week 1; preseason is the discovery set
ALERT_TYPE = "total_walking"
FLOOR_N = 100
FLOOR_GAMES = 40
ROI_CI_LOWER_BOUND = -5.0            # conjunctive gate, percentage points
BOOTSTRAP_ITERS = 10000
_SEED = 20260909

# Discovery numbers, recorded so drift is visible. NEVER pooled with the test.
DISCOVERY = {"n": 14, "games": 12, "mean_points": -0.85,
             "ci": (-1.114, -0.531), "toward_flagged": 1}


def _consensus_total(books: dict) -> float | None:
    vals = []
    for book in books.values():
        raw = book.get("total_line")
        if raw is None:
            continue
        try:
            vals.append(float(raw))
        except (TypeError, ValueError):
            continue
    return sum(vals) / len(vals) if vals else None


def _decimal(american: float) -> float:
    a = float(american)
    return 1 + 100 / abs(a) if a < 0 else 1 + a / 100


def load_observations(db: DatabaseManager) -> list[dict]:
    """Regular-season total_walking alerts with a trigger and a later close.

    Fade-side CLV in POINTS, signed so positive = the line moved toward the
    fade side (i.e. the flagged side overshot, as H predicts).
    """
    alerts = db.execute(
        """
        SELECT a.id, a.side, a.created_at, a.matchup_id, a.details_json,
               a.outcome, m.game_date
        FROM line_alerts a
        JOIN nfl_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'nfl' AND a.alert_type = %s
          AND m.season_type = 'regular'
          AND m.commence_time >= %s
        ORDER BY a.id
        """,
        (ALERT_TYPE, SEASON_START),
    )
    if not alerts:
        return []

    captures: dict[int, list] = collections.defaultdict(list)
    for row in db.execute(
        """
        SELECT h.matchup_id, h.captured_at, h.books
        FROM game_odds_history h
        JOIN nfl_matchups m ON m.id = h.matchup_id
        WHERE h.sport = 'nfl' AND h.books IS NOT NULL
          AND m.commence_time IS NOT NULL AND h.captured_at < m.commence_time
          AND m.season_type = 'regular'
        ORDER BY h.captured_at
        """,
    ):
        books = row["books"]
        if isinstance(books, str):
            books = json.loads(books)
        captures[row["matchup_id"]].append((row["captured_at"], books))

    out: list[dict] = []
    for a in alerts:
        series = captures.get(a["matchup_id"])
        if not series:
            continue
        prior = [(t, b) for t, b in series if t <= a["created_at"]]
        if not prior or prior[-1][1] is series[-1][1]:
            continue                     # no post-trigger observation
        entry = _consensus_total(prior[-1][1])
        close = _consensus_total(series[-1][1])
        if entry is None or close is None:
            continue
        # Flagged side overshoots => line comes BACK => fade side gains.
        fade_clv = (entry - close) if a["side"] == "over" else (close - entry)
        details = a["details_json"] or {}
        out.append({
            "game": a["matchup_id"],
            "date": str(a["game_date"]),
            "flagged_side": a["side"],
            "fade_clv_points": fade_clv,
            "exec_decimal": details.get("exec_decimal"),
            "exec_book": details.get("exec_book"),
            "outcome": a["outcome"],
        })
    return out


def cluster_bootstrap(values: list[float], groups: list, *,
                      iters: int = BOOTSTRAP_ITERS,
                      seed: int = _SEED) -> tuple[float, float]:
    """Resample GAMES, not alerts. Two alerts on one game are one observation
    of that game's line behaviour, not two independent ones."""
    by: dict = collections.defaultdict(list)
    for g, v in zip(groups, values):
        by[g].append(v)
    keys = list(by)
    rng = random.Random(seed)
    means = []
    for _ in range(iters):
        s: list[float] = []
        for _ in range(len(keys)):
            s.extend(by[keys[rng.randrange(len(keys))]])
        means.append(sum(s) / len(s))
    means.sort()
    return means[int(0.025 * iters)], means[int(0.975 * iters)]


def report(db: DatabaseManager) -> dict:
    obs = load_observations(db)
    n = len(obs)
    games = len({o["game"] for o in obs})

    print(f"\n{'=' * 70}")
    print(f"NFL total_walking FADE STUDY · {STUDY_VERSION} · sealed {REGISTERED_AT}")
    print(f"{'=' * 70}")
    print("H: total_walking flags OVERSHOOT; the OPPOSITE side gets a better")
    print("   number than the close. Fade-side CLV in points, game-clustered.")
    print(f"population   regular season only, commence >= {SEASON_START}")
    print(f"floors       n >= {FLOOR_N} AND >= {FLOOR_GAMES} distinct games")
    print(f"kill         CI includes zero at floor => dead, no re-slicing")
    print(f"\ndiscovery (preseason, FROZEN — never pooled): "
          f"n={DISCOVERY['n']} mean {DISCOVERY['mean_points']:+.2f}pts "
          f"CI [{DISCOVERY['ci'][0]:+.2f},{DISCOVERY['ci'][1]:+.2f}]")

    print(f"\n-- CONFIRMATION SAMPLE --")
    if n == 0:
        print("  0 observations — regular season has not started "
              f"(Week 1 is {SEASON_START}).")
        return {"n": 0, "verdict": "not started"}

    if n < FLOOR_N or games < FLOOR_GAMES:
        won = sum(1 for o in obs if o["outcome"] == "won")
        lost = sum(1 for o in obs if o["outcome"] == "lost")
        print("  DESCRIPTIVE ONLY - NOT A FINDING")
        print(f"  n {n}/{FLOOR_N}  ·  games {games}/{FLOOR_GAMES}  ·  "
              f"flagged-side record {won}W-{lost}L")
        print("  No mean, no interval and no verdict may be computed below floor.")
        return {"n": n, "games": games, "verdict": "descriptive-only"}

    vals = [o["fade_clv_points"] for o in obs]
    grp = [o["game"] for o in obs]
    mean = statistics.fmean(vals)
    lo, hi = cluster_bootstrap(vals, grp)
    toward = sum(1 for v in vals if v > 0)

    priced = [o for o in obs if o.get("exec_decimal") and o["outcome"] in ("won", "lost")]
    roi_txt = "  ROI gate: no frozen prices available"
    roi_ok = False
    if priced:
        # The FADE side wins when the flagged side loses.
        units = [(_decimal(o["exec_decimal"]) - 1) if o["outcome"] == "lost" else -1.0
                 for o in priced]
        r_lo, r_hi = cluster_bootstrap([100 * u for u in units],
                                       [o["game"] for o in priced])
        roi = 100 * sum(units) / len(units)
        roi_ok = r_lo > ROI_CI_LOWER_BOUND
        roi_txt = (f"  ROI gate (fade side @ frozen price): {roi:+.2f}% "
                   f"CI [{r_lo:+.2f},{r_hi:+.2f}] n={len(priced)} -> "
                   f"{'PASS' if roi_ok else 'FAIL'}")

    print(f"  n={n} across {games} games")
    print(f"  fade-side CLV {mean:+.3f} points   95% CI [{lo:+.3f}, {hi:+.3f}]")
    print(f"  moved toward the fade side: {toward}/{n} ({100 * toward / n:.0f}%)")
    print(roi_txt)
    books = collections.Counter(o["exec_book"] for o in obs if o.get("exec_book"))
    if books:
        print(f"  execution books: {dict(books)}")

    if lo > 0 and roi_ok:
        verdict = "PASS -> live shadow period; the detector is NOT flipped yet"
    elif lo > 0:
        verdict = "CLV positive but ROI gate failed - better number, still loses"
    else:
        verdict = "DEAD - CI includes zero. No re-slicing, no re-tuning."
    print(f"  VERDICT: {verdict}")
    return {"n": n, "games": games, "mean": mean, "ci": (lo, hi),
            "verdict": verdict}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report(DatabaseManager(load_config().database_url))
