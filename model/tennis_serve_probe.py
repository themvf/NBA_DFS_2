"""FEASIBILITY PROBE (step 1) — can serve stats predict total games?

Before building a full serve-based Monte Carlo for tennis derivative markets
(total games, player games, set betting, tiebreak), we must first answer the
cheap go/no-go question: does a serve-hold model's PREDICTED total games track
ACTUAL total games out-of-sample, better than a naive baseline?

If the number isn't even calibrated against reality, there's no point building
the market-facing model — same gate discipline as the P3 moneyline test.

Data: TML-Database (ATP) — the only source with per-match serve points AND the
score string. We:
  1. Walk matches chronologically, maintaining each player's point-in-time
     serve% / return% (shrunk to the league mean; snapshot BEFORE each match, so
     no leakage).
  2. For each test-window match, combine both players' serve/return into per-game
     hold probabilities, Monte-Carlo the match (best-of aware, tiebreaks), and
     take mean total games as the prediction.
  3. Compare predicted vs actual total games (MAE, correlation) against a naive
     baseline (train-period mean total games by best_of), overall and on grass.

This is ATP-only (WTA has no serve stats). Not wired into anything — it prints a
verdict. Usage:  python -m model.tennis_serve_probe [--from 2011] [--test 2023]
"""

from __future__ import annotations

import argparse
import logging

import numpy as np

from ingest.tennis_history import _fetch_tml_year, _normalize_name, _to_int

logger = logging.getLogger(__name__)

LEAGUE_SVC = 0.635          # ATP league avg serve-points-won (all surfaces)
_PRIOR_PTS = 200            # shrinkage strength (points) toward league mean
_MIN_PTS = 300              # min served points before a player is "warmed up"
_SIMS = 300


def _parse_total_games(score: str) -> int | None:
    """Sum games across all sets from a score like '7-6(4) 3-6 6-3'. None if invalid/incomplete."""
    if not score:
        return None
    s = score.strip()
    if any(tok in s for tok in ("RET", "W/O", "DEF", "Def", "Walkover", "walkover", "ABN")):
        return None
    total = sets = 0
    for token in s.split():
        core = token.split("(")[0]
        if "-" not in core:
            continue
        a, _, b = core.partition("-")
        ga, gb = _to_int(a), _to_int(b)
        if ga is None or gb is None:
            return None
        total += ga + gb
        sets += 1
    return total if sets >= 2 else None


def _pgame(p: float) -> float:
    """P(server wins a game) given per-point serve-win prob p (deuce-aware closed form)."""
    q = 1.0 - p
    pdeuce = 20 * p**3 * q**3
    pwin_deuce = p**2 / (p**2 + q**2) if (p**2 + q**2) else 0.5
    return p**4 + 4 * p**4 * q + 10 * p**4 * q**2 + pdeuce * pwin_deuce


def _sim_total_games(pHA: float, pHB: float, best_of: int, rng, n: int = _SIMS) -> float:
    """Vectorized Monte-Carlo mean total games. pHA/pHB = per-game hold probs."""
    need = 3 if best_of == 5 else 2
    gA = np.zeros(n, int); gB = np.zeros(n, int)
    setsA = np.zeros(n, int); setsB = np.zeros(n, int)
    gtotal = np.zeros(n, int); gameno = np.zeros(n, int)
    done = np.zeros(n, bool)
    tbA = pHA * (1 - pHB) / (pHA * (1 - pHB) + pHB * (1 - pHA) + 1e-9)

    for _ in range(600):
        if done.all():
            break
        active = ~done
        serverA = (gameno % 2 == 0)
        tb = active & (gA == 6) & (gB == 6)
        reg = active & ~tb

        holdp = np.where(serverA, pHA, pHB)
        a_game = np.where(serverA, rng.random(n) < holdp, rng.random(n) >= holdp)  # A won the game?
        tb_awin = rng.random(n) < tbA

        gA += (reg & a_game).astype(int) + (tb & tb_awin).astype(int)
        gB += (reg & ~a_game).astype(int) + (tb & ~tb_awin).astype(int)
        gtotal += active.astype(int)
        gameno += active.astype(int)

        wonA = active & (((gA >= 6) & (gA - gB >= 2)) | (gA == 7))
        wonB = active & (((gB >= 6) & (gB - gA >= 2)) | (gB == 7))
        setsA += wonA.astype(int); setsB += wonB.astype(int)
        end = wonA | wonB
        gA[end] = 0; gB[end] = 0
        done = done | (setsA >= need) | (setsB >= need)

    return float(gtotal.mean())


class _P:
    __slots__ = ("spw", "sp", "rpw", "rp")

    def __init__(self):
        self.spw = self.sp = self.rpw = self.rp = 0

    def serve_pct(self) -> float:
        return (self.spw + _PRIOR_PTS * LEAGUE_SVC) / (self.sp + _PRIOR_PTS)

    def return_pct(self) -> float:
        return (self.rpw + _PRIOR_PTS * (1 - LEAGUE_SVC)) / (self.rp + _PRIOR_PTS)

    def warm(self) -> bool:
        return self.sp >= _MIN_PTS and self.rp >= _MIN_PTS


def run(from_year: int, test_year: int) -> None:
    import datetime

    this_year = datetime.datetime.now(datetime.timezone.utc).year
    matches = []
    for y in range(from_year, this_year + 1):
        rows = _fetch_tml_year(y)
        for m in rows:
            matches.append((_to_int(m.get("tourney_date")) or 0,
                            _to_int(m.get("match_num")) or 0, m))
    matches.sort(key=lambda t: (t[0], t[1]))
    logger.info("Loaded %d ATP matches %d-%d", len(matches), from_year, this_year)

    players: dict[str, _P] = {}
    rng = np.random.default_rng(42)

    # Train-period totals by best_of (for the naive baseline) — from pre-test matches only.
    train_tot = {3: [], 5: []}
    preds, actuals, surfaces, base = [], [], [], []

    for tdate, _, m in matches:
        wk, lk = _normalize_name(m.get("winner_name", "")), _normalize_name(m.get("loser_name", ""))
        if not wk or not lk:
            continue
        w, l = players.setdefault(wk, _P()), players.setdefault(lk, _P())
        year = tdate // 10000
        best_of = _to_int(m.get("best_of")) or 3
        actual = _parse_total_games(m.get("score", ""))

        # Snapshot serve% BEFORE updating (leak-free).
        if actual is not None and year >= test_year and w.warm() and l.warm() and best_of in (3, 5):
            sw, rw = w.serve_pct(), w.return_pct()
            sl, rl = l.serve_pct(), l.return_pct()
            pA = min(max(sw - rl + (1 - LEAGUE_SVC), 0.50), 0.90)
            pB = min(max(sl - rw + (1 - LEAGUE_SVC), 0.50), 0.90)
            pred = _sim_total_games(_pgame(pA), _pgame(pB), best_of, rng)
            preds.append(pred); actuals.append(actual)
            surfaces.append(m.get("surface", "")); base.append(best_of)
        elif actual is not None and year < test_year and best_of in (3, 5):
            train_tot[best_of].append(actual)

        # Update serve/return accumulators.
        w_svpt, l_svpt = _to_int(m.get("w_svpt")), _to_int(m.get("l_svpt"))
        w_won = (_to_int(m.get("w_1stWon")) or 0) + (_to_int(m.get("w_2ndWon")) or 0)
        l_won = (_to_int(m.get("l_1stWon")) or 0) + (_to_int(m.get("l_2ndWon")) or 0)
        if w_svpt and l_svpt:
            w.spw += w_won; w.sp += w_svpt
            l.spw += l_won; l.sp += l_svpt
            w.rpw += (l_svpt - l_won); w.rp += l_svpt
            l.rpw += (w_svpt - w_won); l.rp += w_svpt

    if not preds:
        print("No test matches met the warmup threshold — widen the window.")
        return

    preds = np.array(preds); actuals = np.array(actuals, float)
    surfaces = np.array(surfaces); base_bo = np.array(base)
    base_mean = {bo: (np.mean(v) if v else np.nan) for bo, v in train_tot.items()}
    baseline = np.array([base_mean[bo] for bo in base_bo])

    def report(mask, label):
        if mask.sum() < 30:
            print(f"  {label}: n={int(mask.sum())} (too few)"); return
        p, a, b = preds[mask], actuals[mask], baseline[mask]
        mae_base = float(np.mean(np.abs(b - a)))
        corr = float(np.corrcoef(p, a)[0, 1])
        bias = float(np.mean(p - a))
        # Mean-calibrated model: remove the constant offset (a free param you always
        # fit in production). This isolates whether the serve-driven VARIATION helps.
        mae_cal = float(np.mean(np.abs((p - bias) - a)))
        lift = (mae_base - mae_cal) / mae_base * 100
        verdict = "SIGNAL" if mae_cal < mae_base - 0.05 else "no lift"
        print(f"  {label:12s} n={len(p):5d}  baseline MAE {mae_base:5.2f}  "
              f"calibrated model MAE {mae_cal:5.2f}  lift {lift:+5.1f}%  "
              f"corr {corr:+.3f}  raw bias {bias:+.2f}  -> {verdict}")

    print(f"\n-- Serve->total-games probe (ATP, test {test_year}+) --")
    print(f"  Prediction: serve-hold Monte-Carlo ({_SIMS} sims). "
          f"Baseline: train mean total games by best_of.")
    report(np.ones(len(preds), bool), "ALL")
    report(surfaces == "Grass", "GRASS (all)")
    report((surfaces == "Grass") & (base_bo == 3), "GRASS bo3")
    report((surfaces == "Grass") & (base_bo == 5), "GRASS bo5")
    report(base_bo == 3, "best-of-3")
    report(base_bo == 5, "best-of-5")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Serve->total-games feasibility probe")
    parser.add_argument("--from", dest="from_year", type=int, default=2011)
    parser.add_argument("--test", dest="test_year", type=int, default=2023)
    args = parser.parse_args()
    run(args.from_year, args.test_year)
