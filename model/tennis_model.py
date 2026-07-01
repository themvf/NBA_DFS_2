"""Fitted tennis win-probability model — our number vs the closing line.

Market-anchored logistic regression (same machinery and philosophy as
``model/mlb_moneyline_model.py``): the vig-free market prob is a FEATURE, so the
model starts from the sharp closing line and learns residual corrections from
Elo, rather than fighting an efficient market from scratch.

    P(favorite wins) = sigma( b0 + b_mkt*market_fav_prob
                                  + b_elo*elo_diff + b_grass*grass_elo_diff )

Everything is favorite-oriented (favorite = lower decimal odds), which is
leak-free and reproducible live. Trained on the ``ingest.tennis_training`` corpus
(both tours, 2013-, de-vigged closing odds + point-in-time Elo).

The tennis moneyline is a very sharp market — the training corpus is near-
perfectly calibrated on its own. So this module is built to *prove or disprove*
an edge out of sample, not to assume one:

  * WALK-FORWARD is the headline test — train on strictly-prior seasons, predict
    the next, accumulate out-of-sample predictions, and only then score. A model
    that looks great on a random holdout but can't beat the line walk-forward has
    no edge.
  * The go/no-go gate (printed by ``evaluate``): our OOS log-loss must beat the
    market's AND the edge-bet ROI must be positive. Otherwise the honest outcome
    is calibration-only (rate <=2 stars) — same conclusion as soccer totals.

Usage:
    python -m model.tennis_model --evaluate                 # both tours
    python -m model.tennis_model --evaluate --tour ATP
    python -m model.tennis_model --evaluate --output data/tennis_eval.json
"""

from __future__ import annotations

import argparse
import json
import logging

import numpy as np

from ingest.tennis_training import FEATURE_COLS, load_corpus

logger = logging.getLogger(__name__)

MODEL_VERSION = "tennis-ml-v2"
_MIN_TRAIN = 2000       # min prior matches before we'll fit a walk-forward season


# ── Fit / predict ─────────────────────────────────────────────────

def _prep(df):
    """Numeric feature matrix + label, dropping rows with any missing feature."""
    import pandas as pd

    d = df.copy()
    for c in FEATURE_COLS:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["y"] = pd.to_numeric(d["y"], errors="coerce")
    d = d[d[FEATURE_COLS].notna().all(axis=1) & d["y"].notna()].reset_index(drop=True)
    return d


def _fit(train):
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    X = train[FEATURE_COLS].values.astype(float)
    y = train["y"].values.astype(int)
    scaler = StandardScaler().fit(X)
    model = LogisticRegression(C=1.0, max_iter=1000).fit(scaler.transform(X), y)
    return model, scaler


def _predict(model, scaler, rows):
    Xs = scaler.transform(rows[FEATURE_COLS].values.astype(float))
    return model.predict_proba(Xs)[:, 1]


# ── Metrics ───────────────────────────────────────────────────────

def _logloss(p, y):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _brier(p, y):
    return float(np.mean((p - y) ** 2))


def _edge_sims(our, mkt, y, fav_dec, dog_dec, thresholds=(0.03, 0.05, 0.08)):
    """Bet the side our prob beats the vig-free market by >= thr; ROI on real odds.

    our/mkt are P(favorite). Betting the favorite pays fav_dec-1 on a win (y==1);
    betting the dog pays dog_dec-1 on a dog win (y==0). Stake 1 unit per bet.
    """
    sims = {}
    for thr in thresholds:
        bets = wins = 0
        profit = 0.0
        for i in range(len(y)):
            fav_edge = our[i] - mkt[i]
            dog_edge = mkt[i] - our[i]  # (1-our)-(1-mkt)
            if fav_edge >= thr:
                won = y[i] == 1
                profit += (fav_dec[i] - 1.0) if won else -1.0
                bets += 1; wins += int(won)
            elif dog_edge >= thr:
                won = y[i] == 0
                profit += (dog_dec[i] - 1.0) if won else -1.0
                bets += 1; wins += int(won)
        sims[f"edge_{int(thr*100)}pp"] = {
            "bets": bets,
            "win_rate": round(wins / bets, 4) if bets else None,
            "roi": round(profit / bets, 4) if bets else None,
        }
    return sims


# ── Walk-forward (the honest test) ────────────────────────────────

def _walk_forward(data):
    """Expanding-window, retrained per season. Returns OOS (our, mkt, y, odds)."""
    d = data.copy()
    d["season"] = d["date"].str[:4].astype(int)
    seasons = sorted(d["season"].unique())

    our_all, mkt_all, y_all, fav_all, dog_all, surf_all = [], [], [], [], [], []
    fitted_seasons = 0
    for s in seasons:
        train = d[d["season"] < s]
        test = d[d["season"] == s]
        if len(train) < _MIN_TRAIN or test.empty:
            continue
        model, scaler = _fit(train)
        our_all.append(_predict(model, scaler, test))
        mkt_all.append(test["market_fav_prob"].values.astype(float))
        y_all.append(test["y"].values.astype(int))
        fav_all.append(test["fav_dec"].values.astype(float))
        dog_all.append(test["dog_dec"].values.astype(float))
        surf_all.append(test["surface"].values.astype(str))
        fitted_seasons += 1

    if not our_all:
        return None
    return (np.concatenate(our_all), np.concatenate(mkt_all), np.concatenate(y_all),
            np.concatenate(fav_all), np.concatenate(dog_all),
            np.concatenate(surf_all), fitted_seasons)


def evaluate_tour(df, tour: str) -> dict | None:
    data = _prep(df[df["tour"] == tour])
    if len(data) < _MIN_TRAIN * 2:
        print(f"  {tour}: only {len(data)} usable matches — skipping")
        return None

    wf = _walk_forward(data)
    if wf is None:
        print(f"  {tour}: insufficient history for walk-forward")
        return None
    our, mkt, y, fav_dec, dog_dec, surf, n_seasons = wf

    our_ll, mkt_ll = _logloss(our, y), _logloss(mkt, y)
    our_br, mkt_br = _brier(our, y), _brier(mkt, y)
    sims = _edge_sims(our, mkt, y, fav_dec, dog_dec)

    # Grass-only slice — the Wimbledon use case, where grass Elo has its best
    # shot at beating a less-grass-specialized market.
    gm = surf == "Grass"
    grass = None
    if gm.sum() >= 200:
        grass = {
            "n": int(gm.sum()),
            "market_logloss": round(_logloss(mkt[gm], y[gm]), 4),
            "our_logloss": round(_logloss(our[gm], y[gm]), 4),
            "edge_sims": _edge_sims(our[gm], mkt[gm], y[gm], fav_dec[gm], dog_dec[gm]),
        }

    # Coefficients from a final full-data fit (interpretability only).
    model, _ = _fit(data)
    coefs = sorted(
        ({"feature": f, "coef": round(float(c), 4)} for f, c in zip(FEATURE_COLS, model.coef_[0])),
        key=lambda r: abs(r["coef"]), reverse=True,
    )

    beats_line = our_ll < mkt_ll
    best_roi = max((s["roi"] for s in sims.values() if s["roi"] is not None), default=None)
    verdict = "SHIP" if (beats_line and best_roi is not None and best_roi > 0) else "CALIBRATION-ONLY"

    res = {
        "tour": tour, "model_version": MODEL_VERSION,
        "n_oos": int(len(y)), "n_seasons": n_seasons,
        "market_logloss": round(mkt_ll, 4), "our_logloss": round(our_ll, 4),
        "market_brier": round(mkt_br, 4), "our_brier": round(our_br, 4),
        "beats_line": beats_line, "edge_sims": sims, "coefs": coefs,
        "grass": grass, "verdict": verdict,
    }

    print(f"\n-- Tennis {tour} ({MODEL_VERSION}) walk-forward, {n_seasons} seasons, OOS n={len(y)} --")
    print(f"  Market  logloss {mkt_ll:.4f}  brier {mkt_br:.4f}")
    print(f"  Our     logloss {our_ll:.4f}  brier {our_br:.4f}   "
          f"({'beats' if beats_line else 'DOES NOT beat'} the line)")
    print("  Edge-bet ROI (bet where our prob beats vig-free market; real closing odds):")
    for k, v in sims.items():
        wr = f"{v['win_rate']*100:.1f}%" if v["win_rate"] is not None else "-"
        roi = f"{v['roi']*100:+.1f}%" if v["roi"] is not None else "-"
        print(f"    {k}: {v['bets']:5d} bets  win {wr:>6}  ROI {roi:>7}")
    if grass:
        gb = "beats" if grass["our_logloss"] < grass["market_logloss"] else "DOES NOT beat"
        print(f"  Grass only (n={grass['n']}): market ll {grass['market_logloss']:.4f}  "
              f"our ll {grass['our_logloss']:.4f}  ({gb} the line)")
        g3 = grass["edge_sims"]["edge_3pp"]
        roi = f"{g3['roi']*100:+.1f}%" if g3["roi"] is not None else "-"
        print(f"    grass edge_3pp: {g3['bets']} bets  ROI {roi}")
    print("  Coefs (standardized):")
    for c in coefs:
        print(f"    {c['feature']:<18} {c['coef']:+.4f}")
    print(f"  VERDICT: {verdict}")
    return res


def evaluate(tours=("ATP", "WTA"), output: str | None = None) -> dict:
    df = load_corpus()
    results = {}
    for tour in tours:
        r = evaluate_tour(df, tour)
        if r:
            results[tour] = r
    if output and results:
        from pathlib import Path
        Path(output).write_text(json.dumps(results, indent=2))
        print(f"\nWrote {output}")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fitted tennis win-probability model")
    parser.add_argument("--evaluate", action="store_true", help="Run walk-forward evaluation")
    parser.add_argument("--tour", choices=["ATP", "WTA"], help="Only this tour (default: both)")
    parser.add_argument("--output", help="Optional path to write evaluation JSON")
    args = parser.parse_args()

    tours = (args.tour,) if args.tour else ("ATP", "WTA")
    if args.evaluate:
        evaluate(tours, args.output)
    else:
        print("Nothing to do. Pass --evaluate (prediction wiring is P4).")
