"""P3 of the Tennis Recency/Fatigue Model spec (CLAUDE.md): the actual
walk-forward test of H1 (recency) and H2 (rank/fatigue) — offline only, no
live deployment. This is where the hypotheses get graded; P1 built the data,
P2 froze the half-life and the H1 subset using only the tuning period
(matches before 2022-01-01). Everything scored here is 2022-01-01+, which P2
never touched.

H1 — recency-weighted rating. Within the FROZEN qualifying subset
(|form_gap| >= FORM_GAP_FREEZE_THRESHOLD, reserved period only — no re-
derivation of the threshold or the subset), compare flat-Elo-implied and
recency-Elo-implied win probabilities against actual outcomes. Both signals
are already point-in-time by construction (Phase 1), so no re-fitting is
needed here — just honest scoring. Kill criterion: if recency does not beat
flat Elo's logloss in this subset (bootstrap CI excluding zero improvement),
H1 is dead.

H2 — rank/fatigue features. Expanding-window walk-forward (retrain every
season on strictly-prior seasons — mirrors model/tennis_model.py's existing
methodology exactly, for consistency), scoring ONLY seasons 2022+ (the
reserved period). Compares the original 3-feature model against an expanded
feature set (rank_diff, log-scaled pts_diff, round_num, fatigue_diff =
matches_played_fav - matches_played_dog). Also tests the specific motivating
sub-claim directly: does fatigue predict ATP best-of-5 favorite losses.
Kill criterion: if the expanded model doesn't beat the baseline's logloss by
more than a bootstrap CI can attribute to noise, H2 is dead.

Usage:
    python -m model.tennis_recency_fatigue_backtest
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from ingest.tennis_training import FORM_GAP_FREEZE_THRESHOLD, load_corpus

_RESERVED_CUTOFF = "2022-01-01"   # P2's tuning/reserved boundary — do not move
_MIN_TRAIN = 2000                  # same floor as model/tennis_model.py
_BOOTSTRAP_ITERS = 2000
_BASELINE_FEATURES = ["market_fav_prob", "elo_diff", "grass_elo_diff"]
_EXPANDED_EXTRA = ["rank_diff_scaled", "pts_diff_log", "round_num", "fatigue_diff"]


def _logloss(p, y) -> float:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _brier(p, y) -> float:
    return float(np.mean((p - y) ** 2))


def _implied_prob(diff) -> np.ndarray:
    return 1.0 / (1.0 + 10 ** (-np.asarray(diff, dtype=float) / 400.0))


def _bootstrap_ci(a_ll_per_row, b_ll_per_row, iters=_BOOTSTRAP_ITERS, seed=0):
    """95% CI on mean(a_per_row) - mean(b_per_row) via paired bootstrap.
    Positive = a has HIGHER logloss than b (b is better)."""
    rng = np.random.default_rng(seed)
    n = len(a_ll_per_row)
    diffs = a_ll_per_row - b_ll_per_row
    boots = np.empty(iters)
    for i in range(iters):
        idx = rng.integers(0, n, n)
        boots[i] = diffs[idx].mean()
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return float(diffs.mean()), float(lo), float(hi)


def _per_row_logloss(p, y) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    y = np.asarray(y, dtype=float)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


# ── H1: recency vs flat Elo, in the frozen subset, reserved period only ──────

def run_h1(df) -> dict:
    print("=" * 70)
    print("H1 — recency-weighted Elo vs flat Elo, in the frozen form-gap subset")
    print(f"    (|form_gap| >= {FORM_GAP_FREEZE_THRESHOLD}, reserved period only, "
          f">= {_RESERVED_CUTOFF})")
    print("=" * 70)

    reserved = df[df["date"] >= _RESERVED_CUTOFF].copy()
    need = ["form_gap", "elo_diff", "recency_elo_diff", "market_fav_prob", "y"]
    reserved = reserved.dropna(subset=need)
    subset = reserved[reserved["form_gap"].abs() >= FORM_GAP_FREEZE_THRESHOLD]

    results = {}
    any_pass = False
    for tour in ("ATP", "WTA"):
        g = subset[subset["tour"] == tour]
        n = len(g)
        if n < 200:
            print(f"\n  {tour}: n={n} — BELOW the pre-registered >=200 minimum, not scored.")
            results[tour] = {"n": n, "verdict": "INSUFFICIENT_SAMPLE"}
            continue

        y = g["y"].values.astype(int)
        flat_p = _implied_prob(g["elo_diff"].values)
        rec_p = _implied_prob(g["recency_elo_diff"].values)
        mkt_p = g["market_fav_prob"].values.astype(float)

        flat_ll, rec_ll, mkt_ll = _logloss(flat_p, y), _logloss(rec_p, y), _logloss(mkt_p, y)
        flat_br, rec_br = _brier(flat_p, y), _brier(rec_p, y)

        flat_rows, rec_rows = _per_row_logloss(flat_p, y), _per_row_logloss(rec_p, y)
        mean_diff, lo, hi = _bootstrap_ci(flat_rows, rec_rows)
        # mean_diff > 0 means flat has HIGHER logloss (worse) => recency wins.
        recency_beats_flat = lo > 0  # CI entirely positive = recency reliably better
        verdict = "PASS (recency beats flat, CI excludes zero)" if recency_beats_flat else "FAIL (no reliable improvement)"
        if recency_beats_flat:
            any_pass = True

        print(f"\n  {tour} (n={n}):")
        print(f"    flat Elo   logloss={flat_ll:.4f}  brier={flat_br:.4f}")
        print(f"    recency Elo logloss={rec_ll:.4f}  brier={rec_br:.4f}")
        print(f"    market     logloss={mkt_ll:.4f}")
        print(f"    bootstrap delta (flat_ll - recency_ll): {mean_diff:+.4f}  "
              f"95% CI [{lo:+.4f}, {hi:+.4f}]")
        print(f"    H1 VERDICT ({tour}): {verdict}")
        results[tour] = {
            "n": n, "flat_logloss": flat_ll, "recency_logloss": rec_ll, "market_logloss": mkt_ll,
            "bootstrap_delta": mean_diff, "ci_lo": lo, "ci_hi": hi, "verdict": verdict,
        }

    print(f"\nH1 OVERALL: {'PASS in at least one tour' if any_pass else 'FAIL — kill H1'}")
    results["overall_pass"] = any_pass
    return results


# ── H2: rank/fatigue features, expanding-window walk-forward ────────────────

def _prep_h2(df):
    d = df.copy()
    d["rank_diff_scaled"] = pd.to_numeric(d["rank_diff"], errors="coerce") / 10.0
    pts = pd.to_numeric(d["pts_diff"], errors="coerce")
    d["pts_diff_log"] = np.sign(pts) * np.log1p(pts.abs())
    d["fatigue_diff"] = (pd.to_numeric(d["matches_played_fav"], errors="coerce")
                          - pd.to_numeric(d["matches_played_dog"], errors="coerce"))
    for c in _BASELINE_FEATURES + _EXPANDED_EXTRA:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["y"] = pd.to_numeric(d["y"], errors="coerce")
    all_cols = list(dict.fromkeys(_BASELINE_FEATURES + _EXPANDED_EXTRA))
    d = d[d[all_cols].notna().all(axis=1) & d["y"].notna()].reset_index(drop=True)
    return d


def _fit(train, features):
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    X = train[features].values.astype(float)
    y = train["y"].values.astype(int)
    scaler = StandardScaler().fit(X)
    model = LogisticRegression(C=1.0, max_iter=1000).fit(scaler.transform(X), y)
    return model, scaler


def _predict(model, scaler, rows, features):
    Xs = scaler.transform(rows[features].values.astype(float))
    return model.predict_proba(Xs)[:, 1]


def _walk_forward_oos(data, features):
    """Expanding-window, retrained per season (mirrors model/tennis_model.py).
    Returns OOS (pred, y) arrays for seasons >= reserved cutoff only."""
    d = data.copy()
    d["season"] = d["date"].str[:4].astype(int)
    reserved_year = int(_RESERVED_CUTOFF[:4])
    seasons = sorted(d["season"].unique())

    preds, ys = [], []
    for s in seasons:
        if s < reserved_year:
            continue  # only score the reserved period; still trains on all prior data
        train = d[d["season"] < s]
        test = d[d["season"] == s]
        if len(train) < _MIN_TRAIN or test.empty:
            continue
        model, scaler = _fit(train, features)
        preds.append(_predict(model, scaler, test, features))
        ys.append(test["y"].values.astype(int))
    if not preds:
        return None, None
    return np.concatenate(preds), np.concatenate(ys)


def run_h2(df) -> dict:
    print("\n" + "=" * 70)
    print("H2 — rank/points/fatigue features vs the baseline 3-feature model")
    print(f"    (expanding-window walk-forward, scored on seasons >= {_RESERVED_CUTOFF[:4]} only)")
    print("=" * 70)

    d = _prep_h2(df)
    results = {}
    any_pass = False
    for tour in ("ATP", "WTA"):
        g = d[d["tour"] == tour]
        base_pred, base_y = _walk_forward_oos(g, _BASELINE_FEATURES)
        exp_pred, exp_y = _walk_forward_oos(g, _BASELINE_FEATURES + _EXPANDED_EXTRA)
        if base_pred is None or exp_pred is None or len(base_y) < 200:
            n = 0 if base_pred is None else len(base_y)
            print(f"\n  {tour}: n={n} OOS — BELOW the pre-registered >=200 minimum, not scored.")
            results[tour] = {"n": n, "verdict": "INSUFFICIENT_SAMPLE"}
            continue

        base_ll, exp_ll = _logloss(base_pred, base_y), _logloss(exp_pred, exp_y)
        base_rows, exp_rows = _per_row_logloss(base_pred, base_y), _per_row_logloss(exp_pred, exp_y)
        mean_diff, lo, hi = _bootstrap_ci(base_rows, exp_rows)
        expanded_beats_base = lo > 0
        verdict = "PASS (expanded beats baseline, CI excludes zero)" if expanded_beats_base else "FAIL (no reliable improvement)"
        if expanded_beats_base:
            any_pass = True

        print(f"\n  {tour} (OOS n={len(base_y)}):")
        print(f"    baseline (3-feature) logloss={base_ll:.4f}")
        print(f"    expanded (+rank/pts/round/fatigue) logloss={exp_ll:.4f}")
        print(f"    bootstrap delta (baseline_ll - expanded_ll): {mean_diff:+.4f}  "
              f"95% CI [{lo:+.4f}, {hi:+.4f}]")
        print(f"    H2 VERDICT ({tour}): {verdict}")
        results[tour] = {
            "n": len(base_y), "baseline_logloss": base_ll, "expanded_logloss": exp_ll,
            "bootstrap_delta": mean_diff, "ci_lo": lo, "ci_hi": hi, "verdict": verdict,
        }

    print(f"\nH2 OVERALL: {'PASS in at least one tour' if any_pass else 'FAIL — kill H2'}")
    results["overall_pass"] = any_pass

    # Specific sub-claim: does fatigue predict ATP best-of-5 favorite losses?
    print("\n  Sub-claim: fatigue_diff coefficient, ATP best-of-5 matches only")
    atp5 = d[(d["tour"] == "ATP") & (pd.to_numeric(d["best_of"], errors="coerce") == 5)]
    if len(atp5) >= 200:
        feats = _BASELINE_FEATURES + ["fatigue_diff"]
        model, scaler = _fit(atp5, feats)
        coef = float(model.coef_[0][feats.index("fatigue_diff")])
        # Bootstrap CI on the coefficient itself.
        rng = np.random.default_rng(1)
        n = len(atp5)
        boots = np.empty(500)
        for i in range(500):
            idx = rng.integers(0, n, n)
            samp = atp5.iloc[idx]
            m, s = _fit(samp, feats)
            boots[i] = m.coef_[0][feats.index("fatigue_diff")]
        clo, chi = np.percentile(boots, [2.5, 97.5])
        ci_excludes_zero = (clo > 0 and chi > 0) or (clo < 0 and chi < 0)
        sig = "SIGNIFICANT (CI excludes 0)" if ci_excludes_zero else "not significant (CI includes 0)"
        print(f"    n={n}  coef={coef:+.4f}  95% CI [{clo:+.4f}, {chi:+.4f}]  {sig}")
        results["atp_fatigue_subclaim"] = {"n": n, "coef": coef, "ci_lo": float(clo), "ci_hi": float(chi)}
    else:
        print(f"    n={len(atp5)} — below 200, not tested")
        results["atp_fatigue_subclaim"] = {"n": len(atp5), "verdict": "INSUFFICIENT_SAMPLE"}

    return results


def run() -> None:
    df = load_corpus()
    h1 = run_h1(df)
    h2 = run_h2(df)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"H1 (recency Elo):     {'PASS' if h1.get('overall_pass') else 'FAIL — no further recency-Elo variants without a new pre-registered study'}")
    print(f"H2 (rank/fatigue):    {'PASS' if h2.get('overall_pass') else 'FAIL — no further feature additions without a new pre-registered study'}")
    if not h1.get("overall_pass") and not h2.get("overall_pass"):
        print("\nBoth hypotheses failed their kill criteria. Per this spec's Non-negotiables:")
        print("tennis moneyline stays calibration-only across every tested feature family.")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
