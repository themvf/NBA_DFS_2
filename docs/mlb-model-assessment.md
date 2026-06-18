# MLB Model Assessment & Improvement Backlog

_Assessment date: 2026-06-18. Based on ~9,000 scored player-rows across ~18 slates
(2026-04-02 → 2026-05-02), the trained HR sub-model's held-out metrics, the hitter
calibration loop, and the optimizer's correlation rules._

## TL;DR

The projection engine and optimizer are well-built and the measurement culture is
strong. **The biggest, cheapest win is data hygiene, not a modeling change**: the
model projects every pitcher in the DK pool as if they're the day's starter, so its
headline accuracy looks ~4 DK points worse than it actually is — purely because it's
being graded on players who never took the field.

---

## How it works

Deterministic, factor-based projection (not ML for the main projection), with a
trained HR classifier for ceiling signal, a hitter calibration feedback loop, and a
correlation-aware optimizer.

| Layer | File | Summary |
|---|---|---|
| Batter projection | `model/mlb_projections.py` | EWMA per-game rates × stacked capped factors: asymmetric env (implied total), park (separate runs/HR), weather (temp + compass wind model), opp SP xFIP, L/R wRC+ split, batting-order PA weight. <10 games → null. |
| Pitcher projection | `model/mlb_projections.py` | Independent. Per-start IP/K/BB, ER from **xFIP/9** (not raw ERA), H from WHIP, scaled by opp wRC+/K% + park, win prob blended from moneyline. |
| HR sub-model | `model/mlb_homerun_v2.json` | Standardized logistic regression (HistGBM twin for feature analysis), 23.5k pregame rows, deployed as JSON for TS porting. |
| Calibration loop | `web/src/app/dfs/mlb-projection-calibration.ts` | Empirical `actual/proj` factors per batting-order slot + implied-total residual, 45-day window, excludes `is_out` + SP/RP, ~0.90 default. **Hitter-only.** |
| Optimizer | `web/src/app/dfs/mlb-optimizer.ts` | Team stacks (max 5), bring-back, HR-correlation order bonus, anti-correlation (no opposing batters vs own pitcher), graceful constraint relaxation. |

## Measured accuracy

- **Combined MAE ~6.0–6.5 DK pts**, stable slate-to-slate.
- Raw **bias = +4 over-projection** on nearly every slate — but mostly an **artifact**.
- **Smoking gun:** of 1,637 pitcher-rows projected ≥10 FPTS, **1,359 (83%) scored zero
  — they never pitched.**
- Filter to players who **actually played**: pitchers near-**unbiased (+0.3, MAE 4.1)**;
  hitters who produced −2.2 (partly a selection artifact from excluding legit 0-fers).
  Underlying projection quality is genuinely decent.
- **HR model:** AUC 0.617, AP 0.169 vs 0.113 base; top-15 daily picks hit HR at
  **22.6% (2.0× lift)**. Honest and well-evaluated — but barely beats the prior
  heuristic (AUC 0.619), i.e. the feature set is near its signal ceiling.

## Strengths

- Defensible factor design — asymmetric env factor, xFIP-over-ERA, separate
  contact/HR park factors, real wind-direction geometry, pervasive caps prevent blow-ups.
- A genuine self-correcting calibration loop for hitters, properly scoped.
- Optimizer correlation logic is correct and complete — the thing that matters most in MLB DFS.
- Strong measurement discipline (postmortem framework distrusts fallback contamination
  and small samples; HR model evaluated against the right baselines).

## Weaknesses / gaps

1. **No starter/lineup gate at projection time (dominant issue).** Pitchers are
   projected regardless of whether they start → 80%+ of "projected starters" are noise.
   Pollutes the pool, inflates the apparent +4 bias, and distorts every postmortem
   metric that doesn't filter DNPs.
2. **No pitcher-side calibration loop** — hitters get empirical correction; pitchers
   (higher variance, higher leverage) don't.
3. **HR model is heuristic-equal** — logistic at AUC 0.62 ties the prior heuristic;
   the current feature set is exhausted.
4. **Thin/stale sample** — meaningful actuals on ~18 of 59 slates, stopping 2026-05-02.

## Improvement backlog (priority order)

| # | Item | Why | Effort |
|---|---|---|---|
| 1 | **Gate pitcher projections on confirmed-probable-starter status** (surface unconfirmed hitters) | Collapses the phantom +4 bias; makes every accuracy metric trustworthy. Biggest cheap win. | Low–Med |
| 2 | **Add a pitcher calibration loop** (by role SP/RP and projection tier) mirroring the hitter one | Pitchers are the highest-leverage slots and currently uncorrected | Med |
| 3 | **Re-baseline league constants** (tagged 2024-25) for 2026 | `LEAGUE_AVG_*` drift silently over-/under-projects | Low |
| 4 | **New HR features (Statcast barrel rate, pitch-type matchups)** rather than retuning | Current HR signal tapped out at ~2× lift | High |
| 5 | Ensure postmortem accuracy views exclude DNPs everywhere | Honest accuracy reads | Low |

## Notes for whoever picks this up

- The "+4 bias" is **not** a reason to globally subtract from projections — it's a
  measurement artifact. Fix the confirmation gate (#1) first, then re-measure before
  touching any formula.
- The hitter calibration loop already shrinks ~0.90; double-check it isn't
  double-counting once the DNP gate lands.
- Related Vegas/run-environment work: `docs/mlb-ou-vegas-phase1.md`,
  `ingest/refresh_mlb_vegas.py`.
