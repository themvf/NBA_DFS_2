# MLB Vegas Page Review & Build Plan

_Review date: 2026-06-18. Covers the MLB tab of `/vegas` — the descriptive
Vegas-accuracy analytics, the data-coverage tracker, and the client-side O/U
recommendation score._

## What the page is today

Rendered through the shared `VegasClient` (`sport="mlb"`). Two layers:

1. **Descriptive Vegas-accuracy analytics** (`web/src/db/queries.ts`)
   - `getMlbOuHitRate` — O/U hit rate by total tier (7.5 → 10.5+), game-total MAE
   - `getMlbTeamTotalAccuracy` — team implied vs actual runs (MAE + bias)
   - `getMlbRunLineCoverage` — favorite cover rate by run-line tier
   - `getMlbVegasCoverageStatus` — data-completeness tracker
2. **Client-side O/U "recommendation score"** (`computeOuScore` in `vegas-client.tsx`)
   — hand-weighted blend: SP xFIP 0.15, SP K/9 0.10, park 0.08, temp 0.04,
   wind 0.06, total-tier history 0.20, home over-history 0.20, away over-history 0.20.

## What's good

- Descriptive analytics are clean and correct — game-level totals are immune to
  the DNP contamination that distorts the projection model.
- `getMlbVegasCoverageStatus` is best-in-repo operational tooling (missing
  scores/odds per date, backfill ranges, provider-partial detection).
- The Phase-1 O/U score reuses real MLB context (SP xFIP/K9, park, weather, and
  the same compass wind geometry as the projection model), with sensible signal
  directions and shrinkage on historical rates.
- `blendSignals` normalizes by total weight → graceful degradation.

## Core gaps

1. **No independent predicted total.** MLB is the only sport without an
   `our_*_pred` column (NBA: `our_game_total_pred`; soccer: `our_total_pred`).
   The O/U "score" is a hand-weighted heuristic measured at **52.4% accuracy
   (53.2% actionable)** in the phase-1 backtest — coin-flip-grade.
2. **40% of the score leans on team over-history**, which is largely the residual
   of an efficient market (noise). Real environment signals carry only ~43%.
3. **Unused context in the DB.** `mlb_team_stats` has `team_wrc_plus`, `team_iso`,
   `bullpen_era`, `bullpen_fip` — none feeds the O/U score. Offense + bullpen are
   first-order run-total drivers.
4. **No actionable bet layer** — only a lean probability. No edge-vs-market flag,
   no settled-bet ledger, no per-tier calibration (unlike the soccer Vegas page).
5. **Minor:** `getMlbOuHitRate` has no season filter (run-env drifts year to year);
   `overRate` includes pushes in the denominator (negligible for half-run totals).

## Build plan (priority order)

| # | Item | Status |
|---|---|---|
| 1 | **Residual-over-Vegas total model** → write `our_total_pred` to `mlb_matchups` (features: SP xFIP/K9, park, weather/wind, team wRC+/ISO, bullpen FIP). `model/mlb_game_total_model.py`. | ✅ Done |
| 2 | Feed unused team offense + bullpen stats into the O/U score | ✅ Done (via the model — the model consumes wRC+/ISO/bullpen FIP and feeds the score as one dominant signal, cleaner than raw feeding) |
| 3 | Down-weight raw team over-history once real environment signals exist | ✅ Done (history signals drop 0.20→0.10 each when the model total is present) |
| 4 | Edge flag + settled-bet backtest mirroring the soccer Vegas ledger | ✅ Done |

## Priority 4 readout (2026-06-18) — calibration + UI indicators

No separate ledger table needed: `predict_and_write` only touches unscored
games, so a completed `mlb_matchups` row IS the frozen settled recommendation.
`getMlbTotalModelBacktest()` grades our lean (side of the line our number takes)
vs the actual, bucketed by edge magnitude. Historical predictions were
**walk-forward backfilled** (`--backfill`, train on strictly-prior games only —
no look-ahead) so the track record is real and out-of-sample.

**Walk-forward backtest (954 graded bets):**

| Edge \|our − line\| | Bets | Win% | ROI (−110) |
|---|---|---|---|
| 0.0–0.5 | 287 | 52.6% | +0.4% |
| 0.5–1.0 | 271 | 49.1% | −6.3% |
| 1.0–1.5 | 177 | **55.9%** | **+6.8%** |
| 1.5+ | 219 | **56.2%** | **+7.2%** |

**Key finding → drives the UI:** edges < 1 run are coin-flips; edges **≥ 1 run hit
~56% / +7% ROI**. So the page now only flags O/U leans when the model disagrees
with the line by ≥ 1 run (`MLB_TOTAL_ACTIONABLE_EDGE = 1.0`).

### Meaningful UI indications added

- **"Our Total" column** with a calibrated strength chip: solid-green
  **Strong O/U** (≥1.5), green **Lean O/U** (≥1.0, the actionable threshold),
  gray sub-threshold (0.5–1.0), faint (<0.5). Color = calibrated confidence.
- **O/U "Actionable" flag + lean direction** now gated on the ≥1-run model edge
  (not the old score band), so the green action badges only fire where the
  backtest says we win.
- **Model O/U Backtest panel** — win%/ROI by edge tier with the actionable tier
  highlighted and a green/red breakeven (52.4%) cue; headline record + ROI.
- **"Qualified O/U Leans"** table + count now use the model edge (shown in runs),
  consistent with the matchup-table badges.
- Self-healing: `refresh_mlb_vegas.py` walk-forward-backfills the trailing 7 days
  each run, so the backtest never has gaps.

## Moneyline (2026-06-18) — honest negative result

Built `model/mlb_moneyline_model.py` (market-anchored logistic predicting
P(home win) from the vig-free line + SP xFIP/K9, team wRC+/ISO, bullpen FIP),
writing `our_prob_home`, with the same walk-forward backfill + edge-tier backtest
(`getMlbMoneylineModelBacktest`, ROI priced at the real moneyline).

**Finding: the MLB moneyline market is efficient and the model has no edge.**
- Holdout: our logloss 0.672 vs market 0.663, our Brier 0.240 vs 0.237 — we are
  slightly *worse* than just using the line.
- Edge-bet sims lose: −2.8% ROI at 3pp, −24% at 5pp.
- Full walk-forward tiers are non-monotonic (−12% / +3% / −10% / +65%); the lone
  positive (6pp+) is a few big-underdog hits (high Dog%), not a repeatable signal,
  and the out-of-sample holdout contradicts it.
- Strength-feature coefficients are tiny and some have the wrong sign (noise) —
  the market already prices SP/offense/bullpen.

**Decision (accountability-first):** deploy the *infrastructure and the proof*,
not a fake signal. The page shows a **Model Moneyline Backtest** panel flagged
"INFORMATIONAL — market efficient, no stable edge," keeps `our_prob_home` as a
muted per-game **Our Win%** context line (where we disagree with the line), and
does **not** add an ML bet flag. This mirrors the soccer first-scorer pattern
where the calibration *is* the value (it tells you not to bet).

Contrast with totals: totals had a real, stable +7% ROI edge on ≥1-run
disagreements → actionable. Moneyline does not → informational only. The
backtest is what lets us tell those two apart honestly.

## Accountability ledger — full parity with the soccer framework (2026-06-18)

MLB now matches the soccer accountability spine. New tables `mlb_bets` +
`mlb_bet_snapshots` (mirror `soccer_bets`), plus `commence_time` on
`mlb_matchups` for the lock.

- `model/mlb_bet_rating.py` — `record_bet` reusing soccer's exact rating math
  (same star rubric); upserts one immutable, **model_version-stamped** row per
  selection and an append-only snapshot. **Locks at first pitch** so the backtest
  uses the number we committed to.
- `model/mlb_game_bets.py` — rates a moneyline + a total bet per game from the
  stored `our_total_pred`/`our_prob_home`, with walk-forward `--backfill` and
  `--settle` (won/lost/void from finals, no draws). Wired into
  `refresh_mlb_vegas.py` (rate slate + settle each run).
- Backfilled **1,999 bets** across 1,015 games; 1,985 settled.
- `getMlbBets` + `getMlbBetBacktest` (calibration by bet-type × star tier:
  expected vs realized win%, ROI at true price, Brier). UI: **Rated Bet Ledger**
  + **Bet Ledger Backtest** panels on `/vegas?sport=mlb`.

**Ledger calibration confirms the two findings, now visible per star:**
- **Totals** — 2★+ all profitable (+3% to +8% ROI), 1★ loses (−15%); realized
  win ~55% matches the edge-tier backtest. Stars are meaningful. (5★ is
  overconfident on probability — exp 70% vs real 55% — but still +ROI on price.)
- **Moneyline** — non-monotonic, expected > realized (overconfident), ROI swings
  are dog-variance. The ledger makes the "no edge" finding visible per tier, so
  for ML you trust the backtest, not the star.

## Build readout (2026-06-18)

`model/mlb_game_total_model.py` — Ridge on `actual_total − vegas_total`, trained on
1,074 completed 2026 games, writes `our_total_pred`. Holdout (n=215):

- Vegas MAE 3.31 / our MAE 3.36 — ~tied on raw MAE (expected; nobody beats an
  efficient line on MAE).
- **O/U side accuracy 54.9%** — beats the old heuristic (~52.4%) and clears the
  −110 breakeven (52.38%). This is the metric that matters for betting.
- Our bias +0.27 vs Vegas −0.68 → better calibrated (2026 totals ran ~0.68 over
  the lines; the model partially corrects toward that).
- Top features: SP xFIP avg/diff, ISO, wRC+, bullpen FIP, temp — all sensible.

Wired into `ingest/refresh_mlb_vegas.py` (retrains + writes daily after odds/scores).
Surfaced on `/vegas?sport=mlb`: new "Our Total" column with O/U edge, and the model
is the dominant O/U-score signal (weight 0.45) with history signals halved.

**Known limitation (for #4 / future):** the model currently leans over on most
games because it's absorbing a global 2026 over-tendency rather than purely
game-specific discrimination. A future iteration should de-mean the global bias
and rank on *relative* edge, plus add the settled-bet ledger so we can verify the
54.9% holds out of sample by edge magnitude.

## Notes

- Reuse the NBA pattern: a Ridge/linear model predicting the Vegas miss
  (`actual_total − vegas_total`) from team-efficiency features, anchored to the
  line so value comes from disagreement. Persist params to a DB table so CI
  predictions are self-sufficient (same approach as `soccer_model_params`).
- Settlement data is already present: `mlb_matchups.home_score/away_score` +
  `vegas_total` give actual-vs-line for backtesting with no new ingestion.
