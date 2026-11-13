# CFB Signal Research and Promotion Contract

**Version:** `cfb-lines-v1`  
**Effective:** 2026-09-01  
**Status:** Prospective research only; no positive-return or executable-edge claim

## Purpose

The CFB terminal records market behavior so that line-movement hypotheses can be evaluated without hindsight. A signal is an immutable observation generated from information available at its trigger time. It is not a recommendation and it cannot be described as an edge until it passes the promotion process below.

## Auditable evidence chain

Every evaluated signal must be reproducible through this chain:

```text
provider quote
  -> game_odds_history row and exact books JSON
  -> versioned line_alerts signal and source history IDs
  -> verified_clv_closes close
  -> official final score
  -> append-only alert_grades result
  -> grouped prospective backtest
```

The signal freezes `signal_version`, `origin`, `dedupe_key`, trigger time, trigger/previous/opening history IDs, consensus method, exact-line support, market-book count, entry book and entry price. Settlement freezes the verified close row/source, line CLV, comparable price CLV when available, result, and one-unit P&L.

Raw provider observations remain source data. Consensus, signal labels, CLV, and backtest summaries are derived data and must be recomputable from the frozen source rows.

## V1 signal definitions

All consensus lines use the deterministic lower median. A signal requires at least four eligible books at the selected consensus line unless its definition explicitly requires comparable same-book quotes.

| Signal | V1 definition |
|---|---|
| Spread steam | Consensus spread moves at least 1 point within 30 minutes, with at least three books moving in the same direction. |
| Total steam | Consensus total moves at least 1.5 points within 30 minutes, with at least three books moving in the same direction. |
| Spread/total walking | Opening-to-current movement meeting the corresponding steam threshold without the 30-minute requirement. |
| Key cross | Home spread crosses 3, 7, 10, or 14. |
| Price pressure | At least a four-percentage-point no-vig probability change at an unchanged line, supported by four comparable books. |
| Reversal | A prior move of at least 0.75 spread points or 1 total point reverses by the same minimum. Pivot and source history rows are frozen. |
| Reference led | A Pinnacle-class reference moves first and retail books subsequently follow. This is descriptive; it is not evidence that the move is sharp. |

Generic moneyline divergence/value detectors may run for CFB, but spread and total research uses the definitions above.

## Settlement contract

- Signals settle only against the scheduled-boundary verified close, never the latest available row.
- The frozen close must precede kickoff and satisfy the close-candidate contract.
- Spread and total outcomes use official final scores and include overtime under normal sportsbook grading rules.
- A cancellation, unresolved final, missing verified close, or unavailable entry quote remains pending or void as appropriate; it must not be imputed as a loss or a win.
- Line CLV is primary. Same-book price CLV is reported only when entry and close are directly comparable at the same line.
- Realized P&L assumes a one-unit stake at the frozen entry price. It is a research normalization, not a bankroll recommendation.
- Re-running settlement must be idempotent. A corrected official result appends a distinguishable grade rather than erasing prior evidence.

## Backtest rules

1. Use `origin = prospective` for promotion decisions. Retrospective reconstructions are allowed for exploration but must be labeled and reported separately.
2. Group results by the exact `signal_version`. A threshold or definition change creates a new version; it never rewrites old signals.
3. Report observation count, settled count, wins/losses/pushes, average line CLV, percentage beating the close, units, ROI per bet, and first/last game date.
4. Treat multiple signals from the same game as correlated. Statistical uncertainty must use game-date or game-cluster resampling, not an independent-bet assumption.
5. Segment diagnostics may inspect market, lead-time bucket, favorite/underdog, total range, conference, book support, and overtime. These are diagnostics, not new strategies unless pre-registered as a new version.
6. Flag overtime games separately when evaluating total signals because their residual distribution has a materially different right tail.
7. Include pushes and voids explicitly. Never silently remove unfavorable or unmapped observations.

## Promotion gates

A signal may move from **research** to **paper-qualified** only after all of the following are true:

- The definition and thresholds were frozen before the evaluation window.
- Mapping accuracy is 100% for accepted provider events; coverage is measured separately.
- Capture and close health meet their documented service targets throughout the test window.
- Results span at least two distinct CFB scheduling regimes and are not driven by one game date, team, or conference.
- Directional line CLV is positive with a game-clustered 95% confidence interval that excludes zero on a held-out or walk-forward sample.
- Realized ROI is reported after a conservative execution-cost/slippage sensitivity test. ROI supports the decision but does not replace the CLV gate.
- A second untouched evaluation window confirms the result before any real-money language or workflow is considered.

Sample-size thresholds should be set from a pre-registered power analysis once early variance is measurable. Until then the terminal must display **No edge claim** regardless of nominal ROI.

## Operational notes

The first deployment keeps the legacy `(sport, matchup_id, alert_type, side)` uniqueness constraint alongside the new dedupe-key index. Consequently V1 records the first breach per game/type/side. This avoids mixed-version deployment failures. Remove the legacy constraint only in a later migration after all writers use `dedupe_key`; raw captures retain every underlying market observation throughout the overlap.

Any model trained from this ledger must use time-ordered splits, fit transformations only on training periods, and preserve the untouched test period. Features must be values known at signal time; closing lines and final scores are labels and may never enter the feature set.

## Ideas checked and closed (2026-11-13)

Two candidates from a literature/web search were checked before registration and
closed. Neither is a registered study; record them so they are not redone.

**DraftKings price vs Pinnacle fair on the same spread/total line.** The
moneyline version already runs (`dk_value`, `pinnacle_divergence` in
`model/line_alerts.py`); spreads and totals had no equivalent. Rule tried:
first capture at least 2 hours before kickoff where DraftKings and Pinnacle
quote the same line and DraftKings' price beats Pinnacle's proportionally
de-vigged fair price by 3% or more. Checked on alert *rates* only (no
outcomes), all 685 CFB games from 2026-09-01 to 2026-11-13: DraftKings' price
was at least Pinnacle-fair in only 20 game-markets and cleared 3% in one.
DraftKings expresses disagreement by moving the line half a point or a point
(thousands of captures), not by pricing the same line generously, so a
same-line detector cannot reach a sample. The version that would fire, "DK's
line is a point better than Pinnacle's", is line shopping: its CLV against
Pinnacle is positive by construction, and whether the point beats the vig
needs thousands of settled bets. Treat it as an execution habit, not a study.

**Underdog implied-score bias (Arscott 2023).** Claim: when the spread and
total imply an underdog scores under ~14, the market misprices it because a
score cannot go below zero. Checked on 3,171 FBS-vs-FBS games, 2022-2025,
canonical closing lines from `cfb_historical_game_lines` and final scores.
Underdog actual minus implied (95% CI): implied under 10, +0.2 [-1.1, +1.6],
n=101; 10-14, +0.9 [-0.3, +2.0], n=226; 14-17.5, +0.7 [-0.2, +1.6]; every
bucket includes zero. Underdog cover 46.5-53.0% and over 46.4-53.0% by bucket,
none above the 52.4% needed at -110, and the low-implied buckets disagree by
season. No bias, no Stage B. Caveat: the historical lines cover FBS-vs-FBS
only; an effect concentrated in FBS-vs-FCS blowouts is untested.
