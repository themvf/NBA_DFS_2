# NFL DFS study findings — 2026-09-03

Official study: `8bab909112d93a5d0f0c1bcfa2674262d7116aebe04d58402cfd7d5e717d033e`

Report: `artifacts/nfl_dfs_research_36cbc63d06d706a9/8bab909112d93a5d/report.json`

## What was run

- 18,581 scored player-week evaluations across 2023–2025, with 753 explicitly excluded observations (insufficient history or game mapping).
- Warm-up and peer history from 34,919 skill-player weeks spanning 2020–2025, plus 1,632 DST weeks spanning 2023–2025.
- Train 2023, select regularization 2024, then retrospective diagnostic 2025. 2025 was previously inspected and is not an untouched holdout.
- Full input histories, features, per-row forecasts, results, alpha trials and metrics saved locally and in the research tables.

## 2025 diagnostic: average absolute error in DK points

Lower is better. These are reconstructed, market-free forecasts on recorded-stat cohorts, not historical DK slate profitability.

| Position | N | Simple recency mean | Historical model | Opportunity candidate | Candidate improvement vs model |
|---|---:|---:|---:|---:|---:|
| QB | 636 | 7.023 | 7.055 | 6.954 | 1.42% |
| RB | 1,507 | 4.726 | 4.733 | 4.664 | 1.47% |
| WR | 2,414 | 4.545 | 4.564 | 4.466 | 2.13% |
| TE | 1,241 | 3.694 | 3.692 | 3.650 | 1.15% |
| DST | 544 | 4.370 | 4.384 | 4.264 | 2.75% |

The historical model does not consistently beat a simple recency average. Complexity alone is not evidence of improvement. The opportunity candidates improve on both in this diagnostic, but the gains are small in absolute terms.

All five opportunity candidates passed the prespecified **shadow-only** gate: lower 2024 validation MAE, at least 1% lower 2025 MAE, a game-clustered 2025 confidence interval below zero, and no worse 2025 boom Brier score. TE's validation confidence interval still crosses zero; the gate required a negative validation point estimate, not significance in both seasons. Results remain exploratory across multiple candidates.

DST's opportunity feature is constant zero: its candidate is a historical calibration correction, not an assertion that offensive usage drives defense scoring.

Closing-total candidates remain research-only despite some apparent improvement. Without availability timestamps, their features cannot establish an executable pregame state. No paid historical odds calls were made.

## Forward use

The pinned `artifacts/nfl_dfs_shadow_config.json` activates only the five opportunity candidates in the independent 2026 shadow ledger. Each freeze records a baseline, candidate, uncertainty, input/history digests and a strictly pregame timestamp. Outcomes and evaluation reports are separate append-only records.

Production `ourProj` and optimizer defaults are unchanged. The daily workflow extension will run after it is pushed/deployed; a local/manual execution can freeze the same ledger now.

Initial forward execution froze 645 player-week baseline/candidate pairs (89 QB, 147 RB, 245 WR, 132 TE, 32 DST), with zero post-kickoff observations. The full history table contains 36,551 rows and the research sample/prediction ledger contains 43,631 rows. No Week 1 outcomes exist yet. Repeated freezes remain separate observations and evaluations count each player-week once.

The next real validation is forward 2026. No forward results exist before those games finish. Historical salary/slate, actual ownership, missing-player/DNP evidence and official DST contest-score reconciliation remain limitations. Nothing here establishes a profitable betting or DFS edge.
