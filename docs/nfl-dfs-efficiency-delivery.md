# NFL Model Lab — conditional efficiency slice

Implemented locally through 2026-09-05. This completes the current audited-field implementation of Slice C. It is research-only: `ourProj`, optimizer inputs, lineup generation, and production defaults are unchanged.

## Visible result

NFL DFS → **Model Lab** → **03 / Efficiency**.

- Mean/P10/median/P90 and boom rate from 1,000 deterministic draws per eligible offensive player and DST.
- An exact DraftKings scoring bridge with passing, rushing, receiving, turnover, rare-play, and yardage-bonus contributions that reconcile to the saved mean.
- A team-integrity panel showing maximum opportunity, completion, passing/receiving-yard, and passing/receiving-TD mismatch across every saved draw. The required result is zero.
- Conditional-rate cards showing the modeled rate, player history, position prior, games, and effective opportunities.
- A saved expanding-window table comparing each rate candidate with unshrunk player recency, including sample size and `actual − projected` bias.
- Team/player selectors, run/dataset hashes, and prominent limitations.

## Model contract

Workload and efficiency are separate. The saved workload supplies pass attempts, carries, and targets. Efficiency estimates completion/catch rates, yards per applicable opportunity, touchdown rates, and interceptions from chronological player history shrunk toward a position prior. A zero denominator is undefined and uses the prior; it is never treated as zero efficiency. Touchdown and interception rates receive stronger pooling than common conversion rates.

Each offense now uses one shared team draw. Team attempts, carries, and targets are allocated to known players plus an explicit unresolved-role bucket. Quarterback completions, passing yards, and passing touchdowns exactly equal the corresponding receiving outcomes across known receivers plus that bucket. Every player draw is scored by the existing exact DraftKings scorer, so threshold bonuses come from simulated outcomes rather than mean yardage. Contribution totals are asserted against the scorer on every draw.

DST remains a separate process. It resamples exact whole-game scoring components from the defense's recent games, the upcoming opponent's historical DST-points-allowed outcomes, and a league prior. This preserves the nonlinear points-allowed bands and negative outcomes. The opponent adjustment is intentionally simple and auditable; possessions, pressure rate, offensive-line availability, and quarterback status are not yet explicit features.

## Retrospective evidence

The 2024–2025 expanding-window audit contains 33,430 paired outcomes: 32,342 offensive rate outcomes plus 1,088 DST outcomes. Seven of nine offensive candidates reduce MAE versus unshrunk player recency. Rushing-TD and receiving-TD rates are slightly worse and remain visible as failed comparisons. The DST candidate reduces MAE from 4.353 to 4.159 DK points on the previously inspected sample.

| Outcome | n | Candidate MAE | Recency MAE |
|---|---:|---:|---:|
| Completions | 1,158 | 2.111 | 2.227 |
| Passing yards | 1,136 | 34.122 | 35.545 |
| Passing TDs | 1,136 | 0.814 | 0.845 |
| Interceptions | 1,158 | 0.623 | 0.640 |
| Rushing yards | 3,724 | 11.738 | 12.323 |
| Rushing TDs | 3,724 | 0.315 | 0.305 |
| Receptions | 7,242 | 0.737 | 0.751 |
| Receiving yards | 6,532 | 12.418 | 12.866 |
| Receiving TDs | 6,532 | 0.309 | 0.305 |
| DST DK points | 1,088 | 4.159 | 4.353 |

This uses previously inspected seasons and oracle realized denominators. It validates conditional conversion only; it is not a full fantasy-projection, contest, or returns backtest and cannot authorize promotion.

## Saved forward evidence

The latest Week 1 artifact is v3 run `c9b03eb167323530157e938fa2759b5278d4753a03bd7d781435414b8309b46b` on dataset `d93767dda89cf83b227018b7f774a48f30db60f50978a60837587a8565fb7bea`, tied to workload run `b1323a61dee55fefb688ad3b0931f27d5b9faad4a27d47bbba9b48aaa78cf6e1`. It contains 451 forecasts: 419 offensive players and 32 DSTs. Digest replay and implementation hashes match, score-component error is below floating-point tolerance, all team reconciliation checks are zero, and all 451 appear as pending `Workload + efficiency research` forecasts in Weekly Player Review. Both workload allocation and efficiency history enforce an explicit target-week cutoff; tests prove adding a target-week outcome cannot change that target's forecast.

The known roster-role limitation remains: the current active roster is not a verified timestamped depth chart, so stale players and multiple quarterbacks can receive work. QB receiving and unsupported non-position workloads remain absent. These are visible limitations, not silently imputed inputs. Stage D can now begin candidate-specific distribution calibration, while role/injury evidence remains a parallel prerequisite for any future promotion.
