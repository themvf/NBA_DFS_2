# Fixed prospective NBA projection experiments

These are three additional tests alongside the unchanged v2/v3 models. The registered protocol is `research/artifacts/nba_points_experiments_v1.json`, including its registration timestamp, model digest and fixed January-only calibration residuals. Its digest is `324bd657f5918f9f1772cc2d38faea2f1f523872a9ed64a02bbf7f2494c8748f`.

## Exact questions and rules

The primary population is the original frozen quoted/model-eligible pool with market points line **at least 20**. Secondary shooting and minute-stability groups are exploratory. All outcomes are full-game points, including overtime, conditional on appearance. DNP/missing outcomes never become zero or replacement selections.

1. **Disagreement:** freeze v3 opportunity points minus the same-time original market line. Fixed bands: <= -4, (-4,-2], (-2,2), [2,4), and >=4. Report paired market/v3 MAE and its difference, actual-minus-line, over/under/push counts, direction accuracy and denominators. The primary endpoint is paired point-error improvement within the disagreement bands; overs alone do not demonstrate an edge or profitability.
2. **Opportunity breakout:** flag projected FGA at least two above prior-ten-game mean OR projected minutes at least two above that mean. Rank by FGA increase, minutes increase, then canonical player key; maximum five alerts. Compare with exactly K players from the SAME high-line eligible pool ranked by frozen market line, prior-ten-game points and v3 opportunity points. The target is actual points strictly above each player's frozen historical P90. Also count realized FGA growth. Preserve every original selection. For direct hit-rate comparisons, rank-pair the lists and exclude a slot from both denominators if either player is missing/DNP; report overlap and coverage. This is an equal-budget selection-policy comparison, not an identical-selected-player comparison.
3. **Ceiling calibration:** freeze a separate conditional-tail challenger using only the existing model's January calibration residuals. Group by predicted scoring band (<10, 10–<20, >=20), predicted 3PA/FGA >=.4 versus lower, and predicted minutes >=30 versus lower. Groups below 100 observations fall back to their scoring band. No coefficients or original point forecasts change. Compare its P90 loss with the original v3 P90 on identical high-line players, and retain historical P90/v2 P90 references on the same complete cohort. Audit exact 30+/40+ probabilities with fixed bins [0,.1,.25,.5,.75,1], Brier scores, hits and denominators. Historical three-point share >=.4 and minute SD >=6 define separate exploratory strata; they are not additional fitted adjustments.

Conditional-tail probabilities remain experimental; a larger historical residual pool is not evidence of current-season calibration. The January season was already inspected, so only new registered prospective outcomes provide untouched evidence for these new hypotheses.

## Enrollment and grading

`research.nba_points_v3 record` now attaches `experiment.json` automatically after a successful prospective v3 recording, if the registered protocol exists. The forecast must have been created AFTER protocol registration and the experiment must finish BEFORE the full slate's verified first tip. The original v3 file is never changed. A separate `experiments-index/` preserves the first experiment per date/protocol. Failed test registration is explicit in the recording receipt and does not destroy a valid v3 forecast.

Never freeze new tests in the morning after the game. Reconstructions require `--allow-retrospective`; they have no prospective index and cannot enter cumulative results.

After the existing v3 postgame source review and v3 grade:

```text
python -m research.nba_points_experiments grade --experiment ORIGINAL_RECORD_DIR/experiment.json --v3-grade NEW_V3_GRADE_DIR/grade.json --diagnostics NEW_V3_SOURCE_REVIEW_DIR/diagnostics.json --output-dir NEW_EXPERIMENT_GRADE_DIR
python -m research.nba_points_experiments report --research-root RESEARCH_ROOT --output-dir NEW_REPORT_DIR
```

Use `C:\Python313\python.exe` from `C:\Users\joshb\.codex\worktrees\nba-points-evidence-fixes\NBADFS_v2`. The research root is `C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\nba-ghost-v3`. Every output directory is new/exclusive. The report validates original experiment indexes and model/protocol digests, takes only the latest grade per original, rejects duplicate originals for a date, and excludes unindexed reconstructions. No eligible originals means an explicit pending report, not sample results.

## Review standard

Daily checks are descriptive monitoring, not repeated significance tests or permission to retune. The formal review checkpoint is **30 completed prospective slate dates and 100 resolved opportunity alerts**. Report date-clustered bootstrap intervals (fixed 2,000 draws and seed), uncertainty, unresolved coverage, and all three registered primary endpoints rather than selecting whichever worked. Fewer than five represented dates produces a pending interval. Reaching the checkpoint does not authorize automatic model promotion, new bets, parameter changes or causal injury claims. New hypotheses or settings need a new version and fresh registration.

The existing **11:35 AM Eastern** follow-up records the fixed tests with v3; the existing **9 AM Eastern** morning review grades them and reports cumulative progress. No extra duplicate daily notification or paid capture is needed. Charts must use actual original experiment/grade/report files, retain pending states and denominators, and follow the existing daily visualization contract.
