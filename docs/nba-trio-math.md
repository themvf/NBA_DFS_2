# Independent trio mathematics trial

This release adds a separate research trial and `/nba/trio-lab`. It does not promote a model or alter v1/v2/v3 forecasts, the three original experiments, the workload challenger, or the registered boom policy.

Registration revision 2 (`research/artifacts/nba_trio_math_protocol_v2.json`) supersedes the zero-enrollment revision 1 during the pre-release integrity review. Prediction parameters are unchanged; the revised code rejects malformed portfolio cardinality and leaves failed publication attempts unindexed. Revision 1 and its digest remain preserved. No revision has retrospective enrollment.

## Acceptance criteria

1. Every original is generated after registration and before the verified full-slate first tip. The new model must predate the original v3 forecast. November 3 has no NBA slate; earliest enrollment is November 4. Historical diagnostics are labeled retrospective and cannot receive an index or prospective publication.
2. All three selection methods use the identical top 12 frozen-market-line players in the original v3 modeled eligible pool. Five unique trios contain three distinct players each. Original point estimates, means, medians and market lines remain distinct quantities.
3. Primary endpoint: actual best entry among the five. Secondary endpoint: actual sum across all five. Compare methods on identical completely resolved selections. Selected DNP/missing players are not replaced or assigned zero. Do not report a partially resolved best as a completed comparison.
4. Source/forecast/model/protocol/code digests, actual observation times and exact game/player joins must verify. Historical fitting uses only dates through December 31, 2025; dependence through January 15, 2026; residual calibration January 16–31. Historical late-ingested replay is not prospective evidence.
5. Verify integer nonnegative scoring identities, uncertainty support, covariance PSD and deterministic selection. Changing a simulated draw count does not establish empirical accuracy.
6. Review after 30 completely paired prospective dates with date-clustered uncertainty, then a separately untouched 10-date confirmation. No automatic promotion or parameter changes. Evaluate opportunity/point errors and P90 losses separately from portfolio results.

## Fixed methods

`research/nba_trio_math.py` contains the frozen settings. A Normal-inverse-gamma change-point filter, hazard 0.05, models minutes, FGA/minute and FTA/minute from up to 20 appearances. Its posterior run mass is an internal model quantity, not a calibrated boom probability. Minutes include overtime; no invented participation mixture is supplied.

Compound counts use negative-binomial FGA/FTA, a beta-binomial three-point split, and independent beta-binomial two/three/free-throw conversion. Conversion priors have strength 20 and development-only league means. Negative-binomial shapes and prior moments are development-fitted. This is a coarse box-score model, without complete rotations, shot locations or possession budgets. It is not the full spatial model from the referenced basketball paper.

Three opportunity channels use separate Ledoit–Wolf correlations from January 1–15 residuals, ordered by away/home scoring rank (five per side). Fewer than 40 complete games produces an explicit independent fallback. Gaussian copulas couple opportunity channels across players; conversion draws remain independent. These are approximate historical role relationships, not verified current-player correlations.

The original v3 joint draws, a compound joint scenario and a compound independent scenario are kept separate. Market baseline selects the five highest frozen line sums. Joint best-of-five greedily maximizes expected maximum entry score under v3; robust best-of-five maximizes the minimum scenario improvement against that same baseline. Both retain the top market trio, use one deterministic local-swap pass and fall back to the baseline if their objective is worse. These are heuristics, not global-optimum guarantees or distributionally robust finite-sample certificates. Scenario/model choice is substantive uncertainty.

P90 is an independent empirical residual adjustment, not a point-mean change. Initial scores come from the untouched-with-respect-to-this-fit January 16–31 calibration segment. Once at least 100 earlier resolved trial scores exist, a fixed window of up to 30 prior graded slate dates replaces that segment; scoring-band groups below 100 use the pooled scores. Only grades actually observed before the next original source cutoff qualify. The quantile uses rank ceil((n+1)*0.9). NBA serial dependence and cohort shift preclude a claimed distribution-free current-season conditional guarantee. No 30+/40+ betting probabilities are published by this trial.

## Recording and grading

The existing successful v3 recording attempts `trio-math.json` after preserving v3 and its existing attachments. `trio-math-index` is separate. Publication appends only to `nba_trio_math_snapshots`, protected by the database first-tip clock and per-date/protocol uniqueness. Only successful publication gets an index; failed attempts preserve their file and receipt without entering prospective counts. Failure never destroys v3. Never create another original or attach a test after tip to fill missing coverage.

```
python -m research.nba_trio_math_runner grade --record ORIGINAL/trio-math.json --diagnostics NEW_SOURCE_REVIEW/diagnostics.json --output ROOT/trio-math-grades/NEW.json --apply
python -m research.nba_trio_math_runner report --root ROOT --output NEW_REPORT.json
```

`--apply` appends to the new math grade table only. Source diagnostics must match the exact original local source snapshot. Reports count only the latest grade per indexed original and fixed protocol/model. Keep incomplete dates and DNP/missing coverage explicit. Reconstructed diagnostics can be evaluated locally without `--apply`, never backfilled as prospective records.

The current v3 recording is local. Its runtime must be updated to this release before the next slate; the existing 11:35/9:00 automations retain their cadence. The main v2 cloud workflow remains unchanged and does not pretend to produce local v3/math originals.

## Postgame pool-gap audit

The diagnostic compares the best verified trio in the full original v3 modeled pool with the best verified trio in the exact frozen top-12 pool. Their difference is the pool gap. For each five-entry portfolio, subtract its best fully graded entry from the top-12 benchmark to get the selection gap. Both gaps add to its shortfall against the broader pool. The benchmark excludes players outside original v3 eligibility and never substitutes a player into a saved entry.

The audit also reports fractional top-three capture and whether the highest scoring entry includes all three with ties allowed, both against the broader frozen pool. Tied portfolio entries use their original saved order. Any unverified pool member, including a DNP, or unfinished slate makes the benchmarks provisional. Any unresolved selection leaves that portfolio's gaps and capture pending.

After publishing the existing math grade, run:

```
python -m research.nba_trio_pool_audit --record ORIGINAL/trio-math.json --forecast ORIGINAL/forecast.json --diagnostics REVIEW/diagnostics.json --math-grade MATH_GRADE.json --output NEW_POOL_AUDIT.json --apply
```

Use a fresh output filename. The audit must link to the published original math grade and exact forecast. Trio Lab displays the audit only when its grade is the latest for that record. Omit `--apply` for local retrospective checks. This descriptive audit is separate from trial selection and promotion.

## Existing website corrections

Top Scorers defaults to the current Eastern date, including an explicit empty state, instead of silently showing yesterday or tomorrow. Archived quotes remain labeled with their actual capture times. Saved boom comparisons are paired by the exact snapshot digest and recognized shot-growth payload; older ceiling-watch records stay separate. Both best-entry and combined-five actuals are shown, while the original combined-five metric remains intact.

## References

- Adams & MacKay, Bayesian Online Changepoint Detection: https://arxiv.org/abs/0710.3742
- Ho, joint basketball shot attempts and success: https://doi.org/10.6339/25-JDS1196
- Ledoit & Wolf, covariance shrinkage: https://www.ledoit.net/Well-conditioned2004.pdf
- Esfahani & Kuhn, distributionally robust optimization: https://arxiv.org/abs/1505.05116 (motivation; this implementation is a finite-scenario heuristic).
- Xu & Xie, sequential conformal inference: https://arxiv.org/abs/2212.03463 (motivation; this implementation does not claim its conditional guarantees).
