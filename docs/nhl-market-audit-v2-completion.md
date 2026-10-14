# NHL audit v2 implementation evidence

Completed October 12, 2026 (Eastern), on `codex/nhl-market-audit-v2`.

## Delivered

- NHL execution-price and probability grading now use one verified frozen close, validate canonical identity and timing, and never substitute an earlier selected-book quote. Missing pairs remain null.
- New alert entries include immutable trigger/book/decision timestamps. NHL price notifications recheck freshness; stale observations remain available for research. The paper-entry button validates freshness again at click time and blocks after scheduled start.
- Persistence follows the original trigger history ID. The scorecard distinguishes price CLV from probability CLV, priced/missing settlement counts, unique games, repeated/opposing alerts, freshness populations, steam interval groups, and game-cluster bootstrap ranges.
- A forward protocol starts at `2026-10-13T02:09:58Z`, retains unchanged thresholds, evaluates the first recorded alert per family/game, preserves stale/missing observations, and compares fresh verified entries with the same-book favorite at the same trigger capture. It requires 100 settled independent games and 30 dates before manual review; it never automatically promotes a signal. Goalie and injury inputs remain explicitly unavailable.
- The hourly NHL workflow emits a read-only forward report and retains it for 90 days. It avoids repeatedly rescanning development grades. This workflow change requires merge to main to become scheduled production behavior.

## Live database repair

`python -m research.nhl_market_audit --repair --output artifacts/nhl-audit-v2-repair-20261012.json`

The dry run preceded mutation. The repair processed 69 begun-game alerts (67 settled, two with close grades only), preserved all 74 original entries and all 136 prior grade records, and appended 69 corrected grades. Original settled outcomes and the +6.3305-unit paper ledger reconciled. A second pass did not append duplicate settled grades. Five future-game alerts were left untouched.

Backups and evidence remain in `artifacts/nhl-audit-v2-repair-20261012-before.json`, `artifacts/nhl-audit-v2-repair-20261012.json`, `artifacts/nhl-audit-v2-live-scorecard.json`, and `artifacts/nhl-audit-v2-forward-final.json`. No original entry, captured odds row, frozen close, or prior grade was rewritten. Only prior grade `is_current` flags changed as the new grades were appended.

The live scorecard reconciled 74 observations across 51 games and 67 settled alerts across 46 games. Decision-time timing was fresh for 39 observations and stale for 35. The pooled paper ROI was +9.45%, with a descriptive game-cluster 95% range of -14.74% to +34.28%. This is not proof of profitability. Thirty-three settled alerts qualified for the fresh-quote paper population.

## Verification

- Adversarial NHL tests cover exact/missing/wrong-book closes, wrong game/history identities, invalid/future timestamps, stale/missing quote timing, entry verification, original-trigger persistence, live versus final settlement, unchanged entry/prior-grade fields, duplicate-current-grade detection, protocol drift, first-alert selection, and matched benchmark calculations.
- The read-only live TypeScript scorecard verifier reconciled counts, source links and unit totals with the database.
- `/nhl?date=2026-10-12` rendered HTTP 200 with correct priced denominators, both CLV columns, steam subsets and uncertainty ranges, and no audit-feed error.
- All 144 runnable web test scripts passed; the existing live-writing pick'em integration test remains explicitly excluded by the repository's runner. TypeScript, changed-file ESLint (zero errors) and the production webpack build passed.
- The initial full Python suite passed. After integration with the latest main, the full suite passed every NHL test and reported one unrelated baseline failure: `tests/test_nfl_model_completion_contract.py::test_shadow_registration_matches_frozen_implementation`. Untouched `origin/main` has the same mismatch for `web/src/app/dfs/nfl/nfl-optimizer.ts` against `docs/nfl-model-completion-candidates.json`. This repair does not change that optimizer or re-register the NFL shadow study. The two existing NFL data-dependent skips remain explicit.

## Release and remaining evidence

The live database grades are repaired. Code/UI/workflow changes are delivered as one PR and have not been merged or deployed. The unrelated NFL registration failure is disclosed rather than weakening its frozen-study check. Future-game evidence is required to assess predictive accuracy; the registered forward study is collecting, with no automatic betting recommendations enabled.
