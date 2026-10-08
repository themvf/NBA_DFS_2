# Longest-touchdown fixes — October 7, 2026

Status: fixed locally; experimental research only. No push or production integration.

## Completed

- Require finalized schedule, quarter, terminal-period evidence and matching source coverage before grading.
- Count actual tie credit per scorer before combining OTHER buckets.
- Do not assign targets or TD chances to passes without identified targets.
- Use regulation-only training and grading, including regulation scores from games that later went to overtime.
- Use a conservative downs/timeout kneel budget and fixed second-half receiver.
- Apply unexpected-scorer reserve to the independent comparison baseline too.
- Verify frozen baseline game, time, request and source.
- Save reproducible comparison registrations, per-game reports, skips and paired summaries.

## Evidence

41 relevant tests passed. Fresh read-only capture: 880 games, 4,114 verified regulation TDs; 778 OT plays excluded. Original Thursday forecast file is unchanged, and its comparison baseline matches its frozen request/source. Thursday outcome correctly remains unknown.

Final-code comparison selected four development games, with 150 simulations per model/game. Three games were graded in each reserve variant; BAL–CIN remains needs_review because of an ambiguous touchdown. Probability accounting and implementation/source digests passed. This small check is not evidence of calibration or superiority over the baseline.

Files: `verification.json`, `final-code-comparison.json`, `final-code-comparison.registration.json`, `pbp-input-v2-reviewed.json.gz`, `thursday-outcome-status.json`. The earlier `development-comparison.json` predates the scoring-scope cleanup; its research implementation is archived in `research-before-scope-cleanup.py`.

## Remaining limits

No validated betting edges. No full overtime or return/defensive scoring forecast. Timeout use before the last two minutes is not modeled. Unattributed passes may be true throwaways or missing participant coverage; these are not automatically interchangeable. The residual scorer pool is not a learned named-player replacement forecast. QB changes, workload uncertainty and air-yard/YAC features need separate studies. The previous 224-game reported results are not independently reproduced here.

Relevant tests passed; the full repository suite and production deployment have not been run in this checkout with unrelated work in progress.
