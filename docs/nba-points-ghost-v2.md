# NBA ghost v2: recent opportunities, separate ceilings, and a research watchlist

## Purpose and release boundary

Implement the October 24 plan as a new shadow trial. Original v1 files and live Top Scorers forecasts remain intact. V2 is not a promoted model and does not establish a betting edge. Code, the fitted model artifact and a GitHub workflow must be integrated and deployed before scheduled cloud collection is active. A local verification does not activate that workflow.

A separate local Codex follow-up, `track-nba-ghost-projections`, is active daily at 11:35 AM Eastern in this chat. It uses the installed managed-worktree implementation, grades prior slates, attempts a valid pre-first-tip freeze and produces cumulative research reports. It depends on the computer and app remaining available. The GitHub workflow is prepared as a deployment option; it is not active from this unpushed worktree. Once a canonical cloud schedule is deployed, retire the overlapping local collector rather than maintain duplicate routines.

Today is October 24, 2026. The common first tip was 6 PM Eastern. V2 was implemented afterward; today's v2 reconstruction is retrospective and excluded from the prospective trial. Do not backdate it or combine it with the original genuinely pregame v1 file.

## 2026-10-31: running in the cloud

The Codex session that ran the local follow-ups was turned off and its branch
was merged to main. The freeze and grade now run from `/api/cron/dispatch`
(jobs `nba-ghost-freeze` 15:37 UTC and `nba-ghost-grade` 11:37 UTC,
October-June) dispatching `nba_points_ghost.yml`; its own GitHub crons (16:50
and 12:37 UTC) are a fallback only. `nba_ghost_snapshots` and
`nba_ghost_grades` are on `/health` (72 h). The ghost code grades with
`research/nba_points_research_grade.py` (`nba-points-research-grade-v1`), a
copy of Codex's revised grader; the live nightly grade
`model/nba_points_calibration.py` stays `nba-points-grade-v1`, because the
registered ladder-v2 tail test reads it and reserves the v2 name. The local
Codex automations (`track-nba-ghost-projections`, `nba-morning-after-review`)
overlap the cloud job and should stay off; v3 and the daily HTML reviews were
local-only and are not scheduled.

## Acceptance criteria

1. Combine canonical NBA season logs with newer ESPN final boxes, using only prior dates and source records fetched before the ghost decision cutoff. Establish regular-season classification from an ESPN event's explicit `season.type=2`, with timestamped classification evidence. A calendar date alone is insufficient.
2. Retain both providers' IDs. Exact canonical names must map uniquely within each provider. Ambiguity, duplicate source identities and conflicting statistics block that player's history. Cross-provider duplicates use date plus home/away teams and player identity, never date alone or unrelated provider game IDs. Both box teams' player points must reconcile to final scores.
3. Preserve complete selected raw inputs and source times in a digest-protected snapshot. Freeze before the earliest scheduled game in the full slate; later/retrospective files cannot enter prospective reports. If a table has been refreshed after an old cutoff, exclude it conservatively rather than assume the overwritten revision existed before lock.
4. Expose mean forecasts, historical reference P90, experimental conditional P90, observed workload facts and experimental watch flags separately. Missing availability evidence remains unverified. Large prior workloads do not become automatic bonuses or invented rookie quantiles.
5. Grade all frozen candidates. Compare point errors on identical player sets; count pushes, exclude and report DNP/unmatched/pending outcomes. Grade each frozen ceiling independently and measure quantile loss as well as hit frequency. Grade watch flags against their frozen historical reference threshold.
6. Preserve one prospective snapshot per date and model version in isolated research storage. Never update it. Regrading appends a new attempt; reports use the latest attempt per original snapshot. Different fitted artifacts must not be pooled.

## New outputs

- `history20` and the original fixed 90/10 blend now include eligible recent boxes, rather than only old season logs.
- Opportunity forecasts: separate development-trained Ridge forecasts of minutes, FGA, 3PA and FTA. Minute means are bounded to 0-48; 3PA cannot exceed FGA. This is a historical statistical forecast, not verified lineup/rotation knowledge.
- `opportunity_points`: forecast attempts multiplied by historical conversion rates, shrunk by 20 attempts toward development-only league rates. The shrinkage and model regularization are fixed research choices.
- `historical_p90`: empirical 90th percentile of up to twenty prior regular-season appearances, minimum ten. It is the unchanged reference for watchlist grading.
- `experimental_p90`: a separate penalized linear 90th-quantile model, adjusted using January residual quantiles by the predefined scoring cohort. It is a candidate estimate, not a certified current-season quantile.
- `market_ceiling_approximation`: original market-ladder interpretation, kept separately because most alternate rungs are one-sided and fair-tail pricing is uncertain.
- `ceiling_watch`: a fixed logistic ranking score and January-frozen alert threshold for beating the historical reference P90. The probability field deliberately stays null. High-scorer threshold selection uses prior-ten-game average >=20, matching model development; the main trial error report separately retains the frozen-market-line >=20 cohort.
- `observed_workload_flags`: descriptive facts such as a previous game with at least 30 minutes and 18 attempts. A rookie can have this fact recorded while P90 and modeled watch status remain unresolved.
- Quality flags identify insufficient history, missing shot share, stale recent role and unvalidated opening-season transfer. Current injury status, minute restrictions and teammate replacements remain unresolved unless an optional timestamped availability record is supplied.

## Fitted artifact and empirical limits

`research/artifacts/nba_ghost_opportunity_v1.json` stores coefficients, imputation values, scaling, development-only league conversion rates, January-frozen thresholds, metadata, evaluation and a content digest. Inference uses plain JSON arithmetic, not executable pickle deserialization. Training/portable-inference agreement is checked to numerical tolerance.

Reproduce with a verified historical input JSON containing `players` and `teams`:

```powershell
python -m research.nba_ghost_model --input INPUTS.json --output NEW_MODEL.json
```

Fixed development through December 2025: 6,173 appearances; January calibration: 4,824; February-April exploratory evaluation: 10,146, including 1,016 higher-scoring appearances. This evaluation period was inspected previously. It is not a pristine holdout, and no opening-week or injury-aware performance is established.

For higher scorers, the exported P90 is exceeded about 9.8% of the time, but its quantile loss is 1.513 versus 1.492 for the simple historical reference. Calibration frequency alone does not demonstrate improved forecasts. The opportunity watch flag identifies 40 hits in 262 flagged appearances versus 69/754 others, but earlier equal-size comparisons did not establish incremental benefit over simpler history rankings. Keep all of these outputs experimental.

## Daily recording and grading

The new workflow reads existing player props, makes only free ESPN classification requests, and requests no Odds API credits. Capture attempts at 15:35 and 16:50 UTC provide a morning read and a fallback after the existing market-capture fallback. A run delayed beyond the slate's first scheduled tip skips prospective recording. The second capture cannot replace an original prospective snapshot. Morning grading at 11:35 UTC rechecks the preceding three slate dates after the normal box collector runs.

All operations default to local output/read-only source access. `--apply` creates/writes only the isolated `nba_ghost_snapshots` and `nba_ghost_grades` research tables. It does not run the application's global schema initialization, overwrite player markets, modify live forecasts, or rewrite v1 calibration. Persistence uses a partial unique index for one original prospective snapshot per date/version and append-only grade rows. No code path updates or deletes either table.

```powershell
# Read existing market history, verify schedule and prior-game classification, freeze locally.
python -m research.nba_ghost_runner freeze --date 2026-10-25 --output artifacts/nba-ghost/20261025.json

# The scheduled workflow adds --apply to persist the first prospective file.
python -m research.nba_ghost_runner freeze --date 2026-10-25 --output artifacts/nba-ghost/20261025.json --apply

# Append grades for stored prospective snapshots; no candidate reranking.
python -m research.nba_ghost_runner grade --days-back 3 --apply

# Cumulative report uses the latest grading attempt for each original snapshot.
python -m research.nba_ghost_runner report --since 2026-10-25 --output artifacts/nba-ghost/report.json
```

Reports fail rather than pool different model digests; use `--model-sha` to select an individual trial. Offline comparison supports `report --grades FILE1.json FILE2.json ...`. Existing local files use exclusive creation and cannot be overwritten. For a later local grading attempt choose a new output filename.

An offline diagnostic can use `freeze --market-snapshot MARKET.json --input-bundle INPUTS.json --model MODEL.json --output FILE.json --allow-retrospective`. The bundle includes source logs, final boxes, games, canonical team names and timestamped phase classifications. Its optional availability records require a source URL and pre-cutoff capture time. A snapshot created after lock stays retrospective even when all input rows predate lock.

## Prospective decision criteria

Review after at least 30 completed slate dates, while keeping model weights and alert rules fixed. This is a review checkpoint, not sufficient evidence by itself. Require paired improvement in primary-cohort point/quantile loss, date-clustered uncertainty supporting it, sensible conditional coverage, and confirmation in a subsequent untouched period before promotion. Compare watchlists at equal daily coverage and report absolute ceilings alongside relative-threshold hits. An observed 15-point personal ceiling is not interchangeable with a star's 35-point ceiling.

Availability-aware scenarios need another version with genuine pregame lineup/injury evidence and empirically supported opportunity allocation. V2 makes those gaps visible rather than assigning unsupported absence bonuses.

## Implementation verification

The installed targeted Python suite passed 44 tests, including v1 compatibility, duplicate/collision handling, source-classification gates, future-input poisoning, frozen thresholds, DNPs and report isolation. A full frozen-data integration matched recent-season boxes to all 78 players in the October 24 observed market pool; 74 had sufficient history for distribution estimates and none was identity-blocked. The resulting file is explicitly retrospective and is excluded from the trial.

The two isolated database research tables were created and verified using that retrospective file. The original snapshot survived a repeated insert unchanged, its JSON digest roundtripped through PostgreSQL, two grading attempts appended without replacing it, and the unique prospective-date/version index was verified. No live forecasts were modified. The original v1 snapshot digest was also verified unchanged. These are mechanical and source-path checks, not proof of predictive accuracy.

Optional `--availability FILE.json` accepts canonical-name keyed evidence with `captured_at`, `source_url`, `status`, and optional `player`/`team`. Evidence after the cutoff or for another identity/game fails closed. A verified Out status suppresses the watch flag; point forecasts remain conditional on an appearance and must not be treated as a prediction that the player will participate.
