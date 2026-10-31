# NBA opportunity reliability fix

Status: reviewed specification, revision 3, October 31, 2026. This document does not register a new experiment, change a forecast, activate collection or authorize deployment. Registration is a separate immutable, timestamped artifact after implementation verification and before enrollment. Companion: [tail reader and grading specification](nba-points-tail-validation-spec.md). The existing October 26 ladder registration governs that distinct test; this document does not rewrite it.

## Decision and intended result

Build a workload-conflict review alongside the existing trials, improve recovery of missing official availability evidence, and test a separate, faster-adapting opportunity forecast. The immediate result is a trustworthy indication that a forecast needs review. A demonstrated improvement in points accuracy is a separate experimental outcome.

Continue grading full-game points, including overtime. Preserve the original v1/v2/v3 snapshots, model artifacts, coefficients, selection policies, registered experiments and notification schedules. Do not turn an alert into a probability, injury bonus or automatic recommendation.

### Evidence motivating the work

- Barnes on October 30: 18.5 FGA per appearance across four current-season games; v2 projected 13.32, v3 projected 13.94; actual 34. The v2 snapshot already contained `recent_opportunity_increase`.
- Barnes on October 28: 19.0 current-season FGA average; v2 projected 12.42; actual 17. The conflict preceded the 41-point result.
- A descriptive audit of five original v2 slate dates found 24 appearances across 14 players with a projection at least three FGA below the current-season average, among 384 appearances with at least three current-season observations. Sixteen exceeded their FGA forecast; eight did not. Twenty-two already had the descriptive workload-increase flag. Mean FGA underprojection was 2.81, or 2.04 after excluding Barnes's October 30 game.
- Those audit rules were chosen after inspecting outcomes. These dates and thresholds are development evidence, not prospective validation. The current-season mean barely reduced FGA MAE in the selected group: 5.18 versus 5.32. That does not support replacing the model with a recent average.
- The October 30 official injury lookup failed. An NBA article published October 27 described Kawhi Leonard's expected week-long absence, but we first investigated that article after the game. It cannot be credited as an original pregame input.

Audit artifacts: `C:/Docs/_AI Python Projects/NBADFS_v2/artifacts/nba-barnes-review/20261031T153342Z/barnes-audit.json` and `systemic-workload-audit.json`. Proposed rules below differ from the descriptive three-FGA audit and must be replayed explicitly; do not reuse 24/384 as their performance.

## Scope and delivery order

1. **Reconcile existing grading context first:** reuse the already implemented same-cohort Brier references and short-minutes functions from commit `6d921d49`, after integration review; add only missing local-report contracts and context labels. Preserve original grades. Audit the already registered ladder evaluator and quote provenance before relying on its verdict. The companion documents exact existing code and integration blockers.
2. **Evidence and review:** reliable source-failure receipts, workload conflicts, timestamped official-news claims, local daily review and recording receipts. These operational improvements do not depend on proving a tail improvement.
3. **Separate research paths:** preserve the already registered one-sided ladder test (October 27 onward, 30 nights/1,000 paired 35+ rows) and independently register the point-only opportunity adaptation after its checks. The broader tail-reader design in the companion is a deferred future candidate, not a replacement registration or a second test automatically started on the same outcomes. Neither path may borrow the other's calibration claim. No new scoring probabilities or P90 values for the opportunity adaptation.
4. **Prospective evaluation:** use the fixed review boundaries below. Hold any new top-three probability or correlated-trio simulator until the separate marginal and joint-distribution gates in the companion specification pass. Comparing three fixed point estimates remains a descriptive selection audit and does not require a probability simulation.

No paid requests beyond the separately approved near-tip collector; no historical paid backfills. No website changes, deployment, extra chat schedule or continuous injury-monitoring claim. Any future monitoring expansion is separate work.

## A. Workload-conflict detector

### Eligible information

Use the exact frozen player identity and history of each original forecast. All historical games must precede the slate date and all source observations must precede the decision cutoff. Exclude DNPs; retain valid low-minute, zero-point and zero-make appearances. Never remove Barnes's 0-for-12 game.

For each pinned model separately, require a finite, nonnegative FGA forecast and at least three prior current-season appearances. Select the most recent five current-season appearances, or all available if three or four exist, ordered by scheduled game time then source game ID. This is the **recent window**. Use the official season and competition type (NBA regular season only for this trial), not an October string match. Record observation counts and exact source game IDs. De-duplicate canonical player/game IDs before window selection; conflicting duplicate values are an exclusion, not a choice of the favorable row. A missing FGA or invalid minutes value in the selected window makes the window incomplete; do not silently select an older replacement.

Do not claim current-team continuity from an old roster or the opposing-team pair in a prop. Record trades, verified team continuity and unresolved team identity separately. The numerical conflict may still be shown with unknown continuity; teammate-specific interpretation requires a verified team join.

### Proposed fixed rules, version 1

Let R be mean FGA per appearance in the recent window, and F the model's FGA forecast.

`lower_workload_conflict = (R - F >= 3.0) AND (R > 0) AND (F / R <= 0.85)`.

Separately tag `recent_workload_rising` when R exceeds the mean FGA of the latest 20 available regular-season appearances strictly preceding the recent window by at least 2.0. Use all available if 10–19 exist; fewer than ten means unknown. Use only the original frozen history: do not fetch extra history retrospectively to fill the window. Record season/team changes across those windows. Do not use an overlapping baseline or imply an injury caused the increase.

Every conflict receives review status; rising workload or already-recorded workload-growth evidence is an explanatory tag, not a different probability. Use no hidden cap that drops flagged players. Prioritize conflicts by R-F descending, then frozen points line descending, then canonical player key. Show the total count even if the compact view displays only five.

Also show recent FGA per minute and frozen FGA per minute, without changing the flag. A difference in expected minutes can create a per-game conflict even when shot intensity agrees. Label that as a possible explanation, not proof of a stale role assumption. Do not equate the existing `recent_opportunity_increase` flag with this new rule.

Store all evaluated players, including non-alerts and ineligible players, so coverage and false alarms can be audited. Distinguish `insufficient_history`, `missing_forecast`, `ambiguous_identity`, `no_conflict` and `review_required`. Keep v2 and v3 conflicts separate; agreement is descriptive.

### Local presentation

Show: player, game, model/version, expected points, forecast FGA/minutes, recent FGA/minutes, reference FGA, window sizes, gap, season/team continuity, availability evidence coverage and observation age. Use plain text such as: **Workload conflict: model 13.3 shots; recent 18.5 across four games. Review role assumptions.**

Do not display an invented confidence percentage or call the flag a P90 alert, scoring-leader prediction or betting edge. Missing availability creates an evidence-gap label; it does not make every unlisted player an injury alert. Include both favorable outcomes and failed alerts in the next-day review.

## B. Availability-source recovery

### Discovery and failure behavior

Keep the official NBA injury PDF as the preferred structured source. Preserve its raw bytes, printed-header validation, publication/observation times and existing fail-closed identity joins.

When discovery fails, write a durable failure receipt containing attempted official URLs, HTTP statuses or timeout categories, observation time, parser result and the next available recovery action. Do this even when no valid report is found. The orchestrator must test that a fresh availability output exists before supplying `--availability`; a missing output must not cause a second avoidable file-not-found failure.

Use bounded recovery: official season index; the existing four recent quarter-hour candidates; up to six earlier hourly candidates within the same day, newest first. Maximum 12 report-discovery HTTP requests per run, including the index and any retry. Never probe future publication times. Older successful reports remain explicitly older; they are not silently described as current or complete.

If structured evidence still fails or a flagged player's relevant team has an unsubmitted report, inspect a bounded secondary queue of official NBA news and official team releases: at most eight candidate articles per run, prioritized by conflict rank and missing team coverage. This is an additional bounded free-source budget, not part of the sportsbook credit cap. Search snippets alone do not create evidence; retrieve and retain the original article. Source discovery may be manual initially. Remaining teams stay unresolved if the budget or source access is exhausted.

### Typed news claims; no silent status conversion

Save news in a separate archive, with source URL, raw content hash, title, exact supporting passage, published time where supplied, actual observed time, player ID, reported team, applicable dates, explicit duration, extraction version and verification status. Never invent a timezone, publication timestamp, return date or end of an absence. Unknown timing remains unknown.

An article reporting a week-long absence is `reported_unavailable_interval`, not a fabricated row in the official PDF. Store bounds separately: "at least a week" gives a minimum duration, not an end or return date. Such an open-ended claim remains historical review evidence after that minimum interval and cannot establish ongoing unavailability without a fresh explicit source. A questionable designation remains a status claim, not a scoring discount. No entry means unknown. A dated report of an expected return is not confirmed availability.

Carry a time-bounded claim into a later **review sidecar** only while its explicit interval applies, team identity matches and no later eligible source supersedes it. Keep source disagreements visible. Omission from a later report does not resolve a contradiction or prove availability. Conflicting explicit claims require review.

News fallback initially provides corroborating review evidence only. It does not change eligibility or availability fields in existing v2/v3 trial files. Converting typed claims into an eligibility policy requires a separately versioned policy and prospective record; no teammate usage adjustment follows automatically.

An article first downloaded after tip remains retrospective, even if published earlier. Likewise, evidence captured after the morning freeze can support a separately timestamped pre-lock review, but cannot be added to the morning snapshot or earlier quote joins.

## C. Separate opportunity-adaptation challenger

Proposed name: `nba-role-opportunity-challenger-v1`. This is a point-only research candidate; it is not v3 promotion and must not inherit v3's probabilities.

Use v3's original deterministic opportunity forecast as the base. Use exactly the detector's three-to-five-game recent window, valid nonnegative attempts in each appearance, positive aggregate minutes and aggregate FGA, and verified same-team continuity between those appearances and the decision-time player team. Individual zero-FGA/zero-FTA/zero-point appearances remain in the window. An appearance marked played with positive attempts and zero minutes is inconsistent and excluded with a data-error receipt; do not replace it with an older game. If continuity cannot be verified, keep the conflict warning but skip the challenger explicitly. A player excluded by the original model for insufficient history remains excluded from this challenger; rookie coverage is a separate unsolved issue.

Current historical team fields do not satisfy that decision-time team requirement by themselves. Phase B therefore needs a timestamped, read-only official NBA/team roster or official current-game roster observation joined by player ID. Define and verify the source during implementation; if it is unavailable, report the eligibility gap rather than manufacturing a current roster. Roster membership establishes team identity, not participation, a starting role or projected minutes. Phase A remains usable while this dependency is unresolved.

Let M0, A0, T0 and L0 denote original expected minutes, FGA, 3PA and FTA. Let observed totals over the recent window be M, A, T and L.

The proposed fixed 50/50 adaptation is:

```
minutes_new = M0
FGA_new = M0 * (0.5 * A0/M0 + 0.5 * A/M)
FTA_new = M0 * (0.5 * L0/M0 + 0.5 * L/M)
three_share_new = 0.5 * T0/A0 + 0.5 * T/A
3PA_new = FGA_new * three_share_new
points_new = 2*(FGA_new-3PA_new)*two_rate0
             + 3*3PA_new*three_rate0 + FTA_new*free_rate0
```

Use the original frozen conversion rates. Require M0, M, A0 and A positive; validate finite values, nonnegative attempts, conversion rates in [0,1], 0 <= three share <= 1 and 3PA <= FGA in both source and candidate. Invalid inputs create an explicit skip, not a silent imputation. Existing minute bounds remain intact. First verify that the original stored deterministic points equal the original attempts-times-conversion calculation within 0.01 point; otherwise skip `base_arithmetic_mismatch`, because an unexplained baseline difference would contaminate the comparison.

Apply the same formula to all eligible players, not just successful alerts or players whose outcomes we like. The 50/50 weight is a deliberately simple research assumption chosen for registration, not a fitted optimum. No grid search over the already-inspected dates and no nightly retuning. A different weight requires a new version and new prospective enrollment.

This addresses shot/FT intensity and attempt mix, not minutes, rotation allocation, opponent assignments or causal injury effects. It would not imply forecasting Barnes's 34 shots. Show the revised point estimate alongside its original, with experimental status. Do not shift an old residual distribution and publish new P90 or 30+/40+ odds; those outputs remain unavailable until a separately fitted and evaluated distribution extension exists.

## D. Recording, identity and provenance

Create a new protocol and registry under `artifacts/nba-role-reliability/`; do not edit `nba_points_experiments_v1.json` or reuse its registered digest. Pin this specification, thresholds, weight, original model hash, recent-window definition, evidence policy and grading rules before first enrollment.

Each exclusive per-slate directory retains `source-snapshot.json` or its verified immutable reference, `conflicts.json`, `availability-recovery.json`, `role-evidence.json`, `challenger.json`, a recording receipt and an original per-date/protocol index. Store raw news evidence separately with content hashes. Separate failures in collection, detector evaluation, challenger eligibility and prospective registration.

Use the verified full-slate first scheduled tip as the common contest cutoff, including games without quoted players. This is an outer limit, not permission to use evidence acquired after a forecast. The challenger inherits the original v3 information cutoff: all predictive inputs, including team-identity evidence, must have been observed by the original v3 freeze timestamp. Refresh necessary roster evidence before the original recording job. Later recovery may appear in a timestamped review sidecar but cannot decide eligibility or inputs for this paired model comparison. Protocol registration must predate the original v3 freeze, and the new challenger recording must finish before first tip. Recheck the earliest observed scheduled/market start before atomic index creation. Later-game updates cannot enter the common-lock forecast. No replacement, append-to-original, backdating or duplicate originals. A reconstruction made after first tip is retrospective even if it uses old files.

Attach the new sidecars **after** successful existing v3 recording, without changing the v3 digest or its three existing experiment attachments. Failure of this addition must not destroy or relabel a valid original v3 recording. Integrate into the existing pregame and morning jobs only after implementation verification; do not add a duplicate notification schedule.

## E. Evaluation and release gates

### Operational acceptance

- Every eligible original player receives an evaluated conflict result or an explicit exclusion; every alert is traceable to frozen raw inputs.
- Source failure creates a reviewable receipt, and the recorder safely continues with unverified evidence when recovery fails.
- Official news with valid identity, observation time and interval appears in the review; late, ambiguous or contradictory claims cannot silently become pregame status.
- A verified replay of Barnes's old files reproduces the proposed rule outcome and the arithmetic. Label it retrospective and exclude it from new prospective counts.
- Original v1/v2/v3 hashes, pinned models, prior experiment protocol and forecasts remain unchanged. No new paid-data request or deployment occurs.

### Forecast and alert evaluation

Primary diagnostic endpoint: FGA MAE change, candidate minus original v3, on identical resolved appearances that are both challenger-eligible and flagged by the original v3 conflict rule. The separate v2 flag never changes this primary cohort. Secondary diagnostic endpoints: FGA-per-minute MAE (positive actual minutes only), FTA/3PA errors and forecast bias. A warning is not a successful prediction merely because the actual score was high.

Primary decision endpoint: point MAE change on the identical resolved conflict cohort. Also report all challenger-eligible players and the frozen market-line >=20 subgroup. Compare with original v3, v2 blend and market readings in the original v3 source snapshot; do not join a later quote or the separate original v2 trial to improve coverage. The recent-rate baseline uses M0*A/M FGA, M0*T/M 3PA and M0*L/M FTA with the same original conversion rates. Every comparison has its own explicit paired denominator. The points line is not an FGA forecast; do not compare it with shot counts.

Report alert prevalence, repeated players, season/team transitions, review coverage, failed alerts, and all opportunities where forecasts were reasonable despite the warning. Descriptive alert outcome: actual FGA > original forecast FGA, with ties separate; this is not a P90 or points hit. Do not add an undefined equal-budget hit-list test to this protocol; the existing registered opportunity-alert experiment retains its own rules. Preserve DNP/missing selections and never replace them or assign zero. Low-minute played appearances remain in the primary endpoint, with the companion minutes strata shown separately.

Freeze the candidate's and original v3's three highest expected scorers within the identical challenger-eligible pool, breaking forecast ties by canonical player ID. Fewer than three eligible players means no trio. Grade their actual total, fractional tied-boundary capture and gap to the best verified trio within that same frozen pool. Keep that pool distinct from the full original v3 slate and market-only pool; unresolved benchmarks remain provisional. Trio outcomes are secondary, not permission to select the best-performing endpoint after inspection or to claim a top-three probability.

First formal review occurs once, at the end of the first prospective date reaching both 30 evaluation-ready slate dates and 100 resolved primary conflict appearances. Evaluation-ready means all scheduled games are final, source collection and grading attempts are complete, and at least 95% of eligible original appearances are resolved as played or explicit DNP. Unmatched/ambiguous rows remain unresolved and count against this coverage threshold; report fully resolved dates separately. Include resolved rows from partial dates in descriptive totals, with coverage shown. Do not stop a record solely because an outcome is unfavorable. At 90 calendar days without the threshold, report insufficient evidence rather than relaxing it.

Date readiness controls review timing, not score inclusion: the formal endpoints include every resolved, originally eligible appearance enrolled through the review date, including partial dates. Do not drop a date's resolved losses because other players are unresolved. Report unresolved/DNP counts for both the primary conflict and all-eligible cohorts.

At the formal review, use 10,000 paired date-cluster bootstrap resamples (seed 20261031) and percentile 95% intervals; resample all eligible appearances of each selected date together and compute player-appearance-weighted mean error changes. A resample with no primary-cohort appearances is undefined: record its count and draw again to reach 10,000 defined replicates, rather than scoring it zero. Preserve within-date dependence; disclose that this does not fully handle serial dependence of repeated players across dates. Repeat with canonical players as clusters (seed 20261032); if either primary improvement or all-player noninferiority fails its stated interval gate in this sensitivity analysis, require further research instead of advancement. Fewer than five represented dates means no interval even descriptively. Daily updates are descriptive monitoring, not repeated significance tests.

Proposed advancement requirements: paired FGA-error and point-error reductions on the primary conflict cohort, with each date-clustered 95% interval below zero; and no material deterioration in all-eligible-player point error, with the upper bound of its paired error-change interval below +0.25 points MAE. Freeze that practical deterioration margin before enrollment. These are joint gates, not a choice among favorable endpoints, and are not a power guarantee. An inconclusive/failed review does not authorize repeated nightly retesting until it passes; any further inferential review requires a new prospectively specified observation horizon.

Only an advancing candidate enters the untouched confirmation period: the first subsequent date reaching ten evaluation-ready slate dates AND 50 new resolved primary conflict appearances (maximum 60 calendar days, otherwise insufficient evidence). Require the same direction of both primary improvements, point-error upper 95% bound below zero, all-player noninferiority upper bound below +0.25 and no material provenance/coverage deterioration. Then seek explicit review before any promotion. Record advancement and confirmation dates; never promote automatically.

Do not evaluate new P90/probability accuracy for a point-only candidate. Continue reporting original v3 probability scores separately, with no borrowed calibration claim.

## Implementation map and meaningful checks

Proposed modules: `research/nba_points_workload_conflicts.py`, `research/nba_points_injury_news.py`, `research/nba_points_role_challenger.py` and a separate protocol artifact/registry. Extend the existing injury collector with failure receipts and bounded discovery; retain its strict PDF parser. Extend recording orchestration and local review only through isolated sidecars. Existing daily reports may link to the new review without pretending their v2 charts contain it.

Required fixtures/integration checks:

1. A Barnes replay and a failed-alert replay such as Castle; reproduce inputs and flags, preserve zero-point appearances and show both outcomes.
2. Insufficient current-season history, traded/unknown team identity, duplicate player IDs, missing minutes, zero denominators and missing conversion rates produce the specified skip/review states.
3. Index 403 and recent PDFs 404, followed by a valid earlier report or official news claim; all sources preserve their actual freshness and provenance.
4. An expired interval, later explicit contradiction, ambiguous article date, unsubmitted team report and omitted player cannot imply Active or silently resolve eligibility.
5. A pre-tip publication first observed after tip, a future historical row, a later quote and an after-cutoff registration are excluded from prospective use.
6. Run the full existing record -> new sidecars -> next-day grade path with originals protected. Verify scorer identities, exact point arithmetic, cohort pairing, DNP handling, artifact digests and restart/duplicate-index behavior.

## Out of scope

No invented injury multipliers, manual Barnes bonus, parameter changes to the current registered tests, retroactive credit for public news discovered today, or claim that these changes would have predicted 41. Better minutes/rotation forecasts, rookie models and calibrated joint scoring distributions remain separate workstreams.

## Revision 2 review findings

The review removed eight implementation ambiguities: a variable reference window; accidental exclusion of zero-shot appearances; undefined conflict cohort and recent-rate comparator; different trio pools; later evidence leaking into a morning comparison; open-ended news treated as a known interval end; repeated significance checks without a stopping rule; and a confirmation stage with no acceptance criteria. It also separates the ladder-reader investigation from the point-only adaptation. The companion document records the verified calibration evidence, version/provenance defects and the limits of the supplied six-point assessment. Neither document claims implementation or prospective registration is complete.

Revision 3 corrects the discovery of existing work: commits `6d921d49` and `65a4bc3c` already contain the ladder registration, baseline/minutes panel helpers, opt-in Python/TypeScript power readers and evaluator. Those implementations are distinct from this opportunity challenger. Reuse rather than duplicate them, preserve their original test rules, and address integration/provenance defects without silently substituting the later proposed trial.
