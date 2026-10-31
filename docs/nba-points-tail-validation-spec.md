# NBA points tail reader and grading context

Status: reviewed design and registration draft, October 31, 2026. No new reader, database grade, experiment registration or deployment is activated by this document. Implementation must pass the checks below before a timestamped protocol and code/input digests can be registered. Companion: [opportunity reliability specification](nba-points-opportunity-reliability-spec.md).

## What the review verified

Read-only audit: `C:/Docs/_AI Python Projects/NBADFS_v2/artifacts/nba-barnes-review/spec-audit-20261031T162535Z/`. Preserve `calibration-rows.json` and `summary.json` as the audit snapshot; later database updates must not silently change this evidence.

The stored table currently has only `nba-points-grade-v1` with `nba-points-ladder-v1` for October 20–30: 1,109 rows, 1,098 graded appearances, six explicit DNPs and five unmatched rows across 11 dates. This is a market-calibration cohort, not the original ghost v1/v2/v3 prospective trial. The worktree's Python grader and web query default to grade-v2 with a v1 fallback, but that code version is not the version currently stored. Report the actual stored version and date range.

| Outcome | Matched appearances | Hits | Mean stated chance | Observed rate | Reader Brier | Same-cohort fitted constant Brier |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 25+ | 835 | 114 | 18.68% | 13.65% | 0.09380 | 0.11789 |
| 30+ | 551 | 51 | 13.68% | 9.26% | 0.07679 | 0.08399 |
| 35+ | 372 | 23 | 9.18% | 6.18% | 0.05309 | 0.05801 |
| 40+ | 219 | 10 | 6.47% | 4.57% | 0.04197 | 0.04358 |

Average tail probabilities are high in the pooled record, but not every threshold is high every night. The reader beats the fitted constant Brier at each pooled threshold. That does not establish out-of-sample calibration, meaningful improvement at 40+ with ten hits, or accurate rankings within stars. A model can retain useful discrimination while being miscalibrated.

The same record has 540/1,098 main-line overs (49.18%), 4.99 points MAE, and 51/459 scores below an available floor (11.11%; no exact-floor ties here). Those are observations, not a demonstrated irreducible noise floor. Players in the same game and across repeated games are not independent coin flips. Main-line over rate alone neither validates tail probabilities nor diagnoses a model flaw.

The supplied Zubac example is present: October 25, two points, seven minutes, 14.5 line. Barnes has zero points in 28 minutes that date. Keegan Murray and Ron Holland have unmatched rows that date. Their absence from this box-score join does not establish why they are absent.

## Confirmed implementation issues and hypotheses

1. `model/nba_points_ladder.py` divides an over-only rung's implied probability by the same book's main-line overround. It also supports under-only rungs and two-sided pairs. Transferring main-line margin to a different rung is an assumption. It is a plausible source of error, not a proven cause of all tail or floor misses.
2. `model/soccer_first_scorer.py::power_devig_exclusive` solves sum(q_i^k)=1 for mutually exclusive outcomes. NBA 25+, 30+, 35+, 40+ are nested events. Never pass those rungs, or multiple players' scoring thresholds, into that normalization together. The published power method uses a complete outcome set; applying a main-line exponent to a one-sided alternate is a separate hypothesis. Background: [Clarke, Kovalchik and Ingram (2017)](https://www.sciencepg.com/article/10.11648/j.ajss.20170506.12).
3. `load_pretip_captures` selects latest main and alternate captures separately, then returns alternate books without capture time/source row ID. The saved calibration row's `captured_at` and `minutes_before_tip` describe the main capture, not necessarily the alternate curve. New comparisons need both; do not label existing curves as synchronized or verified closing prices.
4. The existing database upsert key is `(grade_version, event_id, player_key)`. Merely changing `LADDER_VERSION` can overwrite an existing grade version. A future persisted reader requires a distinct grade version too. The current web summary filters grade version but does not independently enforce ladder version; the new report must fail closed on mixed reader/protocol digests.
5. Existing summary code excludes `push` rows from every probability endpoint, even when a threshold outcome is known. Preserve those historical semantics and grades. A new evaluation version must distinguish main-line push exclusion from known `points >= N` outcomes; never silently change the old denominator.
6. The reader can add a book's main quote and an alternate at the same point twice, inflating `books` and monotonic-fit weights. Its integer-line over/under pair also lacks the push mass needed for an unconditional exceedance curve. The new trial specifies one book contribution per point and half-point quotes only. Keep these structural corrections common to both trial arms so they cannot be mistaken for the effect of power de-vig.

## First delivery: reference scores and observed minutes

These are local reporting improvements, not model changes. Implement first in a separately versioned summary (`nba-ladder-context-v1`) reading existing grades. Do not regrade, overwrite raw probabilities, change eligibility, alter scheduled jobs or deploy the live panel as part of this specification. A later UI integration uses the same tested summary contract and requires its normal release workflow.

### A. Brier reference contract

For each `(grade_version, ladder_version, threshold, time-policy)` separately, use exactly the rows with a valid probability and resolved outcome for that endpoint. For the legacy report, retain its original `graded`-only convention and show the count of known push rows omitted. Compute:

```
y_i = 1(actual_points >= N)       # main over uses actual_points > line
p_bar = mean(p_i)
q_hat = mean(y_i)
brier = mean((p_i-y_i)^2)
fitted_constant_brier = q_hat*(1-q_hat)
descriptive_skill = 1 - brier/fitted_constant_brier
```

Label the reference **same-sample constant**, not a pregame forecast or out-of-sample baseline. It is a useful descriptive yardstick fitted to evaluation outcomes. When its Brier is zero, skill is null with `constant_has_zero_loss`; never infinity or a divide-by-zero. An empty cell has null rates/scores and n=0, not zero error. Preserve numerator, denominator, dates, probability coverage and version.

For prospective validation, additionally freeze a threshold-specific constant using only previously resolved outcomes actually available before that date's recording. Use `(prior_hits+1)/(prior_n+2)` with a minimum 100 prior eligible appearances per threshold; otherwise the reference is unavailable. Save the training-row IDs/digests, outcome observation times, last training date and frozen value. Do not substitute a full-sample rate, leave-one-out future outcomes, or an unstamped historical reconstruction. Prior data must satisfy the trial's quote/cohort policy. Until this reference has eligible history, report its coverage gap; do not claim it has been beaten.

To ask whether the ladder adds information beyond scoring level, show separate fixed main-line bands `<15`, `[15,20)`, `[20,25)`, `>=25` with counts and forecast/observed rates. A later conditional constant can use the same prior-only rule per threshold/band, with min100 and pooled prior fallback explicitly recorded; it is secondary, not a posthoc replacement primary. Beating a pooled constant does not establish skill within stars, and failing to beat it does not prove there is no information in the forecasts. Reliability diagrams and paired proper scores answer different questions; include both.

Reliability bins are fixed `[0,.05), [.05,.10), [.10,.20), [.20,.35), [.35,.50), [.50,.75), [.75,1]`, showing mean forecast, observed rate, hits and n; empty bins remain empty. Do not infer calibration from a low Brier caused by rare outcomes. Date-clustered uncertainty is descriptive until the fixed review point.

### B. Minutes and unresolved-outcome contract

Keep grading status (`graded`, `push`, `dnp`, `unmatched`, `ambiguous`, `no_box_score`) separate from observed participation context:

- `played_under_15`: confirmed played, valid actual minutes in [0,15).
- `played_15_plus`: confirmed played, minutes >=15.
- `played_minutes_unknown`: played and points verified, minutes missing/invalid. Invalid values also carry a data-quality flag.
- `not_applicable`: no verified appearance, including DNP and unresolved rows.

Zero displayed minutes can be rounding of a brief appearance; do not infer DNP from minutes alone. Keep valid zero points and low-minute appearances in every applicable full-game primary outcome. Show all-played results first, then the three strata with identical metric-specific denominator rules; their counts must reconcile. For the audited legacy record this gives 40 under-15 and 1,058 at-least-15 graded appearances, zero unknown.

Call this **low-minute appearance**, not automatically an early injury exit. Injury, ejection, foul trouble, blowout, normal bench role and rotation changes require their own cited evidence; unknown reason stays unknown. A causal exit label must include source and observation time. Bookmaker settlement rules can differ by book, product and promotion; this research report measures actual full-game points, not universal settlement or betting P&L.

Unmatched exclusion is the safe grading behavior, not proof the ingestion/join is correct. Audit expected versus observed rosters, unique player identity, aliases and event/team joins. Reclassify as a verified scratch only with explicit suitable evidence, retaining its timestamp. Outcome corrections create a new local grade attempt and preserve the previous receipt. Never replace a selected player or assign a missing player zero. Postgame status evidence does not become a pregame input.

## Separate tail-reader protocol: registration draft

Proposed protocol: `nba-ladder-tail-comparison-v1`; candidate reader: `nba-points-ladder-v2`; new evaluation semantics: `nba-ladder-tail-grade-v1`. None of these identifiers is activated by writing this document. If a future production calibration row is written, use a new unique grade version (proposed `nba-points-grade-v3`, after checking it remains unused); never write new-reader results into grade-v1/v2. Keep all ghost model and existing experiment identifiers unchanged.

### Enrollment and raw input policy

One exclusive original per slate/protocol; all recordings finish before the verified full-slate earliest scheduled/market start, including games with no quotes. The information cutoff is the recording's actual source snapshot time, not permission to use data until first tip afterward. Use existing archived quotes only, never paid backfills. Do not change the approved near-tip collector's markets, frequency or credit cap.

For each player/event/book, preserve main and alternate source row IDs, capture keys, captured/observed times, provider update times where supplied, book prices, raw payload hashes and exact cutoff evidence. Main and alternate captures must each be <=120 minutes old at recording and no more than five minutes apart. Both must have been observed before recording and before the earlier scheduled/market start; provider times, when supplied, must not be future. Unknown provider timestamps remain explicitly unknown rather than fabricated. These freshness limits are fixed design assumptions, not tuned cutoffs.

Identity requires a unique canonical player within the correctly matched event/teams. Missing or conflicting identity/time/source data causes explicit exclusion. Do not carry forward a removed player's quote from an older event snapshot. Record coverage of all offered players, books, rungs and full-slate games, including those excluded by freshness or identity. A new prospective record must retain its actual reader output before games; computing it after games is retrospective even with old quotes.

The first-tip cohort and a last-before-individual-tip market audit are distinct time policies. The latter may contain information unavailable at contest lock and cannot fill a missing first-tip prediction. The new test uses first-tip originals only. Existing market calibration is retrospective replay evidence unless an independently verifiable pregame output recording exists.

### Shared structural preparation

Apply the same preparation to both arms, independent of outcomes:

1. Use valid half-integer point lines only. A half-point over/under pair exhausts the integer scoring outcomes; an integer pair does not, because a push is possible. Report excluded integer quotes, do not estimate push probability.
2. At each `(player,event,book,point)`, prefer a valid two-sided alternate pair, otherwise a two-sided main quote at that point, otherwise a one-sided alternate. Duplicate conflicting quotes within the same selected source are ambiguous and excluded. Never give a book two votes at one point.
3. Paired prices must be from the same book, player, point and source observation. Require valid American prices and finite raw probabilities strictly between zero and one; require pair booksum >=1. For a one-sided rung, require that book's valid two-sided half-point main pair. Missing/underround anchor excludes the rung in both arms. Do not invent the opposite price or silently fallback only for the candidate.
4. Aggregate median per point, book-count-weighted monotonic projection, interpolation and no extrapolation as in the frozen existing reader, including its minimum-three-point and ladder-book requirements. Pin exact source code and numerical tolerances. Preserve pre/post monotonic-fit values for diagnostics. Missing thresholds remain missing.

The control is the current proportional-margin arithmetic on these common prepared inputs: two-sided q_over/(q_over+q_under), one-sided over q_over/main_booksum, under-only 1-q_under/main_booksum. Retain an additional exact legacy-reader replay on unmodified archived inputs, labeled separately; differences due to preparation must not be credited to power de-vig.

### Candidate formula

For each valid two-sided quote, solve `q_over**k + q_under**k = 1`. If booksum=1, k=1; otherwise solve for k>1 by deterministic bisection, doubling the upper bracket from 2 up to 128 as needed, then 80 iterations with residual <=1e-10. If no bracket/residual failure occurs, exclude that unit from both paired arms and report it. Do not reuse the soccer helper's unchecked k<=12 boundary.

Use `q_over**k` for a two-sided rung. For a one-sided rung, use the same book's **main-pair k**: `q_over**k_main` for over, or `1-q_under**k_main` for under. This exponent transfer is the registered assumption under test, not an identified fair probability from a one-sided quote. Keep the existing final numerical clamp identical in both arms. Do not fit exponents, weights or tail residuals against the already inspected games. Leave consensus point lines and original main-line over probabilities unchanged; this candidate affects only the alternate distribution.

Do not normalize nested thresholds to sum to one. Require `P(25+) >= P(30+) >= P(35+) >= P(40+)`, all in [0,1], after monotonic projection. This is a consistency test, not calibration evidence. A corrected marginal curve is not a complete distribution beyond its observed support.

### Endpoints and fixed review rule

Target: full-game points including overtime, conditional on a verified appearance, including low-minute and zero-point appearances. For each threshold N, the event is `actual_points >= N`; no push exclusion is inherited from a different main line. DNP and unresolved outcomes stay excluded and reported, never zero.

Primary cohort: original eligible player-appearances with finite control and candidate probabilities at **all four** of 25+,30+,35+,40+. Freeze membership at recording, before outcomes. Preserve missing outcomes and coverage counts. Do not redefine the tail cohort by who subsequently scored 25/35/40.

Primary score: each appearance's mean Brier across the four thresholds, then the mean across appearances. Primary contrast: candidate minus control, paired on identical appearances. This weights thresholds equally without pretending their nested outcomes are independent observations. Report each threshold's paired Brier and calibration separately, both on the complete primary cohort and on its own maximum common-support secondary cohort. The 20+ endpoint, floor/ceiling behavior, low-minute split and probability bins are secondary; none can replace a failed primary score.

One formal review at the end of the first date reaching **30 evaluation-ready prospective slate dates, 500 resolved primary player-appearances and 50 unique primary appearances with actual points >=35**. These are minimum information requirements, not a power guarantee. Evaluation-ready: all games final, collection/grading attempts complete, >=95% of original eligible appearances resolved as played or explicit DNP. Show fully resolved dates separately. Stop at 90 calendar days for an insufficient-evidence review if the minimums are not reached. Do not choose a favorable date or reduce thresholds. Event-count stopping is outcome-dependent; describe bootstrap intervals as decision aids, not a fixed-horizon significance guarantee.

Date readiness controls review timing, not score inclusion: score every resolved eligible original through the review date, including resolved rows from partial dates. Never omit resolved losses because other players on their date remain unresolved. Report counts and missingness for the primary and secondary cohorts.

Use 10,000 paired date-cluster bootstrap resamples, seed 20261031, and percentile 95% intervals for primary error differences; resample all appearances/thresholds of each date together and calculate appearance-weighted averages. Draw again if a resample has no applicable primary/reference cohort, logging the undefined count rather than assigning zero. Repeat with canonical players as clusters (seed 20261032) because dates alone do not capture serial player dependence. A failed advancement gate under this sensitivity analysis blocks advancement. No repeated daily significance claims. Candidate advancement requires all of:

- Upper 95% interval of primary candidate-minus-control Brier <0.
- Upper 95% interval of paired candidate-minus-prior-only-constant Brier <0 on the reference-covered primary cohort, with at least 90% primary-cohort reference coverage; no same-sample constant used for this gate.
- No material per-threshold deterioration: the upper 95% error-change bound <+0.005 Brier at each of 25/30/35/40. This fixed practical margin and joint gate cannot be selected after results.
- Coverage/provenance checks pass; conflicting date/player sensitivity conclusions or unresolved systemic source errors block advancement. Report effect sizes, not only gate outcomes.

After advancement, confirm unchanged on the first subsequent date with **15 evaluation-ready slate dates, 250 resolved primary appearances and 25 unique 35+ appearances**, maximum 60 calendar days. Apply the same paired score gates and coverage rules to this untouched period. A failure/inconclusive result does not authorize daily retesting until it passes. Any further inferential horizon must be prospectively specified. Explicit human review is required before use in live forecasts; no automatic promotion or tuning.

### Registration and artifacts

Store new exclusive directories under `artifacts/nba-ladder-tail/`: protocol, code/fixture hashes, source policy, raw quote references/copies, raw per-book transformations, original control/candidate probabilities, frozen baseline values/training evidence, cohort membership, receipt/index and later grade attempts. Index identity includes slate date, protocol digest, reader digest and timing policy. A restart never replaces an original; latest verified grade counts once per original. Keep reconstructions and all old inspected dates out of prospective counts.

Before enrollment, write and hash a machine-readable protocol encoding every rule above, verify implementation reproduces it, and save its actual registration timestamp. A markdown draft, backfilled file time or version string alone is not pre-registration. Any change to formula, thresholds, freshness, grade semantics or eligibility requires a new version and fresh prospective enrollment. Immutable old trial artifacts remain available.

## Gates before any new top-three probability simulation

Point-only trio rankings in the companion specification can be audited now. A new probabilistic trio system needs more than the tail-reader Brier gates:

1. Demonstrate marginal calibration for the relevant high-scoring eligible population, not only pooled low-probability cells. Preserve probability uncertainty, missing-player coverage and discrete ties.
2. Define a normalized score distribution including unsupported lower/upper tails. The current reader does not extrapolate beyond quoted rungs; do not silently fill missing mass or truncate maxima to manufacture draws. Freeze a separate support/completion policy and validate it prospectively.
3. Define the contest pool and target: top-three membership, highest trio total and joint selection success are different outcomes. Freeze tie credit and participation handling; conditional-on-appearing probabilities are not unconditional contest probabilities.
4. Independent draws may be used as an explicitly independent research baseline after those checks, not as validated top-three odds. Fit any dependence model only on prior data, with teammate/opponent, minutes, overtime and availability context. Raw box-score correlations conflate role and schedule effects; they are not automatically residual score correlations.
5. Register the new joint model and compare top-three outcomes against frozen ranking/independent baselines on future full eligible slates, including negative results and missing coverage. Passing marginal tests alone cannot authorize a joint probability claim.

No new simulator is implemented or promoted in this work. The point-only workload trial and operational injury recovery need not wait for a marginal-distribution study; they simply cannot publish tail or top-three odds.

## Implementation map and acceptance checks

Proposed local summary module: `research/nba_ladder_context.py`, reading saved calibration rows and emitting JSON/Markdown (later chart input). Proposed isolated reader/recorder/grader: `research/nba_ladder_tail.py` plus an immutable protocol. No changes to the live Python/TypeScript reader or `LADDER_VERSION` during the shadow test. Future production adoption requires Python/TypeScript fixture parity, separate grade-version migration and an explicit release.

Meaningful checks before claiming these reporting additions work:

1. Hand-calculated Brier/reference examples, all-zero/all-one outcomes, empty cells and different threshold coverage. Reconcile per-threshold rows rather than using all main-line players.
2. Changing a future outcome cannot change an earlier frozen baseline. Missing prior observation timestamps prevent prospective baseline use.
3. Zubac seven-minute, Barnes zero-point/28-minute, exact-15, rounded-zero-minute played, unknown-minute, DNP, unmatched and ambiguous fixtures. Minutes strata reconcile without changing original grades.
4. Legacy push semantics preserved in legacy summaries; new threshold grading includes known outcomes independently of main-line push, with new evaluation version.
5. Real archived main/alternate source timestamps survive raw -> reader -> saved original -> grade -> report. Stale, mismatched, post-record and post-tip sources are excluded; a main-only T-minus-one capture cannot make an older ladder look one minute old.
6. Paired symmetric and asymmetric odds, fair booksum, underround, one-sided missing anchor, under-only complement, duplicate point/book, integer lines and root-bracketing failure. Validate shared control preparation and exact legacy replay separately.
7. Finite probabilities, monotone survival, no extrapolation and identical paired cohorts. Do not count four thresholds as four independent player-games.
8. Mixed reader/grade/protocol versions fail closed; atomic recording and digest checks withstand duplicate invocation. Later grade corrections do not replace originals or get counted twice.
9. Reconcile generated report rows/scores to the audit JSON, and preserve original ghost v1/v2/v3 and registered experiment hashes. No paid captures, original database upserts, schedule changes or deployment.

## Disposition of the six supplied suggestions

Accept pre-registration, reference scores, low-minute context and deferring probability simulation. Correct the proposed baseline to identical per-threshold cohorts and distinguish same-sample from prospective constants. Do not assert all nights agree, five-point MAE is an irreducible floor, unmatched means inactive, or fewer than 15 minutes proves an injury exit. The verified current record supports a controlled margin-method experiment; it does not establish that power adjustment is already the right fix.
