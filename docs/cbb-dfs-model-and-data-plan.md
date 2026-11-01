# College Basketball DFS: implementation and model plan

Scope: DraftKings men's college basketball **Classic and Showdown Captain**, added November 1, 2026. Player fantasy points are the requested score outcome. Team game-score forecasts are a distinct future task.

## Working version

- `/cbb` is the sport landing page; `/cbb/dfs` is the workspace. Sport navigation recognizes CBB routes independently of NBA and CFB. No CFB files or database tables are reused or changed.
- Salary CSV import preserves DraftKings IDs, team, opponent, game, scheduled Eastern tipoff, eligibility, salary, historical average, and file status. A salary CSV cannot distinguish CBB from all other basketball competitions; the uploader must confirm the sport. No live status verification is implied.
- Classic: G/G/G/F/F/F/UTIL/UTIL, $50,000, at least two games and teams. Showdown: CPT plus five UTIL, one game and both teams, no repeated athlete; uses the actual Captain ID and salary from the paired salary row. Captain projection and salary are 1.5 times utility. Full sport-specific official rules were not retrievable through web access during implementation; compare against the actual contest rules and a real salary file before entering contests. Fixtures verify mechanics, not a real contest upload.
- Projections can be imported explicitly or calculated from a mapped box-score CSV. Projection imports replace the current pool of estimates. Unknown projections remain unknown and are excluded from builds. DK AvgPointsPerGame is displayed separately and never substituted for a forecast.
- Sources and timestamps are mandatory. AsOf/CapturedAt/PlayedAt must precede slate lock and cannot be later than the decision time. Box-score capture must follow game start. Imports must represent finalized box scores; the importer cannot independently verify final-game status. Post-lock captures are rejected for pre-lock evidence. Downloadable snapshots retain original salary text, resulting estimates, sources, timestamps, exclusions, and lineups. Keep original imported history files alongside snapshots; raw box scores are not retained in browser storage.
- Current inputs persist in this browser only. No database migration, live scheduler, shared slate history, late swap, exposure control, or contest entry submission is included. CSV export contains roster columns, not an existing entries-file update. Builds block after first tipoff.
- Integer optimization maximizes supplied expected fantasy points, capped at 20 lineups. Each additional lineup differs by at least one athlete; Captain swaps alone do not count as player diversity. Ownership does not affect this objective. This does not measure contest win probability, duplication, or expected profit.

## Sources and acquisition gaps

| Input | Candidate / evidence | Current state |
|---|---|---|
| Slate IDs, eligibility, salary, scheduled tipoff | User's actual DraftKings salary export | Implemented CSV reader; synthetic fixtures tested |
| Player box scores and recorded minutes | [CBBD games API](https://api.collegebasketballdata.com/api/games), `/games/players` | Documentation reviewed; no `CBBD_API_KEY` configured, no live request or ingestion implemented |
| Season coverage | [CBBD availability](https://api.collegebasketballdata.com/data-availability) | Provider documents player statistics from 2003 onward with early-year minute gaps; requested seasons/teams still need field-level audits |
| Canonical teams and current rosters | CBBD `/teams`, `/teams/roster` | Candidate, not integrated or identity-verified |
| Pace and opponent strength | CBBD team box scores and adjusted ratings | Candidate, not integrated |
| Availability and expected rotations | Official team releases, game notes, confirmed lineups; provider cross-check | Not integrated; salary-file status is insufficient |
| Actual ownership | Complete DraftKings final standings for specific contests | No CBB corpus imported or evaluated |
| Player points/rebounds/assists lines | The Odds API `basketball_ncaab`, selected event and US books | Connected at `/cbb/props` and inside DFS; source times and missing coverage visible. Separate from fantasy projections; see [player-props contract](cbb-player-props.md) |
| Possible contest archive | Existing repository's `ingest/statapi_contest.py` and configured `STAT_API_KEY` | NFL integration exists. CBB access, contest coverage, entitlement, and completeness have **not** been verified. Do not assume free previews or top-lineup samples represent the field |

CBBD uses ending-year seasons: 2026–27 is `season=2027`. Documented `/games/players` responses are capped at 1,000 games, so a future collector must partition by date, detect saturated windows, deduplicate by source game/player identity, retain raw responses and capture timestamps, and verify minutes/stat completeness. A successful request alone would not establish a usable history corpus.

Never join basketball names alone or treat matching team abbreviations as canonical identity evidence. Maintain a versioned DK-player/source-player mapping with team, season, effective dates, review method, and unresolved conflicts. The current CSV path deliberately requires an explicit DK ID and matching team. Transfers and Captain/utility rows need explicit treatment.

## Fantasy-point model

Scoring implemented: points + 0.5 × made threes + 1.25 × rebounds + 1.5 × assists + 2 × steals + 2 × blocks − 0.5 × turnovers. At most one double-double bonus of 1.5 and one triple-double bonus of 3, using points/rebounds/assists/steals/blocks. A triple-double receives both bonuses. Do not apply bonuses to averaged stat lines or apply them twice to imported projections.

### Transparent starting baseline (implemented, not validated)

For the current team and ending-year season, use up to the last 20 played games before the decision time. Base FPPM = sum of base DK points / sum of minutes. Expected minutes = average of the latest five played games, capped at 40. Projection = expected minutes × base FPPM + observed average bonus per game. These window sizes are manually chosen starting assumptions, not fitted parameters.

DNP rows do not estimate rotation minutes. Historical overtime minutes may contribute to rates; the forecast does not model future overtime. No current-season played games leaves the forecast unresolved. The baseline has no sample-size shrinkage, transfer adjustment, freshmen prior, pace/opponent adjustment, team-level 200-minute constraint, injury redistribution, foul-trouble, blowout, or recent-role model. Small samples and old appearances remain material limitations. The historical bonus average does not estimate how bonus probability changes when projected minutes change. This baseline should not be represented as production-ready DFS advice.

### Production candidate

1. Model availability and conditional minutes separately. Build the entire team rotation, not just the players priced on DK. Reconcile expected regulation minutes to 200 per team. Preserve uncertain lineup/availability scenarios and distinguish assumptions from confirmations.
2. Estimate each stat's production per possession or minute using partial pooling across player/team/league, recency, opponent, pace, home/neutral venue, projected role, and teammate availability. Handle newcomers and transfers explicitly; previous-team minutes do not transfer automatically.
3. Generate joint stat outcomes conditional on minutes and game context, preserving relevant correlations and rare stat tails. Calculate exact DK scoring and double/triple-double bonuses per outcome. Do not independently scale all components by team implied points: that would confound scoring with rebounds, assists, and defensive stats.
4. Expected fantasy points are the mean of those scored outcomes. Only show percentiles or event probabilities after distributional evaluation. Captain multiplies each outcome by 1.5, not its probability.

## Ownership model

Ownership is **contest entry selection share**, not fantasy-point rank, optimal-lineup frequency, or probability of success. The first version accepts explicitly sourced ownership imports and does not create ownership numbers automatically.

Build one training row per eligible player/contest using complete finalized standings and frozen pre-lock slate inputs. Include zero-rostered eligible players only when complete field coverage proves zero. Preserve buy-in, field size, max entries, format, date, contest ID, number of entries captured, field coverage, and DK identity mapping. Deduplicate entry IDs, including tied finishes. Never train on winners alone, a handful of users, or a preview of standings.

Features available before lock: projection and value ranks within the slate, salary and salary percentile, expected minutes, starting/availability status, role-change flags, eligible positions and roster scarcity, team game context, slate size, available alternatives, public consensus projections if licensed/captured, and contest characteristics. Train separate Classic and Showdown models, or use explicit format effects with separate validation.

Recommended first fitted model: regularized entry-weighted binomial regression for marginal selection share with player/contest features, followed by a constrained lineup-choice layer that respects roster feasibility and salary. Compare against simple uniform and salary/value baselines. Fit coefficients and any field-sampling temperature using training data, never by hand to match the latest missed slate. A normalization to a fixed ownership sum alone does not establish legal lineup feasibility or predictive validity.

Showdown requires **two separate targets**: Captain share and utility share. Overall roster share is their sum and cannot exceed 100% per athlete. For complete pools, Classic ownership sums to 800%; Showdown Captain sums to 100%, utility to 500%, overall to 600%. If an external source provides overall ownership instead of utility ownership, label/convert it explicitly before treating it as utility share. The current import's `Ownership` column is Classic share in Classic, utility share in Showdown; `CaptainOwnership` is Captain share. The UI displays inputs but does not treat them as calibrated or silently normalize them.

Ownership is contest-specific. Evaluate smaller-field, single-entry, and large-field multi-entry contests separately; a single global percentage is not established by the current evidence.

## Acceptance and validation gates

The implemented import/builder acceptance checks cover scoring with stacked bonuses, timezone conversion, malformed/mismatched salary formats, paired Captain IDs/salaries/statuses, exclusions, unknown versus zero ownership, source/time boundaries, duplicate historical games, same-team identity, and no cross-season history fallback. Small-fixture exhaustive enumeration independently checks both optimization objectives. These are mechanical checks, not evidence of prediction quality.

Before automatic projection use:

- Audit identity match rates, missing stat/minutes fields, capture times, final-game status, availability evidence, and complete team rotations for every slate. Freeze inputs and model version before lock.
- Use chronological train/validation/test splits by slate. Recreate each historical decision using only information available then; a database populated after the event cannot prove pregame availability of an injury/rotation feature.
- Compare minutes and fantasy-point MAE, RMSE, and bias to DK historical averages, last-five-game averages, and the implemented minutes-rate baseline on the **same eligible player rows**. Publish coverage, sample counts, and errors for starters/bench, salary bands, freshmen/transfers, availability uncertainty, early/late season, and formats. Do not exclude difficult unmatched rows without reporting coverage loss.
- For distributions, evaluate interval coverage, calibration, and tail outcomes. Separate simulation count from empirical evidence.

Before automatic ownership use:

- Verify complete entry coverage and the roster-share mass checks above; retain missing/unresolved rows rather than inventing zeroes.
- Hold out whole later slates/contests. Evaluate percentage-point MAE, entry-weighted log loss/deviance, bias, calibration buckets, chalk ordering, and performance by contest type/format, with uncertainty and sample counts. Compare all models on identical coverage.
- Show exploratory labels until stable out-of-time improvement over the simple baselines is demonstrated. ROI or reliable leverage claims require a separate contest-aware evaluation including fees, payouts, duplication, and exposure; ownership error alone cannot establish profitable lineups.

## Verification commands

From `web/`:

```text
node --import tsx scripts/test-cbb-dfs.ts
npx tsc --noEmit --incremental false
```

No deployment or database changes are required for the current import-based version. Production ingestion, availability reconciliation, a frozen historical corpus, trained ownership, empirical projection validation, and real-slate/entries-file verification remain open work.
