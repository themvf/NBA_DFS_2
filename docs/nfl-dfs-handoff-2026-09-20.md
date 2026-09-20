# NFL DFS Lab follow-up — 2026-09-20

Review baseline: production commit `5a8623b348fd7ab341f271d80a5b773451bb793a`.
The application fixes below were implemented on September 20. Production
activation and the replacement study pin are recorded in the completion section.
The local checkout
was older than production, so the review used `git show 5a8623b:<path>`.

## Confirmed production defect: QB transfer logic

The Python availability path selects the shallowest available same-position
teammate even when the absent player is a backup. It scales the recipient by
`absent_attempts / recipient_attempts`, which can reduce a healthy starter's
projection. It also lacks the two-game history guard added to the web path.

Verified in production snapshot `86f143f0-7177-5108-87f4-d9a510ced0bc`
(2026 week 2, as-of 2026-09-20 14:34:32 UTC):

| Recipient | Before | After | Absent donor | Donor historical games |
| --- | ---: | ---: | --- | ---: |
| Lamar Jackson | 21.847 | 14.838 | Skylar Thompson | 2 |
| Matthew Stafford | 22.264 | 16.311 | Matthew Caldwell | 0 |
| C.J. Stroud | 16.205 | 13.730 | Graham Mertz | 0 |

Independent synthetic reproduction against the merged Python source: an
available depth-1 QB at 20 points is reduced to 12 when a depth-3, zero-history
QB is IR. See `replacement_for`, `transfer_opportunity`, and `apply` in
`model/nfl_dfs_availability.py`. Fix role eligibility and evidence requirements;
merely clamping the multiplier would not establish that a transfer is valid.
The Python path also re-scores mean stats while the web path preserves the
simulated projection and adds marginal points. Align the transfer contract.

## Additional findings from source review

1. **Projection/slate compatibility:** `latestProjectionRun` in
   `web/src/app/dfs/nfl/actions.ts` orders all runs by season, week, and as-of.
   `loadNflSalaryCsv` uses it without matching the uploaded slate's games.
   An older slate or an advance-week run can therefore get incompatible
   projections. Resolve the intended season/week from games and reject or
   explicitly report incompatible coverage. Today's new run matches week 2;
   this is a code-path defect, not a claim today's upload used the wrong week.
2. **Unpaid capped workload:** `opportunity-redistribution.ts` reports an
   unresolved pool only when no recipient qualifies. A multiplier cap can
   leave units unpaid without adding an unresolved remainder. Existing tests
   explicitly demonstrate 16 carries offered, 12 paid, and 4 dropped. The
   recipient does expose `cappedFrom`, but zero unresolved pools does NOT prove
   all work was placed. Add pool-level offered/paid/unassigned accounting.
3. **Incomplete stale-lineup warning:** `nfl-dfs-client.tsx` records and compares
   only mode, source, workload positions, and situation settings for its
   settings-changed notice. Changes to locks, exclusions, exposures, lineup
   count, salary constraints, uniqueness, and stacking can leave displayed
   lineups unchanged without that notice. Compare the full generation inputs.
4. **Refresh UX:** saved uploads remain pinned to their original projection
   run. The latest saved `DKSalaries (3).csv` was still linked to September 16
   when checked. Add an explicit create-refreshed-slate action and a newer-run
   notice, preserving old lineup audits; do not silently rewrite old runs.
5. **Operational status:** production snapshot success and shadow-research
   failure share a workflow status. Separate their job results and expose
   projection freshness. Investigate/re-run the research needed to restore
   the study pin; do not bypass the baseline-drift guard.

## Verified refresh facts and limits

Workflow run: https://github.com/themvf/NBA_DFS_2/actions/runs/35516863010

Snapshot persisted 1,086 players. All 89 OUT players had zero projected points
and availability notes. 81 RB/WR/TE players retained stat lines; eight QB
donors had cleared lines. Clearing those QB lines proves processing occurred,
not that the transfers were correct. Earlier readiness advice was too broad.

The workflow completed with only the shadow step failing:
`Baseline implementation drifted from the pinned study; rerun research before shadow`.

The FLEX filter uses shared `NFL_FLEX_POSITIONS` (RB/WR/TE), and explicitly
labels its Classic meaning on Showdown. No membership defect found in this
source review. This was not a full browser or optimizer validation.


## Implementation and verification

- Availability v2 requires a starter-to-backup promotion and two observed games
  on both sides. Healthy QB1s are not scaled to absent backups. Transfers retain
  the simulation's bonus expectation and report incremental workload accounting.
- The web path honors rejected pipeline transfers, requires QB depth evidence,
  and reports offered/assigned/unassigned units, including capped remainders.
- Salary uploads resolve every dated matchup against the schedule and choose
  only a run for that season/week. Mixed or unmatched weeks fail explicitly.
- Refresh projections clones a saved salary slate using the same validated
  ingest path, retaining comparison imports. Complete existing snapshots are
  reused, never rewritten. Existing lineup audits remain attached to old runs.
- All generation settings, locks, exclusions, and exposure inputs participate in
  the settings-changed warning. FLEX continues to use the shared RB/WR/TE rule.
- Production and research now have separate workflow jobs. The production job
  publishes its saved snapshot and completion time to the run summary.
- Research sample cache keys include the baseline implementation hash so a
  baseline change cannot silently reuse the old study's sampled forecasts.

Verified locally: 56 Python availability/projection/shadow tests; TypeScript
slate-week/settings, redistribution, stale-run, FLEX, workspace, atomic-persist,
and OUT-projection suites; production build. The real 670-player upload matches
week 2. Browser checks verified the refresh notice, workload accounting panel,
FLEX (555 players), and warning on a salary-limit change without regenerating.


The pipeline also requires a roster capture no more than 72 hours old, not in
future, before accepting its depth order. Missing/stale roles are unresolved,
not guessed. A real-data dry run confirmed 1,086 players and restored the
unmodified simulated means for Stroud (16.2048), Lamar (21.8468), and Stafford
(22.2635). That dry run preceded the stricter roster-age gate, which only removes
unsupported promotions and cannot reintroduce those backup-driven changes.
