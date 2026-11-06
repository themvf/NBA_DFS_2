# TD Comparison website

Route: `/nfl/td-comparison`, linked from NFL navigation and Game Leaders.
Download: `/api/nfl/td-comparison?season=2026`.

The page uses the canonical regular-season schedule, with historical season,
week and game selectors. It displays immutable pregame probabilities from a
paired three-model forecast alongside independently reconciled full-game rushing
and receiving touchdown counts. Unknown outcomes remain pending. Overtime is
included; passing, defensive/return touchdowns and conversions are excluded.

This release implements the comparison surface, database storage, guarded
publication and scoring. It does **not** implement/train the two proposed models,
generate predictions, or schedule Sunday captures or result ingestion. Until
those consumers publish actual evidence, the live page displays schedule-backed
pending states and empty scorecards. No demonstration numbers are published.

## Storage and publication

Run commands from `web/` with the production DATABASE_URL provided securely:

```
npm run publish:nfl-td-comparison -- init
npm run publish:nfl-td-comparison -- forecast path/to/reviewed-forecast.json
npm run publish:nfl-td-comparison -- result path/to/reconciled-results.json
```

`src/lib/nfl/td-comparison.ts` defines the versioned `Forecast` and `Result`
contracts. All three model probabilities are required for every player. Players
must have unique GSIS identities, canonical teams, position and an availability
label. Source timestamps/digests, model versions, implementation digest and
availability notes travel with every forecast. Review dual-provider depth,
week-matched injuries and official inactives under AGENTS.md before publishing.
The publisher validates the artifact structure; it does not independently
download or authenticate source files or certify model quality.

`nfl_td_comparison_forecasts` stores one final forecast per canonical game. The
database sets its freeze timestamp and the INSERT checks the 20-minute pregame
deadline again. There is no publisher UPDATE/DELETE path. A failed or late run
cannot create a retrospective pregame forecast. Provisional model research
artifacts remain outside this final-results ledger.

Results are append-only revisions in `nfl_td_comparison_results`, keyed to a
frozen game. The reviewed result artifact must include full-game PBP credits and
independent box credits, explicit zeros for every forecast player, every omitted
scorer and official team rushing+receiving TD totals. Import verifies unique
identities, per-player PBP/box agreement, team totals, completed canonical game,
and timestamps. Missing/inconsistent data reject the entire result. New source
corrections append a revision; the latest valid imported revision is displayed.
Both publishers retain the raw artifact digest. No public write endpoint exists.

The reader repeats canonical and payload validation. It rejects malformed
records visibly and excludes them from scoring. Database errors produce an
unavailable state instead of invented zero forecasts. Forecasts are retained
after kickoff irrespective of subsequent availability information.

## Grading

Each verified forecast player contributes one binary outcome (`TD count > 0`).
All models use precisely the same rows. Inactive/DNP rows are retained when the
complete reviewed result explicitly records zero. Extra scorers remain in the
evidence and appear as coverage gaps; they do not retrospectively expand the
forecast cohort. Week and season summaries report Brier score, log loss, game
count and player-game count. Calibration has five fixed 20-point bins with
sample counts; the final bin includes 100%. Log-loss calculation clips to
`[1e-12, 1-1e-12]`; displays retain original probabilities. Mixed model versions
are listed and pooled results are explicitly descriptive. No significance test,
automatic promotion or profitability claim is made.

Tests: `npm run test:nfl-td-comparison`. Synthetic values live in tests only.
Deployment requires table initialization before the first production request.

## Release verification (2026-11-06)

- 17 dedicated validation/scoring checks passed; TypeScript and changed-file
  ESLint passed; `next build --webpack` passed.
- Local production-server checks passed for current week, historical Week 1,
  the Week 9 Sunday-night game, and the no-store JSON download. The production
  DB reader returned 272 games, no forecast rows and no invalid records.
- All 144 package test scripts were attempted. 140 passed initially; X-news
  and pool-capture tests passed after correcting the runner's npm-bin PATH.
  The unchanged Pick'em freeze-evidence integration assertion failed, and
  the unrelated pool-rule optimizer study exceeded its 180-second timeout.
  Those two suites are not reported as passing. Logs remain in local
  `artifacts/td-comparison-verification/`.
- Browser automation is disabled in the user's app settings, so visual browser
  inspection was unavailable. HTTP checks verified rendered content and routes,
  not pixel layout.
