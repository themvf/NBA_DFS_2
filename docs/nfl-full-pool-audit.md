# NFL full-pool audit

The Full Pool Audit at `/dfs/nfl/pool-review` retains the whole saved DraftKings
salary pool, including OUT players, reserves, zero projections, missing
projections, and unmatched identities. It does not depend on generating lineups.

## Evidence and timing

`nfl_dfs_pool_captures` is append-only, enforced by an UPDATE/DELETE rejection
trigger. Each game capture contains the complete resolved player rows, alternate
projection sources, workload/availability/role evidence, simulation/estimate
labels, redistribution context, salary-file digest, model run, code revision,
game identity and scheduled kickoff. Canonical JSON SHA-256 is verified on read;
the stored digest combines that hash with the capture idempotency key.

Live observations use the database clock at persistence. There is no API for
backdating them. Historical optimizer recovery verifies the original full-input
digest and player count, preserves its original observation time, and separately
records the new archival time. A legacy source's unrecorded code revision and
mixed distribution semantics are explicitly unknown, not reconstructed.

Vercel calls `/api/cron/nfl-pool-capture` every minute with `CRON_SECRET`.
The job examines the latest complete revision of each saved salary file, saves
an initial snapshot within 24 hours of each included game, and captures every
minute during the final 20 minutes. A first observation up to 15 minutes after
kickoff is retained as **late**, never represented as pregame. Job runs and
errors are retained in `nfl_dfs_pool_capture_runs`; the UI flags absent/stale
heartbeats. No exact-second delivery guarantee is assumed. No paid data APIs
are called by this job: it reads saved database evidence.

One observation per upload/game/minute prevents duplicate cron deliveries from
duplicating a minute. Older upload revisions remain independently reviewable.
The review defaults to the last successful observation strictly before each
game's kickoff; if none exists, it displays the earliest late observation and
excludes it from accuracy. The separate Latest saved view exposes later changes.
Before kickoff, the default selection can advance as new observations arrive.

## Results and exports

Results come from the existing immutable `nfl_dfs_player_week_results` ledger,
populated by `refresh_nfl_dfs_projections.yml` twice daily. They are joined by
canonical player, exact game, team and position. The latest result revision is
used, including a later exclusion replacing a formerly exact result. Game
completion and unchanged scheduled kickoff are required for grading.

Missing results are never filled with zero. Exact recorded zero is valid.
Unmatched/missing/unscorable/late rows stay visible with explicit statuses.
MAE and bias use only scored pregame rows; bias is actual minus projection.
Interval coverage requires a recorded baseline simulation scenario and valid
ordered bounds. Legacy mixed ranges and availability estimates are excluded.

CSV exports all selected rows, independent of search/position/status filters.
JSON exports the selected frozen game payloads plus the exact result IDs,
digests, scoring versions, component evidence and evaluation time used for
that review. Further result revisions do not change an already downloaded
export. All intermediate observations remain in the database ledger.

## September 20, 2026 recovery

- Upload: `f067c8b9-f920-4b26-b77c-36f43a2c9df0`.
- Model run: `73cd196d-cc66-51f4-be0b-f19f7a98a015`.
- Recovered optimizer run: `79839671-4397-4e78-9026-227d1f6d5b05`.
- Original observation: `2026-09-20T16:03:43.535Z` (12:03:43 p.m. Eastern).
- All **670 salary players / 13 games** recovered with original input digest
  verified. Re-import inserted zero additional records.
- Jones's original preserved projection is **19.764**. This intentionally retains
  the pre-fix forecast. It is not silently replaced with a later corrected value.
- Separate current-pool capture: `17:21:56Z–17:22:02Z`. Early games are late;
  five afternoon games are pregame. Later automatic observations can advance
  only those afternoon games' default audit selection before kickoff.
- Verification found all 670 distinct salary IDs, 80 OUT flags and 29 unmatched
  identities in the selected evidence. Unknown results remain pending.

## Operations and validation

From `web`, use the existing local database environment:

```powershell
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx scripts/capture-nfl-pool.ts --due
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx scripts/capture-nfl-pool.ts --upload UPLOAD_UUID
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx scripts/capture-nfl-pool.ts --archive-optimizer RUN_UUID
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx scripts/verify-nfl-pool-audit.ts UPLOAD_UUID
node --import tsx scripts/test-nfl-pool-audit.ts
```

The regression checks cover capture windows, strict pregame selection, multiple
kickoffs, invalid/future timestamps, full OUT/unmatched pools, zero versus missing
scores, identity mismatches, result corrections/exclusions, schedule changes,
interval eligibility and metric denominators. The live-database verifier checks
complete unique salary coverage, capture integrity, and the immutability trigger.
Related report-card, OUT projection and opportunity regressions passed; build,
TypeScript, and local browser checks passed. The browser verified the 670-player
pool, Jones's original evidence, and the distinct late observation view.

For Tuesday, refresh results and export the pregame view. If results remain
missing, inspect the existing NFL results ingestion job and source availability;
do not grade absence of data as zero or recompute the frozen forecast.
