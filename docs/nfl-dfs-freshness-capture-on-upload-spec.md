# NFL DFS freshness: capture opponent adjustments on upload

**Status:** implementation spec for fix 2. **Base:** `claude/nfl-dfs-freshness` (fixes 1 and 3). This spec does not authorize an approved defensive default; captures remain experimental evidence.

## Goal

When a user uploads a salary file or moves that file to a newer projection run, request defensive forecast captures for the resulting upload immediately. The user can then use fresh injury-aware projections and, when the frozen checks pass, opponent adjustments on that same upload. A failed or unavailable capture must remain visible and must never make the baseline slate unusable.

The relevant captures are `nfl_matchup_forecast_runs` and `nfl_matchup_player_forecasts`, read by `readDefensiveCaptures()`. The separate `nfl_dfs_pool_captures` audit is outside this change.

## Existing contract

- `persistSalarySlate()` binds a complete salary upload to one `nfl_dfs_projection_runs.run_id`. An existing `(file_digest, projection_run_id)` upload is reused; a refreshed run creates a different upload. The header and players are written in one batch transaction, followed by a row-count check.
- Fix 3 calls `refreshNflSlateProjections()` when an eligible saved slate opens with a newer run. `Update data` already uses the same move. Saved optimizer runs stay pinned to their original upload and projection inputs.
- The twice-daily `refresh_nfl_dfs_projections.yml` research job captures the latest saved upcoming slate. It can run before a new upload exists or miss a refresh after its last pregame slot.
- `research/nfl_matchup_implementation.py --upload-id ... --persist` writes PFR efficiency captures against the upload's pinned v5 baseline. `research/nfl_allowed_rushing_volume_capture.py --upload-id ... --persist` writes allowed-rushing-volume captures. Both publish append-only rows through `persist_forecasts()`.
- `readDefensiveCaptures()` requires the exact upload, baseline run, profile, and pre-decision cutoff. `resolveDefensiveForecast()` separately checks identity, game, reproduced baseline, and full distribution. A captured row is not proof that an adjustment applies.

## Required behavior

1. **Trigger:** After a new upload passes the complete-player verification, enqueue one capture request for that `upload_id` and pinned `projection_run_id`. Do this for direct salary uploads and projection refreshes, including fix 3's automatic move. Do not enqueue when an existing complete upload is reused. A retry of a failed request may be explicitly requested without making a new upload.
2. **Timing:** Never perform the Python capture in the web upload request. Return the usable baseline slate promptly and show the capture as pending. A server-side dispatcher starts a targeted GitHub Actions workflow; a scheduled worker also drains pending requests so dispatch failures do not strand them.
3. **Eligibility:** Validate the upload still exists, has its claimed number of salary rows, is linked to an exact supported `nfl-dfs-historical-v5` run, and every salary game is verifiably before its canonical kickoff when work starts. If those conditions fail, record a terminal, specific reason. Do not backdate or copy a capture from an older upload. Do not start a new capture after the first slate kickoff; a race at kickoff must fail closed.
4. **Profiles:** Attempt PFR efficiency and allowed rushing volume independently. One profile's failure must not erase the other's successful capture. Use each profile's existing frozen model, source, and reproduction rules. No fitting, model promotion, baseline rewrite, or change to optimizer policy belongs to this fix.
5. **Identity:** Pass the exact upload ID and projection run ID to the worker. The PFR runner must pin `--baseline-run-id`; the allowed-volume runner already reads the upload's pinned run. Before publishing, recheck the binding and the kickoff cutoff. The web reader continues selecting one compatible immutable run per profile, never mixing rows across runs.
6. **Idempotency:** Give each `(upload_id, projection_run_id, profile)` a durable request identity and unique constraint. Concurrent upload, automatic refresh, manual retry, and scheduled jobs must not dispatch duplicate active work. A successful request is satisfied only after a matching `nfl_matchup_forecast_runs` row and its expected player rows are committed. Existing append-only artifacts remain intact on retries.
7. **Visibility:** Slate Check reports `pending`, `captured`, `captured but 0 applied`, `failed`, `ineligible`, or `not requested` for each profile. It shows captured and applied player counts separately. On success, reload the slate coverage without moving uploads or resetting build settings. On failure, retain the baseline and show a retry action while pregame. Keep fix 1's next scheduled capture text as fallback, but do not imply a scheduled slot is the only path after this change.
8. **Saved runs:** Capturing later never changes a saved optimizer run. A new build may use a compatible newly captured profile; reopening an old build displays its pinned inputs and numbers.
9. **Rollout:** On deployment, enqueue the newest complete, still-pregame upload for each saved slate signature if it has no compatible capture or request. This one-time backfill lets existing saved slates benefit without forcing a projection refresh. Do not backfill historical or started slates.

## Implementation seams

| Area | Change |
|---|---|
| Upload persistence | In `web/src/app/dfs/nfl/actions.ts`, add an enqueue step after `assertSlateFullyPersisted()` for a newly created upload. Keep the existing-upload return path free of new requests. Ensure a capture-dispatch failure cannot turn a successful salary upload into an apparent upload failure. |
| Durable work | Add an additive table, for example `nfl_dfs_defensive_capture_requests`, with upload ID, projection run ID, profile, state, attempts, timestamps, GitHub run URL/ID, result counts, and last error. Constrain the three identity columns to be unique. A short lease or claim transaction prevents concurrent workers from processing one request. |
| Dispatch | Add a dedicated `workflow_dispatch` workflow with `upload_id` and `projection_run_id` inputs. Reuse server-only `GITHUB_DISPATCH_TOKEN` and the existing dispatch helper. Run on the deployed default branch with database credentials; never expose the token to the client. A scheduled pass picks up pending/expired-lease work. |
| Worker | Run the two existing Python capture commands for their respective requests. Add a shared preflight and post-write verifier for upload/run/row-count/kickoff identity and matching persisted profile rows. Return structured `captured`, `applied`, `fallback`, and error counts. |
| Status read/UI | Add a bounded status read keyed by the current upload. Poll while pending, then refresh `opponentAdjustmentCoverage()` and Slate Check. The UI must not infer success from GitHub dispatch acceptance alone. |
| Operations | Log one request and one terminal result per profile, including upload/run IDs and a link to the worker run. Surface stuck leases and retry counts in pipeline health. |

## Failure and cutoff rules

- The upload remains usable with baseline projections if enqueue, dispatch, capture, or status polling fails. Record the failure and keep it visible; do not report “no player passed” when no capture exists.
- If a worker finds a different pinned run than the request recorded, fail that request. Never silently retarget it to the newest week-level run.
- At or after first kickoff, stop new upload-triggered capture and manual retry. Existing pregame captures remain readable. The scheduled workflow's independent policy remains unchanged.
- If a capture publishes rows but no player passes `resolveDefensiveForecast()`, report `captured but 0 applied` with reason counts; do not loop retries merely to seek a nonzero effect.
- If only one profile succeeds, retain that profile and report the other profile's failure. Retrying the failed profile must not regenerate or mutate the successful artifact.

## Acceptance tests

1. New complete salary upload queues both profile requests exactly once; replaying the same file and run reuses the upload and adds no request.
2. Refresh onto a newer run, including automatic move on saved-slate open, creates requests bound to the new upload and new run. Old captures and saved lineups remain on their original upload.
   The rollout backfill also queues an eligible pre-existing upload once and skips a started slate.
3. Incomplete player persistence, unsupported model version, game mismatch, and first-kickoff boundary produce explicit non-success states and no publish.
4. Dispatch failure is retried by the scheduled worker. Concurrent worker claims and repeated GitHub deliveries do not create duplicate active requests or overwrite immutable captures.
5. A successful matching PFR capture and allowed-volume capture become visible for the new upload; the Slate Check's `captured` and `applied` counts agree with the same reader and resolver used by generation.
6. A valid capture with zero applicable players says so. A failed read, missing capture, pending capture, and failed capture each have distinct messages.
7. Build, save, reopen, and export with Experimental mode preserve the selected forecast bundle. A previously saved lineup remains unchanged after a later capture or projection refresh.
8. Live verification on a pregame slate records the upload ID, pinned projection run ID, worker run, capture run IDs, per-profile captured/applied counts, and a saved optimizer input showing the selected bundle. Verify the baseline path also works when both captures fail.

## Delivery boundary

Implement this against `claude/nfl-dfs-freshness` or a descendant, then run the focused capture, slate, and optimizer tests plus TypeScript checks. The earlier report's 120 passing web suites verify fixes 1 and 3 only; they do not verify this trigger. A live pregame check is required before claiming that upload-time captures work in production.
