# Replacement upside: the weekly grading gate (pre-registered)

Grade version `nfl-replacement-upside-grade-v2`, grading feature
`nfl-replacement-upside-v2` ([feature doc](nfl-replacement-upside.md)).
Registered 2026-09-28 as v1; re-registered as v2 on 2026-09-29, before the
first week-4 capture and kickoff (Thursday 2026-10-01, PIT@CLE). No
week-4-or-later outcome existed when either was written.

**Why v2 (2026-09-29).** v1 skipped every ruled-out starter the projection
pipeline already knew was out, because the pipeline zeroed his stored range
and the feature read the zero (Dallas Goedert on PHI@CHI, 2026-09-28). Only
late DraftKings OUT tags could fire it. From availability v3 the pipeline
records the range it zeroed (`pre_availability`, same run and cutoff), and
feature v2 reads it. That changes which absence events are flagged, so under
the non-negotiables below it is a new feature version and a new grade version,
not an edit. v1 accrued nothing: no week-4 capture ran under it. π, the
thresholds, the trigger, the mixture, the population rules, metrics, gates,
floors, bootstrap and seed are copied unchanged from v1.

Code: `web/src/lib/nfl-dfs/replacement-upside-grade.ts` (the gate, pure),
`web/scripts/grade-nfl-replacement-upside.ts` (weekly run),
`web/src/db/nfl-replacement-upside-grade.ts` (run log and frozen verdict).
Tests: `npm run test:nfl-replacement-upside-grade` (includes a check that the
code's constants match this document). The weekly run is automatic (see
"Automation" below); `cd web && npm run grade:nfl-replacement-upside` runs it
by hand.

## The question

When a starter sits, the slate shows his backups a second range, "if he gets
the job". Is that range's ceiling (P90) and boom rate a better forecast of what
DraftKings paid than the baseline shown next to it? And is it better for the
reason the feature claims (the backup may inherit the starter's role), not
just because every history-based ceiling runs a little low?

## What was already seen

- π was fitted on 2020–25 and rechecked on 2014–18. Both periods were
  examined; neither is evidence here.
- 2026 weeks 1–3: some outcomes were discussed while the feature was designed
  (the Rams' week-2 box scores and week-3 Sunday night game). **All of weeks
  1–3 are excluded**, never pooled, not even as a secondary.
- The floors below were set from a power check on the 2020–25 fit seasons only
  (see "Power").

## Evidence: the frozen pregame record

The grade reads only `nfl_dfs_pool_captures`: an append-only, digest-verified
copy of every player row the workspace showed. A trigger rejects updates and
deletes. The per-minute pool-capture cron writes one copy within 24 hours of
each kickoff and one every minute in the last 20 minutes. Each row carries the
full workspace player, including `replacementUpside` (baseline and "if job"
ranges, π, role, the ruled-out starter) and `replacementUpsideUnchanged`.

From this registration on, each capture also records the slate-level upside
report (`context.replacementUpside`: version, flagged count, skipped starters,
error), so a capture where the feature did not run can be told apart from one
where it ran and flagged no one.

**Selection.** For each player-game, the last live pregame capture that
contains the player, across all complete uploads of that week:

- live only: `origin = live_pool`; saved-optimizer archives are excluded;
- `observed_at` strictly before that capture's kickoff;
- ties broken by the newest upload, then by digest. Neither tiebreak looks at
  the outcome.

The player-game is graded only if that selected capture ran
`nfl-replacement-upside-v2` without error. An earlier capture is never
substituted, because that would grade a staler number than the one shown last.

## Population

| Group | Definition | Role in the grade |
|---|---|---|
| Flagged player-games | Selected capture carries a v2 `replacementUpside` for the player with finite baseline and "if job" mean and P90 | Graded (G1–G3) |
| Absence events | One per game, team and room (RB/FB, TE, WR): all backups behind one ruled-out starter | Bootstrap cluster |
| Controls | RB/FB, WR, TE in the same selected captures, in a team-room with no one ruled out and no upside marker, below the room's top baseline projection, projecting ≥ 1.0 DK points with a positive ceiling | Fit the generic widening `k` (G2) |
| Unchanged | Top remaining WR behind a ruled-out WR (π = 0) | Descriptive only |

Seasons: 2026 weeks 4–18 (regular season only). If the floors are not met in
2026, accrual continues through the 2027 regular season under the same rules.
A later feature version (any change to π, thresholds, trigger or the mixture)
is not pooled. It needs its own registration.

## Outcome

DraftKings points from `nfl_dfs_player_week_results`: the latest `exact` row
per player and game. The rule is DraftKings' own and is the same as
`nfl-dfs-slate-report-v1`: **a listed player in a completed game that has at
least one exact result, with no row of his own, scored 0.** A backup who never
touched the ball is exactly the "didn't get the job" case; dropping him as
"missing" would bias the grade toward the mixture.

These are counted and excluded, never guessed:

- `pending_result`: the game is not completed.
- `awaiting_source`: no exact result for the game yet.
- `schedule_changed`: the kickoff moved after capture.
- `result_identity_conflict`: the result's team differs from the slate's.

## Metrics and gates

Losses are 90th-percentile pinball: `L(q, y) = 0.9·(y − q)` if `y ≥ q`, else
`0.1·(q − y)`.

| Gate | Statistic, over flagged player-games | Passes when |
|---|---|---|
| **G1** ceiling beats baseline | mean of `L(if-job P90, y) − L(baseline P90, y)` | 95% CI entirely below 0 |
| **G2** ceiling beats a generic widening | mean of `L(if-job P90, y) − L(k × baseline P90, y)` | 95% CI entirely below 0 |
| **G3** boom not worse | mean of log loss(if-job boom) − log loss(baseline boom), on `actual ≥` the position's boom line (RB/WR 25, TE 20), probabilities clipped to [0.001, 0.999] | point estimate ≤ 0 (CI reported, not gated) |

- **k** is the multiplier on control ceilings, from 1.00 to 2.50 in steps of
  0.01, that minimizes mean control pinball loss (ties go to the smaller k). It
  is fitted once, on all controls, and held fixed inside the bootstrap. The
  control set is roughly 50 times larger than the flagged set, and this
  omission is stated rather than modeled.
- **Intervals** come from a cluster bootstrap over absence events: 10,000 draws,
  seed 20260928, percentile 95% intervals. All three statistics use the same
  resampled events.
- **G3 is only a direction check.** Boom rates of 1–15% cannot be resolved at
  this sample size, so its interval is reported but does not gate. FB rows (no
  boom line) and rows missing a boom rate are left out of G3.

## Floors and the one look

| Floor | Required |
|---|---:|
| Absence events (clusters) | 60 |
| Flagged player-games | 130 |
| Distinct weeks with a flagged player | 8 |

- **Blinded until every floor is met.** Each weekly run reports accrual (counts
  by role and week, outcome statuses) and pipeline health (games with a feature
  capture, skipped starters, code revisions). It reports no loss, exceedance
  rate, `k`, actual score or verdict. The code enforces this: a blinded report
  does not contain those fields.
- **One look.** The first run in which every floor is met computes the
  verdict. It writes it to `nfl_replacement_upside_grade_verdicts` in the same
  statement as its run record. That table allows one row per grade version and
  rejects updates and deletes, so the verdict cannot be written twice or
  edited. The look happens when the sample size is reached, not when a result
  looks good, so there is no optional stopping.
- **After the look**, later runs print the frozen verdict and label everything
  else post-verdict monitoring, which cannot change it.

## Automation

Nothing in this grade needs a person to run it.

| Step | What runs it | When |
|---|---|---|
| Freeze what the slate showed | Vercel cron `/api/cron/nfl-pool-capture` (existing pool audit) | Every minute; one copy within 24 hours of each kickoff, then every minute of the last 20 |
| DraftKings results | `refresh_nfl_dfs_postweek.yml`, job `review` | Tuesday 14:07 and Wednesday 10:07 UTC, dispatched by Vercel cron (`/api/cron/dispatch`, job `nfl-dfs-postweek`); GitHub schedule 14:41 UTC the same days as fallback. Tuesday moved from 10:07 on 2026-09-29, before any graded week: Monday night's stats landed at 11:34 UTC, so 10:07 always missed them; 14:07 also follows the 13:07 pbp relabel that DST scoring reads. Timing only; the grade is unchanged |
| Grade | Same workflow, job `grade-replacement-upside`, after `review` | Same slots. It first runs the gate's own tests and refuses to grade if they fail |
| Record | `nfl_replacement_upside_grade_runs` (every run) and `nfl_replacement_upside_grade_verdicts` (the one look) | Each run |
| Show | "If he gets the job" grade card on `/dfs/nfl/results` | Floor progress while blinded, the frozen verdict after |

Each run also writes a job summary and a report file kept 90 days as a
workflow artifact. Running twice on the same data is harmless. Before the
floors, a second run is another blinded count. After them, the verdict is
already frozen.

**Amendment, 2026-09-28, before any week-4 game.** The first version of this
registration froze the verdict to a JSON file in `artifacts/` that had to be
committed by hand. An automated run starts from a fresh checkout and cannot
remember an earlier look, so the verdict moved to the append-only table above.
Only where the verdict is stored changed. The population, metrics, gates,
floors and decision table are unchanged.

## Verdicts and what each one licenses

| Result | Verdict | Licenses |
|---|---|---|
| G1 and G2 pass, G3 ≤ 0 | `PROMOTE` | The GPP objective may read the "if job" ceiling and boom rate for flagged players, as a separate, versioned optimizer change. Cash mode, the projection column and ownership keep the baseline. |
| G1 and G2 pass, G3 > 0 | `PROMOTE_CEILING_ONLY` | As above, ceiling only; boom stays on the baseline. |
| G1 passes, G2 fails | `NOT_PROMOTED_GENERIC` | The gain is general ceiling under-coverage, not the job mechanism. The display stays. Widening every ceiling needs its own registration. |
| G1 CI entirely above 0 | `RETIRE` | The "if job" ceiling is confirmed worse. Remove the second range from the display. |
| Anything else | `NOT_PROMOTED` | Display stays, labelled unvalidated. |

## Descriptive only (reported after the look, gates nothing)

- **Per-role results** (RB lead, RB other, TE lead, TE other, WR other): each
  role's mean pinball change and how often actual scores beat the ceiling.
  Roles are never separate verdicts: a pooled pass promotes every π > 0 role,
  and a failed pool is not rescued by the role that looks best.
- **Ceiling beat rates** for the baseline, the widened baseline and the "if
  job" ceiling, for flagged players and for controls. The target is 10%.
- **Top remaining WR** (the unchanged, π = 0 role): how often he beats his
  baseline ceiling, against the controls. This checks the π = 0 decision.
- **Projection accuracy**: squared error of the "if job" mean vs the baseline
  mean. The projection column is not in scope for promotion.

## Power (design stage, 2020–25 fit seasons only)

These numbers come from the historical empirical-sample version of the
mixture, not the live model ranges, so they are approximate:

- The paired pinball change was −0.74 per flagged player-game (SD 3.0, about
  2.3 backups per event).
- A generic widening fitted on 8,392 full-strength backups came out at k =
  1.12.
- Against that widening, the mixture still improved by −0.58.

| Absence events | Power G1 | Power G2 |
|---:|---:|---:|
| 50 | 0.77 | 0.61 |
| 60 | 0.84 | 0.69 |
| 80 | 0.92 | 0.81 |

The floor is 60 events, where G2 (the harder test) has about 70% power.

## Expected timing

A full season of 2020–25 averaged 78 events and about 180 flagged
player-games. Weeks 4–18 are 15 of 18 weeks, so about 65 events if every game
is on an uploaded slate. Games missing from uploads lower that. Expect the look
at the end of the 2026 season or early in 2027.

## Non-negotiables

- Constants, population, metrics, gates and floors are frozen. Changing any of
  them is a new grade version and a new registration, never an edit.
- π, thresholds and the trigger do not change during accrual. A change is a
  new feature version, and its rows are not pooled with v1.
- Weeks 1–3 of 2026 never enter the grade.
- No outcome metric before the floors. No second look.
- Promotion licenses a versioned optimizer change. It does not flip the
  projection column, cash mode or ownership.
