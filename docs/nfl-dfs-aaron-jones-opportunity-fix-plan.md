# NFL DFS opportunity redistribution: Aaron Jones handoff

Date: 2026-09-20. Status: investigation and implementation plan; no fix implemented by this handoff.

Continuation: a local baseline-retention stage has now been implemented and verified. See [the execution audit](nfl-dfs-aaron-jones-opportunity-fix-audit.md) for the exact live-data reproduction, Murray donor cause, before/after numbers, checks, and remaining release gates. It is not deployed or a completed replacement allocator.

## Objective

Fix unsupported absence-driven workload inflation and make the NFL DFS pool, Why breakdown, opportunity shares, and optimizer describe the same projection scenario. Do not target a predetermined Aaron Jones projection or introduce a player-specific cap.

First read `CLAUDE.md` and `docs/nfl-dfs-handoff-2026-09-20.md`. Preserve the existing QB promotion, fresh role evidence, slate/week matching, immutable refresh, workload accounting, and research release contracts. Inspect the working tree before editing; unrelated changes are present.

## Reported reproduction

The user saw Aaron Jones Sr., RB, MIN vs CHI, $5,100:

- Projection: 19.8 DK points; value: 3.88x, Elite value; RB rank: #5 of 128.
- Subtitle: mean of 2,000 simulated games, historical, model confidence 100%.
- Boom rate: 1%.
- Inheritance: +13.8 carries from Jordan Mason, Kyler Murray, Jauan Jennings, and Ben Yurosek, or 2.05x Jones's own 13.1 carries.
- Receiving inheritance: +0.6 receptions from Mason, Jennings, and Yurosek, or 1.28x his own 2.3 receptions.
- The note says efficiency is unchanged and the interval is scaled proportionally, not re-simulated.

Rounded displayed inputs imply 26.9 carries and about 2.9 receptions. This is evidence of a large adjustment, not proof that 26.9 carries is impossible. The issue is whether the model supports that expectation.

## Confirmed source findings

1. **No team workload constraint in the slate redistribution.** `web/src/lib/nfl-dfs/opportunity-redistribution.ts`, `redistributeOutOpportunity`, sums eligible absent players' modeled per-game opportunity and adds it proportionally to available teammates' baselines. It does not reconcile those independent averages to a common team/game budget. `MAX_MULTIPLIER = 4` limits individual scaling but does not establish that the offered work exists. Historical averages from different roles or games can overlap; the current method does not establish that the baseline has not already absorbed the absence.
2. **QB donor discrepancy.** Current `POOLS.rush.donors` includes RB/WR/TE and excludes QB. QB rushing scales with a supported QB promotion. The reported Kyler Murray carry donation conflicts with that contract. An old deployed implementation, wrong position mapping, or stale browser response is possible; the cause has NOT been verified. Do not claim this is fixed merely because local source excludes QB.
3. **Mixed distributions in one explanation.** The redistribution adds marginal linear points to the original simulated mean and scales floor/ceiling by a ratio. It does not re-simulate. In `web/src/app/dfs/nfl/actions.ts`, the explanation returns the adjusted mean/floor/ceiling alongside the original median and boom rate. `player-explanation-panel.tsx` still calls the headline the mean of the simulated games. The 1% boom rate does not evaluate the added workload.
4. **Stat-line mismatch in the drawer.** The explanation builds `statMeans` from the immutable projection row, while its headline can use the adjusted slate projection. The transfer function produces adjusted stats, but those are not the stats returned by this explanation path. Inspect the resulting stat bars and point attribution: an injury uplift must not be presented as yardage bonuses or simulation effects.
5. **Confidence label overstates its meaning.** `model/nfl_dfs_historical.py` computes historical confidence as `min(1, own_games / 12)` with a missing-team-total penalty (and a DST adjustment). That can reach 100% without validating redistribution, recipient roles, or the final forecast. It is not a calibrated probability of projection accuracy.
6. **The method is explicitly unvalidated.** The redistribution module describes its live transfer assumptions as awaiting backtesting. It uses receptions as a proxy for targets, which should not be described as true target opportunity. Historical games >= 2 is a useful prior guard, but does not prove current same-team role or incremental vacated work.

The live slate, donor identities/statuses, exact pre-adjustment Jones projection, deployed revision, and opportunity-share surface were NOT independently retrieved in this investigation. Keep these as open checks, not established facts. Do not infer team membership from a player's name or remembered roster.

## Execution plan

### 1. Capture a reproducible failing case

- Identify the exact upload, season/week, matchup date, projection run, deployed revision, and redistribution version from the affected app. Compare the saved slate with an explicitly refreshed copy.
- Capture Jones and every listed donor/recipient: stable IDs, team, position, official/DK availability evidence and timestamp, depth evidence, observed games, baseline stats, upstream transfer decision, and final inheritance.
- Trace each donor through salary mapping, identity registry, roster, projection, and slate transformation. Establish why Murray reached this carry list. Confirm eligibility blocks are not being treated as injury evidence.
- Record each donor's actual contribution and the full team totals before/after, including excluded, unsupported, and unassigned workload. A list of names is insufficient to explain +13.8 carries.
- Locate and inspect the user's opportunity-share view. Record whether its denominator comes from the full roster, salary pool, available players, or an independent workload model. Preserve the case as a deterministic fixture without secrets.

### 2. Correct the workload contract

- Review existing workload/team-context/role models before adding another allocator: `model/nfl_dfs_workload.py`, `model/nfl_dfs_workload_ranges.py`, `model/nfl_dfs_team_context.py`, corresponding web modules, and the opportunity/efficiency specifications. Reuse compatible contracts, but do not silently promote shadow-only research.
- Define a shared team/game budget and a mutually consistent baseline allocation. Explain how missing roles and uncertainty are reserved. Sum of assigned opportunity plus unassigned reserve must equal the applicable budget within tolerance.
- Establish incremental vacated work from the donor's supported current role and the baseline's availability assumptions. Do not sum independent historical averages as though they were simultaneous team shares. Prevent an absence already reflected upstream or in the baseline from being paid again.
- Allocate using supported remaining roles; do not renormalize all work into the few players visible in a filtered salary pool. Missing roster members, unsupported roles, and capped gains must remain explicitly unassigned where appropriate.
- Keep QB rushing separate from RB inheritance. Require the existing starter-to-backup evidence for QB promotions. Verify identity, team, game, position, and freshness before a transfer; reject unsupported cases with an explanation.
- Preserve offered/assigned/unassigned accounting and expose donor-specific amounts and evidence. Distinguish carries, targets, and receptions; converting targets into receptions requires the recipient's supported catch assumptions.
- If a supported team budget or role cannot be established, retain the base projection and mark the adjustment unresolved. Do not choose an arbitrary smaller multiplier just to make Jones's output look plausible.

### 3. Make scoring and display internally consistent

- Preferred completed path: apply the supported workload scenario to simulation draws and recompute DK scoring per draw, including bonuses, then derive mean, median, interval, boom rate, and mean stats together. Record the scenario/version and actual draw count.
- Preserve the existing mean-versus-distribution rule: never replace expected DK scoring with scoring of a mean stat line.
- If re-simulation requires a staged release, label a temporary adjustment as an estimate, show the original baseline separately, and suppress or explicitly segregate stale distribution metrics. Do not call an adjusted estimate a newly simulated mean or claim the full simulation work is complete.
- Return one resolved projection payload to the pool, drawer, shares, rankings/value labels, and optimizer. Carry adjusted stats and explicit injury point deltas through the waterfall; keep bonus attribution separate.
- Replace “model confidence 100%” with an accurately named history-support measure and show adjustment evidence/uncertainty separately. Do not fabricate a replacement confidence percentage.
- Give opportunity shares an explicit unit, team/game denominator, scenario, and unassigned reserve. Shares must reconcile with displayed workload and remain unchanged by table filtering.

### 4. Verify behavior and preserve existing contracts

Add focused regression cases for:

- The captured Jones/Mason/Murray case, including correct donor identities and per-donor accounting.
- QB carries never entering the RB pool; no healthy QB1 reduction from an absent backup; supported promotion still works.
- Overlapping historical workloads and multiple absent players cannot inflate the team budget; already accounted-for absences are not paid again.
- Missing/stale role data, prior-only players, cap leftovers, and missing salary-pool teammates retain explainable unresolved workload.
- Target/reception semantics, nonnegative opportunity, budget conservation, and independence from row order and display filters.
- Adjusted mean/stats/median/interval/boom come from one scenario. A deterministic threshold-crossing case verifies DK yardage bonuses are scored per draw.
- Drawer/pool/shares/optimizer parity, explicit inheritance attribution, and accurate simulation/confidence copy.
- Immutable original slates and lineup audits, compatible explicit refresh, and existing upstream/web double-payment protection.

Run the relevant existing TypeScript redistribution, scoring, OUT-projection, availability, projection-audit, refresh, and optimizer suites plus affected Python availability/projection/workload tests. Run type/build checks for changed web paths. Inspect the real refreshed Jones case in the browser; a passing unit fixture alone does not resolve a deployed-version discrepancy.

Evaluate allocation changes on pregame historical absence cases with held-out outcomes and no future information. Compare workload and points error, interval coverage, and boom calibration to the unchanged baseline. Preserve existing release gates; one plausible player projection is not validation.

## Completion evidence

Deliver the root cause with verified upload/run/code versions; changed files; focused test/build results; and a before/after Jones audit showing team budget, donor amounts, own/inherited/final workload, points, and distribution provenance. Explain any remaining uncertainty and whether the fix is local, deployed, or only a staged display correction.

Acceptance requires that Murray's appearance is explained and resolved, unsupported opportunity cannot inflate the team budget, every displayed metric states the scenario it measures, and opportunity shares reconcile with both the workload and the optimizer. A lower Jones number by itself is not acceptance.
