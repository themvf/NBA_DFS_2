# NFL defensive adjustments: finish the optimizer connection

Date: September 27, 2026. Status: implementation handoff; runtime activation is not delivered by this document.

Baseline audited: main commit `d13fca98882ec9b6c52ea1ba84b008465cad0ce4`, including integration commit `df4e489`. Reconcile newer main changes before implementation.

## 1. Required outcome

The user must be able to select defensive adjustments in the normal NFL DFS optimizer, generate legal lineups using those adjusted forecasts, save them, reopen the same results, and export their DraftKings entries. Changing a comparison panel does not satisfy this requirement.

Deliver two distinct milestones:

1. **Usable implementation:** an explicitly selected experimental defensive-adjustment mode, fully connected to lineup generation, save/reload, explanations, and export. It can ship while statistical qualification is pending. Existing pregame, identity, availability, and lineup-legality requirements still apply.
2. **Approved default:** server-controlled activation of an exact qualified configuration, with versioning, paired monitoring, and rollback. Implement and test this activation route now; switch the real default only when the existing qualification contract permits it.

An engineer may report milestone 1 complete without claiming milestone 2 is active. Neither another research page nor an unexercised feature flag completes milestone 1. Report the actual production mode at handoff.

This document covers defenses as opponents affecting offensive-player projections. It does not introduce a new DST fantasy-points model, a generic defense rank multiplier, or a new pick'em winner model.

## 2. What the preceding release actually delivered

The earlier release implemented substantial computation but stopped before normal optimizer consumption:

| Area | Delivered | Still missing |
|---|---|---|
| Data | PFR advanced-stat ingestion through nflverse, preserved captures and player identities, PBP/context integration, coverage and cutoff checks | No new provider is required for this handoff |
| Numerical forecasts | Fitted, frozen QB pressure and RB contact challengers; exact DraftKings rescoring; existing allowed-rush-volume variant | A selectable runtime policy that supplies these forecasts to the optimizer |
| Evaluation | Context-variant grader, paired forecast grading, independent and coherent scenario comparisons, registrations and archives | Qualification is not a deployed consumer policy; no qualified default has been activated |
| Interface | PFR evidence, player comparison explanations, research portfolios and ranges | Normal generation, saved-run provenance, and export using the selected defensive profile |
| Operations | Recurring captures, grading, publication and freeze health | Health checks proving a selected profile reached optimizer inputs and saved output |

The release deliberately leaves `active_projection_changes = 0` in `ingest/nfl_dfs_projections.py`. `shadow_projection()` returns `authority = shadow_only` and `active_delta = 0`. `readMatchupComparison()` serves the Why panel. `runNflOptimizer()` does not load those candidate distributions, and the research portfolio adapter returns `exportAuthorized: false`.

The September 27 test found 149 adjusted research rows among 643 matched salary entries. It did **not** demonstrate that the normal optimizer used 149 defensive adjustments. Earlier statements that the defensive-adjustment implementation was complete were broader than the delivered behavior.

## 3. User-facing contract

Add **Defensive adjustments** alongside the existing projection-source selector:

| Mode | Behavior |
|---|---|
| Off | No additional defensive profile beyond the selected approved baseline; the reproducible comparator |
| Experimental | Use the selected frozen defensive profile in generated lineups; visibly label the run experimental |
| Approved | Resolve an eligible profile through the server's active policy; show whether it applied or retained the approved baseline |

Initially support only the exact historical baseline configuration for which each profile can reproduce its baseline. Do not stack a historical-v5 residual on workload, calibrated, competitor, custom, or DK-average projections. Reject unsupported combinations explicitly; user selection must never create a qualification record.

Show the selected profile, capture time, number of adjusted players, number retaining baseline, and concise fallback reasons. Every adjusted player's displayed projection and Why explanation must match the forecast consumed by generation. “Under evaluation” describes evidence status; “Applied to this lineup” describes actual use. Display both when appropriate.

Experimental mode must generate ordinary saved lineups and permit the existing pregame CSV export flow. A concise mode label is sufficient; do not add repeated approval dialogs. Preserve an Off comparison. If no player receives an adjustment, label the result baseline-only rather than claiming defenses were applied.

Existing saved runs keep their original mode and numbers. Loading one must not apply a newly selected default or the latest candidate capture. A deliberate regenerate action creates a new run.

## 4. Profiles and numerical scope

Ship separate frozen profiles for **PFR efficiency** and **Allowed rushing volume**. Do not hide different mechanisms behind one unversioned multiplier. A combined profile is a separate registration and can be offered experimentally only after its complete formula, baseline, and evaluation contract are registered before its captures.

### 4.1 PFR efficiency

Reuse `model/nfl_matchup_projection.py` and the committed fitted artifacts; do not refit during implementation or refresh.

| Component | Inputs | Changed fields | Unchanged fields |
|---|---|---|---|
| Pressure | Offensive pressure faced and opposing defense's pressure history, with current completeness rules | QB `passing_yards` | Attempts, passing TDs, interceptions, rushing, receivers, DST |
| Contact | Own and opponent-allowed RB yards before/after contact, with matched carry denominators | RB `rushing_yards` | Carries, rushing TDs, passing and receiving |

Preserve the existing standardized ridge residual calculation, frozen coefficients, and maximum relative efficiency change of 10%. Apply the factor to its declared field in each reproduced draw, then rescore the complete stat line. This includes yardage bonuses; multiplying total fantasy points or rescoring only the average stat line is incorrect.

Preserve existing minimum history/completeness checks. Pressure presently requires at least two complete matching games per side. Contact requires at least 20 matched RB carries per side. Unsupported positions retain their exact baseline. Missing pressure/contact evidence does not become a neutral-looking fabricated measurement.

### 4.2 Allowed rushing volume

Reuse `model/nfl_dfs_environment_variants.py`, `rush_factor()`, and the frozen prior in `model/nfl_dfs_workload_opponent.py`:

```text
candidate_carries = own_carries + 0.5 * (opponent_allowed_carries - league_carries)
rush_factor = clamp(candidate_carries / own_carries, 0.5, 1.5)
```

The frozen implementation applies this factor **after the team environment**, to `rushing_yards` and `rushing_tds` only, for supported QB/RB/WR/TE rows. It does not change the recorded carries, passing, receiving, K, or DST fields. Missing history or nonpositive own carries yields a documented baseline fallback.

Do not reinterpret the existing study as a physical carry-allocation model. Preserve its coefficient, prior, clamp, and scoring semantics, including its fractional historical TD draws. Changing those semantics requires a new model and registration. The existing compact shadow payload is insufficient as an optimizer distribution: reproduce the full frozen draw loop and verify its original mean, tails, and boom values before exposing the complete bundle.

### 4.3 Composition and excluded mechanisms

For a separately registered combined profile, compute both factors from the declared unmodified baseline, apply each exactly once, and rescore:

```text
RB rushing_yards = baseline_rushing_yards * rush_factor * contact_factor
RB rushing_tds   = baseline_rushing_tds * rush_factor
```

Contact must not be recomputed using already volume-scaled efficiency. Record an ordered component ledger, including any interaction contribution, so attribution reconciles to the final points delta. Passing pressure remains separate.

Keep the failed points-allowed `opponent_mode` off. Do not add adjustments from defense rankings, receiving drops, defensive injuries, sacks, touchdown rates, or PBP features merely because those fields are available. They need their own stated incremental effect and evidence contract. An approved component does not automatically qualify its combination with another component.

## 5. Resolve a complete forecast, not just a different mean

The current GPP objective uses upper-tail and boom information; cash mode uses the lower tail. Updating only `ourProj` would leave material parts of selection driven by old forecasts.

Introduce one server-resolved forecast bundle per player containing:

- Baseline run, model version, full configuration hash, availability/scenario identity, and any defensive components already embedded in that baseline.
- Defensive mode/profile/version, candidate run and artifact hashes, feature manifest, implementation and scoring versions, effective cutoff, and qualification reference when applicable.
- Baseline and selected mean, P10, median, P90, boom rate, and stat means; full draw/bank reference where used.
- Ordered applied components with factor and point contribution, or an explicit fallback reason.
- A digest of the complete selected bundle.

Use the same bundle for player ordering, candidate generation, GPP/cash objective, lineup totals, explanations, input snapshots, and saved results. Candidate mean with baseline P90/boom is prohibited. A valid zero is not missing and must not activate DK-average fallback.

Freeze the exact prepared inputs passed to optimization, including effective ownership and its source; do not snapshot only the earlier `slate.players` if resolution changed those inputs. Save or deterministically reconstruct the same eligibility/exposure/legality/ownership evidence and run-bound overrides on reload instead of clearing them. Missing QA evidence must not be interpreted as a passed check at export.

The existing sum-of-player lower/upper-tail display is not a calibrated lineup percentile. Do not label it P10/P90 for the whole lineup unless it is computed from an appropriate joint bank. Source adjustments must still reach that display consistently.

### Availability and source matching

Resolve platform eligibility and current availability first. OUT players stay zero/excluded in every mode. Never give defensive credit to an unavailable player or pay an injury transfer twice.

Use the exact post-availability baseline distribution when it can be reproduced. The existing replay tolerance is 0.00011 for saved four-decimal summaries and stat means. If opportunity redistribution created a point estimate without recoverable draws, preserve that actual baseline and record why the defensive profile was withheld. Do not fabricate a distribution from a mean and two quantiles.

Any later implementation that changes availability modeling to recover those draws is a separately versioned baseline change. Do not silently relax the current reproduction check.

The current explanation reader selects the latest capture by upload/player and is not a sufficient optimizer authorization query. The new reader must match the selected upload, canonical player, game, baseline run/configuration, availability identity, profile, and decision cutoff. Reject incompatible or post-lock captures, even when they are the newest rows. Do not mix candidate runs row by row without a frozen, explicit slate manifest.

## 6. Concrete code changes

Names for new modules below are proposed; existing entry points are the audited release names.

| Work | Existing seam / suggested implementation |
|---|---|
| Full profile distributions | Reuse `model/nfl_matchup_projection.py` and `model/nfl_dfs_environment_variants.py`; add a separate immutable distribution publisher rather than mutating protected research outputs |
| Server policy and bundle reader | Extend `web/src/db/nfl-matchup.ts` or add `web/src/db/nfl-defensive-projections.ts`; return all slate bundles in one bounded query |
| Unified resolution | Add `web/src/lib/nfl-dfs/defensive-projection.ts`; integrate with availability and `resolved-projection.ts` |
| Generation | `actions.ts`: `workspaceSlate()`, `runNflOptimizer()`, `saveOptimizerResult()` must resolve and freeze the selected defensive policy before calling the optimizer |
| Selection and presentation | `nfl-optimizer.ts`: settings validation, `projectionFor()`, `objective()`, `resolveProjectionAudit()`, roster assembly and range summaries must consume the complete selected bundle |
| Persistence | Extend optimizer run settings, input snapshots/digests and saved slot audits; use an additive migration if typed columns are required |
| UI | `nfl-dfs-client.tsx`, `player-explanation-panel.tsx`, lineup review and saved-workspace restoration must identify the actual applied profile |
| Export | Preserve the selected saved rosters and DraftKings identities; exported projection metadata/audit must reference the saved run, never recompute from current forecasts |
| Operations | Extend the existing refresh/publication path and health report with usable-profile coverage, source identity and optimizer-consumption checks |

Keep the immutable shadow evidence unchanged. Experimental use is a new consumer policy, not a rewrite of `authority: shadow_only` inside old captures. Do not flip the research adapter's `exportAuthorized` flag and assume the rest of generation is connected.

`latestProjectionRun()` currently chooses the newest season/week run without a qualified-model allowlist. Before publishing any new candidate version into production projection tables, implement an approved-policy selector. A newer research run must never become production merely because it was inserted last.

Include profile, baseline, availability, scorer, source digest, and seed in applicable cache keys. Profile changes must invalidate optimizer/scenario caches; opening an old saved run must use its pinned snapshot.

## 7. Tournament, Showdown, and lock behavior

Both single-/three-entry and multi-entry users must be able to generate adjusted lineups using the existing supported search policy. Retain salary, uniqueness, exposure, stack, archetype, and entry-count constraints. A new defensive source is not permission to relax them.

The initial implementation can use the existing marginal/tail-based construction objective. Identify it honestly. It must not claim a new calibrated contest payout or duplication advantage. Independent marginal draws do not establish correlated stack value.

If a generation path selects portfolios using a scenario bank, it must use the bank for the exact selected model/profile, with complete eligible-player support and separate selection/evaluation streams. Do not combine adjusted candidate means with an unchanged bank or silently drop uncovered players. When a compatible bank is unavailable, explicitly use the documented construction objective or make that portfolio option unavailable.

The coherent-v3 research bank changes more than PFR efficiency, including role and joint-event modeling. It remains a separately versioned model. Do not silently activate it to finish this connection, or propagate a QB-only pressure adjustment to receiver/DST scores without a separately specified configuration.

For Showdown, use one base forecast per real player and multiply its scoring result by 1.5 exactly once at Captain. Respect the salary file's Captain identity, eligibility, and salary. Defensive factors must not be applied again at the slot level. K and DST retain their supported baseline under these profiles.

**Initial scope is pre-lock generation, not late swap.** The current export path is not an entry-aware late-swap engine. Add server-side checks at generation start and before saving: no new adjusted slate generation once the first game on the saved slate has started. The UI alone is not enforcement. Reading or downloading an immutable historical lineup does not authorize editing started slots. Any future late-swap release must preserve started entries/slots and remaining salary with its own acceptance tests.

Rewriting an uploaded DraftKings entry CSV is different from downloading historical results. Before that rewrite, validate the selected saved run, its roster and QA evidence, entry identities, and current pre-lock status. Reject an entry rewrite after the first kickoff until an entry-aware late-swap engine exists; do not rely on an earlier generation-time check or browser state.

## 8. Qualification, promotion, and rollback

Implement the policy state machine now with tests for pending, PASS, FAIL, expired/incompatible qualification, and revoked policy. A client toggle cannot manufacture a PASS. Experimental mode is an explicit user-selected consumer, not a qualified production default.

For allowed carries, preserve [the opponent-term contract](./nfl-dfs-opponent-term-spec.md): eight scorable forward weeks, RB paired-MAE CI upper bound below zero, other-position no-harm requirements and position/sample floors. The approved release uses the specified `nfl-dfs-historical-v6` version and same-change shadow re-pin, followed by its frozen paired v6/v5 rollback rule. No live v6 promotion is justified by today's unplayed slate.

Once v6 embeds the allowed-carries component, the resolver must identify that exact component/configuration as `already_in_baseline` and never scale rushing a second time. Off means no additional profile beyond the selected baseline; it does not remove a component embedded in v6. Keep the pinned v5 arm explicitly for v6-versus-v5 evaluation. Include this promotion transition in the no-double-application test.

For PFR efficiency, use [the registered study index](./nfl-matchup-studies.json) and immutable registrations/amendments, including their exact cohort, sample, eight-week floor, fixed endpoint, multiplicity, materiality, and harm rules. Read those contracts rather than substituting a simplified “eight weeks passed” check. Fitted training performance is not forward qualification.

PFR models pinned to v5 do not automatically qualify on v6. Pressure/contact passing separately does not qualify their combination with allowed carries, a new availability model, or the coherent bank. Register a changed combination before capturing its eligible evidence; preserve earlier registrations and results.

An activation record must bind consumer, configuration and code hashes, scoring version, baseline, supported positions/format, verdict identity, effective time, and rollback policy. Do not invent a generic successful-study boolean. Approved defaults must fall back deterministically when the active configuration no longer matches.

Provide one operational rollback that stops new use of a profile without rewriting previously saved runs. Source incompatibility, replay failures, OUT-player violations, or scoring reconciliation failures must block affected new adjustments immediately; forecast-performance rollback follows the existing frozen statistical rule.

## 9. Acceptance tests and release evidence

All of the following are required for the usable-implementation milestone:

1. **Mechanics:** each profile changes only its allowed stat fields; zero-effect fixtures reproduce Off; component application is idempotent; bonus thresholds are rescored; OUT stays zero.
2. **Distribution consumption:** a controlled fixture with equal means but different candidate P90/boom changes GPP ranking. A corresponding P10 fixture exercises cash selection. A mean-only replacement must fail these tests.
3. **Real selection:** a deterministic near-tie slate produces a different legal selected roster when the intended defensive profile is enabled. Assert the selected IDs, consumed bundle digest, and saved values, not just displayed deltas. Real slates are allowed to retain the same roster when adjustments are small.
4. **Historical regression:** reproduce the frozen September 27 comparison by its saved IDs: 643 matched rows, 149 PFR adjustments. Examples: Breece Hall 14.23 to 15.27; Javonte Williams 14.19 to 15.05; Bucky Irving 13.79 to 13.23; Dart remains zero. Treat these as archived regression evidence, never as a current pregame capture after lock.
5. **Current end-to-end:** before kickoff, use a real current Classic slate and two saved upcoming Showdown cases. If two upcoming Showdown cases are unavailable, retain that live coverage limitation and exercise archived cases through a separate replay harness, not production actions. Never relax the lock guard to make an archived fixture pass. Generate 1, 3 and 20 entries where feasible; verify requested counts or a truthful constrained-search failure. Compare Off and Experimental with equal settings and seeds.
6. **Persistence/export:** save, reload after a newer forecast arrives, and confirm the original profile, rosters, summaries and digest remain identical. Restore or rebuild QA evidence and run-bound overrides; missing evidence must block entry rewriting. Export through the actual CSV path and validate salaries, slot identities, Captain multiplier, exposures, uniqueness and entry IDs. Test an export-time lock race separately from historical-result downloads. No old draft should be overwritten as a side effect.
7. **Fallbacks:** cover missing PFR, incomplete participants, unavailable baseline draws, changed baseline configuration, stale availability, conflicting identity, absent candidate row and unsupported source combinations. Distinguish per-player baseline fallback from whole-profile failure.
8. **Time enforcement:** a game starts during generation; a caller bypasses the UI; a post-lock candidate is newer than a valid one. Reject affected new generation/publication without altering saved historical lineups.
9. **Policy:** synthetic PASS/FAIL/NO_VERDICT and rollback tests prove only the exact allowed consumer/configuration can become approved; wrong scorer, baseline or combination remains unqualified. Do not edit live qualification data to make a test pass.
10. **Live deployment:** verify the deployed commit and perform a browser check that selects Experimental, generates, saves, reopens and downloads adjusted lineups. Prove the saved optimizer input contains the selected candidate bundle. A successful build alone is insufficient evidence of consumption.

Retain a compact release report with baseline/candidate run IDs, profiles, data/code/scoring hashes, applied/fallback counts, per-player changes, roster differences, legality/export results, and active default state. Separate forecast-only effects from changed selection policy; do not label simulated scores or roster differences as realized ROI.

## 10. Delivery order and final handoff

1. Build the immutable profile distribution/bundle publisher and strict baseline matching.
2. Implement server policy resolution and wire all optimizer objectives to the selected bundle.
3. Complete UI, save/reload, export, cutoff enforcement, and Classic/Showdown acceptance.
4. Implement and test the approved-default promotion/rollback route without falsifying present qualification.
5. Deploy, exercise the actual user flow, and publish the release report.

The final engineering report must answer plainly: **Can the user generate and export lineups with defensive adjustments? Which profiles and formats work? Are they experimental or approved? Which is the default? How many players actually used an adjustment?** If the answer to the first question is no, the optimizer connection is unfinished.

Related specifications: [matchup architecture](./nfl-matchup-data-projection-spec.md), [release evidence](./nfl-matchup-implementation-2026-09-27.md), [study operations](./nfl-matchup-study-operations.md), [GPP portfolio contract](./nfl-gpp-portfolio-improvement-spec.md).
