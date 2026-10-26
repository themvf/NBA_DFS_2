# NFL DFS ghost-lineup factorial (`nfl-ghost-factorial-v1`, registered 2026-10-16)

Status: **registered before any slate was frozen.** The first enrolled slates
are the 2026 week-6 Sunday main Classic (October 18, 1:00 PM ET), Sunday night
DAL @ GB and Monday night WAS @ SF. Nothing here changes the live optimizer,
its presets or defaults, and no ghost lineup is ever entered or exported.

## What it is

For every gradable slate, with no owner action, the system builds:

- a **house** portfolio: the current framework, frozen as code
  (`web/src/lib/nfl-dfs/ghost/house.ts`, `nfl-ghost-house-v1`), and
- **32 arms** from a fixed design over the factors below. Each arm changes
  several settings at once; each factor's effect is read across all arms with
  the slate held fixed.

All 33 portfolios use one frozen input per slate. Each is saved as a research
run before lock, and graded on Tuesday against the flagship contest. Owner
builds on the same slate are graded beside them as their own rows. The house
never reads them, and nothing waits for them.

## Population (fixed)

- **Sunday main Classic:** the DraftKings draft group whose teams are exactly
  that Sunday's games from the first noon-to-7 PM ET kickoff until 7 PM ET. A
  morning London game, "Early Only", "Afternoon Only", "Turbo" and "Sun-Mon"
  groups are excluded by that rule.
- **Every primetime Showdown:** a single-game group kicking off at 7 PM ET or
  later (Thursday, Sunday and Monday nights, plus holiday primetime games).
- These are the contests stat-api serves in full without a key, so they are
  the ones we can grade. Every other slate is skipped, with the reason recorded.
- Season 2026 from week 6 Sunday, then every week. A slate without a gradable
  flagship is reported as not graded, never dropped silently.

## Pipeline (fixed)

| when | what |
|---|---|
| continuously | `refresh_nfl_dk_pool.yml` snapshots every NFL draft group's pool: salary, position, team, status and, from 2026-10-16, DraftKings' printed average (`ppg`) |
| 105 to 20 minutes before first kickoff | `/api/cron/nfl-ghost` (Vercel, every 5 min) builds a **system slate** from the newest pool snapshot (`web/src/lib/nfl-dfs/ghost/system-slate.ts`), reads it through the owner's own workspace path, and freezes the inputs once (`nfl_dfs_ghost_freezes`) |
| same tick and every tick after | one function call per missing arm (`/api/nfl-ghost/arm`) builds from the frozen input and saves a research run plus its `nfl_dfs_ghost_arms` row in one batch. No build starts within 4 minutes of first kickoff, and none saves within 60 seconds |
| Tuesday and Wednesday | `refresh_nfl_dfs_postweek.yml` imports the flagships, then `python -m research.nfl_ghost_grade` grades every subject and writes the report |

**Amendment, 2026-10-25 (owner's decision: ghosts build from the freshest
lines).** The four freezes before this date ran at the first tick in the window
(~T-105) and all used the day-before prop lines and pre-inactive availability:
the pregame prop capture lands 60-75 minutes before kickoff (11:52 ET for the
week-6 Sunday slate, which froze at 11:15 ET). From this date a slate in its
window waits until prop lines for its first kickoff group have been captured
within 80 minutes of kickoff, which is after inactives (~T-90), then freezes.
If none have landed by T-40 it freezes anyway and its notes say `lines: STALE`
(`web/src/lib/nfl-dfs/ghost/freeze-timing.ts`). Each tick it waits writes a
`freeze_wait` event and is not a freeze failure. Every arm on a slate still
reads the same frozen input, so the factor effects are unaffected; what
changes is that the house and arms now see the projections a post-inactives
owner build would. Late games on the Sunday main slate are still frozen at the
first kickoff group's time. The design version is unchanged.

**What the system slate is:** an ordinary upload tagged `origin = 'system'`,
stored with no projection-run link and a salted slate signature. Every reader
that lists or picks uploads therefore never sees it. That covers the slate
list, slate checks, pool capture, report cards, contest linking, and the
hash-pinned matchup selectors, which join uploads to projection runs. The
freeze records the projection run it used.

**Parity check, done before registration:**
- On the uploaded SEA @ DEN Showdown, all 56 pool players matched the
  owner's CSV on name, position, team and salary.
- Every captain salary in the CSV is exactly 1.5x the flex salary, which is
  how the system slate derives captains.
- The pool's statuses were fresher than the CSV's: four players had been
  ruled out after the file was downloaded.

**Known differences from an owner build:**
- No defensive adjustments, because opponent captures are requested only for
  uploaded slates.
- No locks, exclusions, captain ranges or confirmed quarterbacks.
- DraftKings' average (which the ownership prior anchors on) is missing from
  pool snapshots captured before 2026-10-16. The freeze records how many
  players lack it.

## Factors (fixed)

The house level is marked with *.

| factor | levels | formats |
|---|---|---|
| max exposure | 35% / 55%* / 75% (Classic); 40% / 60%* / 80% (Showdown) | both |
| ownership leverage exponent | 0 (off) / 0.5* / 1.0 | both |
| projection source | market-anchored* / model's own | both |
| starter-aware absence commit | on* / off | both |
| GPP ceiling cap | 2.5x* / off | both |
| min unique players | 2 / 4* (Classic); 2* / 3 (Showdown) | both |
| cheap-player policy | no punts* / role-qualified | both |
| Winners TE/WR pool rules | on* / off | Classic |
| Winners RB/DST pool rules | on* / off | Classic |
| bring-back share | 0 / 50%* / 100% | Classic |
| Winners QB exposure cap | on* / off | Classic |
| DST against own offense capped at 5% | on* / off | Classic |
| archetype plan | Standard ceiling* / balanced / chalk captain | Showdown |
| kickers | banned* / allowed | Showdown |
| 4-2 split | banned* / allowed | Showdown |
| max captain exposure | 30%* / 50% | Showdown |
| captain positions | QB/RB/WR* / any | Showdown |

**Fixed in every arm:**
- The Winners salary floor ($48,100 from nfl-winners-shape-v6, set 2026-10-17 before the first slate).
- At least one stacked pass catcher (Classic).
- Randomness 0.08.
- The observed-history rule.
- The house's GPP signal codes.

**Lineups per arm:** 40 Classic and 20 Showdown.

**Ownership is the field model and is held fixed across arms.** It is computed
once from the house projections. An arm that changes projections changes
what the optimizer believes, not what the field is assumed to do.

Projection reverts are exact:
- **Model's own:** the model's number before the market anchor.
- **Absence commit off:** the model's number before the commit. An anchor that
  had overridden a commit keeps the overridden record, so both can be undone.

## Design (fixed)

`web/src/lib/nfl-dfs/ghost/design.ts`: 32 arms per format plus the house as arm 0.

- **Two-level factors** take odd-weight columns of the 32-run Sylvester
  Hadamard matrix. They are exactly orthogonal, and no main effect is aliased
  with a two-factor interaction among them (resolution IV).
- **Three-level factors** are balanced columns placed by a deterministic
  exchange search.
- **Worst cross-factor correlation:** 0.108 in both formats.
- **No arm repeats**, and none equals the house.
- **Pinned digests** (enforced by the test): `2a2eb6debe8a` (Classic) and
  `8c96dd79697c` (Showdown).

Changing a factor, a level, the house, the arm count or the seed is a new
version and a new cohort.

## Grading and analysis (fixed)

`model/nfl_ghost_grade.py`, `nfl-ghost-grade-v1`:

- **Scoring:** each subject is scored with the flagship's own player points
  (contest ownership rows and top users' seats), then with our weekly results
  matched by player id. A lineup with any unknown player is **unscored** and
  counted as such, never summed as zero.
- **Payouts:** thresholds come from the contest's score curve (top 1% and
  0.1%). Payouts come from the contest's own score-to-payout table, stored at
  import from complete standings, with ties already split by DraftKings. A
  ghost lineup is not inserted into the field, the standard approximation.
- **Primary outcome:** top-1% hits per scored lineup.
- **Secondary outcomes:** return per dollar entered, and completion (built
  over requested). Every Winners Showdown build so far has stopped short, so
  completion is itself an outcome.
- **Effects:** within-slate OLS of each outcome on indicators of each
  non-house level. Uncertainty comes from 2,000 resamples of whole slates.
  The test suite shows the slate bootstrap is slightly anti-conservative at
  8 to 10 slates, which is one reason candidates must be confirmed.

**Milestones:**
- **Screening:** at 8 graded slates per format. Before that the report is
  descriptive and every effect is withheld.
- **Candidates:** at screening, a level whose bootstrap two-sided p passes
  Benjamini-Hochberg at q = 0.10 becomes a **candidate**. A candidate changes
  nothing.
- **Confirmation:** candidates are re-tested on the next 8 fresh slates under
  a separately registered confirmation, with only the candidate factors
  varied. A default changes only if the candidate confirms there, and only by
  the owner's decision.
- **Expected timing:** about 3 primetime Showdowns a week, so Showdown
  screening arrives around week 9. Classic is one slate a week, so it
  arrives around week 13.

**What this cannot say:**
- GPP payouts are heavy-tailed, so most effects will not reach a verdict this
  season. Failure to detect is not equivalence.
- Outcomes are portfolio-level and slate-correlated.
- Interactions are only partly separable.
- Nothing here is a betting or entry recommendation.

## Supersedes

`nfl-classic-te-wr-pool-v1` and `nfl-showdown-kicker-rule-v1` test single
rules against an owner build, which needs an owner build each week. Both rules
are factors here, so from 2026-10-18 the runner refuses `--save` for those two
studies; dry runs still work. Their one saved pair (week-6 SEA @ DEN) remains
as descriptive history.

## Failure visibility

- **Every stage writes an event row** (`nfl_dfs_ghost_events`). That includes
  freezes that failed, arms that ran into the lock window, and arms whose
  settings the optimizer could not satisfy. An infeasible arm is stored as
  `research_failed` and counted as a result.
- **The cron's heartbeat on /health goes red** when a slate in its freeze
  window fails to freeze, when a frozen slate is still missing arms within 30
  minutes of kickoff, or when an arm build errors.
- **The NFL results page** shows each frozen slate, its build count, the
  house result, the best arm and the owner builds, plus the screening status.

## Pre-registration dry runs (2026-10-16, nothing saved)

On the live database for the week-6 Sunday slates, slate detection returned
exactly the main Classic (154941), DAL @ GB (154959) and WAS @ SF (154960).

| slate | players frozen | freeze took | house built | build time |
|---|---:|---:|---:|---:|
| main Classic | 525 | 9.5 s | 40 of 40 | 4.3 s |
| DAL @ GB | 55 | 6.2 s | 18 of 20 | 0.2 s |

Offline on the week-5 Classic fixture, Classic arms took 1.7 to 13.9 seconds.
Both dry-run system uploads were invisible to every run-joined reader and
were deleted afterwards.

## Amendment (2026-10-26): long-memory ranges from week 8

`nfl-dfs-range-split-v1` (owner's decision, docs/nfl-dfs-range-split-study.md)
changes every RB/WR/TE's floor and ceiling from week 8 on; projections are
unchanged. It reaches every arm equally: the frozen input carries the new
ranges, and `armProjection` re-applies the same ratios when an arm reverts to
the model's or the pre-commit projection. Arm effects are estimated within a
slate, so the change does not bias them, but weeks 1-7 and 8+ differ in the
house's ranges: report effects by that boundary as well as pooled.
