# NFL DFS ownership — handoff spec (2026-10-19, after the week 6 Monday Showdown build)

For an agent with database access (`DATABASE_URL` / `.env.local` in `web/`)
continuing the ownership work. Read in this order: this file,
`docs/nfl-ownership-scorecard-v1.md` (the grading contract, DRAFT),
`docs/nfl-ownership-review-2026-10-19.md` (the evidence and the corrections),
`docs/nfl-ownership-model.md` (prior v1-v4 and the Phase 2 promotion gate),
and the "Field-ownership prior" section of `CLAUDE.md`. Every claim below
that came from a database was made by an independent reviewer, not by the
authoring session, which had no database; re-verify before building on it.

## 1. State of the code

Merged to `main` (PR #477, squash `b76d66e`, 2026-10-19 ~23:20 UTC):

- `web/src/lib/nfl-dfs/ownership-prior.ts`: `nfl-ownership-prior-v4` is the
  default. Classic budgets QB 100 / RB 230 / WR 335 / TE 135 / DST 100.
  Showdown: punt tier (salary < $3,000 shares 60 of 500 flex points on
  points^1, cap 30, unused slice returned), kicker flex x 0.6, DST x 1.3 /
  0.7 by `favoriteTeam` / `underdogTeam`, captain x TE 0.7 / K 0.25 / DST
  0.25. `variant: "v3"` reproduces v3 exactly. Result carries `notes`
  (`puntTierPlayers`, `puntTierPct`, `gameScriptKnown`).
- `web/src/app/dfs/nfl/actions.ts` `attachOwnership` passes `team` and the
  slate's favorite/underdog. The prior is computed AFTER availability
  zeroing, the absence commit and the market anchor, so `ourProj` for an
  anchored player is already the market number.
- `web/scripts/nfl-ownership-calibration.ts`: averaged tied ranks; a missing
  label is a reconciled zero only when the contest's recorded mass is
  >= 99.5% of roster slots, else the row is `unresolved` and excluded;
  prints the outcome-selected >= 1% diagnostic beside full-pool numbers.
- Tests: `test:nfl-ownership-prior` pins v3 through the variant and asserts
  v4 behavior; `tests/test_nfl_showdown_ownership_eval.py` pins the Python
  mirror of v3 (`model/nfl_showdown_ownership_eval.prior_showdown`). There
  is NO Python mirror of v4.

On branch `claude/nfl-dfs-ownership-projection-r5m0fr` (restarted from the
merged main, three doc/comment commits, not in a PR): the v4 note says WAS@SF
and week 6 (the merged copy says HOU@SEA / week 7, which is wrong), and the
review doc's regrade step is reworded. Fold these into your first PR.

Dormant, never integrated: `research/nfl_ownership.py` + `model/nfl_ownership.py`
(a ridge on log ownership, 16 features, trained on two contests,
`trained_forward_folds: 0`, artifacts in `artifacts/nfl-ownership-v1/`), and
`model/nfl_ownership_model.py` (3-feature logistic). Three ownership code
paths exist; decide one (task 7).

## 2. Data you will use

| table | content | caveats |
|---|---|---|
| `nfl_dfs_field_contests` | one row per imported contest: format, season, week, `slate_upload_id`, `entry_count`, `score_curve` (full standings only), `payout_curve` | `slate_upload_id` may be NULL when mass coverage was below 0.95 at import; such contests are skipped by the calibration script |
| `nfl_dfs_field_ownership` | per contest: `normalized_name`, `drafted_pct` (overall), `drafted_by_slot` (`{"CPT":x,"FLEX":y}` in Showdown, `{position: x}` in Classic), `fpts` | stat-api imports assemble ownership from fetched users' seats; a player no fetched user captained gets `CPT: 0.0` SYNTHESIZED (`captain.get(key, 0.0)` in `model/statapi_contest.py`). Treat those as `synthesized_zero`, never as observed |
| `nfl_dfs_field_structure` | duplication, multi-entry share, stack rates, pair lift per contest | Showdown stack rates come from top-100 users (`population: top_users`), outcome-selected; Classic from the whole field |
| `nfl_dfs_field_top_entries`, `nfl_dfs_field_user_builds` | top finishers' lineups | outcome-selected; never a training target for field ownership |
| `nfl_dfs_pool_captures` | pregame pool with resolved player evidence; one capture inside 24 h of first kickoff, then every tick in the final 20 minutes (`poolCaptureDue`, `web/src/lib/nfl-dfs/pool-audit.ts`) | contents per week are NOT yet inventoried (task 1) |
| `nfl_dfs_optimizer_runs` (`input_snapshot`, `settings`) | frozen inputs of saved builds, including `ownPct` and per-player `marketAnchor` | exclude `settings ? 'researchStudy'` rows; system slates have `origin='system'` and must be excluded from owner-facing readers |
| `nfl_dfs_ghost_freezes` | one frozen input per ghost slate (Classic ~5 MB compressed) | present only from 2026-10-16 |
| `nfl_dfs_published_winners` | 46 Millionaire rank-1 lineups 2023-25 with draft % per player | strategy evidence only; cannot calibrate ownership |

Reconciled facts from the independent review (verified by them, not by us):
week-4 Millionaire frozen forecast MAE 1.065 over 629 salary-listed players
with zero-filled missing labels, 3.538 over the 183 observed-label players,
same forecast; recorded mass 895.44 of 900%; PIT-CLE (196187074) and ATL-NO
(196285155) both really have 88,235 entries; recorded TE totals weeks 3-5
137.71 / 134.09 / 137.12%; tie-correct Spearman week 2 full pool 0.663,
frozen week 4 / 5 forecasts 0.661 / 0.710 on observed-label populations.

## 3. Tasks, in dependency order

Each task names its acceptance criteria. Report state per the delivery
contract in `CLAUDE.md` (built / tested / backtested / prospectively
validated), never "done".

### Task 1 — Inventory frozen forecasts and pool captures

Produce a table, one row per (slate, source, capture time): format, week,
contest(s) linked, which fields the capture holds (projections, availability,
DK status, `ownPct` + version, per-player `marketAnchor`), capture time
relative to first kickoff, and whether a canonical-horizon capture (last one
at or before kickoff minus 20 min) exists. Sources: `nfl_dfs_pool_captures`,
`nfl_dfs_optimizer_runs.input_snapshot` (owner builds only), `nfl_dfs_ghost_freezes`.
Count DISTINCT slates separately from captures. Write it to
`artifacts/nfl-ownership-scorecard/inventory-<date>.md`.
Accept: every 2026 slate with an imported contest appears, with a stated
reason when no frozen forecast exists. Do not claim the history grew until
the table says so.

### Task 2 — Label provenance

For every row in `nfl_dfs_field_ownership`, classify per scorecard section 3:
`observed`, `reconciled_zero`, `synthesized_zero`, `unresolved`. Needs the
import path per contest (DraftKings export vs stat-api; stat-api mass and
whether the full standings were fetched). Add the class as a column or a side
table (`nfl_ownership_label_provenance`, append-only, keyed by contest and
normalized name) with a version tag. Accept: counts by class per contest;
every stat-api captain zero is `synthesized_zero`; a test asserts the
classifier on a fixture with a seen-only-as-flex player.

### Task 3 — Freeze the scorecard

Settle the open constants in `docs/nfl-ownership-scorecard-v1.md`:
`N_RELEVANT` per position, predicted-chalk thresholds, salary-tier edges, the
stat-api mass floor for reconciled zeros. Move status DRAFT to REGISTERED with
a date, before any grade from task 4 is read. Changing a constant afterwards
is `scorecard-v2`.

### Task 4 — Baseline regrade, weeks 2 to 6

Run the scorecard once over every frozen forecast from task 1: both horizons
where available, all three populations, Spearman with averaged ties, label
classes from task 2, split by the anchor evidence STORED per player (anchored
/ unanchored counts per slate), ownership version, and capture date. Write
rows to an append-only `nfl_ownership_scorecard_rows` (proposed in the
scorecard section 7) and a readable artifact. v4 appears only for week 6
onward; weeks 2-5 are tagged `design` for v4 (its constants were read from
them) and for v3 Showdown (weeks 2-3). Accept: one row per (contest,
horizon, version, population); the week-4 1.065 / 3.538 pair reproduces as
two population rows of one forecast; no number from this run changes any
constant.

### Task 5 — Grade v4 on week 6 and after

The WAS@SF Showdown (2026-10-19) is the first v4 observation. After Tuesday's
post-week job imports the flagship contest, grade it under task 4's machinery
and record: punt-tier players' predicted vs actual flex, kicker, both DSTs,
captain shares by position, and the full-pool and relevant-population
metrics beside the v3 variant on the same frozen input (`variant: "v3"` makes
the counterfactual exact). Accept: a per-slate row for each of v3 and v4;
no verdict until the scorecard's floors.

### Task 6 — Python mirror of v4

Port v4 to `model/nfl_showdown_ownership_eval.py` as `prior_showdown_v4` (keep
`prior_showdown` as the v3 mirror; its pins must not move), pinned to the TS
numbers on the same fixture pool in both `tests/test_nfl_showdown_ownership_eval.py`
and `web/scripts/test-nfl-ownership-prior.ts`. Then run
`python -m research.nfl_showdown_ownership_eval` with v3 and v4 inputs on the
imported Showdowns: projected points kept at equal real-field duplication.
Accept: both languages agree to 0.02 on every fixture row; the harness result
is reported per contest with the seed set stated.

### Task 7 — One ownership code path

Decide between integrating `research/nfl_ownership.py` as the Phase 2 fitted
model or deleting it, and delete `model/nfl_ownership_model.py` unless
something imports it. If integrating: fit as a residual on top of the prior
(log target, prior log-share as a fixed offset) AND as a direct model, compare
both against the prior under task 4's chronological, slate-level holdout.
Features: DK average, market-implied projection (`nfl_prop_implied_lines`),
our projection, their gaps (sign LEARNED, not assumed), salary tier,
`priorOpportunity`, depth rank, DK status change inside 48 h,
`nfl_season_games.market_spread_line` / `market_total_line`. The fitted
source declares itself non-heuristic and supplies the calibration record;
`assessOwnership` already refuses `validated` without one. Accept: the Phase 2
gate (Spearman >= 0.70 tie-correct, MAE <= 2.0, |bias| <= 0.5, >= 4 held-out
Classic slates) evaluated on scorecard population 1; a FAIL leaves the prior
in place and is recorded.

### Task 8 — Learned budgets and Showdown slot models (after task 7's first result)

Classic: predict next slate's RB/WR/TE FLEX split and DST demand from prior
weeks with shrinkage, never from the graded slate's own totals. Showdown:
captain by position from FULL-FIELD captain shares (`drafted_by_slot.CPT`,
`observed` class only), flex salary-tier and kicker terms, team split from
measured rates, and the duplicated-lineup forecast for the duplication
model. Each is a new prior version graded under the scorecard.

### Task 9 — Field generator (after 8)

A chalk simulation or constrained joint model that produces pair
co-ownership and salary feasibility, accepted only on agreement with complete
fields on exposures, stacks, salary spend and duplication
(`nfl_dfs_field_structure`), never on seed count. Feeds the duplication model
and the portfolio objective's field sampler (`nfl-field-stacked-v2`, which
failed its freeze gate on level, not shape).

## 4. Rules that bind every task

- Never tune a constant against the slates it is graded on; weeks 2-5 are
  v4's design slates. A changed constant is a new version.
- Two populations are always reported, neither substituted: full pool with
  verified labels (the gate) and the pregame-defined relevant set. The
  actual >= 1% cut is a diagnostic.
- Label provenance classes are never collapsed; `synthesized_zero` and
  `unresolved` never enter metrics or training.
- Holdout by slate, never by row; labels usable only when
  `labels_available_at <= captured_at`.
- Split by the anchor evidence stored per player, not by the 2026-10-12 date.
- Exclude research runs (`settings ? 'researchStudy'`) and system slates
  (`origin='system'`) from "our build".
- Winners and top users are outcome-selected: strategy evidence, never
  ownership targets.
- Do not set a read-only session setting on `DATABASE_URL`; SELECTs only.
- Commit locally, one PR per batch, no push without the owner's go-ahead
  except a pre-lock fix. Run `npm run test:nfl-ownership-prior`,
  `test:nfl-gpp-ownership`, `tsc`, ESLint on changed files and the Python
  tests before any push. CI runs every `test:*` script.

## 5. Commands

```
cd web && npm ci
npm run calibrate:nfl-ownership                      # tie-correct, label-aware, read-only
npm run test:nfl-ownership-prior
python -m pytest tests/test_nfl_showdown_ownership_eval.py -q
python -m research.nfl_showdown_ownership_eval --contest ID=PATH ...
python -m research.nfl_ownership --help              # dormant ridge pipeline
python -m ingest.statapi_contest --date 2026-10-19 --sport nfl --format showdown   # tonight's flagship, after it closes
```
