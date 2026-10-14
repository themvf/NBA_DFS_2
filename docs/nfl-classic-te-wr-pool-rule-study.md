# NFL DFS Classic: do the Winners-shape TE and WR pool rules cost us? (pre-registered 2026-10-13)

Status: **registered; capture runner implemented, no forward slate enrolled by this change.** No optimizer or setting change follows
from this document; it fixes the test before any outcome is seen.

## Why this exists

The read-only Classic attribution study
(`artifacts/nfl-classic-attribution-2026-10-13/report.md`, PR #428) found, on
the only two slates where the rules can be measured:

- With perfect information, the Winners-shape TE rule (keep tight ends under
  $4,000 with 4+ targets a game) and WR rule (keep receivers with 25%+ of
  their team's targets, or under $5,000) together cost the week-5 optimum
  about 32 points on their own, and removing either from full Winners
  recovered 20-23 points.
- Every one of week 5's verified top-100 entries, and 98 of week 4's, used a
  player those rules exclude (96/100 an excluded tight end, 90/100 an
  excluded receiver in week 5).

That is discovery evidence from the same two slates, hindsight-only, with a
partially reconstructed week 4. It motivates this test; it cannot confirm it.
The study does not support blaming most of our Classic underperformance on
these rules: selection regret is far larger and is descriptive only.

## Hypothesis

H: on Classic main slates, the current Winners shape with only its TE and WR
pool rules removed produces portfolios at least as good as current Winners
shape, by the metrics below.

## Arms (identical except the two rules)

- **A, current:** Winners shape (`nfl-winners-shape-v4` or its successor at
  the time; the version is recorded per slate).
- **B, test:** the identical configuration with the TE and WR pool rules
  switched off. RB and DST pool rules, salary floor, stacking, bring-back
  share, QB cap, DST-vs-offense share, exposure, uniqueness, plan, leverage
  setting, projections, ownership, locks and exclusions are all identical.

Both arms are built pregame from the same frozen pool and saved as optimizer
runs, at the same lineup count as the slate's real build (default 20 if none).
Leverage is held at whatever the real build used; it is not a factor here.

## Population and timing

Every DraftKings Classic main slate from 2026 week 6 on that has a stored
contest with a score curve and top-100 entries (the weekly flagship import).
Weeks 1-5 are discovery and are excluded. Both arms must be saved before the
slate's first kickoff; an arm saved after kickoff voids that slate.

## Metrics (fixed now)

Per slate and arm:

1. **Feasibility:** lineups requested vs built.
2. **Median lineup points** (primary). Paired difference B - A per slate.
3. **Top finishes:** number of lineups inside the contest's top 1% and top
   0.1% (from the stored score curve).
4. **Best lineup rank** (secondary only; a single draw).
5. **Excluded-player participation:** share of arm-B lineups using a player
   arm A's rules exclude, and the same share among that contest's verified
   top-100 entries (entry-weighted and unique lineups).

Players with no recorded score are resolved with
`research/nfl_attribution_resolve_missing.py`; verified non-participants score
0, unresolved players leave a lineup unscored and are counted.

## Decision rule (fixed now)

- No verdict before **8 eligible slates**.
- **B is supported** if the paired median difference B - A is positive in at
  least 6 of 8 slates and its mean is positive with a slate-clustered bootstrap
  95% interval above zero, and B's top-1% count is not lower in total.
- **B is rejected** if the mean paired median difference is negative with the
  interval below zero.
- Otherwise **inconclusive**; the rules stay and the test continues to 16
  slates, with the same bars.
- One look per milestone (8, then 16). The bars do not move.

## What this test cannot say

It compares two portfolio constructions on the same projections. It does not
establish future profitability, an optimal exposure level, or that the rules
explain our results. Eight slates are little evidence; the bars are set to
detect only a large, consistent effect.

## Required before the first slate

A research runner now implements this capture:
`research/nfl_pool_rule_study.py --study classic`, with the unchanged
production optimizer called by `web/scripts/research-nfl-pool-rule-study.ts`.
The same runner serves the Showdown kicker study
(`docs/nfl-showdown-kicker-rule-study.md`); the two never share a cohort.

From the repository root, with Python dependencies, `web/node_modules`, Node,
and `DATABASE_URL` configured:

```powershell
python -m research.nfl_pool_rule_study --study classic --source-run <UUID> --main-slate --out artifacts/classic-pool-week6-dry
python -m research.nfl_pool_rule_study --study classic --source-run <UUID> --main-slate --out artifacts/classic-pool-week6-saved --save
```

Choose the source run explicitly; the runner does not infer that the largest
saved build was entered. It requires a saved Classic GPP build and keeps its
requested lineup count and settings. `--main-slate` attests the weekly main
slate. Postgame flagship score-curve/top-100 coverage remains an enrollment
condition to check later, not a source of pregame selection inputs.

Without `--save`, the database is read-only. With it, both arms (including
failed or partial builds) are inserted in one transaction before the first
canonical kickoff. There are no fallback settings or retries with loosened
constraints. An advisory lock and existing-pair check permit only one saved
pair per study/season/week; repeating the command cannot silently reroll it.
Any second-arm write failure or expired lock check rolls back the entire pair.

The repository schema migration must be installed before `--save`. It adds
`research_complete`, `research_partial`, and `research_failed` to the existing
optimizer run status constraint. Ordinary saved-build lists already exclude
these statuses, and export rejects research runs. Neither source runs nor
default settings are updated. The runner itself never runs DDL, but the
migration lives in `db/schema.py` MIGRATIONS and `web/src/db/ensure-schema.ts`,
so merging it installs it on the next Python job or web cold start; `--save`
refuses until the widened constraint is present.

Every reader that picks "our build" from the optimizer ledger excludes research
arms explicitly (`status IN ('complete','partial') AND NOT (settings ?
'researchStudy')`): the saved-runs list, `research/nfl_top_builds.py` (and so
the weekly flagship review), both attribution exporters and
`research/nfl_archived_contests.py`. Key on `researchStudy`, never on
`buildShape`: arm A is a Winners build stored as `standard`.

`--save` refuses a working tree with uncommitted tracked changes; the dry run
records the dirty state and file list in the captured implementation.

The source run's input digest is checked before generation. Snapshot aliases
for projections, ownership, DK status and forecast adjustments are restored
without changing values. Missing `priorOpportunity` is captured before kickoff
using the product's prior-three-week queries and projection label cutoff; it is
explicitly a **new pregame capture**, not evidence frozen with the original
run. Participant rows lack historical version timestamps. Both arms share the
identical enriched snapshot, including null records and query provenance. If
the source is Winners and has a saved exclusion report, captured exclusions
must match it exactly; otherwise enrollment is refused pending a fresh source.

The adapter resolves Winners using `applyWinnersShape`, retaining minimum-pool
fallbacks and lock exceptions. It calls the optimizer with the resolved
settings and exclusions, removing only WR/TE shape exclusions for arm B. Thus
the stored effective `buildShape` is `standard` (to prevent reapplying the
preset); `settings.researchStudy` records the actual arm, Winners version,
common settings, shape notes, rule exclusions and source identity. Manual
exclusions remain in both arms. Regression tests require arm A to match native
Winners generation. The optimizer's fixed jitter seed is 20260902 in both arms.

Each new output directory preserves input and pair JSON, compressed copies,
SHA-256 hashes of the uncompressed bytes, code-file hashes, and (after save) a
receipt with both run IDs. The same snapshots, settings, input digests and
research metadata are saved in the optimizer ledger. An existing output
directory is never overwritten. No schedule/automation is installed: invoke
the command before kickoff for each eligible slate. No forward performance
claim follows from the mechanical regression tests.
