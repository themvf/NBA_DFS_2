# NFL DFS — Opponent term: implementation spec

Written 2026-09-27 for handoff. Every file, line and number below was verified
against `main` that morning. Status: **not started**. Owner: unassigned.

## 0. What this is

Add an opponent (defense) component to the NFL DFS player projection, in the
only form that has survived a pre-registered test, on the only path this
repo allows for a projection change: grade it in shadow, promote by version.

The motivating case is Quinshon Judkins on the 2026 week-3 slate: $5,500,
9.8 projected, against a Carolina defense allowing 36.3 RB PPR points per
game (most in the league on two games; 8th most in 2025). Production's
projection row carried `opponent_factor: null`. The matchup was not in the
number at all — not underweighted, absent.

The end state: a versioned model (`nfl-dfs-historical-v6`) whose rushing
lines are scaled by the opponent's allowed rush volume, with the factor and
its evidence visible in the player's "Why?" panel, and a graded shadow
result behind the promotion.

## 1. Current state (verified 2026-09-27)

### 1.1 The plumbing exists and is switched off

`model/nfl_dfs_historical.py`:

- `MODEL_CONFIG["opponent_mode"] = "off"` (line 44), `opponent_exponent = 0.35` (line 39).
- `ProjectionContext.opponent_factor: float | None = None` (line 145).
- `opponent_factors(rows, season, mode)` (line 194): `(position, defense) →`
  DK points allowed relative to the league, shrunk with 16 league-average
  games (`OPPONENT_SHRINK_GAMES`), clipped 0.80–1.20. Modes `off | all |
  recent` (`OPPONENT_MODES`, line 190). Returns `{}` when off.
- `adjust_stat_line` (line ~254): `yardage = team_factor^0.35 ×
  clip(opponent_factor, 0.8, 1.2)^0.35`, applied to `passing_yards`,
  `rushing_yards`, `receiving_yards`, `receptions`. **Touchdowns are scaled
  by the team environment only** — the opponent factor never touches TDs.
- `feature_snapshot.opponent_factor` is recorded on every projection row
  (line 380); it is `null` in production today.

`ingest/nfl_dfs_projections.py` line 337: `build_week` already constructs
`ProjectionContext(team_implied_total=..., opponent_factor=defenses.get((position,
normalize_team(opponent))))`. With mode `off`, `defenses` is empty.

So mechanism A — *points allowed by position* — is fully wired and one
config flip from production. **Do not flip it.** See §1.2.

### 1.2 Evidence on mechanism A: DO NOT ENABLE

`model/nfl_dfs_opponent_screen.py`, result in
`artifacts/nfl_dfs_opponent_screen.json`. Pre-registered: arms OFF / ALL /
RECENT at historical-v5 with the team implied total; tuned on 2023–24,
graded once on 2025.

| arm | tuning MAE | RB |
|---|---:|---:|
| OFF | 4.7027 | 4.6016 |
| ALL | 4.7003 | 4.5993 |
| RECENT | 4.6973 | 4.5885 |

Verdict as recorded: *"DO NOT ENABLE — RECENT won tuning but the held-out
CI includes zero or favours OFF."* The tuning gap is 0.005 points of MAE.
The screen's stated reason to doubt the term stands: production already
prices the defense through the Vegas team implied total, and a
points-allowed term on top may count the same defense twice.

This verdict is not reopened by this spec. A points-allowed term is out of
scope unless a new, separately registered screen says otherwise.

### 1.3 Evidence on mechanism B: allowed rush volume — the survivor

`model/nfl_dfs_workload_opponent.py` (CLAUDE.md, "Opponent Term Study",
2026-09-21). Pre-registered; paired MAE versus the workload budget on 1,136
team-games per field, 2024 w1 → 2026 w2, weeks-clustered bootstrap.

| field | V1: own + 0.5 × (opp allowed − league) | V2: archetype pace |
|---|---|---|
| attempts | −0.054 [−0.111, +0.003] dead | dead |
| **carries** | **−0.107 [−0.164, −0.050] survives** (Bonferroni-6: [−0.184, −0.032]) | dead |
| targets | −0.051 [−0.113, +0.009] dead | dead |

Read: teams run more on defenses that get run on. Pass volume allowed is
not persistent. Effect size is small — ~0.1 carries of MAE, ≈0.07 team DK
points, a bell-cow RB moves ≤ 0.35 points on average — and it decays by
season (2024 −0.158, 2025 −0.074, 2026 +0.047 at n=62). The 0.5 weight was
a stated prior, never fitted. **It stays 0.5.**

### 1.4 The shadow variant already exists — and has no grader

`docs/nfl-dfs-environment-and-interval-studies.md` (registered 2026-09-22)
study (ii), `opp_carries`, implemented in
`model/nfl_dfs_environment_variants.py`:

- `rush_factor(prior, team, opponent)` (line 70): `v1 = own_carries + 0.5 ×
  (opp_allowed_carries − league_carries)`; `factor = clip(v1 / own_carries,
  0.5, 1.5)`; returns `None` when own or allowed history is missing.
- `_project_with_rush_factor` (line 108): v5's draw loop with
  `rushing_yards` and `rushing_tds` (`RUSH_FIELDS`, line 46) multiplied by
  `factor` **after** `adjust_stat_line`. Only those two fields differ.
- Inputs come from `model.nfl_dfs_workload_opponent.Prior(team_rows,
  plays_faced, cutoff)`: shrunk EWMA of a defense's allowed carries, using
  only rows strictly before `(season, week)`.

`ingest/nfl_dfs_shadow.py` (lines 107, 151–157) freezes it into every
shadow row's `context_variants`. State of the ledger on 2026-09-27:

| week | shadow rows | with `context_variants` |
|---|---:|---:|
| 2026 w1 | 9,105 | 0 |
| 2026 w2 | 5,193 | 0 |
| 2026 w3 | 4,025 | 4,025 (644 players carry `opp_carries`) |

The shadow job then failed every run from 2026-09-17 to 2026-09-26 on the
study-pin guard (re-pinned in #283). Confirm whether week 4 froze before
its kickoffs; if not, week 4 is lost to the study and the eight-week floor
moves out one week.

**Nothing grades `context_variants`.** `ingest/nfl_dfs_reportcard.py`
scores four variants — `production`, `shadow_baseline`, `opportunity`,
`efficiency_research` — and no code outside the two files above references
`context_variants` or `opp_carries`. The registered gate cannot currently
produce a verdict.

The registered gate (quoted):

> (ii) `opp_carries` — opponent allowed-rush volume improves RB rushing
> lines. PASS: paired MAE CI upper bound < 0 at **RB** (QB/WR/TE must not
> have a lower bound > 0). Kill: CI includes zero at RB. No verdict before
> the eighth forward week is scorable. A PASS licenses a production
> candidate with its own version bump and shadow re-pin, never a direct
> config edit. A FAIL closes the variant; a different weight, clamp, bucket
> or prior is a new registration.

## 2. The decision

Build mechanism B and only mechanism B, on the registered path:

1. **Grade what is already frozen** (WP1). This is the missing piece and the
   first deliverable.
2. **Keep freezing** through the eighth scorable forward week (WP2).
3. **On PASS, ship `nfl-dfs-historical-v6`** with the carries term as a
   first-class part of the model, re-pin the shadow study, and expose the
   step in the UI (WP3, WP4).
4. **Grade v6 against v5** on the slate report cards after promotion (WP5).

On FAIL, the variant closes and WP3–WP5 are not built.

## 3. Definition of the term (exact, frozen)

For a player on team `T` facing defense `D` in week `w` of season `s`:

```
own      = Prior.own(T, "carries").mean          # shrunk EWMA of T's carries, rows < (s, w)
allowed  = Prior.allowed(D, "carries")            # shrunk EWMA of carries D has allowed, rows < (s, w)
league   = Prior.league_mean["carries"]
v1       = own + 0.5 × (allowed − league)
factor   = clip(v1 / own, 0.5, 1.5)
```

Applied, after the team-environment adjustment, to exactly
`rushing_yards` and `rushing_tds` in each simulation draw. Positions QB,
RB, WR, TE. DST excluded by design. Passing and receiving fields are
untouched — the pass-volume terms are dead.

Missing inputs (no own history, no allowed history, `own ≤ 0`) → `factor =
1` and the audit step reads `not_applied` with the reason. Never a silent 1.

Constants — the 0.5 weight, the [0.5, 1.5] clamp, `RUSH_FIELDS`, the
Prior's shrinkage — are frozen. Re-fitting any of them on graded data is a
new registration, not a tuning pass.

Relationship to the team implied total: the environment factor prices
*points*; this term prices *rush volume*. They overlap but are not the same
quantity, which is why the screen's double-count doubt for mechanism A does
not transfer directly. The grading in WP1 answers whether the overlap
matters, since the variant is scored on top of `env_baseline`.

## 4. Work packages

Follow the repo's delivery order: data contract → schema → grading → model
→ UI → tests → real-data run → visual check → docs. Each package lists its
acceptance criteria; a package is not done while any criterion fails.

### WP1 — Grade the frozen variants (blocks everything else)

**Build:** a scorer for `context_variants` and a study script that applies
the registered gate.

- Extend `ingest/nfl_dfs_reportcard.py` so each shadow row's
  `context_variants` entries (`env_baseline`, `env_trailing`,
  `opp_carries`, `interval_rq`, `prior8`) become forecast rows with
  `variant = context:<name>`, on the identical population and outcomes as
  `shadow_baseline`. Reuse its mean/median/p10/p90/boom fields.
- New `model/nfl_dfs_context_variant_study.py`: for each variant, paired
  MAE delta versus `env_baseline` per position, weeks-clustered bootstrap
  (mirror `model/nfl_dfs_workload_opponent.py`), one row per player-week.
  Encode the PASS/kill rules from §1.4 verbatim. Refuse a verdict until
  eight forward weeks are scorable; print the count and say so.
- Write results to `artifacts/nfl_dfs_context_variant_study.json` with
  `n`, weeks, per-position delta and CI, and the verdict string.

**Acceptance:**
1. Running the study on today's ledger prints `no verdict: 1 of 8 forward
   weeks scorable` (or the correct count) — not a number dressed as a
   result.
2. `tests/test_nfl_dfs_context_variant_study.py`: a synthetic ledger where
   `opp_carries` is strictly better at RB and neutral elsewhere yields PASS;
   better at RB but with WR lower bound > 0 yields no PASS; fewer than
   eight weeks yields no verdict regardless of the numbers.
3. The report card's variant count for week 3 shows 644 `context:opp_carries`
   rows, matching the freeze.

### WP2 — Keep the freeze alive

- Verify week 4 froze with `context_variants` before its first kickoff
  (query `nfl_dfs_shadow_predictions` by week). Record the answer in
  `docs/nfl-dfs-environment-and-interval-studies.md`.
- The research job that freezes it is `refresh_nfl_dfs_research.yml`
  (split out 2026-09-28), which runs after each green
  `refresh_nfl_dfs_projections.yml`, dispatched by `/api/cron/dispatch`
  (job `nfl-projections`, 13:35/21:35 UTC daily, 16:05/19:05 UTC Sundays).
  Add a `pipeline_health` check: a regular-season week with zero
  `context_variants` rows by Saturday 21:35 UTC is a failure, not a quiet
  week.

**Acceptance:** the health report names the check; a synthetic empty week
trips it.

### WP3 — Production candidate `nfl-dfs-historical-v6` (only on PASS)

- Move `rush_factor` from `model/nfl_dfs_environment_variants.py` into
  `model/nfl_dfs_historical.py` as the single implementation; the variants
  module imports it. One formula, two callers.
- `ProjectionContext` gains `opponent_carries: OpponentCarries | None`
  carrying `factor, own_carries, opp_allowed_carries, opp_games,
  league_carries`. Keep `opponent_factor` (mechanism A) untouched and off.
- `adjust_stat_line` applies `factor` to `RUSH_FIELDS` after the environment
  scaling, only when the context carries it.
- `MODEL_CONFIG` gains `"opponent_carries_weight": 0.5` and
  `"opponent_carries_clamp": [0.5, 1.5]`; `opponent_mode` stays `"off"`.
- `build_week` constructs one `Prior` per (season, week) — it is indexed
  once per week by design — and passes the context for every skill
  player. Team abbreviations go through `normalize_team` (LA/LAR, WAS/WSH,
  AZ/ARI, JAC/JAX) on both the player's team and the opponent.
- `feature_snapshot` records the five fields above; `stat_means` reflects
  the scaled rushing fields so the web layer's opportunity redistribution
  and linear scoring see the same line the projection was built from.
- Bump `MODEL_VERSION` to `nfl-dfs-historical-v6` with a note in the same
  style as the v5 note (what changed, the study, the numbers).
- **Re-pin the shadow study in the same PR** (`python -m
  ingest.nfl_dfs_research --source-root <main checkout> --draws 200
  --persist`, then point `artifacts/nfl_dfs_shadow_config.json` at the new
  run). Editing `nfl_dfs_historical.py` without this breaks the shadow job
  on its next run — see `docs/nfl-dfs-implementation-handoff-2026-09-22.md`
  §"Shadow re-pin". The gate window may need an amendment; record it.

**Acceptance:**
1. `tests/test_nfl_dfs_historical.py` (extend): a player with `factor 1.15`
   sees rushing yards and rushing TDs scaled by 1.15 and every other stat
   unchanged; `factor None` reproduces v5 draw for draw (same seed).
2. The v6 production run on the next slate writes `feature_snapshot.
   opponent_carries` for every skill player, `null` with a reason string
   for anyone whose team or opponent has no history.
3. `python -m pytest tests/test_nfl_dfs_shadow*.py` passes after the re-pin;
   the shadow job's next scheduled run is green.
4. No change to K or DST projections (assert equality against v5 on a real
   slate).

### WP4 — Show it in the web app

- `web/src/lib/nfl-dfs/projection-audit.ts`: add an audit step `"Opponent
  rush volume"` via the existing `add(label, reason, points, calculation)`
  contract, with `points` = the projection delta attributable to the
  factor (compute by re-scoring the stat line with and without it through
  `scoreNflOffenseLinear`, the same marginal method the redistribution
  layer uses) and `calculation` = the five snapshot fields. `not_applied`
  with the reason when the factor is `null` or 1.
- `web/src/app/dfs/nfl/player-explanation-panel.tsx`: render the step where
  the environment step renders, in the same style; show the opponent's
  allowed carries against league and the games behind it.
- The pool "Why?" panel already reads `projectionAudit.steps`; no new
  surface.

**Acceptance:** on a real slate, Judkins-class row shows the step with a
non-zero delta and its inputs; a DST row shows no such step; a player with
missing opponent history shows `not_applied` and the reason. Screenshot
attached to the PR.

### WP5 — Grade the promotion

- The slate report card (`nfl-dfs-slate-report-v1`) and the weekly report
  card already grade production. Add the previous model version as an
  alternative forecast stream on the same population for the first four
  weeks after promotion, so v6 vs v5 is a paired comparison, not two
  separate scores.
- Rollback rule, frozen now: if v6's RB paired MAE versus v5 has a lower
  bound > 0 after four scorable weeks, revert `MODEL_VERSION` to v5 and
  re-pin. Not a tuning pass.

**Acceptance:** the comparison is printed by the existing report-card
command with both streams named; the rollback rule is in this doc and in
the PR.

## 5. Data and point-in-time contracts

- **Prior** rows are strictly before `(season, week)`. A defense's
  allowed-carries series never includes the target game or any later one.
  `plays_faced` comes from `nfl_pbp_archetypes` (`defteam`, per game);
  team rows from `ingest.nfl_dfs_workload.raw_history`.
- The projection run's `history_cutoff_week` is **exclusive**: a run with
  cutoff week N is valid for a week-N slate (CLAUDE.md, "Projection
  Compression Study"). The opponent inputs must respect the same cutoff.
- Missing is `None`, never 0. A defense with no history yields `factor = 1`
  and a recorded reason.
- Team codes: `normalize_team` on both sides. A mismatch produces a silent
  `None`, which the audit step must surface as `not_applied`, so a
  code-mapping regression is visible on the page rather than as a quiet
  return to v5 behavior.
- Opponent inputs are frozen in `feature_snapshot`; the page never
  recomputes them from today's data.

## 6. Acceptance criteria (delivery contract)

| # | requirement | implementation | test | evidence |
|---|---|---|---|---|
| 1 | `opp_carries` is graded per the registered gate | WP1 study script | synthetic PASS / no-PASS / no-verdict | `artifacts/nfl_dfs_context_variant_study.json` |
| 2 | Report card scores context variants on the shadow population | WP1 reportcard | week-3 count = 644 | report-card output |
| 3 | An empty freeze week is a health failure | WP2 | synthetic empty week | pipeline health report |
| 4 | v6 scales only `rushing_yards`/`rushing_tds`, by the frozen formula | WP3 | factor 1.15 / factor None | pytest |
| 5 | v6 records the factor and its inputs on every row | WP3 | real-slate run | projection rows |
| 6 | K/DST unchanged v5 → v6 | WP3 | equality on a real slate | pytest |
| 7 | Shadow re-pinned in the same PR; shadow job green | WP3 | shadow tests | workflow run |
| 8 | Step visible in the Why? panel with inputs; `not_applied` when missing | WP4 | — | screenshot |
| 9 | v6 vs v5 paired on the same population for four weeks; rollback rule frozen | WP5 | — | report-card output |

No row may be marked done by compilation, a merged commit, or a schema
column alone.

## 7. Non-negotiables

- Mechanism A (`opponent_mode`) stays `off`. Its screen said no.
- No production change before WP1 returns PASS at eight scorable weeks.
  "Directionally right on three weeks" is not a result; the compression
  study and the DST work in CLAUDE.md are the record of what that costs.
- The 0.5 weight and [0.5, 1.5] clamp do not move. If the variant fails,
  it fails; a different constant is a new registration.
- Pass-volume opponent terms (attempts, targets) are not built. Both died.
- A projection-model edit and its shadow re-pin ship together.
- Every number the page shows comes from the frozen `feature_snapshot`.

## 8. Worked example (illustrative numbers, real structure)

Judkins, 2026 week 3, production v5: `stat_means.carries 13.49`,
`rushing_yards 38.80`, `rushing_tds 0.203`, projection 9.83.

Suppose at freeze the Prior said Cleveland's own carries 24.0, Carolina's
allowed 29.5 over 2 games (shrunk), league 25.5. Then
`v1 = 24.0 + 0.5 × (29.5 − 25.5) = 26.0`, `factor = 26.0 / 24.0 = 1.083`.
Rushing yards 38.80 → 42.0, rushing TDs 0.203 → 0.220. Marginal DK points:
`0.1 × 3.2 + 6 × 0.017 ≈ +0.42`. Projection ≈ 10.3.

That is the size of effect to expect: a few tenths of a point on a
mid-volume back, a point or so on a bell-cow against an extreme defense.
The study measured ≈0.07 team points of MAE. If an implementation moves a
player by several points, something other than this term is happening.

## 9. Out of scope

- Enabling `opponent_mode` (points allowed). Screened: DO NOT ENABLE.
- Opponent terms on passing attempts or targets. Screened: dead.
- DST projections. The DST screens in CLAUDE.md found no persistent
  defensive signal; this term is about the *offense's* rush volume.
- Re-fitting any constant on graded weeks.
- Ownership. The prior (`nfl-ownership-prior-v1`) reads the projection;
  it needs no change for v6.

## 10. References

- `model/nfl_dfs_historical.py` — model, config, `opponent_factors`, `adjust_stat_line`
- `model/nfl_dfs_environment_variants.py` — `rush_factor`, `_project_with_rush_factor`, `RUSH_FIELDS`
- `model/nfl_dfs_workload_opponent.py` — `Prior`, `plays_faced`, the study
- `model/nfl_dfs_opponent_screen.py`, `artifacts/nfl_dfs_opponent_screen.json` — mechanism A verdict
- `ingest/nfl_dfs_shadow.py` — freeze; `ingest/nfl_dfs_reportcard.py` — grading
- `ingest/nfl_dfs_projections.py` — `build_week`, `ProjectionContext` assembly
- `docs/nfl-dfs-environment-and-interval-studies.md` — WP6/WP7 registration, study (ii)
- `docs/nfl-dfs-implementation-handoff-2026-09-22.md` — shadow re-pin procedure
- `web/src/lib/nfl-dfs/projection-audit.ts`, `web/src/app/dfs/nfl/player-explanation-panel.tsx` — UI contract
- CLAUDE.md sections: "Opponent Term Study (2026-09-21)", "Red-Zone Trips", "Projection Compression Study", "Two forward accuracy streams"
