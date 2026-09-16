# NFL DFS — Team Opportunity Ceiling — Spec (2026-09-16)

## Origin

Week 1 2026, production report card. Three of the ten largest misses were one
team (LAR: Stafford −18.5, Bennett −14.6, Nacua −13.9) and two of the ten
largest beats were another (CAR: Coker +27.5, Young +19.8). The board that is
meant to rank *player projection quality* was substantially ranking
*team-game outcomes*.

Inside that cluster is a defect rather than variance. **Both Rams
quarterbacks were projected as if each would play**: Stafford 23.6 and
Bennett 16.0, which is 39.6 DK points of quarterback production from one
team's QB slot. No team produces that. Corroborating: 37 quarterbacks
recorded a stat line across 32 teams in Week 1.

## Mechanism (verified in code, not inferred)

`ingest/nfl_dfs_projections.py::build_week` is a per-player loop:

```python
for player in players:
    env = environment[player["team_abbrev"]]
    projection = project_player(..., context=ProjectionContext(
        team_implied_total=env["team_implied_total"]), ...)
    projections.append({...})
```

There is no team-level pass anywhere between `project_player` and
`persist_week`. Each player is projected in isolation.

`team_implied_total` is already loaded per team and *is* consumed — but as a
**scale factor**, not a budget: `model/nfl_dfs_historical.py:177` computes
`team_factor = team_implied_total / config["league_team_points"]` and applies
it to each player independently. Scaling every player of a team by the same
factor preserves whatever the sum was; it cannot constrain it.

So the projection layer has no representation of the fact that a team's
quarterback snaps, carries and targets are a finite pool.

## What this is, and what it is not

**This is a guardrail, not the cure.** The root cause is that the model has no
notion of *probability of starting*. The correct fix weights each player by
the chance he plays that role, which needs depth-chart and injury inputs —
both registered in `model/nfl_dfs_feature_audit.py` as `"Deferred"` with
`supported=False` (`depth_chart_position`, `injury_status`, `offense_snaps`,
`routes`, `red_zone_targets`). Until a source for those is contracted, a
ceiling check catches the symptom. It should not be described as fixing the
cause.

## P0 — Measure the incidence BEFORE building anything

Do not build a fix for an unquantified problem. One Week-1 anecdote is an
observation, not a rate.

Compute, over 2023–2025 from `nfl_dfs_player_week_results` (realized) and the
projection history (`nfl_dfs_player_projections` where available):

1. The realized distribution of **summed DK points per (team, week, position
   group)** — the empirical ceiling, per position group, by percentile.
2. How often projected team-position sums exceeded the realized 95th
   percentile, and by how much.
3. Of those exceedances, how many coincide with a team-week where two players
   at that position each recorded meaningful volume (two QBs with ≥ 8 pass
   attempts; two RBs with ≥ 8 carries) — the starter-change signature.

**Gate to proceed to P1:** exceedances occur in **≥ 2% of team-position-weeks**
and their mean excess is **≥ 3 DK points**. Below either bar, the Rams case is
rare enough that the check is not worth its own failure modes — record the
measurement and stop. Say so plainly if that is the answer.

## P1 — Flag only. Change no projection.

A reconciliation pass after the per-player loop, before `persist_week`:

```
for each (team, position group):
    projected_sum = Σ model_proj_fpts
    ceiling       = empirical p95 for that position group
    if projected_sum > ceiling:
        record a team_ceiling_exceedance row
```

Emitted as **evidence, never as an adjustment**. Surfaces in the weekly digest
(`ingest/nfl_dfs_review_digest.py`) and on the workspace as a pre-slate
warning: *"LAR QB projections sum to 39.6 against a p95 of 27.4 — two
quarterbacks are projected as starters."*

`MODEL_VERSION` does **not** change in P1, because no projection changes.
That is the point: it is observable for a full season at zero risk to the
numbers.

**Why flag-first is not timidity here.** This repo shipped `ff-independent-v1.7`
— a full DST history regression, unbacktested — and it scored *worse* than the
flat constant it replaced. A projection change that looks obviously right and
is not measured is the documented failure mode.

## P2 — Rescale, only if P1 earns it

Gated on P1 producing, over **≥ 8 weeks and ≥ 40 flagged team-position-weeks**:
flagged groups whose realized sum is below the projected sum **more often than
unflagged groups** (the flag predicts over-projection), with a bootstrap CI on
the difference excluding zero.

If it passes, the intervention is a proportional scale-down of the flagged
group to the ceiling, and it is a **new model version** (`nfl-dfs-historical-v3`)
with its own evidence cohort. v2 observations may not pool with v3.

Kill: if flagged groups are *not* more over-projected than unflagged ones, the
flag is detecting arithmetic (a large sum) rather than error, and P2 is dead.
Do not re-tune the percentile and re-test — that is threshold shopping.

## Metrics, fixed now

- Primary: MAE and bias on **flagged** team-position groups vs unflagged,
  walk-forward, date-clustered bootstrap.
- Secondary: per-position (QB is the sharpest case — a team has one QB slot;
  RB/WR are genuine committees and the constraint is weakest there).
- Reported always, gating nothing: exceedance rate, mean excess, and the
  starter-change coincidence rate from P0.

## Non-negotiables

- P0 before P1. No fix is built before the incidence is measured.
- P1 changes no projection and does not bump `MODEL_VERSION`.
- P2 is a new model version, not an edit to v2's semantics.
- The ceiling is fitted on seasons strictly before the week being projected —
  never on the season being graded.
- A team-level constraint is not evidence of an edge. It removes an internal
  inconsistency; whether that improves accuracy is P2's question, and the
  honest prior is that it helps a little at QB and almost not at all at WR.

## Honest expected value

The defect is real and the arithmetic is indefensible, so it is worth fixing on
correctness grounds alone. But the likely size is small: it fires on a minority
of team-weeks, concentrated at quarterback, which is a single roster slot in
Classic. Expect a fraction of a point of MAE at QB and approximately nothing
elsewhere. The larger prize is the deferred availability inputs this spec
cannot reach.
