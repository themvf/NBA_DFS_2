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

## Correction (2026-09-16): injury data IS contracted

An earlier draft of this spec claimed the availability inputs were
uncontracted. That was wrong, and the mistake matters because it pointed the
work at a guardrail when part of the cure is already ingested.

What actually exists:

- **Two providers, captured weekly.** Sleeper is canonical and opens, updates
  and clears episodes. FantasyPros is captured every run by
  `ingest/nfl_dfs_availability.py` — a step already in
  `refresh_nfl_dfs_projections.yml` — with richer fields, in shadow mode.
- **A schema built for exactly this question.**
  `ff_player_injury_observations` carries `normalized_status`,
  `practice_status`, `provider_updated_at`, `weeks_out_min/max`,
  `expected_return_min/max` and — already defined — **`availability_probability`**.
  `ff_player_injuries` carries the canonical episode with `status` constrained
  to QUESTIONABLE/DOUBTFUL/OUT/IR/PUP/NFI/SUSPENDED/UNKNOWN, plus `confidence`,
  `estimate_basis` and `source_conflict`. `ff_injury_events` is an append-only
  transition log that already includes `PRACTICE_UPGRADE`/`PRACTICE_DOWNGRADE`.
- **A programme with its own phases**, in [`docs/NFL Injury.md`](NFL%20Injury.md):
  phase 5 is a shadow availability model storing P10/P50/P90 missed, phase 6 is
  the promotion gate. Its acceptance criteria already state that *no new
  timeline field changes the live projection until its promotion gate passes*.

So the input is not missing. It is **deliberately quarantined**, and the
quarantine is recorded in code: `capture_contract` stamps every capture
`model_eligible=False`, `confidence_multiplier=0`, `fallback_tier='C'`, with
`eligibility_reason='Injury evidence for review; provider timestamp timezone
unverified'`.

## Why the quarantine is correct, and what unlocks it

`provider_updated_at` with an unverified timezone cannot prove the status was
knowable before kickoff. Feeding it to a model anyway would be a point-in-time
leak — the same class of defect this repo has already found three times (the
`mlb_matchups.our_prob_home` overwrite, `mlb_bets.event_commence`, and the
team/pitcher stats join that read June statistics into a March prediction).
Every one of those inflated apparent skill. A fourth would too.

**Verifying that timezone is therefore the highest-value item in this spec,
and the cheapest.** It is a provider-contract question — compare
`provider_updated_at` against known kickoff times across a season of captures
and establish whether the offset is fixed, DST-shifting, or absent — not a
modelling question. Until it resolves, no injury field may enter a projection.

## Two fixes, addressing different failure modes

They are complementary and neither substitutes for the other:

| | catches | misses |
|---|---|---|
| **Team ceiling** (this spec) | two players projected as if both start, *whatever the cause* — including no cause at all, which is the Rams case if Stafford was healthy pre-game | says nothing about which of the two should get the volume |
| **Start probability** (injury programme phase 5/6) | gets the allocation right when a designation exists pre-game | cannot help when the injury happens mid-game, which is unforecastable by anything |

The ceiling is the constraint; availability informs the split beneath it. A
start-probability model without a team constraint can still sum to 39.6; a
ceiling without availability data will scale two quarterbacks down
proportionally when the right answer is 90/10.

**This changes the ordering.** The ceiling check is the *interim* guardrail —
worth having because it is observable at zero risk and needs no new data —
but it is subordinate to the injury programme, not a substitute for it. If
forced to choose one, verify the timezone.

## P00 — Verify the provider timestamp timezone (do this first)

Ahead of everything below, because it is cheap, unblocks the larger fix, and
may change what is worth building at all.

Across a season of stored captures, compare `provider_updated_at` on
`ff_player_injury_observations` against the kickoff of the game each
observation pertains to. Establish whether the provider offset is fixed,
DST-shifting, or simply absent, and whether the field is a provider
publication time or our retrieval time.

**Outcome A — the offset resolves.** Update the capture contract to stamp
`model_eligible=True` with the verified basis recorded, and the injury
programme's phase 5 becomes reachable on data already collected. This is the
real fix and it outranks the rest of this document.

**Outcome B — it does not resolve.** The quarantine stands, `model_eligible`
stays False, and the ceiling check below is the only move available. Record
the negative result; do not work around it by assuming a timezone.

Under no circumstance does an unverified timestamp enter a projection. A
plausible guess at an offset is indistinguishable from a leak, and a leak is
invisible precisely because it improves the backtest.

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

- **P00 before everything.** No injury field reaches a projection while its
  timestamp basis is unverified, regardless of how well it would score.
- Injury inputs are governed by [`docs/NFL Injury.md`](NFL%20Injury.md) and its
  promotion gate, not by this spec. This document may not promote an injury
  field; it can only consume one the injury programme has already promoted.
- The Sleeper/FantasyPros conflict policy is still pending in that programme.
  Until it lands, a start-probability model must name which source it reads and
  must not silently blend two providers that disagree.
- P0 before P1. No fix is built before the incidence is measured.
- P1 changes no projection and does not bump `MODEL_VERSION`.
- P2 is a new model version, not an edit to v2's semantics.
- The ceiling is fitted on seasons strictly before the week being projected —
  never on the season being graded.
- A team-level constraint is not evidence of an edge. It removes an internal
  inconsistency; whether that improves accuracy is P2's question, and the
  honest prior is that it helps a little at QB and almost not at all at WR.

## Honest expected value

Reordered after the correction above.

**The ceiling check** is worth doing on correctness grounds — the arithmetic is
indefensible — but the likely size is small: a minority of team-weeks,
concentrated at quarterback, which is one Classic roster slot. Expect a
fraction of a point of MAE at QB and approximately nothing at receiver.

**The timezone verification** is a few hours of provider-contract work whose
downside is a recorded negative and whose upside is unlocking a season of
already-captured, already-schema'd availability data for the injury
programme's phase 5. That is the better bet by a wide margin, and it was
mis-ranked in the first draft of this spec because the author assumed the data
did not exist rather than checking.

**Neither addresses in-game injury.** A quarterback who leaves in the second
quarter was not forecastable from any pre-game feed, and grading that as
projection error is a measurement problem, not a modelling one — it belongs to
the separate availability-aware grading split, which is why the report card
currently fuses "we were wrong" with "he got hurt" in a single QB MAE of 7.31.
