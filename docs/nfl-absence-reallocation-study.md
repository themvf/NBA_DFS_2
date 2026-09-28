# NFL absence reallocation — fixed-pie study (pre-registered 2026-09-28)

Study id `nfl-absence-reallocation-v1`. Registered and committed **before** any
allocation error, bias or points outcome was computed. Beforehand only file
schemas, the `status` vocabulary of nflverse weekly rosters, and one semantic
spot check (a known 2026 week-2 inactive reads `INA`) were inspected.

## Why

`web/src/lib/nfl-dfs/opportunity-redistribution.ts` (v3) withholds every
non-QB absence adjustment. The earlier additive version — hand each ruled-out
player's historical average to teammates in proportion to their own — was
graded on the 2026 week-2 classic slate and made projections worse at every
position (paired MAE 4.37 → 5.00, CI [+0.29, +1.02]). The module names the
reason: there was no team budget, so the transfer could hand out volume that
was never vacated (players already absent from teammates' baselines, position
priors, QB scrambles) and could exceed what a team plays.

Earlier research (`docs/nfl-volume-share-research.md`) could not validate an
absence allocator because it had **zero historical pregame availability
observations**. nflverse weekly rosters carry each player's gameday status
(`ACT`, `INA` gameday inactive, `RES` reserve/IR), which supplies them.

## Hypothesis

Allocating only the team volume that is actually left over — a fixed team
budget minus what the active players are already projected to take, minus the
volume a full-strength team normally leaves to unmodelled players — predicts
active teammates' volume better than leaving projections alone.

## Data

- nflverse `stats_player_week_{season}.csv` (targets, carries, receiving and
  rushing lines, `fantasy_points_ppr`), `roster_weekly_{season}.csv` (status,
  position), `games.csv` (regular-season schedule). Seasons 2021–2026, REG only.
- 2021 is history warm-up only. **Discovery: 2022–2023. Confirmation:
  2024–2025.** 2026 weeks with complete data are reported descriptively only.
- Active = weekly roster `status == 'ACT'` for that team and week. A player
  who is active with no stat row recorded 0. Semantics caveat: the current
  week's roster snapshot can predate gameday inactives (2026 week 3 SNF shows
  a known inactive as `ACT`); historical weeks carry the final status.

## Pools

| pool | unit | team budget counts | donors | recipients |
|---|---|---|---|---|
| targets | targets | all players | RB/FB/WR/TE | RB/FB/WR/TE |
| carries | carries | all non-QB players | RB/FB/WR/TE | RB/FB |

FB is grouped with RB. QB rushing is outside the carries pool (the QB pass
pool is unchanged and out of scope).

## Walk-forward quantities (for a team game g of team t, pool unit u)

Everything is computed from games strictly before g.

- **Window H:** team t's last 8 regular-season games before g (may cross a
  season). Weights `w_k = 0.5^(k/4)`, k = 0 for the most recent.
- **Established:** at least 2 `ACT` games for team t inside H.
- **Player baseline `B0_i`:** weighted mean of u over his `ACT` games in H.
- **Team budget `T̂`:** weighted mean of the team's pool total over H
  (requires ≥ 4 games in H).
- **Full-strength reserve `R̂`:** for each h in H,
  `r_h = U_h − Σ B0_i(h)` over players established and `ACT` at h. `R̂` is the
  weighted mean of `r_h` over *full-strength* games in H (no material donor of
  the pool absent at h); with fewer than 3 such games, over all games in H with
  `r_h` defined. Requires ≥ 3 defined `r_h`.
- **Donors:** established players of donor positions on team t's week roster
  with status `INA` or `RES`. `D = Σ B0_j` over donors.
- **Material donor:** `B0_j ≥ 3.0` targets or `≥ 5.0` carries.
- **Event:** a team game with at least one material donor for the pool and
  defined `T̂`, `R̂`.
- **Recipients (evaluation rows):** established `ACT` players of recipient
  positions with `B0_i > 0`. Actual = u in g.
- **Active sum `A`:** Σ `B0_i` over every established `ACT` player counted by
  the pool's budget.

## Methods

- **BASE** (current production behaviour): `pred_i = B0_i`.
- **OLD** (the withdrawn additive transfer, reproduced for reference):
  `pred_i = min(B0_i + D · B0_i / Σ_rec B0, 4 · B0_i)`.
- **FIX(φ, α)** — the fixed pie:
  `V = φ · min(D, max(0, T̂ − A − R̂))`. `V` is split across donors in
  proportion to `B0_j`. Each donor's part goes `α` to recipients sharing his
  position group (in proportion to `B0`) and `1 − α` to all recipients (in
  proportion to `B0`); with no same-group recipient, all of it goes to all
  recipients. `pred_i = min(B0_i + gain_i, 4 · B0_i)`; volume above the cap is
  dropped and counted.

Grid: `φ ∈ {0.5, 1.0}`, `α ∈ {0, 0.5, 1.0}` (6 configurations; α is inert for
carries, where every recipient is an RB).

## Selection (discovery 2022–2023)

For each pool, the configuration with the lowest recipient-row MAE on
discovery events is frozen. Ties (to 4 decimals) go to the smaller φ, then the
smaller α. Only the frozen configuration is graded on confirmation; the other
five are not reported for confirmation.

## Confirmation metrics (2024–2025, per pool)

Uncertainty from a bootstrap resampling **events** (team games), 10,000 draws,
seed 20260928.

- **Units:** paired MAE delta FIX − BASE on recipient rows.
- **Points:** PPR points, DK bonuses excluded. `pred_pts_i = B0pts_i +
  (pred_i − B0_i) · ppu_i`, where `B0pts_i` is his weighted mean
  `fantasy_points_ppr` over `ACT` games in H and `ppu_i` his weighted linear
  points per unit over H (targets: receptions + 0.1·yards + 6·TD per target;
  carries: 0.1·yards + 6·TD per carry). Actual = `fantasy_points_ppr` (0 with
  no stat row). Paired MAE delta FIX − BASE.
- Reported, gating nothing: OLD − BASE on both scales (expected to reproduce
  the week-2 finding), bias (actual − pred), volume dropped by the cap, by
  season and by donor position.

## Gate (per pool, all three required)

- **G1 units:** 95% CI of the units MAE delta lies entirely below 0.
- **G2 points (non-inferiority):** points MAE delta point estimate < 0 and
  95% CI upper bound < +0.05.
- **G3 sample:** ≥ 200 events and ≥ 800 recipient rows.

Verdicts: `PROMOTE` (all pass), `NOT_PROMOTED` (G3 passes, G1 or G2 fails),
`INSUFFICIENT` (G3 fails).

## What a verdict licenses

A `PROMOTE`d pool may be applied live at the slate layer with the frozen
configuration, using a budget artifact computed by the same code for the slate
week, only for teams whose history is complete through their last game, and
only for donors the slate itself rules out. A pool that is not promoted keeps
the v3 behaviour: baseline retained, the scenario shown as unapplied research.
Neither outcome licenses re-tuning φ, α, thresholds or the window on
confirmation data; that is a new registration.

## Result (run 2026-09-28, after registration commit 69dda8c)

No deviations from the registration. Reproduce with
`python -m model.nfl_absence_reallocation study`; full output, source digests
and data-quality counts in `artifacts/nfl_absence_reallocation_v1.json`.

**Neither pool is promoted. The app keeps baseline projections for non-QB
absences (v3 behaviour).**

### Discovery (2022–2023) selection

| pool | events / rows | BASE MAE | best FIX (frozen) | OLD MAE |
|---|---|---:|---:|---:|
| targets | 497 / 4,755 | **1.684** | 1.699 (φ 0.5, α 0.5) | 1.952 |
| carries | 247 / 639 | 3.552 | **3.306** (φ 0.5) | 4.804 |

Every targets configuration was already worse than BASE in discovery; the
rule still carries the best one to confirmation.

### Confirmation (2024–2025), frozen configuration

| pool | events / rows | units Δ FIX−BASE | points Δ FIX−BASE | G1 | G2 | G3 | verdict |
|---|---|---|---|---|---|---|---|
| targets | 502 / 4,662 | −0.006 [−0.017, +0.005] | +0.040 [+0.021, +0.058] | ✗ | ✗ | ✓ | **NOT_PROMOTED** |
| carries | 266 / 654 | **−0.367 [−0.525, −0.210]** | −0.005 [−0.112, +0.102] | ✓ | ✗ | ✗ | **INSUFFICIENT** |

The withdrawn additive transfer, reproduced for reference, is worse in both
pools: targets +0.256 units [+0.217, +0.295] and **+0.527 points [+0.461,
+0.594]**; carries +1.014 units and **+0.654 points [+0.372, +0.936]**. That
is the same size and sign as the 2026 week-2 grading (+0.63), so the
withdrawal stands on two independent samples.

Carries by season: 2024 −0.43 [−0.69, −0.17], 2025 −0.32 [−0.51, −0.13] —
the volume gain is stable, but it never reaches fantasy points. Even without
the row floor, G2 fails. 2026 weeks 1–3 (descriptive, 15–17 events) show no
consistent direction.

### Why targets fail (discovery diagnostics)

In a discovery absence game the donors averaged 8.5 targets, but the fixed pie
found only 3.2 left over: established teammates' baselines already covered the
rest, because most absences run several weeks and those averages had already
absorbed them. What the recipients actually gained was smaller still, +1.15
targets combined. The team threw 0.7 fewer targets than its budget, and 2.1
per game went to players outside the modelled recipients (call-ups and depth
receivers without two recent active games, plus the odd QB or lineman).
Spread over ~10 recipients, the real per-player shift is a few tenths of a
target, below the noise, and the extra points estimate made points slightly
worse.

For carries the backup back does inherit the work: recipients gained +2.95
carries, and φ = 0.5 of the leftover (3.2) matched it. The fantasy value of
those carries is what the method does not capture.

### What this changes

- The missing team budget was the stated reason for withholding non-QB
  absence adjustments. It now exists and has been tested; withholding stands
  on evidence rather than on its absence.
- A carries follow-up needs its own registration: something that turns the
  volume gain into points (backup efficiency, goal-line share), graded on
  points, with a pool-appropriate sample floor.
- Targets: no further fixed-pie variant on this data. Anything new here needs
  new information (depth-chart or snap-share role data at the time of the
  absence), not another split rule.
