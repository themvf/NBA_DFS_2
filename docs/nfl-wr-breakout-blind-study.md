# Star WR out: do the remaining pass catchers break out? (pre-registered 2026-09-28)

Study id `nfl-wr-breakout-blind-v1`. Registered and pushed **before any
2014–2018 wide receiver or tight end outcome was computed**. Implementation:
`model/nfl_wr_breakout_blind.py`.

## Why

[`nfl-absence-reallocation-v1`](nfl-absence-reallocation-study.md) found that
when a receiver sits, his targets barely move anyone's *average*: the leftover
is split across many players, a few tenths of a target each. That graded the
mean. A GPP is won in the tail. The claim tested here is different: when a
star receiver sits, **one of the remaining pass catchers has a big game more
often than their recent games predict**, even if nobody can say which one in
advance. (Prompt: Puka Nacua out, Davante Adams breakout, 2026 week 2.)

## Measure (frozen before any outcome was computed)

- **Star:** a WR with at least **7 targets a game** over the team's last 8
  games (v1 window, half-life 4, ≥ 2 active games), inactive or on reserve.
- **First game:** the star played the team's previous game. Primary
  population. (Once an absence is under way, recent games already include it,
  which is why the RB study found nothing there.)
- **Rows:** every active established WR and TE on the team. Roles: the top
  remaining WR by target baseline (`lead_WR`), other WRs, TEs.
- **Big game:** **20+ DraftKings points** (PPR plus the 100-yard receiving and
  rushing bonuses; fumbles and 2-point plays at PPR values). 25+ is secondary.
- **Expected chance:** the recency-weighted share of that player's own active
  games in the window that reached 20+. That is also roughly what a model that
  resamples past games would say.
- **Control:** team games with no pass catcher of 3+ targets missing. The
  history-based estimate is slightly off in general (in control games it
  over-predicts by about half a percentage point), so the measure is:

  `effect = (actual − expected big-game rate, first-game rows)
           − (actual − expected, control rows)`

- Uncertainty: two independent bootstraps over team games (absence, control),
  10,000 draws each, seed 20260928.

## Discovery (2020–2025, true inactive status; seasons earlier studies used for averages only)

148 first-game events, 1,046 WR/TE rows; 1,633 control games, 12,340 rows.

| measure | actual | expected | effect vs control | 95% CI |
|---|---:|---:|---:|---|
| **primary: any WR/TE, 20+** | 6.1% | 3.8% | **+2.81 pp** | [+1.47, +4.20] |
| 25+ | 3.4% | 1.9% | +1.82 pp | [+0.71, +2.97] |
| team: at least one WR/TE hits 20+ | 39.2% | 25.0% | +16.97 pp | [+8.64, +25.29] |
| role: top remaining WR | 25.0% | 15.9% | +13.19 pp | [+5.87, +20.66] |
| role: other WRs | 3.8% | 1.4% | +2.21 pp | [+0.48, +4.22] |
| role: TEs | 2.2% | 2.3% | +0.11 pp | [−1.31, +1.63] |
| pass-heavy half (target share ≥ 0.609) | 7.2% | 4.9% | +2.86 pp | [+0.79, +5.06] |
| run-heavy half | 5.1% | 2.8% | +2.76 pp | [+1.16, +4.45] |
| absence already under way (224) | 6.6% | 5.2% | +1.89 pp | [+0.75, +3.04] |

All six seasons have a positive point estimate (+0.95 to +4.57). Pass-heavy
teams do not show a larger effect than run-heavy ones.

**Frozen discovery effect: +2.81 pp.**

## Proxy validation (2019–2025; passed)

The 2014–2018 activity proxy (stat or snap-count row = active; see the
[RB blind study](nfl-rb-first-game-blind-study.md)) against true `INA` status:

| check | threshold | result |
|---|---|---|
| V1 recall of true first-game events | ≥ 90% | 148 / 148 = 100% ✓ |
| V2 precision of proxy first-game events | ≥ 90% | 148 / 152 = 97.4% ✓ |
| V3 proxy effect inside the true-status CI | [+1.47, +4.20] | +2.78 ✓ |

## Blind structure (counts only, computed before registration)

2014–2018: **86 first-game events** (2014: 11, 2015: 19, 2016: 22, 2017: 20,
2018: 14), 549 WR/TE rows; 103 already under way; 1,442 control games, 10,338
rows. The 2014–2018 seasons were unmasked for the RB blind test, but only
running backs' points in RB-absence games were computed; no WR or TE outcome
from those seasons has been looked at.

## Gate (primary: first-game WR/TE rows, 20+, 2014–2018; all required)

- **G1:** 95% CI lower bound of the effect above 0.
- **G2:** effect at least **half the discovery effect** (≥ +1.40 pp), so a
  much smaller effect that barely clears zero does not pass.
- **G3 sample:** ≥ 80 events and ≥ 500 rows.

Verdicts: `PROMOTE`, `NOT_PROMOTED` (G3 passes, another fails),
`INSUFFICIENT` (G3 fails). `unmask` re-runs the proxy validation and refuses to
grade if it fails (`VOID`).

**Disclosed change:** the sample floor was written as 150 events / 400 rows
before any count was seen. Discovery then showed ~25 first-game events a
season, and the blind seasons contain 86. A floor of 150 would guarantee
`INSUFFICIENT`, so it was lowered to 80 / 500 **before any blind outcome was
computed**. At 86 events the expected CI half-width is about ±1.8 pp, below the
+2.8 pp discovery effect.

**Stated in advance, gating nothing:** the top remaining WR carries most of the
effect and TEs show none; the team-level "at least one" rate is above expected;
25+ shows the same direction; pass-heavy teams are not expected to differ.

## What a verdict licenses

- **PROMOTE:** a live **shadow** breakout flag. When the slate rules out a WR
  with 7+ targets a game who played his team's last game, his teammates are
  flagged with the measured big-game uplift (the top remaining WR most of
  all), shown beside their normal ceiling and graded weekly against
  DraftKings results. Projections and optimizer ceilings do not change until
  that forward record exists.
- **NOT_PROMOTED / INSUFFICIENT:** nothing changes. No retuning of the star
  threshold, the 20-point line, the window or the proxy on 2014–2018.

## Protocol

1. Registration, code, tests and the pre-registration artifacts
   (`artifacts/nfl_wr_breakout_blind_v1_{discovery,validate,blind}.json`) are
   committed and pushed before `unmask` runs.
2. `python -m model.nfl_wr_breakout_blind unmask` runs once; its output is
   recorded below verbatim.

## Result (unmasked once, 2026-09-28, after registration commit 3b1bf90)

No deviations from the registration. Proxy validation re-ran inside `unmask`
and passed. Full output and source digests:
`artifacts/nfl_wr_breakout_blind_v1_unmask.json`.

**Verdict: PROMOTE** (G1 ✓, G2 ✓, G3 ✓), **with the lower bound at the edge.**

Primary — first game of a star WR's absence, every remaining WR/TE, 20+ DK
points, 2014–2018 (86 events, 549 rows; control 1,442 games, 10,338 rows):

| | actual | expected from history | gap |
|---|---:|---:|---:|
| first-game rows | 6.4% | 4.6% | +1.8 pp |
| control rows | 7.4% | 7.7% | −0.3 pp |
| **effect** | | | **+2.09 pp [+0.01, +4.17]** |

G2: +2.09 ≥ +1.40 (half of discovery's +2.81). The blind effect is about 75% of
discovery's, and its CI lower bound is +0.01 pp: a pass, not a comfortable one.

Stated in advance, gating nothing:

| prediction | 2014–2018 result | held? |
|---|---|---|
| top remaining WR carries most of it | 15.1% vs 17.7% expected; −0.22 pp [−8.57, +8.37] | **no** |
| other WRs | 5.4% vs 1.8%; **+3.47 pp [+0.86, +6.23]** | (effect moved here) |
| TEs show nothing | 4.1% vs 2.6%; +1.59 pp [−1.05, +4.45] | inconclusive |
| team: at least one WR/TE hits 20+ is above expected | 33.7% vs 27.1%; +7.77 pp [−2.50, +18.56] | direction yes, CI crosses 0 |
| 25+ same direction | +1.56 pp [−0.06, +3.36] | direction yes, CI crosses 0 |
| pass-heavy teams not different | pass-heavy +1.45 [−1.73, +4.77], run-heavy +2.77 [+0.30, +5.41] | yes |

Absence already under way: +0.89 pp [−0.78, +2.66]. By season, all five point
estimates are positive (+0.25 to +6.24); none resolves alone.

### What it means

- **The general claim holds on data nobody had looked at.** In the first game
  a star WR misses, the remaining WRs and TEs as a group reach 20+ DraftKings
  points more often than their own recent games predict: about +2 to +3
  percentage points each, on a base of 4–5%, so roughly one and a half times
  as often. Pooled over both periods (234 events) the effect is about +2.5 pp.
- **Who breaks out is not stable.** Discovery pointed at the top remaining WR;
  the blind seasons pointed at the other WRs. The honest reading is that the
  extra big games are spread across the room and we cannot say in advance
  which receiver gets one. That supports a team-level flag on every remaining
  WR/TE, not a boost pinned to one player.
- **Pass-heavy teams are not where it lives.** Neither period shows a larger
  effect on pass-heavy teams.
- **It is a first-game effect.** Once the absence is under way, recent games
  already carry the new roles and the gap mostly closes.
- The lower bound sits at zero, so the forward shadow record carries real
  weight here: it has to keep showing the uplift before anything changes in
  the optimizer.

### What happens next (per the license above)

A live shadow flag, not a projection change: when the slate rules out a WR
with 7+ targets a game who played his team's last game, flag every remaining
WR/TE with the measured uplift (about +2.5 pp on a 20+ game, pooled), shown
beside their normal ceiling and graded weekly against DraftKings results.
